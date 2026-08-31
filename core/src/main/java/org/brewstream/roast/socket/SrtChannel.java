package org.brewstream.roast.socket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.AbstractChannel;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelMetadata;
import io.netty.channel.ChannelOutboundBuffer;
import io.netty.channel.ChannelPromise;
import io.netty.channel.DefaultChannelConfig;
import io.netty.channel.EventLoop;

import java.net.SocketAddress;
import java.util.ArrayDeque;
import java.util.Queue;

/**
 * One SRT connection, as a Netty {@link Channel}. A child of the shared datagram
 * channel the connection is multiplexed over, in the same shape Netty's QUIC
 * codec gives a QUIC connection — which is the closest existing analogue, being
 * another connection-oriented protocol with its own handshake, crypto and
 * retransmission riding on a single UDP socket.
 *
 * <p>This exists so an application can put its own handlers on the data path —
 * an MPEG-TS demultiplexer, an RTP packetiser, traffic shaping, logging — rather
 * than only receiving finished buffers through a callback. Reached via {@link
 * SrtConnection#pipeline()} and {@link SrtConnection#channel()}.
 *
 * <p><b>Composition, not inheritance.</b> {@link SrtConnection} <em>has</em> one
 * of these rather than <em>being</em> one, deliberately. Netty's
 * {@code write(Object)} means "queue, don't flush" and returns a future, while
 * {@code SrtConnection.write(ByteBuf)} means "send this"; one class cannot carry
 * both meanings of the same word without misleading somebody. Keeping them
 * separate leaves the callback API — which is what most applications want —
 * exactly as simple as it was, while an application bridging SRT to another
 * transport gets a real channel to coordinate with.
 *
 * <p><b>Backpressure is genuine, and tied to the protocol.</b> {@link #doWrite}
 * stops handing packets to the send buffer once the peer's negotiated flow
 * window is full, leaving the rest in Netty's outbound buffer, where the
 * standard write water marks turn it into {@link #isWritable()} going false.
 * Before this existed there was no backpressure signal of any kind: writes were
 * accepted unconditionally and an application outrunning the link found out when
 * the heap did. Pending writes are retried on the connection's own ~10ms tick.
 *
 * <p><b>Reads honour {@code autoRead}.</b> With it off, delivered payloads queue
 * here rather than being fired at the pipeline, which is what makes the usual
 * Netty proxy idiom work when bridging to a slower downstream:
 * {@code srt.channel().config().setAutoRead(downstream.isWritable())}. Note the
 * queue is local — it does not yet narrow the receive window advertised to the
 * peer, so backpressure stops at this process rather than reaching the sender.
 *
 * <p><b>On {@code channelActive}.</b> The channel is registered when the
 * connection is constructed, so a handler added later joins a channel that is
 * already active and will not see {@code channelActive}. Use {@code
 * handlerAdded} together with {@link #isActive()}, as you would when attaching
 * to any live channel.
 */
public final class SrtChannel extends AbstractChannel {

    private static final ChannelMetadata METADATA = new ChannelMetadata(false);

    private final SrtConnection connection;
    private final ChannelConfig config = new DefaultChannelConfig(this);
    private final Queue<ByteBuf> pendingInbound = new ArrayDeque<>();

    private volatile boolean open = true;
    private volatile boolean active;
    private boolean readPending;
    private boolean writeIncomplete;

    SrtChannel(Channel parent, SrtConnection connection) {
        super(parent);
        this.connection = connection;
    }

    /**
     * Hands a delivered payload to the pipeline, or queues it when {@code
     * autoRead} is off and no read has been requested. Called on the event loop
     * from the connection's TSBPD delivery, in sequence order.
     */
    void deliverInbound(ByteBuf payload) {
        if (!open) {
            payload.release();
            return;
        }
        if (config.isAutoRead() || readPending) {
            readPending = false;
            pipeline().fireChannelRead(payload);
            pipeline().fireChannelReadComplete();
            return;
        }
        pendingInbound.add(payload);
    }

    /**
     * Retries a write the flow window forced us to stop short on. Called every
     * connection tick; a no-op unless {@link #doWrite} actually left something
     * behind.
     */
    void onTick() {
        if (writeIncomplete && open) {
            writeIncomplete = false;
            flush();
        }
    }

    /** Marks the channel active once the connection is live. */
    void markActive() {
        active = true;
    }

    /** Closes from the connection's own teardown, without calling back into it. */
    void closeFromConnection() {
        active = false;
        if (open) {
            close();
        }
    }

    @Override
    protected AbstractUnsafe newUnsafe() {
        return new SrtUnsafe();
    }

    @Override
    protected boolean isCompatible(EventLoop loop) {
        // Always registered on the parent datagram channel's own loop, which is
        // the thread every packet for this connection already arrives on.
        return true;
    }

    @Override
    protected SocketAddress localAddress0() {
        return parent().localAddress();
    }

    @Override
    protected SocketAddress remoteAddress0() {
        return connection.metadata().peerAddress();
    }

    @Override
    protected void doBind(SocketAddress localAddress) {
        throw new UnsupportedOperationException("an SRT connection is bound by its listener, not directly");
    }

    @Override
    protected void doDisconnect() {
        doClose();
    }

    @Override
    protected void doClose() {
        open = false;
        active = false;
        ByteBuf pending;
        while ((pending = pendingInbound.poll()) != null) {
            pending.release();
        }
        // Tear the SRT connection down too, so closing the channel closes the
        // connection as a user would expect. Idempotent: SrtConnection.close
        // guards on an atomic, so the reverse direction does not recurse.
        connection.close();
    }

    @Override
    protected void doBeginRead() {
        readPending = true;
        ByteBuf payload;
        while (readPending && (payload = pendingInbound.poll()) != null) {
            readPending = false;
            pipeline().fireChannelRead(payload);
            pipeline().fireChannelReadComplete();
        }
    }

    @Override
    protected void doWrite(ChannelOutboundBuffer in) {
        for (;;) {
            Object message = in.current();
            if (message == null) {
                writeIncomplete = false;
                return;
            }
            if (!(message instanceof ByteBuf payload)) {
                in.remove(new UnsupportedOperationException(
                        "an SRT connection carries ByteBuf payloads, got " + message.getClass().getName()));
                continue;
            }
            if (!connection.hasSendWindowRoom()) {
                // Leave the rest queued: Netty's water marks turn a growing
                // outbound buffer into isWritable() == false, which is the
                // signal an application needs to stop producing. Retried from
                // onTick once the window drains.
                writeIncomplete = true;
                return;
            }
            if (payload.readableBytes() > connection.metadata().maxPayloadSize()) {
                in.remove(new IllegalArgumentException("payload of " + payload.readableBytes()
                        + " bytes exceeds this connection's " + connection.metadata().maxPayloadSize()
                        + "-byte limit; live mode sends one packet per write and does not split messages"));
                continue;
            }
            connection.enqueueForSend(payload.retain());
            in.remove();
        }
    }

    @Override
    public ChannelConfig config() {
        return config;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public boolean isActive() {
        return open && active;
    }

    @Override
    public ChannelMetadata metadata() {
        return METADATA;
    }

    private final class SrtUnsafe extends AbstractUnsafe {
        @Override
        public void connect(SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise) {
            promise.setFailure(new UnsupportedOperationException(
                    "an SRT connection is established by SrtCaller or SrtListener, not by connecting this channel"));
        }
    }
}
