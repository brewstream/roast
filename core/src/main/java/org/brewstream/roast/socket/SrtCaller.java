package org.brewstream.roast.socket;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.AddressedEnvelope;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultAddressedEnvelope;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.util.concurrent.ScheduledFuture;
import org.brewstream.roast.codec.SrtFrameDecoder;
import org.brewstream.roast.codec.SrtFrameEncoder;
import org.brewstream.roast.crypto.EncryptionContext;
import org.brewstream.roast.handshake.CallerHandshake;
import org.brewstream.roast.handshake.ConclusionReplyOutcome;
import org.brewstream.roast.packet.ControlPacket;
import org.brewstream.roast.packet.ControlType;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.packet.cif.HandshakeCif;
import org.brewstream.roast.packet.cif.KeyEncryption;
import org.brewstream.roast.packet.cif.HandshakeType;
import org.brewstream.roast.util.CircularNumber;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.SecureRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Connects out to a peer's listener: the caller side of the HSv5 induction→
 * conclusion exchange ({@link CallerHandshake}), driven over a dedicated Netty
 * channel bound to an ephemeral local port — the same pipeline
 * ({@link SrtFrameDecoder}/{@link SrtFrameEncoder}/{@link SrtSocketIdDemultiplexer})
 * {@link SrtListener} uses, since neither codec cares whether the channel talks to
 * one known peer or many. One {@code connect()} call, one dedicated channel/event
 * loop group — unlike a listener's port, shared across many accepted connections,
 * this one exists only for the resulting {@link SrtConnection} and is torn down
 * with it (see the package-private constructor overload {@link SrtConnection} uses
 * for this).
 *
 * <p>Everything after {@link #connect}'s bind completes runs on that one channel's
 * event loop — handshake replies arrive there, the connect timeout fires there —
 * so, like {@link SrtConnection} itself, this needs no internal synchronization.
 *
 * <p>Single-shot: no induction/conclusion retry on packet loss, matching gosrt's
 * {@code dial.go} (which doesn't retry either) — a known simplification relative to
 * real libsrt, which does retry with backoff per spec. No HSv4 fallback.
 */
public final class SrtCaller {

    private static final Logger LOG = Logger.getLogger(SrtCaller.class.getName());
    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * How often an unanswered handshake request is repeated. libsrt's own rule
     * ({@code core.cpp}: "avoid sending too many requests, at most 1 request per
     * 250ms"); the overall {@link #CONNECT_TIMEOUT_SECONDS} still bounds the
     * attempt.
     */
    private static final long HANDSHAKE_RETRY_MILLIS = 250;

    private final Channel channel;
    private final SrtTransport transport;
    private final SrtSocketIdDemultiplexer demultiplexer;
    private final CallerHandshake callerHandshake;
    private final InetSocketAddress remoteAddress;
    private final InetAddress localAddress;
    private final SrtSocketId ownSocketId;
    private final CircularNumber ownInitialSequenceNumber;
    private final String streamId;
    private final EncryptionContext encryptionContext;
    private final SrtConfig config;
    private final CompletableFuture<SrtConnection> result;
    private final long startNanos = System.nanoTime();

    private ScheduledFuture<?> timeoutTask;
    private ScheduledFuture<?> retryTask;
    private HandshakeCif pendingRequest;
    private boolean conclusionSent;

    private SrtCaller(Channel channel, SrtTransport transport, SrtSocketIdDemultiplexer demultiplexer,
            InetSocketAddress remoteAddress, InetAddress localAddress, SrtSocketId ownSocketId,
            CircularNumber ownInitialSequenceNumber, String streamId, EncryptionContext encryptionContext,
            SrtConfig config, CompletableFuture<SrtConnection> result) {
        this.channel = channel;
        this.transport = transport;
        this.demultiplexer = demultiplexer;
        this.remoteAddress = remoteAddress;
        this.localAddress = localAddress;
        this.ownSocketId = ownSocketId;
        this.ownInitialSequenceNumber = ownInitialSequenceNumber;
        this.streamId = streamId;
        this.encryptionContext = encryptionContext;
        this.config = config;
        this.callerHandshake = new CallerHandshake(config.flowWindowPackets(), config.maxMss());
        this.result = result;
    }

    /** Connects to {@code remoteAddress}, completing once the handshake finishes (or failing on rejection/timeout). */
    public static CompletableFuture<SrtConnection> connect(InetSocketAddress remoteAddress, String streamId) {
        return connect(remoteAddress, streamId, null, 0, SrtConfig.defaults());
    }

    /** Connects with explicit settings; see {@link SrtConfig}. */
    public static CompletableFuture<SrtConnection> connect(InetSocketAddress remoteAddress, String streamId,
            SrtConfig config) {
        return connect(remoteAddress, streamId, null, 0, config);
    }

    /**
     * Connects with encryption. Unlike the listener — which adopts whatever the
     * peer announces — the <em>caller</em> generates the salt and both Stream
     * Encrypting Keys and offers them in its CONCLUSION, so {@code keyLength}
     * really is this side's choice here (16, 24, or 32 bytes). The connection
     * fails if the peer doesn't echo the key material back, which is what it
     * does when it can't unwrap it with the same passphrase.
     *
     * <p>{@code passphrase} is copied, so the caller may zero its own; the
     * connection zeroes the copy when it closes. Pass {@code null} for an
     * unencrypted connection.
     */
    public static CompletableFuture<SrtConnection> connect(InetSocketAddress remoteAddress, String streamId,
            char[] passphrase, int keyLength) {
        return connect(remoteAddress, streamId, passphrase, keyLength, SrtConfig.defaults());
    }

    /** Connects with encryption and explicit settings. */
    public static CompletableFuture<SrtConnection> connect(InetSocketAddress remoteAddress, String streamId,
            char[] passphrase, int keyLength, SrtConfig config) {
        return connect(remoteAddress, streamId, passphrase, keyLength, config, SrtTransport.owned());
    }

    /**
     * Connects on an application's own Netty resources rather than resources
     * Roast creates — see {@link SrtTransport}. Worth preferring wherever more
     * than a handful of outbound connections are made: the default gives each
     * one its own event loop group, and so its own thread.
     */
    public static CompletableFuture<SrtConnection> connect(InetSocketAddress remoteAddress, String streamId,
            char[] passphrase, int keyLength, SrtConfig config, SrtTransport transport) {
        CompletableFuture<SrtConnection> result = new CompletableFuture<>();
        EncryptionContext encryptionContext = passphrase == null
                ? null
                : EncryptionContext.generating(passphrase, keyLength,
                        config.keyRefreshPackets(), config.keyPreAnnouncePackets());
        SrtSocketIdDemultiplexer demultiplexer = new SrtSocketIdDemultiplexer();
        Bootstrap bootstrap = new Bootstrap()
                .group(transport.eventLoopGroup())
                .channel(transport.channelType())
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel ch) {
                        ch.pipeline().addLast(new SrtFrameDecoder(), new SrtFrameEncoder(), demultiplexer);
                    }
                });

        bootstrap.bind(0).addListener((ChannelFutureListener) future -> {
            if (!future.isSuccess()) {
                shutdownIfOwned(transport);
                result.completeExceptionally(future.cause());
                return;
            }

            Channel channel = future.channel();
            SrtSocketId ownSocketId = new SrtSocketIdGenerator().generate();
            InetAddress localAddress = ((InetSocketAddress) channel.localAddress()).getAddress();
            CircularNumber ownInitialSequenceNumber = randomInitialSequenceNumber();

            new SrtCaller(channel, transport, demultiplexer, remoteAddress, localAddress, ownSocketId,
                    ownInitialSequenceNumber, streamId, encryptionContext, config, result)
                    .start();
        });

        return result;
    }

    private void start() {
        demultiplexer.register(ownSocketId, this::onHandshakeReply);
        timeoutTask = channel.eventLoop().schedule(this::onTimeout,
                config.connectTimeout().toMillis(), TimeUnit.MILLISECONDS);
        pendingRequest = callerHandshake.buildInductionRequest(ownSocketId, localAddress);
        send(pendingRequest);
        // Repeat whichever step is still unanswered. Without this a single lost
        // INDUCTION or CONCLUSION fails the whole connect - invisible on
        // loopback, routine on a real network. Resending is safe on both sides:
        // the cookie is derived, not stored, so INDUCTION is stateless, and
        // SrtListener already dedupes a retried CONCLUSION by replaying its
        // cached response rather than re-running accept.
        retryTask = channel.eventLoop().scheduleAtFixedRate(
                this::resendPendingRequest, HANDSHAKE_RETRY_MILLIS, HANDSHAKE_RETRY_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    private void resendPendingRequest() {
        if (result.isDone() || pendingRequest == null) {
            return;
        }
        LOG.log(Level.FINE, "Repeating unanswered handshake request to {0}", remoteAddress);
        send(pendingRequest);
    }

    private void onHandshakeReply(AddressedEnvelope<SrtPacket, InetSocketAddress> msg) {
        SrtPacket packet = msg.content();
        if (result.isDone()) {
            // Already failed (e.g. the connect timeout) - ignore a late reply.
            packet.body().release();
            return;
        }
        if (!(packet instanceof ControlPacket control) || control.type() != ControlType.HANDSHAKE) {
            packet.body().release();
            return;
        }

        HandshakeCif reply = HandshakeCif.decode(control.body(), false);
        control.body().release();
        if (reply == null) {
            LOG.log(Level.FINE, "Dropping malformed handshake reply from {0}", msg.sender());
            return;
        }

        if (reply.handshakeType() == HandshakeType.INDUCTION) {
            handleInductionReply(reply);
        } else {
            handleConclusionReply(reply);
        }
    }

    private void handleInductionReply(HandshakeCif reply) {
        if (!callerHandshake.isSupportedInductionReply(reply)) {
            fail(new IOException("peer doesn't support SRT handshake v5"));
            return;
        }
        if (conclusionSent) {
            // A duplicate induction reply, because our own conclusion hasn't been
            // answered yet. The retry task is already repeating it; rebuilding
            // here would only churn the key material.
            return;
        }
        // The caller generates the keys, so the conclusion is where we announce them.
        HandshakeCif conclusionRequest = callerHandshake.buildConclusionRequest(
                reply, ownSocketId, localAddress, ownInitialSequenceNumber, config.srtVersion(),
                config.latencyMillis(), config.latencyMillis(), streamId,
                encryptionContext == null ? null : encryptionContext.keyMaterial(KeyEncryption.BOTH));
        conclusionSent = true;
        pendingRequest = conclusionRequest;
        send(conclusionRequest);
    }

    private void handleConclusionReply(HandshakeCif reply) {
        ConclusionReplyOutcome outcome = callerHandshake.validateConclusionReply(
                reply, config.srtVersion(), config.latencyMillis(), config.latencyMillis());

        if (outcome instanceof ConclusionReplyOutcome.Connected connected) {
            // A listener that could unwrap our key material echoes it back as
            // KMRSP. No echo means it could not - it either has a different
            // passphrase or none at all - and continuing would send it payloads
            // it can never read.
            if (encryptionContext != null && reply.keyMaterial() == null) {
                fail(new IOException("peer did not accept our key material - passphrase mismatch?"));
                return;
            }

            timeoutTask.cancel(false);
            stopRetrying();
            demultiplexer.unregister(ownSocketId);

            AcceptedConnection metadata = new AcceptedConnection(
                    ownSocketId, reply.srtSocketId(), remoteAddress, streamId,
                    connected.receiveLatencyMillis(), connected.sendLatencyMillis(), connected.srtVersion(),
                    reply.maxFlowWindowSize(), reply.maxTransmissionUnitSize());
            SrtConnection connection = new SrtConnection(channel, demultiplexer, metadata, ownInitialSequenceNumber,
                    () -> {
                        channel.close();
                        shutdownIfOwned(transport);
                    },
                    encryptionContext, config.peerIdleTimeout().toNanos() / 1_000);

            if (!result.complete(connection)) {
                // A racing timeout already failed this connect - don't leak the connection we just built.
                connection.close();
            }
            return;
        }

        if (outcome instanceof ConclusionReplyOutcome.Rejected rejected) {
            fail(new IOException("connection rejected: " + rejected.reason()));
            return;
        }

        ConclusionReplyOutcome.ProtocolViolation violation = (ConclusionReplyOutcome.ProtocolViolation) outcome;
        sendShutdown(reply.srtSocketId());
        fail(new IOException(violation.reason()));
    }

    private void stopRetrying() {
        pendingRequest = null;
        if (retryTask != null) {
            retryTask.cancel(false);
        }
    }

    private void onTimeout() {
        fail(new TimeoutException("connection timeout: peer didn't respond"));
    }

    private void fail(Exception cause) {
        if (result.isDone()) {
            return;
        }
        timeoutTask.cancel(false);
        stopRetrying();
        demultiplexer.unregister(ownSocketId);
        channel.close();
        shutdownIfOwned(transport);
        result.completeExceptionally(cause);
    }

    private void sendShutdown(SrtSocketId peerSocketId) {
        send(new ControlPacket(ControlType.SHUTDOWN, 0, elapsedMicros(), peerSocketId, channel.alloc().buffer(0)));
    }

    private void send(HandshakeCif cif) {
        ByteBuf cifBuf = channel.alloc().buffer();
        cif.encodeTo(cifBuf);
        send(new ControlPacket(ControlType.HANDSHAKE, 0, elapsedMicros(), SrtSocketId.ZERO, cifBuf));
    }

    private void send(SrtPacket packet) {
        channel.writeAndFlush(new DefaultAddressedEnvelope<>(packet, remoteAddress));
    }

    private int elapsedMicros() {
        return (int) ((System.nanoTime() - startNanos) / 1000);
    }

    /**
     * Only a transport Roast created is ours to shut down; one an application
     * lent us may be carrying its other traffic.
     */
    private static void shutdownIfOwned(SrtTransport transport) {
        if (transport.shutdownWithOwner()) {
            transport.eventLoopGroup().shutdownGracefully();
        }
    }

    private static CircularNumber randomInitialSequenceNumber() {
        int value = RANDOM.nextInt() & (int) SrtPacket.MAX_SEQUENCE_NUMBER;
        return CircularNumber.of(value, SrtPacket.MAX_SEQUENCE_NUMBER);
    }
}
