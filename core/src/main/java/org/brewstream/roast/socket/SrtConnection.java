package org.brewstream.roast.socket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.AddressedEnvelope;
import io.netty.channel.Channel;
import io.netty.channel.DefaultAddressedEnvelope;
import io.netty.util.concurrent.ScheduledFuture;
import org.brewstream.roast.packet.ControlPacket;
import org.brewstream.roast.packet.ControlType;
import org.brewstream.roast.packet.DataPacket;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.cif.AckCif;
import org.brewstream.roast.packet.cif.AckVariant;
import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.recv.AckSender;
import org.brewstream.roast.recv.DeliveryResult;
import org.brewstream.roast.recv.LossList;
import org.brewstream.roast.recv.NakGenerator;
import org.brewstream.roast.recv.ReceiveBuffer;
import org.brewstream.roast.util.CircularNumber;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A live, accepted connection: owns {@link LossList}, {@link AckSender}, and
 * {@link ReceiveBuffer} for its lifetime, registers itself as the accepted
 * socket ID's {@link SrtPacketSink}, and drives them via a ~10ms tick on the
 * same Netty event loop the packets themselves arrive on — everything here runs
 * on that one thread, so (like its three collaborators individually) this class
 * needs no internal synchronization.
 *
 * <p>DATA, KEEPALIVE, and SHUTDOWN are handled. ACKACK isn't yet (needs RTT
 * measurement machinery this pass doesn't add) — anything else arriving for this
 * socket ID is logged and dropped, matching this codebase's established "drop
 * what you don't handle yet" pattern rather than crashing the connection.
 *
 * <p>KEEPALIVE is echoed back immediately on receipt, matching gosrt's
 * {@code handleKeepAlive} (connection.go) exactly — worth flagging: if a peer
 * mirrors this same "echo on receipt" behavior (this codebase's own future
 * caller/sender side might), two such peers talking to each other could in
 * theory tight-loop echoing each other's keepalives forever, since neither
 * gosrt nor the RFC's own KEEPALIVE description gates this with a rate limit.
 * Verified safe against libsrt specifically (it doesn't echo on receipt), which
 * is this codebase's actual interop target so far — reconsider a rate limit
 * before this codebase gets its own keepalive-originating side.
 *
 * <p>RTT/RTTVar/buffer/rate figures fed to {@link AckSender#tick} are hardcoded
 * to 0 — not measured yet, same gap {@code AckSender} already documents. The
 * periodic NAK re-announcement interval is a fixed 20ms floor (gosrt's own floor
 * when RTT isn't known), not RTT-adaptive either.
 */
public final class SrtConnection {

    private static final Logger LOG = Logger.getLogger(SrtConnection.class.getName());
    private static final long TICK_INTERVAL_MILLIS = 10;
    private static final long PERIODIC_NAK_INTERVAL_MICROS = 20_000;

    private final Channel channel;
    private final SrtSocketIdDemultiplexer demultiplexer;
    private final AcceptedConnection metadata;
    private final LossList lossList;
    private final AckSender ackSender;
    private final ReceiveBuffer receiveBuffer;
    private final ScheduledFuture<?> scheduledTick;
    private final long startNanos = System.nanoTime();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private long lastPeriodicNakMicros;
    private int fullAckCounter;

    private volatile Consumer<ByteBuf> onData = payload -> {
    };
    private volatile Consumer<LossRange> onLoss = range -> {
    };
    private volatile Consumer<LossRange> onTlpktDrop = range -> {
    };
    private volatile Runnable onClose = () -> {
    };

    public SrtConnection(Channel channel, SrtSocketIdDemultiplexer demultiplexer, AcceptedConnection metadata,
            CircularNumber initialSequenceNumber) {
        this.channel = channel;
        this.demultiplexer = demultiplexer;
        this.metadata = metadata;
        this.lossList = new LossList(initialSequenceNumber);
        this.ackSender = new AckSender(lossList);
        this.receiveBuffer = new ReceiveBuffer(initialSequenceNumber, metadata.receiveLatencyMillis() * 1000L);

        demultiplexer.register(metadata.socketId(), this::onPacket);
        this.scheduledTick = channel.eventLoop().scheduleAtFixedRate(
                this::tick, TICK_INTERVAL_MILLIS, TICK_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    public AcceptedConnection metadata() {
        return metadata;
    }

    /** Fires once per delivered packet, in sequence order, post-TSBPD. The handler owns releasing the {@link ByteBuf}. */
    public void onData(Consumer<ByteBuf> handler) {
        this.onData = handler;
    }

    /** Fires once per newly-detected gap (mirrors the immediate NAK this also sends). */
    public void onLoss(Consumer<LossRange> handler) {
        this.onLoss = handler;
    }

    /** Fires once per gap {@link ReceiveBuffer}'s TLPKTDROP gives up on. */
    public void onTlpktDrop(Consumer<LossRange> handler) {
        this.onTlpktDrop = handler;
    }

    /** Fires once this connection has fully torn down — from either side closing it. */
    public void onClose(Runnable handler) {
        this.onClose = handler;
    }

    /**
     * Tears the connection down: sends our own SHUTDOWN to the peer (mirroring
     * gosrt's {@code close()}, which does this unconditionally regardless of
     * whether the peer's SHUTDOWN is what triggered this call — a symmetric
     * teardown handshake, not just a one-sided notification), unregisters from
     * the demultiplexer, cancels the tick, releases any buffered-but-undelivered
     * payloads, and fires {@link #onClose}. Safe to call more than once, from any
     * thread — only the first call does anything, matching gosrt's
     * {@code sync.Once}-guarded {@code close()} for the same reason (this can be
     * reached both from a peer's SHUTDOWN arriving on the event loop thread, and
     * from {@link SrtListener#close()} on whatever thread the app called that on).
     */
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        sendShutdown();
        scheduledTick.cancel(false);
        demultiplexer.unregister(metadata.socketId());
        receiveBuffer.dispose();
        onClose.run();
    }

    private void onPacket(AddressedEnvelope<SrtPacket, InetSocketAddress> msg) {
        SrtPacket packet = msg.content();
        if (packet instanceof DataPacket data) {
            handleData(data);
            return;
        }

        if (packet instanceof ControlPacket control) {
            if (control.type() == ControlType.KEEPALIVE) {
                control.body().release();
                sendKeepAlive();
                return;
            }
            if (control.type() == ControlType.SHUTDOWN) {
                control.body().release();
                close();
                return;
            }
        }

        LOG.log(Level.FINE, "Dropping unhandled packet type for socket {0}", metadata.socketId());
        packet.body().release();
    }

    private void handleData(DataPacket data) {
        CircularNumber seq = CircularNumber.of(data.sequenceNumber() & 0x7FFF_FFFF, SrtPacket.MAX_SEQUENCE_NUMBER);
        List<LossRange> immediateLoss = lossList.onPacketReceived(seq);
        ackSender.onPacketReceived();

        for (LossRange range : immediateLoss) {
            sendNak(List.of(range));
            onLoss.accept(range);
        }

        receiveBuffer.add(data, elapsedMicros());
    }

    private void tick() {
        ackSender.tick(elapsedMicros(), 0, 0, 0, 0, 0, 0).ifPresent(this::sendAck);

        long now = elapsedMicros();
        if (now - lastPeriodicNakMicros >= PERIODIC_NAK_INTERVAL_MICROS) {
            lastPeriodicNakMicros = now;
            List<LossRange> outstanding = lossList.outstanding();
            if (!outstanding.isEmpty()) {
                sendNak(outstanding);
            }
        }

        DeliveryResult result = receiveBuffer.deliver(elapsedMicros());
        for (LossRange abandoned : result.abandoned()) {
            lossList.abandon(abandoned.end());
            onTlpktDrop.accept(abandoned);
        }
        for (DataPacket delivered : result.delivered()) {
            onData.accept(delivered.body());
        }
    }

    private void sendKeepAlive() {
        send(new ControlPacket(ControlType.KEEPALIVE, 0, (int) elapsedMicros(), metadata.peerSocketId(),
                channel.alloc().buffer(0)));
    }

    private void sendShutdown() {
        send(new ControlPacket(ControlType.SHUTDOWN, 0, (int) elapsedMicros(), metadata.peerSocketId(),
                channel.alloc().buffer(0)));
    }

    private void sendNak(List<LossRange> ranges) {
        send(NakGenerator.build(ranges, (int) elapsedMicros(), metadata.peerSocketId()));
    }

    private void sendAck(AckCif ackCif) {
        // "Acknowledgement Number": a sequential counter for Full ACKs (0 otherwise), per
        // draft-sharabayko-srt.md - lives in the packet header, not the CIF. See AckCif's javadoc.
        int ackNumber = ackCif.variant() == AckVariant.FULL ? ++fullAckCounter : 0;
        ByteBuf cifBuf = channel.alloc().buffer();
        ackCif.encodeTo(cifBuf);
        send(new ControlPacket(ControlType.ACK, ackNumber, (int) elapsedMicros(), metadata.peerSocketId(), cifBuf));
    }

    private void send(SrtPacket packet) {
        channel.writeAndFlush(new DefaultAddressedEnvelope<>(packet, metadata.peerAddress()));
    }

    private long elapsedMicros() {
        return (System.nanoTime() - startNanos) / 1000;
    }
}
