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
import java.util.Optional;
import java.util.concurrent.TimeUnit;
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
 * <p>Only DATA packets are handled. KEEPALIVE, SHUTDOWN, and ACKACK aren't
 * implemented yet — anything else arriving for this socket ID is logged and
 * dropped, matching this codebase's established "drop what you don't handle
 * yet" pattern rather than crashing the connection.
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

    private long lastPeriodicNakMicros;
    private int fullAckCounter;

    private volatile Consumer<ByteBuf> onData = payload -> {
    };
    private volatile Consumer<LossRange> onLoss = range -> {
    };
    private volatile Consumer<LossRange> onTlpktDrop = range -> {
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

    /** Unregisters from the demultiplexer, cancels the tick, and releases any buffered-but-undelivered payloads. */
    public void close() {
        scheduledTick.cancel(false);
        demultiplexer.unregister(metadata.socketId());
        receiveBuffer.dispose();
    }

    private void onPacket(AddressedEnvelope<SrtPacket, InetSocketAddress> msg) {
        SrtPacket packet = msg.content();
        if (!(packet instanceof DataPacket data)) {
            LOG.log(Level.FINE, "Dropping non-DATA packet for socket {0} (not yet handled)", metadata.socketId());
            packet.body().release();
            return;
        }

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
