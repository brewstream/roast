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
import org.brewstream.roast.packet.cif.LossListCodec;
import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.recv.AckSender;
import org.brewstream.roast.recv.DeliveryResult;
import org.brewstream.roast.recv.LossList;
import org.brewstream.roast.recv.NakGenerator;
import org.brewstream.roast.recv.ReceiveBuffer;
import org.brewstream.roast.send.SendBuffer;
import org.brewstream.roast.util.CircularNumber;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A live, accepted connection: owns {@link LossList}, {@link AckSender},
 * {@link ReceiveBuffer}, and {@link SendBuffer} for its lifetime, registers
 * itself as the accepted socket ID's {@link SrtPacketSink}, and drives them
 * via a ~10ms tick on the same Netty event loop the packets themselves arrive
 * on — everything here runs on that one thread, so (like its four
 * collaborators individually) this class needs no internal synchronization,
 * {@link #write} excepted (see its own javadoc).
 *
 * <p>DATA, KEEPALIVE, SHUTDOWN, ACK, NAK, and ACKACK are handled — anything else
 * arriving for this socket ID is logged and dropped, matching this codebase's
 * established "drop what you don't handle yet" pattern rather than crashing
 * the connection.
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
 * <p><b>RTT measurement</b>: every Full ACK we send is recorded (ack number →
 * send time); when the matching ACKACK arrives (its packet-header
 * Type-specific Information field echoes that ack number), the elapsed time
 * becomes an RTT sample, folded into a running estimate via
 * draft-sharabayko-srt.md §4.10's smoothing (matching gosrt's {@code
 * rtt.Recalculate} exactly: {@code rtt = rtt*0.875 + sample*0.125},
 * {@code rttVar = rttVar*0.75 + |rtt-sample|*0.25}, seeded at gosrt's own
 * defaults of 100ms/50ms before any sample arrives). This feeds both the RTT/
 * RTTVar figures {@link AckSender#tick} reports and the periodic NAK
 * re-announcement interval — {@code (rtt + 4*rttVar) / 2}, floored at 20ms,
 * gosrt's own {@code NAKInterval()} formula — replacing the fixed floor this
 * class used before any RTT was known. Buffer/rate figures are still hardcoded
 * to 0 — that needs the receive-side stats this codebase doesn't track yet.
 *
 * <p><b>Sending</b>: {@link #write} queues a payload on {@link SendBuffer},
 * which owns the sending side's loss list and TLPKTDROP the same way {@link
 * LossList}/{@link ReceiveBuffer} own the receiving side's — see {@code
 * SendBuffer}'s own javadoc. Two design notes worth recording, both traced
 * directly from gosrt's {@code connection.go} rather than assumed:
 * <ul>
 *   <li>Both directions share the <em>same</em> initial sequence number —
 *       HSv5's handshake carries one such field, mirrored by both peers (see
 *       {@code ListenerHandshake}, which echoes the peer's own value back
 *       rather than generating a fresh one) — so {@link #sendBuffer} is seeded
 *       from the exact same {@code initialSequenceNumber} the receive-side
 *       collaborators are.</li>
 *   <li>RTT is one shared estimate updated from <em>either</em> direction: an
 *       ACKACK round trip (above) supplies a raw sample, and a Full ACK's own
 *       reported RTT figure ({@link AckCif#rtt()}) is fed through the exact
 *       same {@link #recalculateRtt} EWMA as if it were one more raw sample —
 *       this is gosrt's real {@code handleACK} behavior, not a Roast
 *       simplification. Since {@link #nakIntervalMicros} already reads {@link
 *       #rttMicros}/{@link #rttVarMicros} live, an ACK-driven update composes
 *       for free — no separate propagation step needed (gosrt manually pushes
 *       a recalculated NAK interval into its receiver after ACKACK; Roast's
 *       compute-on-read design doesn't need the equivalent).</li>
 * </ul>
 * A Full ACK also triggers an ACKACK reply back to the peer (gosrt: gated to
 * {@code !IsLite && !IsSmall}, i.e. {@link AckVariant#FULL} only — pruning
 * {@code SendBuffer}'s loss list via an inbound ACK happens for every
 * variant, unconditionally). {@link #onRetransmit} fires per retransmitted
 * packet — DESIGN.md's "Extensibility & observability" section names this as
 * a hook to add once the producing mechanism exists; ACK-sent/received hooks
 * and live pollable stats stay deferred, matching how {@code onData}/{@code
 * onLoss}/{@code onTlpktDrop} were rolled out incrementally too.
 */
public final class SrtConnection {

    private static final Logger LOG = Logger.getLogger(SrtConnection.class.getName());
    private static final long TICK_INTERVAL_MILLIS = 10;
    private static final long MIN_NAK_INTERVAL_MICROS = 20_000;
    private static final double INITIAL_RTT_MICROS = 100_000;
    private static final double INITIAL_RTT_VAR_MICROS = 50_000;

    private final Channel channel;
    private final SrtSocketIdDemultiplexer demultiplexer;
    private final AcceptedConnection metadata;
    private final LossList lossList;
    private final AckSender ackSender;
    private final ReceiveBuffer receiveBuffer;
    private final SendBuffer sendBuffer;
    private final ScheduledFuture<?> scheduledTick;
    private final long startNanos = System.nanoTime();
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<Integer, Long> pendingAcks = new HashMap<>();
    private final Runnable onChannelOwnerClose;

    private long lastPeriodicNakMicros;
    private int fullAckCounter;
    private double rttMicros = INITIAL_RTT_MICROS;
    private double rttVarMicros = INITIAL_RTT_VAR_MICROS;

    private volatile Consumer<ByteBuf> onData = payload -> {
    };
    private volatile Consumer<LossRange> onLoss = range -> {
    };
    private volatile Consumer<LossRange> onTlpktDrop = range -> {
    };
    private volatile Consumer<DataPacket> onRetransmit = packet -> {
    };
    private volatile Runnable onClose = () -> {
    };

    public SrtConnection(Channel channel, SrtSocketIdDemultiplexer demultiplexer, AcceptedConnection metadata,
            CircularNumber initialSequenceNumber) {
        this(channel, demultiplexer, metadata, initialSequenceNumber, () -> { });
    }

    /**
     * Used only by {@code SrtCaller}: a caller-created connection owns a dedicated
     * channel/event-loop-group (1:1, unlike a listener's port shared across many
     * connections), which {@code onChannelOwnerClose} tears down as part of {@link
     * #close}'s existing fixed internal teardown — deliberately not routed through
     * the app-facing {@link #onClose} hook, since that's a single overridable slot
     * an application setting its own {@code onClose} handler would otherwise
     * silently clobber, leaking the channel.
     */
    SrtConnection(Channel channel, SrtSocketIdDemultiplexer demultiplexer, AcceptedConnection metadata,
            CircularNumber initialSequenceNumber, Runnable onChannelOwnerClose) {
        this.channel = channel;
        this.demultiplexer = demultiplexer;
        this.metadata = metadata;
        this.onChannelOwnerClose = onChannelOwnerClose;
        this.lossList = new LossList(initialSequenceNumber);
        this.ackSender = new AckSender(lossList);
        this.receiveBuffer = new ReceiveBuffer(initialSequenceNumber, metadata.receiveLatencyMillis() * 1000L);
        long dropThresholdMicros = Math.max((long) (metadata.sendLatencyMillis() * 1000L * 1.25), 1_000_000L) + 20_000L;
        this.sendBuffer = new SendBuffer(initialSequenceNumber, metadata.peerSocketId(), dropThresholdMicros, this::sendData);

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

    /** Fires once per retransmitted DATA packet, right before it's sent. */
    public void onRetransmit(Consumer<DataPacket> handler) {
        this.onRetransmit = handler;
    }

    /**
     * Queues {@code payload} for sending as one DATA packet — no message
     * chunking, matching the "no MSS negotiation yet" known gap and how
     * {@link #onData} already delivers one packet at a time on the receive
     * side. Ownership of {@code payload} transfers to this connection.
     * Unlike every other method here, this one is meant to be called from an
     * arbitrary application thread (it's the actual public write API), so —
     * to preserve this class's "everything runs on one event-loop thread"
     * invariant that lets {@link #sendBuffer} and friends skip internal
     * synchronization — the real work is marshaled onto {@link #channel}'s
     * event loop, the same way {@code Channel.writeAndFlush} already does
     * internally for the sends this class performs elsewhere. Fire-and-forget,
     * matching gosrt's own {@code Write()} (a non-blocking send to an internal
     * queue). A no-op (payload released) once {@link #close}d.
     */
    public void write(ByteBuf payload) {
        channel.eventLoop().execute(() -> {
            if (closed.get()) {
                payload.release();
                return;
            }
            sendBuffer.push(payload, elapsedMicros());
        });
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
        sendBuffer.flush();
        onChannelOwnerClose.run();
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
            if (control.type() == ControlType.ACKACK) {
                control.body().release();
                handleAckAck(control);
                return;
            }
            if (control.type() == ControlType.ACK) {
                handleAck(control);
                return;
            }
            if (control.type() == ControlType.NAK) {
                handleNak(control);
                return;
            }
        }

        LOG.log(Level.FINE, "Dropping unhandled packet type for socket {0}", metadata.socketId());
        packet.body().release();
    }

    /**
     * The ACKACK's echoed Type-specific Information field is the ack number.
     * Cleans up any older still-pending ack numbers too — once a later one is
     * confirmed, earlier ones are presumed stale (matches gosrt's
     * {@code handleACKACK}), so this map can't grow forever against a
     * compliant peer. Also feeds the raw (unsmoothed) RTT sample and the
     * ACKACK's own header timestamp into {@link ReceiveBuffer#addDriftSample}
     * for clock-drift correction — see that method's javadoc.
     */
    private void handleAckAck(ControlPacket control) {
        int ackNumber = control.typeSpecificInfo();
        Long sentAtMicros = pendingAcks.remove(ackNumber);
        if (sentAtMicros == null) {
            LOG.log(Level.FINE, "Got ACKACK for unknown ack number {0} on socket {1}", new Object[]{ackNumber, metadata.socketId()});
            return;
        }
        long nowMicros = elapsedMicros();
        long sampleMicros = nowMicros - sentAtMicros;
        recalculateRtt(sampleMicros);
        receiveBuffer.addDriftSample(control.timestamp(), nowMicros, sampleMicros);
        pendingAcks.keySet().removeIf(pending -> pending < ackNumber);
    }

    /**
     * Prunes {@link #sendBuffer}'s retransmission state up to the acknowledged
     * sequence number, for every ACK variant. A Full ACK additionally feeds its
     * own reported RTT figure into {@link #recalculateRtt} and triggers an
     * ACKACK reply — gated to Full only, matching gosrt's {@code handleACK}
     * ({@code !IsLite && !IsSmall}). See the class javadoc's "Sending" section.
     */
    private void handleAck(ControlPacket control) {
        AckCif cif = AckCif.decode(control.body());
        control.body().release();
        if (cif == null) {
            LOG.log(Level.FINE, "Dropping malformed ACK for socket {0}", metadata.socketId());
            return;
        }
        sendBuffer.ack(cif.lastAckPacketSequenceNumber());
        if (cif.variant() == AckVariant.FULL) {
            recalculateRtt(cif.rtt());
            sendAckAck(control.typeSpecificInfo());
        }
    }

    private void handleNak(ControlPacket control) {
        List<LossRange> ranges = LossListCodec.decode(control.body());
        control.body().release();
        sendBuffer.nak(ranges);
    }

    private void recalculateRtt(long sampleMicros) {
        double sample = sampleMicros;
        rttMicros = rttMicros * 0.875 + sample * 0.125;
        rttVarMicros = rttVarMicros * 0.75 + Math.abs(rttMicros - sample) * 0.25;
    }

    private long nakIntervalMicros() {
        return Math.max(MIN_NAK_INTERVAL_MICROS, (long) ((rttMicros + 4 * rttVarMicros) / 2));
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
        ackSender.tick(elapsedMicros(), (int) Math.round(rttMicros), (int) Math.round(rttVarMicros), 0, 0, 0, 0)
                .ifPresent(this::sendAck);

        long now = elapsedMicros();
        if (now - lastPeriodicNakMicros >= nakIntervalMicros()) {
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

        sendBuffer.tick(elapsedMicros());
    }

    /** {@link SendBuffer}'s deliver callback: fires {@link #onRetransmit} for a resend, then sends it. */
    private void sendData(DataPacket packet) {
        if (packet.retransmitted()) {
            onRetransmit.accept(packet);
        }
        send(packet);
    }

    private void sendAckAck(int ackNumber) {
        send(new ControlPacket(ControlType.ACKACK, ackNumber, (int) elapsedMicros(), metadata.peerSocketId(),
                channel.alloc().buffer(0)));
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
        int ackNumber = 0;
        if (ackCif.variant() == AckVariant.FULL) {
            ackNumber = ++fullAckCounter;
            pendingAcks.put(ackNumber, elapsedMicros());
        }
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
