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
import org.brewstream.roast.packet.cif.ExtensionType;
import org.brewstream.roast.packet.cif.KeyEncryption;
import org.brewstream.roast.packet.cif.KeyMaterialCif;
import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.crypto.EncryptionContext;
import org.brewstream.roast.recv.AckBoundaryResult;
import org.brewstream.roast.recv.AckSender;
import org.brewstream.roast.recv.DeliveryResult;
import org.brewstream.roast.recv.LossList;
import org.brewstream.roast.recv.NakGenerator;
import org.brewstream.roast.recv.ReceiveBuffer;
import org.brewstream.roast.recv.ReceiveRateEstimator;
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
 * <p><b>KEEPALIVE follows libsrt's model, not gosrt's:</b> originate one when
 * nothing has gone out for a second, and on receipt do nothing beyond the
 * proof-of-life every inbound packet already provides. The two references differ
 * here, and each is internally consistent — gosrt echoes on receipt and never
 * originates, libsrt originates on an idle send timer and never echoes — but the
 * halves do not mix. This class used to echo, which was safe only for as long as
 * nothing here originated; combining the two would let two Roast peers echo each
 * other's keepalives at wire speed with nothing to damp it, since neither gosrt
 * nor the RFC's KEEPALIVE section rate-limits the echo.
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
 * class used before any RTT was known. Every other figure in a Full ACK is now
 * real too: the available-buffer-size figure (see {@link #tick} — it was
 * hardcoded to 0, which cost ~35-42% of a real published stream, since a peer
 * reads it as our flow-control window), and the packet-rate/link-capacity/
 * receiving-rate figures, from {@link ReceiveRateEstimator}. Nothing in the ACK
 * CIF is a placeholder any more.
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
 * packet. See {@link SrtConnectionListener} for the full set of observability
 * events, and {@link #stats()} for the counters that back them.
 */
public final class SrtConnection {

    private static final Logger LOG = Logger.getLogger(SrtConnection.class.getName());
    private static final long TICK_INTERVAL_MILLIS = 10;
    private static final long MIN_NAK_INTERVAL_MICROS = 20_000;
    /** How long {@link #close} waits for teardown to run on the event loop before giving up on it. */
    private static final long CLOSE_DRAIN_TIMEOUT_MILLIS = 500;
    private static final double INITIAL_RTT_MICROS = 100_000;
    private static final double INITIAL_RTT_VAR_MICROS = 50_000;

    /**
     * Default for how long a peer may stay silent before we treat it as gone.
     * Five seconds is both references' own default (gosrt's {@code
     * PeerIdleTimeout}, libsrt's {@code SRTO_PEERIDLETIMEO}). <em>Any</em> packet
     * resets the clock — data, ACK, KEEPALIVE alike — so only a genuinely
     * unreachable peer reaches it. Overridable via {@link
     * SrtConfig#withPeerIdleTimeout}, since a link with long outages is a real
     * reason to want longer.
     */
    private static final long DEFAULT_PEER_IDLE_TIMEOUT_MICROS = 5_000_000;

    /**
     * Send a KEEPALIVE when nothing else has gone out for this long — libsrt's
     * {@code COMM_KEEPALIVE_PERIOD_US}, measured from the last packet we sent
     * rather than the last one we received.
     */
    private static final long KEEPALIVE_INTERVAL_MICROS = 1_000_000;

    /**
     * Fallback receive window, in packets, used only when the handshake didn't
     * yield a usable one — matches {@code CallerHandshake}'s own advertised
     * default. Normally {@link AcceptedConnection#flowWindowSize()} (the value
     * actually agreed on the wire) is used instead; this exists so a peer that
     * advertises 0, or a malformed handshake, can't silently reproduce the
     * advertise-a-zero-window bug that cost ~35-42% of a real stream (see
     * {@link #tick}). gosrt sidesteps the question by always reporting its own
     * configured {@code FC} ({@code connection.go}'s {@code sendACK}, default
     * 25600) rather than anything negotiated. Matches {@code SrtConfig}'s own
     * default; the negotiated value normally wins.
     */
    private static final int FALLBACK_RECEIVE_FLOW_WINDOW_PACKETS = 8192;

    private final Channel channel;
    private final SrtSocketIdDemultiplexer demultiplexer;
    private final AcceptedConnection metadata;
    private final LossList lossList;
    private final AckSender ackSender;
    private final ReceiveBuffer receiveBuffer;
    private final ReceiveRateEstimator receiveRateEstimator = new ReceiveRateEstimator();
    private final SendBuffer sendBuffer;
    private final ScheduledFuture<?> scheduledTick;
    private final int receiveFlowWindowPackets;
    private final long startNanos = System.nanoTime();
    /** Guards {@link #close} against running teardown twice; set from any thread. */
    private final AtomicBoolean closed = new AtomicBoolean(false);
    /**
     * Whether teardown has actually run. Distinct from {@link #closed}, which is
     * set the moment close is <em>requested</em>: a {@link #write} already
     * marshaled onto the event loop must still be sent, and checking the
     * requested-flag would drop it instead. Event-loop only, so no atomic.
     */
    private boolean tornDown;
    private final Map<Integer, Long> pendingAcks = new HashMap<>();
    private final Runnable onChannelOwnerClose;
    private final EncryptionContext encryptionContext;
    private final long peerIdleTimeoutMicros;

    private final EventDispatcher events;

    private long packetsSent;
    private long packetsRetransmitted;
    private long packetsReceived;
    private long bytesSent;
    private long bytesReceived;
    private long packetsLost;
    private long packetsDropped;

    private long lastPeriodicNakMicros;
    /**
     * When we last heard anything at all from the peer, and when we last sent
     * anything to it — the two clocks behind {@link #peerIdleTimeoutMicros} and
     * {@link #KEEPALIVE_INTERVAL_MICROS}. Both are written and read on the event
     * loop only, like {@link #lastPeriodicNakMicros}, so neither needs to be
     * volatile. Starting at zero (the connection epoch) is correct: a peer that
     * never says anything after the handshake is reaped on the same schedule as
     * one that goes quiet later.
     */
    private long lastReceiveMicros;
    private long lastSendMicros;
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
        this(channel, demultiplexer, metadata, initialSequenceNumber, () -> { }, null,
                DEFAULT_PEER_IDLE_TIMEOUT_MICROS);
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
        this(channel, demultiplexer, metadata, initialSequenceNumber, onChannelOwnerClose, null,
                DEFAULT_PEER_IDLE_TIMEOUT_MICROS);
    }

    /**
     * With encryption. {@code encryptionContext} may be {@code null}, in which
     * case this connection neither encrypts nor decrypts and behaves exactly as
     * before - the whole data path below is inert without it.
     */
    SrtConnection(Channel channel, SrtSocketIdDemultiplexer demultiplexer, AcceptedConnection metadata,
            CircularNumber initialSequenceNumber, Runnable onChannelOwnerClose,
            EncryptionContext encryptionContext) {
        this(channel, demultiplexer, metadata, initialSequenceNumber, onChannelOwnerClose, encryptionContext,
                DEFAULT_PEER_IDLE_TIMEOUT_MICROS);
    }

    /**
     * As above, with the peer idle timeout from {@code SrtConfig} rather than the
     * default. Only the timeout is threaded through rather than the whole config
     * object: everything else a connection needs was already negotiated with the
     * peer and reaches it via {@link AcceptedConnection}, and passing settings
     * that no longer apply would invite reading the wrong one.
     */
    SrtConnection(Channel channel, SrtSocketIdDemultiplexer demultiplexer, AcceptedConnection metadata,
            CircularNumber initialSequenceNumber, Runnable onChannelOwnerClose,
            EncryptionContext encryptionContext, long peerIdleTimeoutMicros) {
        this.peerIdleTimeoutMicros = peerIdleTimeoutMicros;
        this.encryptionContext = encryptionContext;
        this.events = new EventDispatcher(metadata.socketId().toString());
        this.channel = channel;
        this.demultiplexer = demultiplexer;
        this.metadata = metadata;
        this.onChannelOwnerClose = onChannelOwnerClose;
        this.lossList = new LossList(initialSequenceNumber);
        this.ackSender = new AckSender();
        this.receiveBuffer = new ReceiveBuffer(initialSequenceNumber, metadata.receiveLatencyMillis() * 1000L);
        this.receiveFlowWindowPackets = metadata.flowWindowSize() > 0
                ? metadata.flowWindowSize()
                : FALLBACK_RECEIVE_FLOW_WINDOW_PACKETS;
        long dropThresholdMicros = Math.max((long) (metadata.sendLatencyMillis() * 1000L * 1.25), 1_000_000L) + 20_000L;
        this.sendBuffer = new SendBuffer(initialSequenceNumber, metadata.peerSocketId(), dropThresholdMicros, this::sendData);

        demultiplexer.register(metadata.socketId(), this::onPacket);
        this.scheduledTick = channel.eventLoop().scheduleAtFixedRate(
                this::tick, TICK_INTERVAL_MILLIS, TICK_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
    }

    /** Peer address, StreamID, and the values negotiated during the handshake. */
    public AcceptedConnection metadata() {
        return metadata;
    }

    /**
     * Registers an observability listener for this connection. Events are
     * delivered off the event loop — see {@link SrtConnectionListener} for the
     * threading contract and why it differs from {@link #onData}.
     */
    public void addEventListener(SrtConnectionListener listener) {
        events.add(listener);
    }

    /**
     * Announces this connection to its listeners. Called by whoever created the
     * connection once it is fully wired, rather than from the constructor — a
     * listener must not observe a half-built object.
     */
    void fireConnected() {
        events.fire(listener -> listener.onConnected(this));
    }

    /** A point-in-time snapshot of this connection's counters and gauges. */
    public ConnectionStats stats() {
        return new ConnectionStats(
                packetsSent, packetsRetransmitted, packetsReceived, bytesSent, bytesReceived,
                packetsLost, packetsDropped, events.droppedEvents(),
                Math.round(rttMicros), Math.round(rttVarMicros),
                receiveRateEstimator.packetsPerSecond(),
                receiveRateEstimator.receivingRateBytesPerSecond(),
                receiveRateEstimator.estimatedLinkCapacityPacketsPerSecond(),
                receiveBuffer.bufferedCount(), sendBuffer.queuedCount(), sendBuffer.inFlightCount(),
                receiveFlowWindowPackets,
                sendBuffer.rates().estimatedInputBytesPerSecond(),
                sendBuffer.rates().estimatedSentBytesPerSecond(),
                sendBuffer.rates().sendLossRatePercent());
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
        // Checked here, not inside the event-loop task below: an exception thrown
        // there would be swallowed by Netty and the caller would never learn its
        // payload went nowhere.
        int maxPayloadSize = metadata.maxPayloadSize();
        if (payload.readableBytes() > maxPayloadSize) {
            int size = payload.readableBytes();
            payload.release();
            throw new IllegalArgumentException("payload of " + size + " bytes exceeds this connection's "
                    + maxPayloadSize + "-byte limit; live mode sends one packet per write and does not "
                    + "split messages, so the caller must chunk");
        }
        channel.eventLoop().execute(() -> {
            if (tornDown) {
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
        // Teardown runs on the event loop so it queues behind any write() already
        // marshaled there - otherwise a write immediately followed by a close
        // drains a send buffer the write hasn't reached yet, and the data is lost.
        // Reachable from the event loop itself (a peer's SHUTDOWN) and from any
        // other thread (an application, or SrtListener.close), so both are handled.
        if (channel.eventLoop().inEventLoop()) {
            doClose();
            return;
        }
        try {
            channel.eventLoop().submit(this::doClose).await(CLOSE_DRAIN_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (RuntimeException e) {
            // The loop is already gone (shutting down); tear down inline so
            // nothing is left registered or leaked.
            doClose();
        }
    }

    private void doClose() {
        tornDown = true;
        // Give already-queued outbound data its chance before announcing the
        // shutdown, so the peer receives it rather than being told to stop first.
        // Bounded by whatever is already queued - this waits for nothing.
        sendBuffer.tick(elapsedMicros());
        sendShutdown();

        scheduledTick.cancel(false);
        demultiplexer.unregister(metadata.socketId());

        // Hand over what already arrived. These packets are in hand and merely
        // waiting on TSBPD deadlines that no longer mean anything now the
        // connection is ending; discarding them truncates the tail of every
        // stream. See ReceiveBuffer.drainAll.
        for (DataPacket remaining : receiveBuffer.drainAll()) {
            try {
                onData.accept(remaining.body());
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "onData threw while draining on close", e);
                remaining.body().release();
            }
        }
        receiveBuffer.dispose();
        sendBuffer.flush();
        if (encryptionContext != null) {
            encryptionContext.destroy(); // zero the passphrase and keys rather than waiting for GC
        }
        onChannelOwnerClose.run();
        onClose.run();
        events.fire(listener -> listener.onDisconnected(this));
        events.close();
    }

    private void onPacket(AddressedEnvelope<SrtPacket, InetSocketAddress> msg) {
        // Every inbound packet counts as proof of life, whatever it turns out to
        // be - matching gosrt, which resets its idle timer at the top of
        // handlePacket before it has looked at the type at all.
        lastReceiveMicros = elapsedMicros();

        SrtPacket packet = msg.content();
        if (packet instanceof DataPacket data) {
            handleData(data);
            return;
        }

        if (packet instanceof ControlPacket control) {
            if (control.type() == ControlType.KEEPALIVE) {
                // Nothing to do beyond the idle-timer reset every inbound packet
                // already got above - see this class's javadoc on why this no
                // longer echoes.
                control.body().release();
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
            if (control.type() == ControlType.DROPREQ) {
                handleDropRequest(control);
                return;
            }
            if (control.type() == ControlType.USER_DEFINED) {
                handleKeyMaterialUpdate(control);
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
        int acknowledged = (int) cif.lastAckPacketSequenceNumber().value();
        int peerRtt = cif.rtt();
        events.fire(listener -> listener.onAckReceived(this, acknowledged, peerRtt));
        if (cif.variant() == AckVariant.FULL) {
            recalculateRtt(cif.rtt());
            sendAckAck(control.typeSpecificInfo());
        }
    }

    /**
     * A peer's Message Drop Request: its sender has discarded a range of
     * packets and they are never coming, so stop waiting for them instead of
     * holding the stream until their TSBPD deadlines expire — up to the
     * negotiated latency of pointless stall per episode.
     *
     * <p>gosrt marks this type "unimplemented", so there is nothing to port
     * there; this follows libsrt's {@code processCtrlDropReq}, <b>including its
     * validation, which exists for a reason libsrt spells out in a comment</b>:
     * a reversed range makes its buffer walk wrap and clear almost everything
     * held, which it calls a DoS primitive. A peer can send this unauthenticated
     * at any time, so a reversed or absurdly distant range is discarded rather
     * than acted on. The CIF is two 32-bit sequence numbers, first and last
     * inclusive.
     */
    private void handleDropRequest(ControlPacket control) {
        ByteBuf cif = control.body();
        if (cif.readableBytes() < 8) {
            LOG.log(Level.FINE, "Dropping undersized DROPREQ on socket {0}", metadata.socketId());
            cif.release();
            return;
        }
        CircularNumber first = CircularNumber.of(cif.readInt() & 0x7FFF_FFFF, SrtPacket.MAX_SEQUENCE_NUMBER);
        CircularNumber last = CircularNumber.of(cif.readInt() & 0x7FFF_FFFF, SrtPacket.MAX_SEQUENCE_NUMBER);
        cif.release();

        if (last.lessThan(first)) {
            LOG.log(Level.FINE, "Discarding reversed DROPREQ range on socket {0}", metadata.socketId());
            return;
        }
        // A range far from what we are actually waiting for is not a drop we can
        // make sense of - libsrt bounds this the same way.
        long distance = first.value() - receiveBuffer.acknowledgedBoundary().value();
        if (Math.abs(distance) > SrtPacket.MAX_SEQUENCE_NUMBER / 4) {
            LOG.log(Level.FINE, "Discarding DROPREQ too distant from our receive window on socket {0}",
                    metadata.socketId());
            return;
        }

        if (!receiveBuffer.abandonUpTo(last)) {
            return; // already past it; nothing to do
        }
        lossList.abandon(last);
        LossRange dropped = new LossRange(first, last);
        packetsDropped += last.value() - first.value() + 1;
        onTlpktDrop.accept(dropped);
        events.fire(listener -> listener.onTlpktDrop(this, dropped));
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
        // Decrypt before anything else observes this packet: a payload we can't
        // decrypt is dropped outright, and dropping it after it had been counted
        // as received would leave the loss list and the ACK boundary claiming a
        // packet that never reached the application.
        if (!decryptIfNeeded(data)) {
            data.body().release();
            return;
        }

        packetsReceived++;
        int payloadBytes = data.body().readableBytes();
        bytesReceived += payloadBytes;
        int arrivedSequenceNumber = data.sequenceNumber();
        boolean wasRetransmitted = data.retransmitted();
        events.fire(listener ->
                listener.onPacketReceived(this, arrivedSequenceNumber, payloadBytes, wasRetransmitted));

        CircularNumber seq = CircularNumber.of(data.sequenceNumber() & 0x7FFF_FFFF, SrtPacket.MAX_SEQUENCE_NUMBER);
        List<LossRange> immediateLoss = lossList.onPacketReceived(seq);
        ackSender.onPacketReceived();
        // Read the payload size before handing the packet to the receive buffer, which
        // takes ownership of the body (and may release it outright as a duplicate).
        receiveRateEstimator.onPacketReceived(
                seq, data.body().readableBytes(), data.retransmitted(), elapsedMicros());

        for (LossRange range : immediateLoss) {
            packetsLost += range.end().value() - range.start().value() + 1;
            sendNak(List.of(range));
            onLoss.accept(range);
            events.fire(listener -> listener.onLoss(this, range));
        }

        receiveBuffer.add(data, elapsedMicros());
    }

    /**
     * Decrypts an inbound payload using the key its KK header field names.
     * Returns {@code false} if the packet can't be decrypted and should be
     * dropped: it's encrypted but we hold no keys, or it names a key we were
     * never told about. An unencrypted packet ({@code kk == 0}) passes through
     * untouched even on an encrypted connection — rejecting those is a policy
     * decision that belongs with the handshake, once the negotiated encryption
     * field says whether cleartext is acceptable at all.
     */
    private boolean decryptIfNeeded(DataPacket data) {
        if (data.kk() == 0) {
            return true;
        }
        if (encryptionContext == null) {
            LOG.log(Level.FINE, "Dropping encrypted DATA on an unencrypted connection {0}", metadata.socketId());
            return false;
        }
        KeyEncryption key = KeyEncryption.fromCode(data.kk());
        if (!encryptionContext.decrypt(data.body(), data.sequenceNumber(), key)) {
            LOG.log(Level.FINE, "Dropping DATA for socket {0}: no usable key for KK={1}",
                    new Object[]{metadata.socketId(), data.kk()});
            return false;
        }
        return true;
    }

    /**
     * Ordered to match gosrt's own {@code Tick()}: the ACK boundary is computed
     * first ({@link ReceiveBuffer#computeAckBoundary} — the actual TLPKTDROP
     * "give up" decision happens there now, not in delivery), fed into {@link
     * LossList#abandon} and the ACK CIF, then periodic NAK, then delivery is
     * gated by that same just-computed boundary. See {@link ReceiveBuffer}'s
     * javadoc for why this order matters — delivery must never run ahead of an
     * ACK boundary computed after it.
     *
     * <p><b>The available-buffer-size figure is load-bearing, not cosmetic.</b>
     * It used to be hardcoded to 0 (alongside the rate figures, which still
     * are). A peer's sender reads it as this receiver's flow-control window:
     * advertising 0 tells it we cannot accept anything, so it stops sending and
     * drops packets it decides can no longer be delivered in time, announcing
     * them with DROPREQ instead. Against real ffmpeg/libsrt that silently cost
     * ~35% of the published stream — the packets were never put on the wire at
     * all, which is why every receive-side loss counter stayed clean while the
     * relayed output was visibly corrupt. See STATUS.md for the full
     * investigation.
     */
    private void tick() {
        long now = elapsedMicros();

        // Checked before anything else, and returning rather than falling through:
        // close() tears down on this same thread, cancelling this task and
        // disposing the receive buffer, so the rest of the tick would be operating
        // on a connection that no longer exists. libsrt checks its own expiry in
        // the same place, and bails the same way.
        if (now - lastReceiveMicros >= peerIdleTimeoutMicros) {
            LOG.log(Level.FINE, () -> "closing " + metadata.socketId() + ": nothing received from "
                    + metadata.peerAddress() + " for " + (peerIdleTimeoutMicros / 1_000) + "ms");
            close();
            return;
        }

        AckBoundaryResult ackBoundary = receiveBuffer.computeAckBoundary(now);
        for (LossRange abandoned : ackBoundary.abandoned()) {
            packetsDropped += abandoned.end().value() - abandoned.start().value() + 1;
            lossList.abandon(abandoned.end());
            onTlpktDrop.accept(abandoned);
            events.fire(listener -> listener.onTlpktDrop(this, abandoned));
        }

        receiveRateEstimator.tick(now);
        int availableBufferSize = Math.max(0, receiveFlowWindowPackets - receiveBuffer.bufferedCount());
        ackSender.tick(now, ackBoundary.lastAckSequenceNumber().inc(),
                (int) Math.round(rttMicros), (int) Math.round(rttVarMicros), availableBufferSize,
                receiveRateEstimator.packetsPerSecond(),
                receiveRateEstimator.estimatedLinkCapacityPacketsPerSecond(),
                receiveRateEstimator.receivingRateBytesPerSecond())
                .ifPresent(this::sendAck);

        if (now - lastPeriodicNakMicros >= nakIntervalMicros()) {
            lastPeriodicNakMicros = now;
            List<LossRange> outstanding = lossList.outstanding();
            if (!outstanding.isEmpty()) {
                sendNak(outstanding);
            }
        }

        DeliveryResult result = receiveBuffer.deliver(ackBoundary.lastAckSequenceNumber(), now);
        for (DataPacket delivered : result.delivered()) {
            onData.accept(delivered.body());
        }

        sendBuffer.tick(elapsedMicros());

        // Keeps us visible to a peer applying the same idle rule we do. This is a
        // backstop, not something that fires in normal operation: AckSender emits
        // a full ACK every 10ms whether or not any data arrived, so lastSendMicros
        // never ages anywhere near a second. Measured, not assumed - instrumenting
        // this branch across the whole suite recorded zero firings. It earns its
        // place the moment that ACK cadence changes; libsrt pairs the identical
        // periodic-ACK timer with the identical last-send keepalive check.
        if (now - lastSendMicros >= KEEPALIVE_INTERVAL_MICROS) {
            sendKeepAlive();
        }
    }

    /** {@link SendBuffer}'s deliver callback: fires {@link #onRetransmit} for a resend, then sends it. */
    private void sendData(DataPacket packet) {
        packetsSent++;
        bytesSent += packet.body().readableBytes();
        if (packet.retransmitted()) {
            packetsRetransmitted++;
            onRetransmit.accept(packet);
            int sequenceNumber = packet.sequenceNumber();
            events.fire(listener -> listener.onRetransmit(this, sequenceNumber));
        }
        send(encryptIfConfigured(packet));
    }

    /**
     * Encrypts an outgoing payload and stamps the DATA header's KK field with the
     * key used, so the peer knows which one to decrypt with. A no-op when this
     * connection has no encryption context, or has one whose keys haven't been
     * negotiated yet.
     *
     * <p><b>Every delivery is encrypted, retransmissions included</b> — which
     * looks like a difference from gosrt, whose {@code pop} skips re-encrypting a
     * packet carrying the retransmitted flag. It isn't: gosrt encrypts its one
     * retained packet object in place on first send, so a retransmit of that same
     * object is already ciphertext. {@link SendBuffer} instead keeps the
     * plaintext original and hands out a fresh {@code retainedDuplicate()} per
     * delivery (see its javadoc), so each delivery arrives here as plaintext and
     * must be encrypted. Porting gosrt's {@code if !retransmitted} guard across
     * would put retransmissions on the wire in the clear. The wire result is
     * identical either way: the same sequence number yields the same keystream,
     * so a retransmit is byte-for-byte the packet it replaces.
     */
    private DataPacket encryptIfConfigured(DataPacket packet) {
        if (encryptionContext == null || !encryptionContext.hasKeys()) {
            return packet;
        }
        KeyEncryption key = encryptionContext.activeKey();
        ByteBuf plaintext = packet.payload();

        // Encrypt into a fresh buffer, never in place. What arrives here is a
        // retainedDuplicate() of the payload SendBuffer retains for
        // retransmission, and a duplicate SHARES the original's memory - so
        // encrypting in place would rewrite the retained plaintext into
        // ciphertext. The retransmission would then be encrypted a second time,
        // and because CTR is XOR against a keystream fixed by the sequence
        // number, a second pass restores the plaintext: the resend would go out
        // in the clear. Caught by
        // aRetransmissionOnAnEncryptedConnectionIsAlsoEncrypted.
        ByteBuf ciphertext = channel.alloc().buffer(plaintext.readableBytes());
        ciphertext.writeBytes(plaintext, plaintext.readerIndex(), plaintext.readableBytes());
        plaintext.release();

        encryptionContext.encrypt(ciphertext, packet.sequenceNumber());
        DataPacket encrypted = new DataPacket(packet.sequenceNumber(), packet.pp(), packet.inOrder(), key.code(),
                packet.retransmitted(), packet.messageNumber(), packet.timestamp(), packet.destination(),
                ciphertext);

        // Advance the rotation schedule by this packet, announcing the next key
        // when the schedule says to. Deliberately after building the packet, so
        // the KM update goes out behind the DATA it relates to rather than ahead
        // of it, and so a switch never applies to the packet being built.
        encryptionContext.onPacketEncrypted().ifPresent(next -> {
            sendKeyMaterial(next, ExtensionType.KMREQ.code());
            events.fire(listener -> listener.onKeyRotated(this));
        });
        return encrypted;
    }

    /**
     * Sends a mid-stream Key Material update. Unlike the handshake's KMREQ/KMRSP
     * extension, this rides in a USER_DEFINED control packet whose subtype names
     * the SRT message (gosrt's {@code sendKMRequest}: {@code CTRLTYPE_USER} plus
     * {@code EXTTYPE_KMREQ}).
     */
    private void sendKeyMaterial(KeyEncryption key, int subtype) {
        ByteBuf cif = channel.alloc().buffer();
        encryptionContext.keyMaterial(key).encodeTo(cif);
        send(new ControlPacket(ControlType.USER_DEFINED, 0, (int) elapsedMicros(),
                metadata.peerSocketId(), cif, subtype));
    }

    /**
     * A peer's mid-stream key rotation. A KMREQ announces the key it is about to
     * start using, which we adopt and echo back as KMRSP so it knows we are
     * ready; a KMRSP confirms an announcement of our own, letting us stop
     * repeating it.
     */
    private void handleKeyMaterialUpdate(ControlPacket control) {
        if (encryptionContext == null) {
            LOG.log(Level.FINE, "Dropping key material for unencrypted connection {0}", metadata.socketId());
            control.body().release();
            return;
        }

        KeyMaterialCif keyMaterial = KeyMaterialCif.decode(control.body());
        control.body().release();
        if (keyMaterial == null) {
            LOG.log(Level.FINE, "Dropping malformed key material on socket {0}", metadata.socketId());
            return;
        }

        if (control.subtype() == ExtensionType.KMRSP.code()) {
            encryptionContext.confirmKeyMaterial();
            return;
        }
        if (control.subtype() != ExtensionType.KMREQ.code()) {
            LOG.log(Level.FINE, "Ignoring user-defined control subtype {0}", control.subtype());
            return;
        }

        if (!encryptionContext.adopt(keyMaterial)) {
            // Wrong passphrase mid-stream, or key material we can't use. Nothing
            // to renegotiate at this point, so tell the peer rather than
            // silently dropping everything it sends from here on.
            ByteBuf cif = channel.alloc().buffer();
            KeyMaterialCif.error(KeyMaterialCif.ERROR_BAD_SECRET).encodeTo(cif);
            send(new ControlPacket(ControlType.USER_DEFINED, 0, (int) elapsedMicros(),
                    metadata.peerSocketId(), cif, ExtensionType.KMRSP.code()));
            return;
        }
        sendKeyMaterial(keyMaterial.keyEncryption(), ExtensionType.KMRSP.code());
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
        int acknowledged = (int) ackCif.lastAckPacketSequenceNumber().value();
        events.fire(listener -> listener.onAckSent(this, acknowledged));

        ByteBuf cifBuf = channel.alloc().buffer();
        ackCif.encodeTo(cifBuf);
        send(new ControlPacket(ControlType.ACK, ackNumber, (int) elapsedMicros(), metadata.peerSocketId(), cifBuf));
    }

    private void send(SrtPacket packet) {
        lastSendMicros = elapsedMicros();
        channel.writeAndFlush(new DefaultAddressedEnvelope<>(packet, metadata.peerAddress()));
    }

    private long elapsedMicros() {
        return (System.nanoTime() - startNanos) / 1000;
    }
}
