package org.brewstream.roast.recv;

import org.brewstream.roast.packet.DataPacket;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.util.CircularNumber;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;

/**
 * Holds accepted DATA packets until their TSBPD delivery deadline arrives (per
 * packet: connection-relative wire timestamp + this receiver's negotiated
 * latency), keeping the ones not yet deliverable sorted by sequence number.
 * Combines what DESIGN.md separately names ReceiveBuffer and TsbpdDeliverer —
 * gosrt keeps them as one struct too ({@code congestion/live/receive.go}'s
 * {@code receiver}, both storage and delivery timing in the same type).
 *
 * <p>Sequence ordering is maintained via {@link CircularNumber} comparisons over
 * a plain list, not a numerically-sorted tree — same reasoning as {@link
 * LossList}: buffered packets are always within a bounded recent window (bounded
 * by latency × bitrate), so a linear scan stays cheap and, more importantly,
 * never risks misordering across the sequence-number wrap the way raw numeric
 * comparison would. gosrt's own receiver uses a plain linked list for the same
 * structure, for the same reason.
 *
 * <p><b>Implements TLPKTDROP</b>: if a buffered packet's own deadline has passed
 * while an earlier sequence number is still missing, that gap is abandoned
 * rather than blocking delivery forever — reported via
 * {@link DeliveryResult#abandoned()} so a caller can clear it from a paired
 * {@link LossList} (stop NAKing it).
 *
 * <p><b>Drift correction</b>: every delivery deadline is computed live (at
 * {@link #deliver}-check time), not frozen when a packet is buffered — {@code
 * timeBaseMicros + carryoverMicros(timestamp) + packet timestamp + latency +
 * driftTracer.drift()}, see {@link #tsbpdTimeMicros} — matching libsrt's
 * live-query {@code getPktTime()} model
 * ({@code srtcore/tsbpd_time.cpp}) rather than gosrt's, which has no working
 * drift implementation at all (a dead field never assigned). This means an
 * already-buffered-but-undelivered packet's deadline re-evaluates if drift
 * shifts while it's waiting, not just newly-arriving packets'. Samples come
 * from {@link #addDriftSample}, fed by the connection on every ACKACK — see
 * that method's javadoc for the exact formula, ported from libsrt's {@code
 * CTsbpdTime::addDriftSample}. No dedicated test exists in either reference
 * for this piece (STATUS.md's testing methodology section has the detail).
 *
 * <p><b>32-bit wire-timestamp wraparound</b> (draft-sharabayko-srt.md §4.5.1.1's
 * "TSBPD Time Base Calculation"): SRT's microsecond timestamps wrap every ~71
 * minutes (2^32 us). {@link #updateWrapPeriod} runs on every arriving DATA
 * packet, faithfully ported from gosrt's {@code handlePacket} state machine
 * (structurally identical to libsrt's {@code CTsbpdTime::updateBaseTime}):
 * entering a "wrap period" once a packet's timestamp comes within 30s of
 * wrapping, and permanently folding a full cycle into {@link #timeBaseMicros}
 * once a later packet's timestamp confirms the wrap actually happened (lands
 * in the 30-60s window past zero — long enough that it can't be a stray
 * late-arriving pre-wrap packet). While the wrap is suspected but not yet
 * confirmed, {@link #carryoverMicros} applies the same cycle provisionally to
 * just that query, matching libsrt's {@code getBaseTimeNoLock} rather than
 * gosrt's separate-field approach — a natural fit since delivery time here is
 * already a live, recomputed-at-{@link #deliver}-time function of stored
 * state (see "Drift correction" above), not frozen at insertion. Neither
 * reference has a dedicated test for this piece either (checked directly);
 * self-designed against both sources — see STATUS.md's testing methodology.
 *
 * <p>Deliberately simplified for this pass, documented as a known gap (see
 * STATUS.md):
 * <ul>
 *   <li>Unlike gosrt, this doesn't let an acknowledgment boundary run ahead of
 *       the delivery boundary for still-buffered in-order packets that simply
 *       haven't reached their deadline yet — gosrt's ACK reporting and TSBPD
 *       delivery are two separate boundary computations over the same buffer;
 *       here they're the same boundary, computed once, in {@link #deliver}.
 *       This is a deliberate simplification, not an oversight — see
 *       {@code ReceiveBufferTest}'s ported {@code TestSkipTooLate} case for
 *       exactly where the two diverge.</li>
 * </ul>
 *
 * <p>Not thread-safe, same as {@link LossList}/{@link AckSender}.
 */
public final class ReceiveBuffer {

    private static final long WRAP_PERIOD_MICROS = 30_000_000L;

    private record Entry(CircularNumber seq, int timestamp, DataPacket packet) {
    }

    private final long latencyMicros;
    private final List<Entry> buffered = new ArrayList<>();
    private final DriftTracer driftTracer = new DriftTracer();

    private Long timeBaseMicros;
    private CircularNumber lastDelivered;
    private long firstRttSampleMicros = -1;
    private boolean tsbpdWrapPeriod;

    public ReceiveBuffer(CircularNumber initialSequenceNumber, long latencyMicros) {
        this.latencyMicros = latencyMicros;
        this.lastDelivered = initialSequenceNumber.dec();
    }

    /**
     * Buffers a DATA packet for later delivery, or drops it (releasing its
     * payload) if it's a duplicate or arrived after its sequence number was
     * already delivered/abandoned. {@code nowMicros} establishes this buffer's
     * time base on the very first call — see the class-level wraparound caveat.
     */
    public void add(DataPacket packet, long nowMicros) {
        if (timeBaseMicros == null) {
            timeBaseMicros = nowMicros - Integer.toUnsignedLong(packet.timestamp());
        }
        updateWrapPeriod(packet.timestamp());

        CircularNumber seq = CircularNumber.of(packet.sequenceNumber() & 0x7FFF_FFFF, SrtPacket.MAX_SEQUENCE_NUMBER);
        if (seq.lessThanOrEqual(lastDelivered)) {
            packet.payload().release();
            return;
        }

        insertSorted(seq, packet.timestamp(), packet);
    }

    /**
     * Ported from gosrt's {@code handlePacket} wrap-period state machine (see
     * the class javadoc's "32-bit wire-timestamp wraparound" section). Runs on
     * every arriving DATA packet, ahead of duplicate/belated handling, matching
     * gosrt's own placement.
     */
    private void updateWrapPeriod(int timestamp) {
        long ts = Integer.toUnsignedLong(timestamp);
        if (!tsbpdWrapPeriod) {
            if (ts > SrtPacket.MAX_TIMESTAMP - WRAP_PERIOD_MICROS) {
                tsbpdWrapPeriod = true;
            }
        } else if (ts >= WRAP_PERIOD_MICROS && ts <= 2 * WRAP_PERIOD_MICROS) {
            tsbpdWrapPeriod = false;
            timeBaseMicros += SrtPacket.MAX_TIMESTAMP + 1;
        }
    }

    /**
     * The not-yet-permanently-committed portion of a wraparound cycle — see
     * the class javadoc. Zero once {@link #updateWrapPeriod} has confirmed and
     * folded a wrap into {@link #timeBaseMicros}, or if no wrap is suspected.
     */
    private long carryoverMicros(int timestamp) {
        long ts = Integer.toUnsignedLong(timestamp);
        return (tsbpdWrapPeriod && ts <= 2 * WRAP_PERIOD_MICROS) ? SrtPacket.MAX_TIMESTAMP + 1 : 0;
    }

    /**
     * Records a clock-drift sample from an ACK/ACKACK round trip — ported from
     * libsrt's {@code CTsbpdTime::addDriftSample} ({@code srtcore/tsbpd_time.cpp}).
     * {@code packetTimestamp} is the ACKACK packet's own header timestamp field
     * (the peer's connection-relative clock at the moment it sent the ACKACK);
     * {@code arrivalMicros} is when it arrived locally; {@code rttSampleMicros}
     * is the raw (unsmoothed) RTT sample for that same exchange.
     *
     * <p>The ACKACK's timestamp is run through the same "packet timestamp -&gt;
     * base time" formula data packets use, then compared against when it
     * actually arrived — the difference, minus half the change in RTT since the
     * first-ever RTT sample (compensating for one-way-delay changes), is the
     * drift sample fed to {@link #driftTracer}. A no-op before {@link #add} has
     * established {@link #timeBaseMicros} (mirrors libsrt's own {@code if
     * (!m_bTsbPdMode) return}).
     */
    public void addDriftSample(int packetTimestamp, long arrivalMicros, long rttSampleMicros) {
        if (timeBaseMicros == null) {
            return;
        }
        if (firstRttSampleMicros == -1) {
            firstRttSampleMicros = rttSampleMicros;
        }

        long rttDeltaMicros = (rttSampleMicros - firstRttSampleMicros) / 2;
        long packetBaseTimeMicros = timeBaseMicros + carryoverMicros(packetTimestamp) + Integer.toUnsignedLong(packetTimestamp);
        long sampleMicros = arrivalMicros - packetBaseTimeMicros - rttDeltaMicros;

        OptionalLong overdriftMicros = driftTracer.update(sampleMicros);
        if (overdriftMicros.isPresent()) {
            timeBaseMicros += overdriftMicros.getAsLong();
        }
    }

    /**
     * Delivers everything ready as of {@code nowMicros}: buffered packets that
     * are next-in-sequence (or become so via TLPKTDROP abandoning what's still
     * missing before them) and whose own delivery deadline has arrived. The
     * caller owns each delivered packet's payload from here on.
     */
    public DeliveryResult deliver(long nowMicros) {
        List<DataPacket> delivered = new ArrayList<>();
        List<LossRange> abandoned = new ArrayList<>();

        while (!buffered.isEmpty()) {
            Entry next = buffered.get(0);
            if (tsbpdTimeMicros(next) > nowMicros) {
                break;
            }

            CircularNumber expected = lastDelivered.inc();
            if (!next.seq().equals(expected)) {
                abandoned.add(new LossRange(expected, next.seq().dec()));
            }

            delivered.add(next.packet());
            buffered.remove(0);
            lastDelivered = next.seq();
        }

        return new DeliveryResult(delivered, abandoned);
    }

    /** Releases every currently-buffered, undelivered packet's payload — call on connection teardown. */
    public void dispose() {
        buffered.forEach(entry -> entry.packet().body().release());
        buffered.clear();
    }

    private long tsbpdTimeMicros(Entry entry) {
        return timeBaseMicros + carryoverMicros(entry.timestamp()) + Integer.toUnsignedLong(entry.timestamp())
                + latencyMicros + driftTracer.drift();
    }

    private void insertSorted(CircularNumber seq, int timestamp, DataPacket packet) {
        int i = 0;
        while (i < buffered.size() && buffered.get(i).seq().lessThan(seq)) {
            i++;
        }
        if (i < buffered.size() && buffered.get(i).seq().equals(seq)) {
            packet.payload().release(); // already buffered
            return;
        }
        buffered.add(i, new Entry(seq, timestamp, packet));
    }
}
