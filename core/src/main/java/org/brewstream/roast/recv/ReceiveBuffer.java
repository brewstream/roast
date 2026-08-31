/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
 * Combines buffering and TSBPD delivery timing in one type —
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
 * <p><b>Implements TLPKTDROP</b>, and does so <em>decoupled from delivery</em> —
 * traced precisely from gosrt's {@code congestion/live/receive.go} this pass
 * (see the "ACK boundary" paragraph below), not the single-boundary design
 * this class started with. If a buffered packet's own deadline has passed
 * while an earlier sequence number is still missing, that gap is abandoned
 * rather than blocking delivery forever — reported via {@link
 * AckBoundaryResult#abandoned()} so a caller can clear it from a paired
 * {@link LossList} (stop NAKing it).
 *
 * <p><b>ACK boundary vs. delivery boundary</b>: this class tracks two separate
 * boundaries, matching gosrt's {@code lastACKSequenceNumber}/{@code
 * lastDeliveredSequenceNumber} split exactly — confirmed via gosrt's own
 * {@code TestRecvDropTooLate}, which asserts them as genuinely distinct
 * fields, and {@code TestIssue67}, a real historical gosrt bug fix for
 * <em>this exact failure mode</em> (a real interop session against libsrt hit
 * the same category of bug independently, before this was ported).
 * {@link #computeAckBoundary} walks {@link #buffered} from {@link
 * #lastAcked} forward: a packet whose own deadline has already passed
 * advances the boundary past it <em>even across a gap</em> — the actual
 * TLPKTDROP "give up" decision happens here, not in {@link #deliver} — while
 * a genuinely next-in-sequence packet advances it normally; anything else
 * stops the walk. This means the ACK boundary can jump past several stale
 * entries in a single call, not just the front one. {@link #deliver} is then
 * a purely mechanical hand-out, gated by <em>both</em> that just-computed
 * boundary and each entry's own deadline — it can never get ahead of what's
 * been acknowledged, but the ACK computation itself isn't gated by delivery's
 * own pace. Callers must call {@link #computeAckBoundary} before {@link
 * #deliver} each tick, passing its result in — matching the order gosrt's own
 * {@code Tick()} calls {@code periodicACK()} then its delivery loop.
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
 * for this piece — checked directly, and gosrt's own drift support is dead
 * code — so the tests here are self-designed against libsrt's source rather
 * than ported from a reference scenario.
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
 * reference has a dedicated test for this piece either (checked directly), so
 * its tests are self-designed against both sources rather than ported.
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
    private CircularNumber lastAcked;
    private CircularNumber lastDelivered;
    private long firstRttSampleMicros = -1;
    private boolean tsbpdWrapPeriod;

    public ReceiveBuffer(CircularNumber initialSequenceNumber, long latencyMicros) {
        this.latencyMicros = latencyMicros;
        this.lastAcked = initialSequenceNumber.dec();
        this.lastDelivered = initialSequenceNumber.dec();
    }

    /**
     * Buffers a DATA packet for later delivery, or drops it (releasing its
     * payload) if it's belated (at or before {@link #lastDelivered}) or
     * already acknowledged/abandoned (before {@link #lastAcked} — ported as a
     * separate check from gosrt's {@code Push}, since that zone can now be
     * ahead of {@link #lastDelivered} — see the class javadoc's "ACK boundary
     * vs. delivery boundary" section). {@code nowMicros} establishes this
     * buffer's time base on the very first call — see the class-level
     * wraparound caveat.
     */
    public void add(DataPacket packet, long nowMicros) {
        if (timeBaseMicros == null) {
            timeBaseMicros = nowMicros - Integer.toUnsignedLong(packet.timestamp());
        }
        updateWrapPeriod(packet.timestamp());

        CircularNumber seq = CircularNumber.of(packet.sequenceNumber() & 0x7FFF_FFFF, SrtPacket.MAX_SEQUENCE_NUMBER);
        if (seq.lessThanOrEqual(lastDelivered) || seq.lessThan(lastAcked)) {
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
     * Ported from gosrt's {@code periodicACK} walk (see the class javadoc's
     * "ACK boundary vs. delivery boundary" section) — the actual TLPKTDROP
     * "give up on this gap" decision happens here, not in {@link #deliver}.
     * Walks {@link #buffered} from {@link #lastAcked} forward: a packet whose
     * own deadline has already passed advances the boundary to it regardless
     * of any gap before it (banking the skipped range as abandoned); a
     * genuinely next-in-sequence packet advances it normally; anything else
     * stops the walk. Call once per tick, before {@link #deliver}, and pass
     * its result's {@link AckBoundaryResult#lastAckSequenceNumber()} straight
     * into that call.
     */
    public AckBoundaryResult computeAckBoundary(long nowMicros) {
        List<LossRange> abandoned = new ArrayList<>();

        for (Entry entry : buffered) {
            if (entry.seq().lessThanOrEqual(lastAcked)) {
                continue; // already acked - shouldn't normally happen, mirrors gosrt's own guard
            }

            if (tsbpdTimeMicros(entry) <= nowMicros) {
                CircularNumber expected = lastAcked.inc();
                if (!entry.seq().equals(expected)) {
                    abandoned.add(new LossRange(expected, entry.seq().dec()));
                }
                lastAcked = entry.seq();
                continue;
            }

            if (entry.seq().equals(lastAcked.inc())) {
                lastAcked = entry.seq();
                continue;
            }

            break;
        }

        return new AckBoundaryResult(lastAcked, abandoned);
    }

    /**
     * Hands out everything ready as of {@code nowMicros}: buffered packets
     * that are at or before {@code ackBoundary} (see {@link
     * #computeAckBoundary}, which must be called first each tick) and whose
     * own delivery deadline has arrived. Purely mechanical — delivery can
     * never get ahead of what's been acknowledged, but doesn't independently
     * decide to give up on anything itself anymore. The caller owns each
     * delivered packet's payload from here on.
     */
    public DeliveryResult deliver(CircularNumber ackBoundary, long nowMicros) {
        List<DataPacket> delivered = new ArrayList<>();

        while (!buffered.isEmpty()) {
            Entry next = buffered.get(0);
            if (next.seq().greaterThan(ackBoundary) || tsbpdTimeMicros(next) > nowMicros) {
                break;
            }

            delivered.add(next.packet());
            buffered.remove(0);
            lastDelivered = next.seq();
        }

        return new DeliveryResult(delivered);
    }

    /**
     * Where the acknowledgement boundary currently sits — the equivalent of the
     * receive-window start libsrt judges an incoming DROPREQ's distance
     * against ({@code getStartSeqNo()}).
     */
    public CircularNumber acknowledgedBoundary() {
        return lastAcked;
    }

    /**
     * Gives up on everything at or before {@code end} immediately, rather than
     * waiting for its delivery deadline to pass — what a peer's DROPREQ asks
     * for when its sender has discarded those packets and they are never
     * coming.
     *
     * <p>Only the acknowledgement boundary moves. Anything in the range that
     * <em>was</em> received is still buffered and still delivered by
     * {@link #deliver}; the point is to stop waiting for what is missing, not
     * to discard what arrived.
     *
     * <p>Returns {@code false} if the boundary was already at or past
     * {@code end}, so a caller can tell a redundant DROPREQ from one that
     * actually advanced anything.
     */
    public boolean abandonUpTo(CircularNumber end) {
        if (end.lessThanOrEqual(lastAcked)) {
            return false;
        }
        lastAcked = end;
        return true;
    }

    /**
     * How many packets are currently held awaiting delivery — i.e. how much of
     * the receive window is in use. Feeds the "available buffer size" figure a
     * Full ACK reports to the peer (see {@code SrtConnection.tick}); a peer's
     * sender treats that figure as its flow-control window, so it has to
     * reflect reality.
     */
    public int bufferedCount() {
        return buffered.size();
    }

    /**
     * Hands out every buffered packet in sequence order, ignoring delivery
     * deadlines, and empties the buffer — for connection teardown, where TSBPD
     * pacing has nothing left to pace. The caller owns the payloads.
     *
     * <p>Without this the end of every stream is silently truncated: a peer that
     * finishes sending and shuts down leaves up to a full latency window of
     * packets sitting here, already received and merely waiting on their
     * deadlines. Both references discard them ({@code gosrt}'s {@code close}
     * flushes, libsrt defaults live-mode linger to zero); holding data we
     * already have and then dropping it is a worse trade than a short burst at
     * end of stream.
     */
    public List<DataPacket> drainAll() {
        List<DataPacket> remaining = new ArrayList<>(buffered.size());
        for (Entry entry : buffered) {
            remaining.add(entry.packet());
            lastDelivered = entry.seq();
        }
        buffered.clear();
        return remaining;
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