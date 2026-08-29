package org.brewstream.roast.recv;

import org.brewstream.roast.packet.DataPacket;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.util.CircularNumber;

import java.util.ArrayList;
import java.util.List;

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
 * <p>Deliberately simplified for this pass, both documented as known gaps (see
 * STATUS.md):
 * <ul>
 *   <li>No drift correction — clock skew between sender and receiver isn't
 *       compensated for (DESIGN.md's separate {@code DriftCorrector}).</li>
 *   <li>No 32-bit wire-timestamp wraparound handling (draft-sharabayko-srt.md
 *       §4.5.1.1's "TSBPD Time Base Calculation") — correct for connections
 *       under ~71 minutes (2^32 microseconds), the common case for now.</li>
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

    private record Entry(CircularNumber seq, long tsbpdTimeMicros, DataPacket packet) {
    }

    private final long latencyMicros;
    private final List<Entry> buffered = new ArrayList<>();

    private Long timeBaseMicros;
    private CircularNumber lastDelivered;

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

        CircularNumber seq = CircularNumber.of(packet.sequenceNumber() & 0x7FFF_FFFF, SrtPacket.MAX_SEQUENCE_NUMBER);
        if (seq.lessThanOrEqual(lastDelivered)) {
            packet.payload().release();
            return;
        }

        long tsbpdTime = timeBaseMicros + Integer.toUnsignedLong(packet.timestamp()) + latencyMicros;
        insertSorted(seq, tsbpdTime, packet);
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
            if (next.tsbpdTimeMicros() > nowMicros) {
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

    private void insertSorted(CircularNumber seq, long tsbpdTime, DataPacket packet) {
        int i = 0;
        while (i < buffered.size() && buffered.get(i).seq().lessThan(seq)) {
            i++;
        }
        if (i < buffered.size() && buffered.get(i).seq().equals(seq)) {
            packet.payload().release(); // already buffered
            return;
        }
        buffered.add(i, new Entry(seq, tsbpdTime, packet));
    }
}
