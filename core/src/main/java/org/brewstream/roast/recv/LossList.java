package org.brewstream.roast.recv;

import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.util.CircularNumber;

import java.util.ArrayList;
import java.util.List;

/**
 * Tracks which received-stream sequence numbers are known missing, deciding what
 * to report on an immediate (gap-just-opened) NAK versus a periodic
 * re-announcement of what's still outstanding. Ranges are stored compactly, not
 * one entry per missing sequence number, and are always appended at the high end
 * — a new gap can only open beyond the highest sequence number seen so far.
 *
 * <p>Mirrors the loss-detection core of gosrt's {@code congestion/live} receiver
 * (congestion/live/receive.go's {@code Push}), without the ACK generation,
 * TSBPD-delivery buffering, or statistics bundled with it there — those are
 * separate, not-yet-built pieces (an {@code AckSender}, a {@code ReceiveBuffer} /
 * {@code TsbpdDeliverer}).
 *
 * <p>Not thread-safe — callers own synchronization, same as everything else in
 * this module so far.
 */
public final class LossList {

    private final List<LossRange> missing = new ArrayList<>();
    private CircularNumber maxSeen;

    /** {@code initialSequenceNumber} is the first sequence number the peer will send. */
    public LossList(CircularNumber initialSequenceNumber) {
        this.maxSeen = initialSequenceNumber.dec();
    }

    /**
     * Records that {@code seq} has arrived. Returns the single newly-opened gap to
     * NAK immediately as a one-element list, or an empty list if this arrival
     * didn't open one — in order, a duplicate, or an out-of-order arrival
     * recovering all or part of an already-known gap.
     */
    public List<LossRange> onPacketReceived(CircularNumber seq) {
        if (seq.lessThanOrEqual(maxSeen)) {
            remove(seq);
            return List.of();
        }

        CircularNumber expected = maxSeen.inc();
        maxSeen = seq;

        if (seq.equals(expected)) {
            return List.of();
        }

        LossRange gap = new LossRange(expected, seq.dec());
        missing.add(gap);
        return List.of(gap);
    }

    /** Still-missing ranges, oldest first, for periodic NAK re-announcement. */
    public List<LossRange> outstanding() {
        return List.copyOf(missing);
    }

    private void remove(CircularNumber seq) {
        for (int i = 0; i < missing.size(); i++) {
            LossRange range = missing.get(i);
            if (seq.lessThan(range.start()) || range.end().lessThan(seq)) {
                continue;
            }

            missing.remove(i);
            int insertAt = i;
            if (!range.start().equals(seq)) {
                missing.add(insertAt++, new LossRange(range.start(), seq.dec()));
            }
            if (!range.end().equals(seq)) {
                missing.add(insertAt, new LossRange(seq.inc(), range.end()));
            }
            return;
        }
    }
}
