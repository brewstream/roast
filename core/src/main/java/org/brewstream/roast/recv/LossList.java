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
 * (congestion/live/receive.go's {@code Push}), without the ACK generation
 * ({@link AckSender}, built on top of this), TSBPD-delivery buffering
 * ({@link ReceiveBuffer}, which reports TLPKTDROP abandonment here via
 * {@link #abandon}), or statistics bundled with it there.
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

    /** The highest sequence number seen so far, in order or not. */
    public CircularNumber highestSeen() {
        return maxSeen;
    }

    /**
     * Marks everything up to and including {@code upTo} as no longer missing,
     * even though it was never actually received — for {@link ReceiveBuffer}'s
     * TLPKTDROP to report giving up on a stale gap, so this stops NAKing it and
     * {@link AckSender}'s next tick reports the advanced boundary. Also advances
     * {@link #highestSeen()} if {@code upTo} is beyond it.
     */
    public void abandon(CircularNumber upTo) {
        List<LossRange> updated = new ArrayList<>(missing.size());
        for (LossRange range : missing) {
            if (range.end().lessThanOrEqual(upTo)) {
                continue;
            }
            if (range.start().lessThanOrEqual(upTo)) {
                updated.add(new LossRange(upTo.inc(), range.end()));
            } else {
                updated.add(range);
            }
        }
        missing.clear();
        missing.addAll(updated);

        if (maxSeen.lessThan(upTo)) {
            maxSeen = upTo;
        }
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