package org.brewstream.roast.packet.cif;

import org.brewstream.roast.util.CircularNumber;

/**
 * One entry in a NAK CIF's loss list: either a single lost packet sequence number
 * ({@code start == end}) or an inclusive range of consecutive lost sequence numbers.
 * Deciding when to coalesce individually-lost sequence numbers into a range is a
 * NAK-generation policy question, not this type's job — it just carries whatever
 * range the caller already decided on.
 */
public record LossRange(CircularNumber start, CircularNumber end) {

    public LossRange {
        if (start.max() != end.max()) {
            throw new IllegalArgumentException(
                    "start/end from different sequence-number domains (max " + start.max() + " vs " + end.max() + ")");
        }
    }

    public static LossRange single(CircularNumber sequenceNumber) {
        return new LossRange(sequenceNumber, sequenceNumber);
    }

    public boolean isSingle() {
        return start.value() == end.value();
    }
}
