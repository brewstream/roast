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