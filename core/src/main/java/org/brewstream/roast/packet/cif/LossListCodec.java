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

import io.netty.buffer.ByteBuf;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.util.CircularNumber;

import java.util.ArrayList;
import java.util.List;

/**
 * Encodes/decodes a NAK packet's CIF (loss list): a run of 32-bit words where bit 31
 * flags whether the word starts a range. A single lost packet is one word with bit 31
 * clear; a range is two words — the first with bit 31 set holding the (inclusive)
 * range start, the second with bit 31 clear holding the range end. See
 * draft-sharabayko-srt.md's "Packet Sequence List Coding" section.
 */
public final class LossListCodec {

    private LossListCodec() {
    }

    public static void encode(List<LossRange> ranges, ByteBuf out) {
        for (LossRange range : ranges) {
            if (range.isSingle()) {
                out.writeInt((int) range.start().value());
            } else {
                out.writeInt((int) (range.start().value() | 0x8000_0000L));
                out.writeInt((int) range.end().value());
            }
        }
    }

    /**
     * Decodes a NAK CIF. Returns {@code null} — rather than throwing — for anything
     * malformed: a length that isn't a multiple of 4 bytes, a range-start word with no
     * following range-end word, or a range whose end precedes its start. A corrupt or
     * adversarial NAK should be dropped like any other bad datagram, not crash the
     * pipeline.
     */
    public static List<LossRange> decode(ByteBuf in) {
        if (in.readableBytes() % 4 != 0) {
            return null;
        }

        List<LossRange> ranges = new ArrayList<>();
        while (in.isReadable()) {
            int word = in.readInt();
            CircularNumber value = sequenceNumberOf(word);

            if ((word & 0x8000_0000) == 0) {
                ranges.add(LossRange.single(value));
                continue;
            }
            if (!in.isReadable()) {
                return null;
            }
            CircularNumber end = sequenceNumberOf(in.readInt());
            if (end.lessThan(value)) {
                return null;
            }
            ranges.add(new LossRange(value, end));
        }
        return ranges;
    }

    private static CircularNumber sequenceNumberOf(int word) {
        return CircularNumber.of(word & 0x7FFF_FFFF, SrtPacket.MAX_SEQUENCE_NUMBER);
    }
}