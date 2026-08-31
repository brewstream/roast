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

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.brewstream.roast.packet.ControlPacket;
import org.brewstream.roast.packet.ControlType;
import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.packet.cif.LossListCodec;
import org.brewstream.roast.packet.cif.LossRange;

import java.util.List;

/** Builds a wire-ready NAK control packet from lost-packet ranges, via {@link LossListCodec}. */
public final class NakGenerator {

    private NakGenerator() {
    }

    public static ControlPacket build(List<LossRange> lostRanges, int timestamp, SrtSocketId destination) {
        ByteBuf cif = ByteBufAllocator.DEFAULT.buffer();
        LossListCodec.encode(lostRanges, cif);
        return new ControlPacket(ControlType.NAK, 0, timestamp, destination, cif);
    }
}