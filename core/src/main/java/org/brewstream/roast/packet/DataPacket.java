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

package org.brewstream.roast.packet;

import io.netty.buffer.ByteBuf;

/**
 * F-bit = 0. {@code pp} is the 2-bit packet-position field (10 = first, 01 =
 * last, 11 = single, 00 = middle); {@code kk} is the 2-bit encryption key flag.
 */
public record DataPacket(
        int sequenceNumber,
        int pp,
        boolean inOrder,
        int kk,
        boolean retransmitted,
        int messageNumber,
        int timestamp,
        SrtSocketId destination,
        ByteBuf payload) implements SrtPacket {

    @Override
    public ByteBuf body() {
        return payload;
    }

    @Override
    public void encodeTo(ByteBuf out) {
        int word0 = sequenceNumber & 0x7FFF_FFFF;
        int word1 = ((pp & 0x3) << 30)
                | ((inOrder ? 1 : 0) << 29)
                | ((kk & 0x3) << 27)
                | ((retransmitted ? 1 : 0) << 26)
                | (messageNumber & 0x03FF_FFFF);
        out.writeInt(word0);
        out.writeInt(word1);
        out.writeInt(timestamp);
        out.writeInt(destination.value());
        out.writeBytes(payload);
        payload.release();
    }
}