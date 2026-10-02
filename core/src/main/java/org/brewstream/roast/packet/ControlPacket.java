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
 * F-bit = 1. {@code typeSpecificInfo} and {@code cif} (the control information
 * field) both have per-control-type meaning that Phase 2/3 handshake and ARQ
 * code will interpret; this layer only frames them.
 */
public record ControlPacket(
        ControlType type,
        int typeSpecificInfo,
        int timestamp,
        SrtSocketId destination,
        ByteBuf cif,
        int subtype) implements SrtPacket {

    /**
     * Without a subtype — every control type except {@link ControlType#USER_DEFINED}
     * leaves those 16 bits zero. Last component rather than beside {@code type}
     * (its wire position) so this shortened form can exist without touching
     * every construction site; same trade already made in {@code HandshakeCif}.
     */
    public ControlPacket(ControlType type, int typeSpecificInfo, int timestamp,
            SrtSocketId destination, ByteBuf cif) {
        this(type, typeSpecificInfo, timestamp, destination, cif, 0);
    }

    @Override
    public ByteBuf body() {
        return cif;
    }

    /**
     * Writes the header and CIF, padding an empty CIF to 4 zero bytes.
     *
     * <p>The spec says Keep-Alive, Shutdown and ACKACK "do not contain Control
     * Information Field (CIF)" (draft-sharabayko-srt.md §3.2.3, §3.2.7, §3.2.8), but
     * libsrt sends 4 zero bytes for each of them ({@code CPacket::pack}: "control
     * info field should be none but writev does not allow this") and its
     * {@code processCtrl} discards any control packet whose CIF is empty or not a
     * multiple of 4 bytes. Sending them bare made every libsrt peer drop our
     * ACKACKs (no RTT samples on its side) and our SHUTDOWN (it noticed the close
     * only at its idle timeout). Done here rather than at each construction site
     * so a control type added later cannot reintroduce it. Roast's own receive
     * side never reads these CIFs, so the padding is ignored on the way in.
     */
    @Override
    public void encodeTo(ByteBuf out) {
        int word0 = 0x8000_0000 | ((type.code() & 0x7FFF) << 16) | (subtype & 0xFFFF);
        out.writeInt(word0);
        out.writeInt(typeSpecificInfo);
        out.writeInt(timestamp);
        out.writeInt(destination.value());
        if (cif.isReadable()) {
            out.writeBytes(cif);
        } else {
            out.writeInt(0);
        }
        cif.release();
    }
}