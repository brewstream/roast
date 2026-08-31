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
 * An SRT packet's fixed 16-byte common header, decoded into one of the two
 * packet families the header's F-bit distinguishes. Variable-length bodies
 * (payload / control information field) are carried as a raw {@link ByteBuf} —
 * per-control-type CIF field parsing is not this layer's job.
 */
public sealed interface SrtPacket permits DataPacket, ControlPacket {

    int HEADER_LENGTH = 16;

    /** Packet sequence numbers wrap at 31 bits — see {@link org.brewstream.roast.util.CircularNumber}. */
    long MAX_SEQUENCE_NUMBER = 0x7FFF_FFFFL;

    /** Timestamps (and ACK numbers, which share the same wire width) wrap at 32 bits. */
    long MAX_TIMESTAMP = 0xFFFF_FFFFL;

    SrtSocketId destination();

    int timestamp();

    /** The variable-length payload (data packets) or CIF (control packets). */
    ByteBuf body();

    /**
     * Encodes this packet's header and body into {@code out} at its current writer
     * index. Consumes (releases) {@link #body()} in the process — callers that need
     * to send the same packet again (e.g. ARQ retransmission) must
     * {@code body().retainedDuplicate()} into a fresh {@link SrtPacket} first.
     */
    void encodeTo(ByteBuf out);

    /**
     * Decodes one SRT packet's common header from {@code in}, consuming the header
     * and the remainder of the buffer as the body. Returns {@code null} if {@code in}
     * is shorter than {@link #HEADER_LENGTH} or names an unrecognized control type —
     * callers should drop such datagrams rather than treat them as a protocol error,
     * since arbitrary UDP noise can land on the port.
     */
    static SrtPacket decode(ByteBuf in) {
        if (in.readableBytes() < HEADER_LENGTH) {
            return null;
        }
        int word0 = in.readInt();
        boolean isControl = (word0 & 0x8000_0000) != 0;
        int word1 = in.readInt();
        int timestamp = in.readInt();
        SrtSocketId destination = SrtSocketId.of(in.readInt());
        ByteBuf body = in.readRetainedSlice(in.readableBytes());

        if (isControl) {
            ControlType type = ControlType.fromCode((word0 >>> 16) & 0x7FFF);
            if (type == null) {
                body.release();
                return null;
            }
            // Bits 15-0 are the Subtype, meaningful only for USER_DEFINED (where
            // SRT carries its own message type, e.g. a mid-stream KM update).
            return new ControlPacket(type, word1, timestamp, destination, body, word0 & 0xFFFF);
        }

        int sequenceNumber = word0 & 0x7FFF_FFFF;
        int pp = (word1 >>> 30) & 0x3;
        boolean inOrder = ((word1 >>> 29) & 0x1) != 0;
        int kk = (word1 >>> 27) & 0x3;
        boolean retransmitted = ((word1 >>> 26) & 0x1) != 0;
        int messageNumber = word1 & 0x03FF_FFFF;
        return new DataPacket(sequenceNumber, pp, inOrder, kk, retransmitted, messageNumber,
                timestamp, destination, body);
    }
}