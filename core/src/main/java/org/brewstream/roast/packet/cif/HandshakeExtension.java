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

/**
 * The HSREQ/HSRSP extension payload (draft-sharabayko-srt.md "Handshake Extension
 * Message"). Always 12 bytes on the wire: SRT version, capability flags, then the
 * receive/send TSBPD latency in milliseconds.
 */
public record HandshakeExtension(
        int srtVersion,
        HandshakeExtensionFlags flags,
        int receiveTsbpdDelayMillis,
        int sendTsbpdDelayMillis) {

    static final int LENGTH = 12;

    static HandshakeExtension decode(ByteBuf in) {
        int srtVersion = in.readInt();
        HandshakeExtensionFlags flags = HandshakeExtensionFlags.decode(in.readInt());
        int receiveTsbpdDelayMillis = in.readUnsignedShort();
        int sendTsbpdDelayMillis = in.readUnsignedShort();
        return new HandshakeExtension(srtVersion, flags, receiveTsbpdDelayMillis, sendTsbpdDelayMillis);
    }

    void encodeTo(ByteBuf out) {
        out.writeInt(srtVersion);
        out.writeInt(flags.encode());
        out.writeShort(receiveTsbpdDelayMillis);
        out.writeShort(sendTsbpdDelayMillis);
    }
}