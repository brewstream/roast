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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

/**
 * Encodes/decodes the handshake CIF's 16-byte peer address field. Unlike every
 * other field in an SRT packet, this one is not big-endian: it's the address's
 * standard bytes (4 for IPv4, 16 for IPv6) written end-to-end reversed, zero-padded
 * to 16 bytes for IPv4. To decode, undo the reversal, then treat it as IPv4-mapped
 * (address in the last 4 bytes, first 12 zero) if those leading 12 bytes are zero.
 *
 * <p>Ported from gosrt's {@code net.IP} Marshal/Unmarshal (github.com/datarhei/gosrt,
 * MIT licensed) and cross-checked against its handshake golden vectors.
 */
public final class PeerAddressCodec {

    private static final int WIRE_LENGTH = 16;

    private PeerAddressCodec() {
    }

    public static void encode(InetAddress address, ByteBuf out) {
        byte[] raw = address.getAddress();
        byte[] wire = new byte[WIRE_LENGTH];
        for (int i = 0; i < raw.length; i++) {
            wire[i] = raw[raw.length - 1 - i];
        }
        out.writeBytes(wire);
    }

    /** Returns {@code null} if fewer than {@value #WIRE_LENGTH} bytes are readable. */
    public static InetAddress decode(ByteBuf in) {
        if (in.readableBytes() < WIRE_LENGTH) {
            return null;
        }
        byte[] wire = new byte[WIRE_LENGTH];
        in.readBytes(wire);

        byte[] standard = new byte[WIRE_LENGTH];
        for (int i = 0; i < WIRE_LENGTH; i++) {
            standard[i] = wire[WIRE_LENGTH - 1 - i];
        }

        // "First twelve bytes zero" alone is not enough: every address in
        // ::/96 satisfies it, and ::1 - IPv6 loopback - was decoded as 0.0.0.1.
        // An IPv4-mapped address has 0xFFFF in bytes 10-11, which is what
        // actually distinguishes the two; a bare IPv4 written into this field
        // leaves them zero, so accept either, and require the rest to be empty.
        boolean lowBytesZero = true;
        for (int i = 0; i < 10; i++) {
            if (standard[i] != 0) {
                lowBytesZero = false;
                break;
            }
        }
        boolean mapped = (standard[10] & 0xFF) == 0xFF && (standard[11] & 0xFF) == 0xFF;
        boolean bare = standard[10] == 0 && standard[11] == 0;
        // The field carries no family marker, so ::N and 0.0.0.N are literally
        // the same sixteen bytes and one of them has to lose. ::N wins, except
        // for all-zero: 0.0.0.0 is what libsrt puts here when it does not know
        // the peer's address, so it is a value that genuinely occurs, whereas
        // 0.0.0.1 is not a host address anyone routes to. The ambiguity is in
        // the wire format, not in this decision.
        boolean looksLikeLowIpv6 = bare && standard[12] == 0 && standard[13] == 0
                && standard[14] == 0 && standard[15] != 0;
        boolean isIPv4 = lowBytesZero && (mapped || bare) && !looksLikeLowIpv6;

        try {
            return isIPv4
                    ? InetAddress.getByAddress(Arrays.copyOfRange(standard, 12, 16))
                    : InetAddress.getByAddress(standard);
        } catch (UnknownHostException e) {
            throw new AssertionError("address array length is always valid here", e);
        }
    }
}