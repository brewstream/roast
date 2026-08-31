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

        boolean isIPv4 = true;
        for (int i = 0; i < 12; i++) {
            if (standard[i] != 0) {
                isIPv4 = false;
                break;
            }
        }

        try {
            return isIPv4
                    ? InetAddress.getByAddress(Arrays.copyOfRange(standard, 12, 16))
                    : InetAddress.getByAddress(standard);
        } catch (UnknownHostException e) {
            throw new AssertionError("address array length is always valid here", e);
        }
    }
}