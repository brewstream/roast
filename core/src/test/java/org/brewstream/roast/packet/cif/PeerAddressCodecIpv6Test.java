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
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.InetAddress;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Address round-tripping, with the IPv6 cases an external review pointed out.
 *
 * <p>The wire field is sixteen bytes with no length or family marker, so the
 * decoder has to infer which it is. Inferring from "the first twelve bytes are
 * zero" matches every address in {@code ::/96} — including {@code ::1}, IPv6
 * loopback, which decoded as {@code 0.0.0.1}. Nothing caught it because every
 * test and every interop run uses {@code 127.0.0.1}.
 */
class PeerAddressCodecIpv6Test {

    private static InetAddress roundTrip(String literal) throws Exception {
        InetAddress original = InetAddress.getByName(literal);
        ByteBuf buffer = Unpooled.buffer();
        PeerAddressCodec.encode(original, buffer);
        return PeerAddressCodec.decode(buffer);
    }

    @ParameterizedTest
    @ValueSource(strings = {"127.0.0.1", "192.168.1.50", "8.8.8.8", "255.255.255.255"})
    void ipv4AddressesRoundTrip(String literal) throws Exception {
        assertThat(roundTrip(literal)).isEqualTo(InetAddress.getByName(literal));
    }

    /** The cases that were broken: everything in ::/96 looked like IPv4. */
    @ParameterizedTest
    @ValueSource(strings = {"::1", "::2", "2001:db8::1", "fe80::1", "::ffff:1:2"})
    void ipv6AddressesRoundTrip(String literal) throws Exception {
        assertThat(roundTrip(literal)).isEqualTo(InetAddress.getByName(literal));
    }

    /**
     * All-zero bytes are both {@code 0.0.0.0} and {@code ::}, and the field
     * carries no family marker, so one of them has to lose. IPv4 wins here
     * because {@code 0.0.0.0} is what libsrt writes when it does not know the
     * peer's address — a value that actually occurs on the wire.
     */
    @Test
    void allZeroBytesDecodeAsTheIpv4Placeholder() throws Exception {
        assertThat(roundTrip("0.0.0.0")).isEqualTo(InetAddress.getByName("0.0.0.0"));
    }

    @Test
    void ipv6LoopbackIsNotDecodedAsAnIpv4Address() throws Exception {
        InetAddress decoded = roundTrip("::1");

        assertThat(decoded.getAddress()).as("sixteen bytes, not four").hasSize(16);
        assertThat(decoded.getHostAddress()).doesNotContain("0.0.0.1");
    }
}