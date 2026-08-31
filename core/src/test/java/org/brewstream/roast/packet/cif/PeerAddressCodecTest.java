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

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

class PeerAddressCodecTest {

    // github.com/datarhei/gosrt packet.CIFHandshake TestHandshakeV4/V5: PeerIP "127.0.0.1".
    private static final String LOCALHOST_GOLDEN_HEX = "0100007f000000000000000000000000";

    @Test
    void encodeMatchesGosrtHandshakeGoldenVectorForIPv4() throws UnknownHostException {
        InetAddress address = InetAddress.getByName("127.0.0.1");

        var buf = ByteBufAllocator.DEFAULT.buffer();
        PeerAddressCodec.encode(address, buf);

        assertThat(ByteBufUtil.hexDump(buf)).isEqualTo(LOCALHOST_GOLDEN_HEX);
        buf.release();
    }

    @Test
    void decodeMatchesGosrtHandshakeGoldenVectorForIPv4() throws UnknownHostException {
        var buf = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(LOCALHOST_GOLDEN_HEX));

        InetAddress decoded = PeerAddressCodec.decode(buf);

        assertThat(decoded).isEqualTo(InetAddress.getByName("127.0.0.1"));
        buf.release();
    }

    @Test
    void roundTripsIPv4() throws UnknownHostException {
        InetAddress address = InetAddress.getByName("192.168.1.42");

        var buf = ByteBufAllocator.DEFAULT.buffer();
        PeerAddressCodec.encode(address, buf);
        InetAddress decoded = PeerAddressCodec.decode(buf);

        assertThat(decoded).isEqualTo(address);
        buf.release();
    }

    @Test
    void roundTripsIPv6WithoutBeingMisdetectedAsIPv4() throws UnknownHostException {
        InetAddress address = InetAddress.getByName("2001:db8::1234:5678");

        var buf = ByteBufAllocator.DEFAULT.buffer();
        PeerAddressCodec.encode(address, buf);
        InetAddress decoded = PeerAddressCodec.decode(buf);

        assertThat(decoded).isEqualTo(address);
        buf.release();
    }

    @Test
    void decodeReturnsNullForShortBuffer() {
        var buf = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
        assertThat(PeerAddressCodec.decode(buf)).isNull();
        buf.release();
    }
}