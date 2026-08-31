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
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.util.CircularNumber;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Golden hex vectors are lifted byte-for-byte from github.com/datarhei/gosrt
 * packet.CIFACK's own tests (TestFullACK, TestSmallACK, TestLiteACK) — see
 * references/gosrt/packet/ack_test.go.
 */
class AckCifTest {

    private static CircularNumber seq(long value) {
        return CircularNumber.of(value, SrtPacket.MAX_SEQUENCE_NUMBER);
    }

    @Test
    void fullAckEncodeMatchesGosrtGoldenVector() {
        AckCif cif = new AckCif(AckVariant.FULL, seq(42), 38473, 9084, 48533, 20, 0, 73637);

        var buf = ByteBufAllocator.DEFAULT.buffer();
        cif.encodeTo(buf);

        assertThat(ByteBufUtil.hexDump(buf)).isEqualTo("0000002a000096490000237c0000bd95000000140000000000011fa5");
        buf.release();
    }

    @Test
    void fullAckDecodeMatchesGosrtGoldenVector() {
        var buf = Unpooled.wrappedBuffer(
                ByteBufUtil.decodeHexDump("0000002a000096490000237c0000bd95000000140000000000011fa5"));

        AckCif cif = AckCif.decode(buf);

        assertThat(cif).isEqualTo(new AckCif(AckVariant.FULL, seq(42), 38473, 9084, 48533, 20, 0, 73637));
        buf.release();
    }

    @Test
    void smallAckMatchesGosrtGoldenVector() {
        AckCif cif = AckCif.small(seq(42), 38473, 9084, 48533);
        String expectedHex = "0000002a000096490000237c0000bd95";

        var out = ByteBufAllocator.DEFAULT.buffer();
        cif.encodeTo(out);
        assertThat(ByteBufUtil.hexDump(out)).isEqualTo(expectedHex);
        out.release();

        var in = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(expectedHex));
        assertThat(AckCif.decode(in)).isEqualTo(cif);
        in.release();
    }

    @Test
    void liteAckMatchesGosrtGoldenVector() {
        AckCif cif = AckCif.lite(seq(42));
        String expectedHex = "0000002a";

        var out = ByteBufAllocator.DEFAULT.buffer();
        cif.encodeTo(out);
        assertThat(ByteBufUtil.hexDump(out)).isEqualTo(expectedHex);
        out.release();

        var in = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(expectedHex));
        assertThat(AckCif.decode(in)).isEqualTo(cif);
        in.release();
    }

    @Test
    void decodeReturnsNullForAnInvalidLength() {
        var buf = Unpooled.wrappedBuffer(new byte[10]); // not 4, 16, or >= 28
        assertThat(AckCif.decode(buf)).isNull();
        buf.release();
    }

    @Test
    void decodeReturnsNullForTooShortToBeFull() {
        var buf = Unpooled.wrappedBuffer(new byte[20]); // between small and full, not a valid length
        assertThat(AckCif.decode(buf)).isNull();
        buf.release();
    }
}