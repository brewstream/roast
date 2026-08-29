package org.brewstream.roast.packet.cif;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.util.CircularNumber;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class LossListCodecTest {

    private static CircularNumber seq(long value) {
        return CircularNumber.of(value, SrtPacket.MAX_SEQUENCE_NUMBER);
    }

    @Test
    void encodeMatchesGosrtGoldenVector() {
        // github.com/datarhei/gosrt packet.CIFNAK TestNAK: single(42), range(45..49).
        List<LossRange> ranges = List.of(LossRange.single(seq(42)), new LossRange(seq(45), seq(49)));

        var buf = ByteBufAllocator.DEFAULT.buffer();
        LossListCodec.encode(ranges, buf);

        assertThat(ByteBufUtil.hexDump(buf)).isEqualTo("0000002a8000002d00000031");
        buf.release();
    }

    @Test
    void decodeMatchesGosrtGoldenVector() {
        var buf = ByteBufUtil.decodeHexDump("0000002a8000002d00000031");
        var wrapped = Unpooled.wrappedBuffer(buf);

        List<LossRange> ranges = LossListCodec.decode(wrapped);

        assertThat(ranges).containsExactly(
                LossRange.single(seq(42)),
                new LossRange(seq(45), seq(49)));
        wrapped.release();
    }

    @Test
    void roundTripsMixedSinglesAndRanges() {
        List<LossRange> original = List.of(
                LossRange.single(seq(1)),
                new LossRange(seq(10), seq(20)),
                LossRange.single(seq(99)));

        var buf = ByteBufAllocator.DEFAULT.buffer();
        LossListCodec.encode(original, buf);

        List<LossRange> decoded = LossListCodec.decode(buf);

        assertThat(decoded).containsExactlyElementsOf(original);
        buf.release();
    }

    @Test
    void decodeReturnsNullForLengthNotMultipleOfFour() {
        var buf = Unpooled.wrappedBuffer(new byte[]{0, 0, 0});
        assertThat(LossListCodec.decode(buf)).isNull();
        buf.release();
    }

    @Test
    void decodeReturnsNullForDanglingRangeStart() {
        var buf = ByteBufAllocator.DEFAULT.buffer();
        buf.writeInt((int) (5 | 0x8000_0000L)); // range-start with no following end word.

        assertThat(LossListCodec.decode(buf)).isNull();
        buf.release();
    }

    @Test
    void decodeReturnsNullWhenRangeEndPrecedesStart() {
        var buf = ByteBufAllocator.DEFAULT.buffer();
        buf.writeInt((int) (50 | 0x8000_0000L)); // start = 50
        buf.writeInt(10); // end = 10, before start

        assertThat(LossListCodec.decode(buf)).isNull();
        buf.release();
    }

    @Test
    void lossRangeRejectsMismatchedDomains() {
        CircularNumber sequenceNumber = seq(1);
        CircularNumber timestamp = CircularNumber.of(1, SrtPacket.MAX_TIMESTAMP);

        assertThatIllegalArgumentException().isThrownBy(() -> new LossRange(sequenceNumber, timestamp));
    }
}
