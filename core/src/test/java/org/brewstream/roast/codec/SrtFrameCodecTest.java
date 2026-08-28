package org.brewstream.roast.codec;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.AddressedEnvelope;
import io.netty.channel.DefaultAddressedEnvelope;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.channel.socket.DatagramPacket;
import org.brewstream.roast.packet.ControlPacket;
import org.brewstream.roast.packet.ControlType;
import org.brewstream.roast.packet.DataPacket;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;

import static org.assertj.core.api.Assertions.assertThat;

class SrtFrameCodecTest {

    private static final InetSocketAddress SENDER = new InetSocketAddress("127.0.0.1", 9000);
    private static final InetSocketAddress LOCAL = new InetSocketAddress("127.0.0.1", 9710);

    @Test
    void decoderProducesEnvelopeWithSenderAddress() {
        EmbeddedChannel channel = new EmbeddedChannel(new SrtFrameDecoder());

        DataPacket original = new DataPacket(1, 0, true, 0, false, 0, 100,
                SrtSocketId.of(55), Unpooled.wrappedBuffer(new byte[]{7, 7}));
        var raw = ByteBufAllocator.DEFAULT.buffer();
        original.encodeTo(raw);

        assertThat(channel.writeInbound(new DatagramPacket(raw, LOCAL, SENDER))).isTrue();

        AddressedEnvelope<SrtPacket, InetSocketAddress> envelope = channel.readInbound();

        assertThat(envelope.sender()).isEqualTo(SENDER);
        assertThat(envelope.content()).isInstanceOf(DataPacket.class);
        DataPacket decoded = (DataPacket) envelope.content();
        assertThat(decoded.destination()).isEqualTo(SrtSocketId.of(55));

        decoded.body().release();
        channel.finishAndReleaseAll();
    }

    @Test
    void decoderDropsShortDatagram() {
        EmbeddedChannel channel = new EmbeddedChannel(new SrtFrameDecoder());
        var raw = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});

        channel.writeInbound(new DatagramPacket(raw, LOCAL, SENDER));

        assertThat((Object) channel.readInbound()).isNull();
        channel.finishAndReleaseAll();
    }

    @Test
    void encoderProducesWireIdenticalDatagram() {
        EmbeddedChannel channel = new EmbeddedChannel(new SrtFrameEncoder());

        ControlPacket packet = new ControlPacket(ControlType.KEEPALIVE, 0, 200,
                SrtSocketId.of(9), Unpooled.wrappedBuffer(new byte[0]));
        AddressedEnvelope<SrtPacket, InetSocketAddress> envelope =
                new DefaultAddressedEnvelope<>(packet, SENDER);

        assertThat(channel.writeOutbound(envelope)).isTrue();
        DatagramPacket out = channel.readOutbound();

        var expected = ByteBufAllocator.DEFAULT.buffer();
        new ControlPacket(ControlType.KEEPALIVE, 0, 200, SrtSocketId.of(9),
                Unpooled.wrappedBuffer(new byte[0])).encodeTo(expected);

        byte[] expectedBytes = new byte[expected.readableBytes()];
        expected.readBytes(expectedBytes);
        byte[] actualBytes = new byte[out.content().readableBytes()];
        out.content().readBytes(actualBytes);

        assertThat(actualBytes).isEqualTo(expectedBytes);
        assertThat(out.recipient()).isEqualTo(SENDER);

        expected.release();
        channel.finishAndReleaseAll();
    }
}
