package org.brewstream.roast.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.AddressedEnvelope;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.brewstream.roast.packet.SrtPacket;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Encodes an {@link SrtPacket} addressed to a recipient into a Netty
 * {@link DatagramPacket} ready to write to a {@code NioDatagramChannel}.
 */
public final class SrtFrameEncoder
        extends MessageToMessageEncoder<AddressedEnvelope<SrtPacket, InetSocketAddress>> {

    @Override
    protected void encode(ChannelHandlerContext ctx, AddressedEnvelope<SrtPacket, InetSocketAddress> msg,
            List<Object> out) {
        SrtPacket packet = msg.content();
        ByteBuf buf = ctx.alloc().buffer(SrtPacket.HEADER_LENGTH + packet.body().readableBytes());
        packet.encodeTo(buf);
        out.add(new DatagramPacket(buf, msg.recipient()));
    }
}
