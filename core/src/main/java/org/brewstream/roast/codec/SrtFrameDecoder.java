package org.brewstream.roast.codec;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultAddressedEnvelope;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.brewstream.roast.packet.SrtPacket;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Decodes inbound Netty {@link DatagramPacket}s into {@link SrtPacket}s, wrapped
 * with their sender address so downstream handlers (socket-ID demux, handshake)
 * know who sent them.
 */
public final class SrtFrameDecoder extends MessageToMessageDecoder<DatagramPacket> {

    private static final Logger LOG = Logger.getLogger(SrtFrameDecoder.class.getName());

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out) {
        SrtPacket packet = SrtPacket.decode(msg.content());
        if (packet == null) {
            LOG.log(Level.FINE, "Dropping malformed/undersized datagram from {0}", msg.sender());
            return;
        }
        out.add(new DefaultAddressedEnvelope<>(packet, (InetSocketAddress) msg.recipient(),
                (InetSocketAddress) msg.sender()));
    }
}
