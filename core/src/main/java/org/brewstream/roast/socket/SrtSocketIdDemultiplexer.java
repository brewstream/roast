package org.brewstream.roast.socket;

import io.netty.channel.AddressedEnvelope;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Routes inbound SRT packets to the connection registered for their destination
 * socket ID, giving Roast connection multiplexing over a single UDP socket
 * without one OS port per connection. Socket ID 0 is reserved by the SRT spec for
 * induction handshake packets sent before a connection exists, so it routes to a
 * separately-registered acceptor sink rather than the per-connection table — that
 * is the seam a future listener handshake handler plugs into.
 */
public final class SrtSocketIdDemultiplexer
        extends SimpleChannelInboundHandler<AddressedEnvelope<SrtPacket, InetSocketAddress>> {

    private static final Logger LOG = Logger.getLogger(SrtSocketIdDemultiplexer.class.getName());

    private final ConcurrentHashMap<SrtSocketId, SrtPacketSink> connections = new ConcurrentHashMap<>();
    private volatile SrtPacketSink acceptor;

    /** Registers the sink that receives socket-ID-0 induction packets, i.e. new-connection attempts. */
    public void setAcceptor(SrtPacketSink acceptor) {
        this.acceptor = acceptor;
    }

    /** Routes packets addressed to {@code id} to {@code sink}. Replaces any existing registration. */
    public void register(SrtSocketId id, SrtPacketSink sink) {
        connections.put(id, sink);
    }

    /** Stops routing to {@code id}; anything still arriving for it is dropped. */
    public void unregister(SrtSocketId id) {
        connections.remove(id);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, AddressedEnvelope<SrtPacket, InetSocketAddress> msg) {
        SrtSocketId destination = msg.content().destination();
        SrtPacketSink sink = destination.isZero() ? acceptor : connections.get(destination);
        if (sink == null) {
            LOG.log(Level.FINE, "Dropping packet for unknown socket id {0}", destination);
            msg.content().body().release();
            return;
        }
        sink.onPacket(msg);
    }
}
