package org.brewstream.roast.socket;

import io.netty.channel.AddressedEnvelope;
import org.brewstream.roast.packet.SrtPacket;

import java.net.InetSocketAddress;

/**
 * Receives packets routed by {@link SrtSocketIdDemultiplexer} for one socket ID.
 * The sink owns the packet's body from this call onward and is responsible for
 * releasing it (see {@link SrtPacket#body()}).
 */
@FunctionalInterface
public interface SrtPacketSink {
    void onPacket(AddressedEnvelope<SrtPacket, InetSocketAddress> packet);
}
