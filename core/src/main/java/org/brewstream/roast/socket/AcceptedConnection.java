package org.brewstream.roast.socket;

import org.brewstream.roast.packet.SrtSocketId;

import java.net.InetSocketAddress;

/**
 * Fired by {@link SrtListener#onConnection} once a connection's handshake has
 * completed. This is connection <em>metadata</em>, not a data-transfer handle —
 * there is no read/write surface yet (that needs the Phase 3 receive/send path);
 * a DATA packet for {@code socketId} is currently logged and dropped.
 */
public record AcceptedConnection(
        SrtSocketId socketId,
        SrtSocketId peerSocketId,
        InetSocketAddress peerAddress,
        String streamId,
        int receiveLatencyMillis,
        int sendLatencyMillis,
        int srtVersion) {
}
