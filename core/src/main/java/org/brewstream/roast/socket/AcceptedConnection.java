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
        int srtVersion,
        int flowWindowSize,
        int maxTransmissionUnitSize) {

    public AcceptedConnection {
        // A peer may send no StreamID at all, and a null here means every
        // consumer has to null-check or trip over it - which is exactly what the
        // CLI did the first time it met a peer without one. ConnectionRequest
        // already normalises the same field; this makes the pair consistent.
        streamId = streamId == null ? "" : streamId;
    }

    /** Without a negotiated MTU — falls back to SRT's standard 1500. */
    public AcceptedConnection(SrtSocketId socketId, SrtSocketId peerSocketId, InetSocketAddress peerAddress,
            String streamId, int receiveLatencyMillis, int sendLatencyMillis, int srtVersion, int flowWindowSize) {
        this(socketId, peerSocketId, peerAddress, streamId, receiveLatencyMillis, sendLatencyMillis,
                srtVersion, flowWindowSize, 1500);
    }

    /**
     * The largest payload one DATA packet can carry on this connection: the
     * negotiated MTU less the IP+UDP (28) and SRT (16) headers. libsrt names the
     * same figure {@code SRT_LIVE_MAX_PLSIZE} (1456 at the standard 1500 MTU).
     */
    public int maxPayloadSize() {
        return maxTransmissionUnitSize - 28 - 16;
    }

    /**
     * The flow window agreed during the handshake, in packets — both sides of
     * this codebase's handshake echo the value the other proposed
     * ({@code ListenerHandshake.buildAcceptResponse} /
     * {@code CallerHandshake.buildConclusionRequest}), so this is literally the
     * number we committed to on the wire. {@link SrtConnection} bounds the
     * "available buffer size" it reports in Full ACKs by it, which is what a
     * peer's sender treats as its flow-control window — see that class's
     * {@code tick}.
     */
    public int flowWindowSize() {
        return flowWindowSize;
    }
}
