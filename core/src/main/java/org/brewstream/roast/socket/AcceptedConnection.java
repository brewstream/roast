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

    /**
     * Normalises a missing StreamID to the empty string, so it is never null. A
     * peer may legitimately send none, and a null would leave every consumer to
     * null-check or trip over it — which is exactly what the CLI did the first
     * time it met such a peer. {@link ConnectionRequest} already normalises the
     * same field; this makes the pair consistent.
     */
    public AcceptedConnection {
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
     *
     * @return the maximum payload size in bytes
     */
    public int maxPayloadSize() {
        return maxTransmissionUnitSize - 28 - 16;
    }

    /**
     * The flow window agreed during the handshake. Both sides echo the value the
     * other proposed, so this is literally the number committed to on the wire;
     * {@link SrtConnection} bounds the available-buffer figure it reports in
     * Full ACKs by it.
     *
     * @return the agreed flow window, in packets
     */
    public int flowWindowSize() {
        return flowWindowSize;
    }
}