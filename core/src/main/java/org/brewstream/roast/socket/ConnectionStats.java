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

/**
 * An immutable point-in-time snapshot of one connection's counters and gauges,
 * from {@link SrtConnection#stats()}.
 *
 * <p>This is the <em>pull</em> half of Roast's observability, deliberately
 * separate from {@link SrtConnectionListener}'s push events. High-frequency
 * activity — every packet, every ACK — is counted here rather than announced as
 * an event, because emitting an event per packet at live bitrates is pure
 * garbage and a throughput hazard. Events are for notable occurrences; counters
 * are for volume.
 *
 * <p>Shaped for a periodic sampler: read it on a schedule and publish the deltas
 * or the gauges. That is exactly how a Micrometer (or any other) binding would
 * consume it, which is why nothing here depends on a metrics library — the
 * binding can live outside this module, or be added later without changing this
 * type.
 *
 * <p><b>Counters</b> ({@code packetsSent} through {@code droppedEvents}) only
 * ever increase over a connection's life, so two snapshots give a rate.
 * <b>Gauges</b> ({@code rttMicros} onward) describe the connection right now.
 *
 * @param packetsSent           DATA packets handed to the socket, retransmissions included
 * @param packetsRetransmitted  DATA packets sent again because a peer NAKed them
 * @param packetsReceived       DATA packets accepted from the peer
 * @param bytesSent             payload bytes sent, retransmissions included
 * @param bytesReceived         payload bytes accepted from the peer
 * @param packetsLost           packets a peer's gap detection reported missing to us
 * @param packetsDropped        packets given up on because their deadline passed (TLPKTDROP)
 * @param packetsRecovered      packets that arrived carrying the retransmit flag — data a peer
 *                              resent because we asked for it. Distinct from
 *                              {@code packetsRetransmitted}, which counts what <em>we</em> resent:
 *                              on a receive-only connection that is always zero, and this is the
 *                              figure showing ARQ actually working
 * @param droppedEvents         observability events discarded because listeners fell behind —
 *                              nonzero means this connection's own reporting is incomplete
 * @param rttMicros             smoothed round-trip time
 * @param rttVarMicros          round-trip time variance
 * @param receiveRatePacketsPerSecond  arrival rate over the last measurement window
 * @param receiveRateBytesPerSecond    arrival rate over the last measurement window
 * @param estimatedLinkCapacityPacketsPerSecond  from the 16th/17th-packet probe pair
 * @param receiveBufferedPackets       packets held awaiting their TSBPD deadline
 * @param sendBufferedPackets          packets queued but not yet due to send
 * @param sendInFlightPackets          packets sent and retained for possible retransmission
 * @param flowWindowPackets            the receive window advertised to the peer
 * @param estimatedInputBytesPerSecond  bytes per second the application offered over the last
 *                                      window — compare against {@code estimatedSentBytesPerSecond}
 *                                      to see the send buffer filling before TLPKTDROP reports it
 * @param estimatedSentBytesPerSecond   bytes per second actually sent over the last window,
 *                                      retransmissions included
 * @param sendLossRatePercent           retransmitted bytes as a percentage of all bytes sent over
 *                                      the last window — a <em>current</em> figure, unlike
 *                                      {@link #retransmitRate()}'s lifetime one
 */
public record ConnectionStats(
        long packetsSent,
        long packetsRetransmitted,
        long packetsReceived,
        long bytesSent,
        long bytesReceived,
        long packetsLost,
        long packetsDropped,
        long packetsRecovered,
        long droppedEvents,
        long rttMicros,
        long rttVarMicros,
        int receiveRatePacketsPerSecond,
        int receiveRateBytesPerSecond,
        int estimatedLinkCapacityPacketsPerSecond,
        int receiveBufferedPackets,
        int sendBufferedPackets,
        int sendInFlightPackets,
        int flowWindowPackets,
        int estimatedInputBytesPerSecond,
        int estimatedSentBytesPerSecond,
        double sendLossRatePercent) {

    /**
     * Retransmissions as a fraction of everything sent — the figure most worth
     * alerting on, since a rising value means the link is degrading well before
     * anything is actually lost. Zero when nothing has been sent yet.
     */
    /**
     * Recovered packets as a fraction of everything that should have arrived.
     *
     * <p>The receive-side counterpart to {@link #retransmitRate()}. A connection
     * losing ten percent of its packets and recovering all of them reads zero
     * loss everywhere else — this is the only figure that shows the work being
     * done, and it should track the network's actual loss rate closely.
     */
    public double recoveryRate() {
        long expected = packetsReceived + packetsDropped;
        return expected == 0 ? 0 : (double) packetsRecovered / expected;
    }

    /**
     * Retransmitted packets as a fraction of all packets sent, over the
     * connection's whole life. See {@link #sendLossRatePercent()} for the
     * last-window equivalent — a connection that recovered from a bad patch
     * shows a low figure in both, while one in trouble right now shows a low
     * lifetime figure and a high current one.
     */
    public double retransmitRate() {
        return packetsSent == 0 ? 0 : (double) packetsRetransmitted / packetsSent;
    }
}