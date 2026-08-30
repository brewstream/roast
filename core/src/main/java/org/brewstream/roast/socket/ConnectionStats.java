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
 */
public record ConnectionStats(
        long packetsSent,
        long packetsRetransmitted,
        long packetsReceived,
        long bytesSent,
        long bytesReceived,
        long packetsLost,
        long packetsDropped,
        long droppedEvents,
        long rttMicros,
        long rttVarMicros,
        int receiveRatePacketsPerSecond,
        int receiveRateBytesPerSecond,
        int estimatedLinkCapacityPacketsPerSecond,
        int receiveBufferedPackets,
        int sendBufferedPackets,
        int sendInFlightPackets,
        int flowWindowPackets) {

    /**
     * Retransmissions as a fraction of everything sent — the figure most worth
     * alerting on, since a rising value means the link is degrading well before
     * anything is actually lost. Zero when nothing has been sent yet.
     */
    public double retransmitRate() {
        return packetsSent == 0 ? 0 : (double) packetsRetransmitted / packetsSent;
    }
}
