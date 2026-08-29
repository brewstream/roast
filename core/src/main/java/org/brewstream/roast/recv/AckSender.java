package org.brewstream.roast.recv;

import org.brewstream.roast.packet.cif.AckCif;
import org.brewstream.roast.packet.cif.AckVariant;
import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.util.CircularNumber;

import java.util.List;
import java.util.Optional;

/**
 * Decides when to send an ACK and which variant, and what "last acknowledged"
 * sequence number to report — driven by {@link LossList}'s already-tracked
 * receive state. Mirrors the ACK half of gosrt's {@code congestion/live} receiver
 * (congestion/live/receive.go's {@code periodicACK}/{@code Tick}): a Full ACK
 * roughly every 10ms (draft-sharabayko-srt.md: "sent every 10 ms"), a Light ACK
 * for every 64 packets received in between at high data rates (draft-sharabayko-
 * srt.md: "recommendation is to send a Light ACK for every 64 packets"). gosrt's
 * own receiver never emits a Small ACK despite the wire format supporting it, and
 * neither does this.
 *
 * <p>"Last acknowledged" here means the highest sequence number contiguously
 * received per {@link LossList} — not "ready to be delivered," which would need
 * TSBPD timing from a not-yet-built {@code ReceiveBuffer}/{@code TsbpdDeliverer}.
 *
 * <p>RTT/RTTVar/buffer/rate figures aren't computed here — they're supplied by the
 * caller on each {@link #tick}, since measuring them needs pieces that don't exist
 * yet (RTT needs ACK/ACKACK round-trip timing; buffer/rate figures need the
 * receive buffer). This class is purely the timing/threshold decision and CIF
 * construction, not the measurement. Not thread-safe, same as {@link LossList}.
 */
public final class AckSender {

    private static final long FULL_ACK_INTERVAL_MICROS = 10_000;
    private static final int LIGHT_ACK_PACKET_THRESHOLD = 64;

    private final LossList lossList;

    private boolean sentFirstFullAck;
    private long lastFullAckMicros;
    private int packetsSinceLastAck;

    public AckSender(LossList lossList) {
        this.lossList = lossList;
    }

    /** Call once for each packet accepted by the paired {@link LossList#onPacketReceived}. */
    public void onPacketReceived() {
        packetsSinceLastAck++;
    }

    /**
     * Call periodically (e.g. every ~10ms). Returns the ACK to send if one is due
     * now, or empty otherwise. {@code rtt}/{@code rttVar}/the rate and buffer
     * figures are only used — and only meaningful — for a Full ACK; ignored when
     * a Light ACK is what's due.
     */
    public Optional<AckCif> tick(long nowMicros, int rtt, int rttVar, int availableBufferSize,
            int packetsReceivingRate, int estimatedLinkCapacity, int receivingRate) {
        if (!sentFirstFullAck || nowMicros - lastFullAckMicros >= FULL_ACK_INTERVAL_MICROS) {
            sentFirstFullAck = true;
            lastFullAckMicros = nowMicros;
            packetsSinceLastAck = 0;
            CircularNumber lastAck = contiguousBoundary().inc();
            return Optional.of(new AckCif(AckVariant.FULL, lastAck, rtt, rttVar, availableBufferSize,
                    packetsReceivingRate, estimatedLinkCapacity, receivingRate));
        }

        if (packetsSinceLastAck >= LIGHT_ACK_PACKET_THRESHOLD) {
            packetsSinceLastAck = 0;
            return Optional.of(AckCif.lite(contiguousBoundary().inc()));
        }

        return Optional.empty();
    }

    private CircularNumber contiguousBoundary() {
        List<LossRange> outstanding = lossList.outstanding();
        return outstanding.isEmpty() ? lossList.highestSeen() : outstanding.get(0).start().dec();
    }
}
