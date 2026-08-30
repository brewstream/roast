package org.brewstream.roast.recv;

import org.brewstream.roast.packet.cif.AckCif;
import org.brewstream.roast.packet.cif.AckVariant;
import org.brewstream.roast.util.CircularNumber;

import java.util.Optional;

/**
 * Decides when to send an ACK and which variant, and builds the CIF for it.
 * Mirrors the timing half of gosrt's {@code congestion/live} receiver
 * (congestion/live/receive.go's {@code periodicACK}/{@code Tick}): a Full ACK
 * roughly every 10ms (draft-sharabayko-srt.md: "sent every 10 ms"), a Light ACK
 * for every 64 packets received in between at high data rates (draft-sharabayko-
 * srt.md: "recommendation is to send a Light ACK for every 64 packets"). gosrt's
 * own receiver never emits a Small ACK despite the wire format supporting it, and
 * neither does this.
 *
 * <p>Unlike an earlier version of this class, the "last acknowledged" sequence
 * number is no longer computed here — it's supplied by the caller on each
 * {@link #tick}, already reflecting {@link ReceiveBuffer#computeAckBoundary}'s
 * TLPKTDROP-aware walk (see that method's javadoc). Deriving it from {@link
 * LossList} alone (as this class used to) meant the ACK boundary could only
 * ever advance as far as {@code LossList}'s own bookkeeping, which stayed
 * frozen at the *oldest* unresolved gap until something else explicitly
 * cleared it — confirmed via real interop testing to starve a real peer's
 * send buffer under sustained loss (see STATUS.md). This class no longer
 * depends on {@link LossList} at all — it's purely the timing/variant
 * decision and CIF construction.
 *
 * <p>RTT/RTTVar/buffer/rate figures aren't computed here — they're supplied by the
 * caller on each {@link #tick}, since measuring them needs pieces that don't exist
 * yet (RTT needs ACK/ACKACK round-trip timing; buffer/rate figures need the
 * receive buffer). Not thread-safe, same as {@link LossList}.
 *
 * <p><b>{@code nowMicros} contract:</b> matches gosrt's {@code lastPeriodicACK},
 * which starts at its zero-value — there is no "always send a Full ACK on the
 * very first tick" shortcut. The first Full ACK fires once {@code nowMicros}
 * itself reaches {@link #FULL_ACK_INTERVAL_MICROS}, so callers must pass
 * microseconds elapsed since this receiver's own start — this codebase never
 * puts wall-clock time on the wire, and the same rule applies to this internal
 * clock — not epoch time.
 */
public final class AckSender {

    private static final long FULL_ACK_INTERVAL_MICROS = 10_000;
    private static final int LIGHT_ACK_PACKET_THRESHOLD = 64;

    private long lastFullAckMicros;
    private int packetsSinceLastAck;

    /** Call once for each packet accepted by the paired {@link LossList#onPacketReceived}. */
    public void onPacketReceived() {
        packetsSinceLastAck++;
    }

    /**
     * Call periodically (e.g. every ~10ms). Returns the ACK to send if one is due
     * now, or empty otherwise. {@code lastAckSequenceNumber} is the CIF's "last
     * ACK" field value directly — the caller's already-computed boundary, one
     * past the highest confirmed sequence number (see {@link
     * ReceiveBuffer#computeAckBoundary}). {@code rtt}/{@code rttVar}/the rate and
     * buffer figures are only used — and only meaningful — for a Full ACK;
     * ignored when a Light ACK is what's due.
     */
    public Optional<AckCif> tick(long nowMicros, CircularNumber lastAckSequenceNumber, int rtt, int rttVar,
            int availableBufferSize, int packetsReceivingRate, int estimatedLinkCapacity, int receivingRate) {
        if (nowMicros - lastFullAckMicros >= FULL_ACK_INTERVAL_MICROS) {
            lastFullAckMicros = nowMicros;
            packetsSinceLastAck = 0;
            return Optional.of(new AckCif(AckVariant.FULL, lastAckSequenceNumber, rtt, rttVar, availableBufferSize,
                    packetsReceivingRate, estimatedLinkCapacity, receivingRate));
        }

        if (packetsSinceLastAck >= LIGHT_ACK_PACKET_THRESHOLD) {
            packetsSinceLastAck = 0;
            return Optional.of(AckCif.lite(lastAckSequenceNumber));
        }

        return Optional.empty();
    }
}
