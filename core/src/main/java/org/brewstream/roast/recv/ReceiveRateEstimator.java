package org.brewstream.roast.recv;

import org.brewstream.roast.util.CircularNumber;

/**
 * The receive-side rate figures a Full ACK reports back to the peer: how fast
 * packets are arriving, how fast bytes are arriving, and an estimate of the
 * link's theoretical capacity. Ported from gosrt's {@code congestion/live}
 * receiver ({@code receive.go}'s {@code Push}/{@code Tick}/{@code PacketRate}),
 * which keeps all three in its one receiver struct — Roast splits that struct
 * across {@link LossList}/{@link AckSender}/{@link ReceiveBuffer}, and none of
 * those is a natural home for rate bookkeeping, so it lives here (same reasoning
 * that carved {@link DriftTracer} out).
 *
 * <p><b>Link capacity comes from the 16th/17th-packet probe pair</b> — the
 * receive-side counterpart to the trick {@code SendBuffer.push} performs when
 * sending. A sender transmits the packet whose sequence number is {@code ≡ 0
 * (mod 16)} and the next one back-to-back; the gap between their arrivals,
 * scaled to what a fully-loaded packet would have taken, estimates how fast the
 * link could carry traffic regardless of how fast the application is actually
 * producing it. Undocumented in the SRT spec proper — gosrt's own comment points
 * at libsrt's {@code core.cpp}/{@code window.h} and {@code PUMASK_SEQNO_PROBE},
 * which is where this behavior is actually pinned down. A retransmitted packet,
 * or a pair that isn't genuinely consecutive, disarms the measurement rather
 * than feeding it a bogus sample.
 *
 * <p><b>The packet/byte rates use a ~1s sliding window</b>, recomputed on
 * {@link #tick} and then reset, exactly as gosrt does. Both are reported as
 * per-second figures, so a window shorter than the nominal one still scales
 * correctly.
 *
 * <p>Unlike gosrt, every method takes {@code nowMicros} explicitly rather than
 * reading a clock: gosrt mixes wall-clock {@code time.Now()} for the probe pair
 * with its tick's own elapsed-microsecond value for the rate window, and this
 * codebase keeps a single elapsed-time source per connection, never wall
 * clock. Callers pass {@code SrtConnection.elapsedMicros()}.
 *
 * <p><b>No reference test exists to ground this against</b> — checked directly:
 * gosrt's {@code receive_test.go} has no rate- or capacity-related cases, so
 * {@code ReceiveRateEstimatorTest} is self-designed against {@code receive.go}'s
 * source, the same rigor tier as this codebase's RTT/drift/wraparound pieces
 * rather than the stronger ported-scenario tier. Not thread-safe, same as this
 * codebase's other per-connection receive state.
 */
public final class ReceiveRateEstimator {

    /** gosrt's {@code packet.MAX_PAYLOAD_SIZE} — the "fully loaded packet" the probe gap is scaled to. */
    private static final double MAX_PAYLOAD_SIZE = 1456;

    private static final long RATE_WINDOW_MICROS = 1_000_000;

    private long windowStartMicros;
    private long packetsInWindow;
    private long bytesInWindow;

    private double packetsPerSecond;
    private double bytesPerSecond;
    private double avgLinkCapacityPacketsPerSecond;

    private long probeTimeMicros;
    private boolean probeArmed;
    private CircularNumber probeNextSequenceNumber;

    /**
     * Call for every accepted DATA packet, before it's buffered (the payload's
     * readable byte count is read here, so pass it while the buffer is intact).
     */
    public void onPacketReceived(CircularNumber sequenceNumber, int payloadBytes, boolean retransmitted,
            long nowMicros) {
        updateLinkCapacity(sequenceNumber, payloadBytes, retransmitted, nowMicros);

        packetsInWindow++;
        bytesInWindow += payloadBytes;
    }

    /**
     * Recomputes the per-second rates once the window has elapsed, then starts a
     * fresh window. Safe to call every connection tick — it's a no-op until a
     * full window's worth of time has passed.
     */
    public void tick(long nowMicros) {
        long elapsedMicros = nowMicros - windowStartMicros;
        if (elapsedMicros <= RATE_WINDOW_MICROS) {
            return;
        }

        double elapsedSeconds = elapsedMicros / 1_000_000.0;
        packetsPerSecond = packetsInWindow / elapsedSeconds;
        bytesPerSecond = bytesInWindow / elapsedSeconds;

        packetsInWindow = 0;
        bytesInWindow = 0;
        windowStartMicros = nowMicros;
    }

    /** Packets per second over the last completed window — the ACK CIF's "packets receiving rate". */
    public int packetsPerSecond() {
        return (int) Math.round(packetsPerSecond);
    }

    /** Bytes per second over the last completed window — the ACK CIF's "receiving rate". */
    public int receivingRateBytesPerSecond() {
        return (int) Math.round(bytesPerSecond);
    }

    /** Probe-pair link-capacity estimate in packets per second — the ACK CIF's "estimated link capacity". */
    public int estimatedLinkCapacityPacketsPerSecond() {
        return (int) Math.round(avgLinkCapacityPacketsPerSecond);
    }

    /**
     * Mirrors gosrt's probe handling exactly, including the detail that a
     * successful measurement does <em>not</em> disarm the probe (only a
     * mismatching packet or a retransmission does) — the next {@code ≡ 0 (mod
     * 16)} packet re-arms it anyway.
     */
    private void updateLinkCapacity(CircularNumber sequenceNumber, int payloadBytes, boolean retransmitted,
            long nowMicros) {
        if (retransmitted) {
            probeArmed = false;
            return;
        }

        long probe = sequenceNumber.value() & 0xF;
        if (probe == 0) {
            probeTimeMicros = nowMicros;
            probeNextSequenceNumber = sequenceNumber.inc();
            probeArmed = true;
            return;
        }

        boolean isSecondOfPair = probe == 1
                && probeArmed
                && sequenceNumber.equals(probeNextSequenceNumber)
                && payloadBytes != 0;
        if (!isSecondOfPair) {
            probeArmed = false;
            return;
        }

        // The arrival gap, scaled to what a fully loaded packet would have taken.
        double scaledGapMicros = (nowMicros - probeTimeMicros) * (MAX_PAYLOAD_SIZE / payloadBytes);
        if (scaledGapMicros != 0) {
            avgLinkCapacityPacketsPerSecond =
                    0.875 * avgLinkCapacityPacketsPerSecond + 0.125 * (1_000_000 / scaledGapMicros);
        }
    }
}
