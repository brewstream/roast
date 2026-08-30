package org.brewstream.roast.recv;

import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.util.CircularNumber;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Self-designed against gosrt's congestion/live/receive.go - checked directly,
 * its receive_test.go has no rate- or capacity-related cases, so there is no
 * reference scenario to port here (same tier as the RTT/drift/wraparound tests).
 */
class ReceiveRateEstimatorTest {

    private static final int PAYLOAD = 1456; // gosrt's MAX_PAYLOAD_SIZE - keeps probe scaling 1:1

    private static CircularNumber seq(long value) {
        return CircularNumber.of(value, SrtPacket.MAX_SEQUENCE_NUMBER);
    }

    @Test
    void reportsZeroBeforeAnyWindowHasElapsed() {
        ReceiveRateEstimator estimator = new ReceiveRateEstimator();

        estimator.onPacketReceived(seq(5), PAYLOAD, false, 1_000);
        estimator.tick(500_000); // half a window

        assertThat(estimator.packetsPerSecond()).isZero();
        assertThat(estimator.receivingRateBytesPerSecond()).isZero();
    }

    @Test
    void computesPacketAndByteRatesOnceTheWindowElapses() {
        ReceiveRateEstimator estimator = new ReceiveRateEstimator();

        for (int i = 0; i < 100; i++) {
            estimator.onPacketReceived(seq(100 + i), PAYLOAD, false, i * 1_000L);
        }
        estimator.tick(2_000_000); // 2s window -> 100 packets over 2s = 50/s

        assertThat(estimator.packetsPerSecond()).isEqualTo(50);
        assertThat(estimator.receivingRateBytesPerSecond()).isEqualTo(100 * PAYLOAD / 2);
    }

    @Test
    void windowResetsSoAQuietPeriodReportsALowerRate() {
        ReceiveRateEstimator estimator = new ReceiveRateEstimator();
        for (int i = 0; i < 100; i++) {
            estimator.onPacketReceived(seq(100 + i), PAYLOAD, false, i * 1_000L);
        }
        estimator.tick(2_000_000);
        assertThat(estimator.packetsPerSecond()).isEqualTo(50);

        // Nothing arrives during the next window at all.
        estimator.tick(4_000_001);

        assertThat(estimator.packetsPerSecond()).isZero();
        assertThat(estimator.receivingRateBytesPerSecond()).isZero();
    }

    /**
     * The 16th/17th probe pair: a full-size packet arriving 1000us after its
     * partner scales 1:1, so capacity is 1_000_000/1000 = 1000 packets/s. The
     * estimate is an EWMA seeded at 0, so one sample yields 0.125 * 1000.
     */
    @Test
    void probePairFeedsTheLinkCapacityEstimate() {
        ReceiveRateEstimator estimator = new ReceiveRateEstimator();

        estimator.onPacketReceived(seq(16), PAYLOAD, false, 10_000); // 16 % 16 == 0 -> arms
        estimator.onPacketReceived(seq(17), PAYLOAD, false, 11_000); // 17 % 16 == 1 -> measures

        assertThat(estimator.estimatedLinkCapacityPacketsPerSecond()).isEqualTo(125);
    }

    @Test
    void repeatedProbePairsConvergeTowardTheRealCapacity() {
        ReceiveRateEstimator estimator = new ReceiveRateEstimator();

        for (int pair = 0; pair < 60; pair++) {
            long base = pair * 100_000L;
            estimator.onPacketReceived(seq(16L * pair), PAYLOAD, false, base);
            estimator.onPacketReceived(seq(16L * pair + 1), PAYLOAD, false, base + 1_000);
        }

        // Converging on 1000 packets/s from below, without overshooting it.
        assertThat(estimator.estimatedLinkCapacityPacketsPerSecond()).isBetween(950, 1000);
    }

    @Test
    void aNonConsecutivePartnerDoesNotProduceAMeasurement() {
        ReceiveRateEstimator estimator = new ReceiveRateEstimator();

        estimator.onPacketReceived(seq(16), PAYLOAD, false, 10_000);
        estimator.onPacketReceived(seq(33), PAYLOAD, false, 11_000); // 33 % 16 == 1, but not seq 17

        assertThat(estimator.estimatedLinkCapacityPacketsPerSecond()).isZero();
    }

    @Test
    void aRetransmittedPacketDisarmsTheProbeRatherThanSkewingIt() {
        ReceiveRateEstimator estimator = new ReceiveRateEstimator();

        estimator.onPacketReceived(seq(16), PAYLOAD, false, 10_000);
        estimator.onPacketReceived(seq(17), PAYLOAD, true, 900_000); // a very late retransmit

        assertThat(estimator.estimatedLinkCapacityPacketsPerSecond()).isZero();
    }

    /**
     * A short packet is scaled up to what a full-size one would have taken:
     * half-size arriving after 500us scales to 1000us, the same estimate the
     * full-size 1000us pair produces above.
     */
    @Test
    void aPartlyFilledProbePacketIsScaledToAFullOne() {
        ReceiveRateEstimator estimator = new ReceiveRateEstimator();

        estimator.onPacketReceived(seq(16), PAYLOAD, false, 10_000);
        estimator.onPacketReceived(seq(17), PAYLOAD / 2, false, 10_500);

        assertThat(estimator.estimatedLinkCapacityPacketsPerSecond()).isEqualTo(125);
    }

    @Test
    void retransmittedPacketsStillCountTowardTheArrivalRates() {
        ReceiveRateEstimator estimator = new ReceiveRateEstimator();

        estimator.onPacketReceived(seq(5), PAYLOAD, true, 1_000);
        estimator.onPacketReceived(seq(6), PAYLOAD, false, 2_000);
        estimator.tick(1_000_001);

        assertThat(estimator.packetsPerSecond()).isEqualTo(2);
    }
}
