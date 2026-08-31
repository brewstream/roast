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

package org.brewstream.roast.send;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Self-designed against the rate block in gosrt's {@code congestion/live/send.go}
 * — checked directly, {@code send_test.go} has no rate cases to port, so this is
 * the weaker "read the reference source" tier rather than the ported-scenario
 * tier. See {@link SendRateEstimator}'s javadoc.
 */
class SendRateEstimatorTest {

    private static final long WINDOW = 1_000_000;

    @Test
    void reportsNothingUntilAWindowHasElapsed() {
        SendRateEstimator estimator = new SendRateEstimator();
        estimator.onPushed(1000);
        estimator.onSent(1000, false);

        // gosrt's condition is `tdiff > period`, so exactly one window is not yet enough.
        estimator.tick(WINDOW);

        assertThat(estimator.estimatedInputBytesPerSecond()).isZero();
        assertThat(estimator.estimatedSentBytesPerSecond()).isZero();
    }

    @Test
    void inputAndSentRatesAreCountedSeparately() {
        SendRateEstimator estimator = new SendRateEstimator();
        // Twice as much offered as actually went out - a buffer filling up.
        for (int i = 0; i < 10; i++) {
            estimator.onPushed(1000);
        }
        for (int i = 0; i < 5; i++) {
            estimator.onSent(1000, false);
        }

        estimator.tick(WINDOW + 1);

        assertThat(estimator.estimatedInputBytesPerSecond()).isEqualTo(10_000);
        assertThat(estimator.estimatedSentBytesPerSecond()).isEqualTo(5_000);
    }

    /** The rates are per-second figures, so a window of a different length must still scale. */
    @Test
    void ratesAreScaledToTheWindowsRealElapsedTime() {
        SendRateEstimator estimator = new SendRateEstimator();
        for (int i = 0; i < 10; i++) {
            estimator.onPushed(1000);
        }

        // Two seconds of accumulation must read as half the per-second rate,
        // not as the raw total.
        estimator.tick(2 * WINDOW);

        assertThat(estimator.estimatedInputBytesPerSecond()).isEqualTo(5_000);
    }

    /**
     * gosrt counts a retransmission into both totals, which is what makes the
     * ratio meaningful — 2 of 10 sent being resends is 20%, not 25%.
     */
    @Test
    void lossRateIsRetransmittedBytesOverAllSentBytesAsAPercentage() {
        SendRateEstimator estimator = new SendRateEstimator();
        for (int i = 0; i < 8; i++) {
            estimator.onSent(1000, false);
        }
        for (int i = 0; i < 2; i++) {
            estimator.onSent(1000, true);
        }

        estimator.tick(WINDOW + 1);

        assertThat(estimator.estimatedSentBytesPerSecond()).isEqualTo(10_000);
        assertThat(estimator.sendLossRatePercent()).isEqualTo(20.0);
    }

    @Test
    void aWindowWithNoTrafficReportsNoLossRateRatherThanTheLastOne() {
        SendRateEstimator estimator = new SendRateEstimator();
        estimator.onSent(1000, true);
        estimator.tick(WINDOW + 1);
        assertThat(estimator.sendLossRatePercent()).isEqualTo(100.0);

        // A silent window must not keep reporting the previous one's figure -
        // a stale value masquerading as current is worse than zero.
        estimator.tick(3 * WINDOW);

        assertThat(estimator.sendLossRatePercent()).isZero();
    }

    /** Each window stands alone; the previous window's bytes must not leak into it. */
    @Test
    void countersResetBetweenWindows() {
        SendRateEstimator estimator = new SendRateEstimator();
        for (int i = 0; i < 10; i++) {
            estimator.onPushed(1000);
            estimator.onSent(1000, false);
        }
        estimator.tick(WINDOW + 1);
        assertThat(estimator.estimatedSentBytesPerSecond()).isEqualTo(10_000);

        estimator.onPushed(2000);
        estimator.onSent(2000, false);
        estimator.tick(2 * WINDOW + 2);

        assertThat(estimator.estimatedInputBytesPerSecond()).isEqualTo(2_000);
        assertThat(estimator.estimatedSentBytesPerSecond()).isEqualTo(2_000);
    }
}