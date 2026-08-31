package org.brewstream.roast.recv;

import org.junit.jupiter.api.Test;

import java.util.OptionalLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * No reference test exists to ground these against: gosrt has no working
 * drift implementation at all (a dead field, never assigned), and libsrt's
 * {@code DriftTracer} (utilities.h), the only real reference, has no
 * dedicated unit tests either. Self-designed directly against libsrt's source,
 * which is a weaker footing than a ported reference scenario and worth knowing
 * when changing it.
 */
class DriftTracerTest {

    @Test
    void fewerThanMaxSamplesNeverUpdates() {
        DriftTracer tracer = new DriftTracer();

        for (int i = 0; i < DriftTracer.MAX_SAMPLES - 1; i++) {
            assertThat(tracer.update(1_000)).isEmpty();
        }

        assertThat(tracer.drift()).isZero();
    }

    @Test
    void completingASpanWithinTheClampUpdatesDriftAndReturnsNoOverdrift() {
        DriftTracer tracer = new DriftTracer();

        OptionalLong result = fillSpan(tracer, 3_000);

        assertThat(result).isEmpty();
        assertThat(tracer.drift()).isEqualTo(3_000);
    }

    @Test
    void completingASpanBeyondThePositiveClampBanksTheExcessAsOverdrift() {
        DriftTracer tracer = new DriftTracer();

        OptionalLong result = fillSpan(tracer, 8_000);

        assertThat(result).hasValue(DriftTracer.MAX_DRIFT_MICROS);
        assertThat(tracer.drift()).isEqualTo(8_000 - DriftTracer.MAX_DRIFT_MICROS);
    }

    @Test
    void completingASpanBeyondTheNegativeClampBanksTheExcessAsOverdrift() {
        DriftTracer tracer = new DriftTracer();

        OptionalLong result = fillSpan(tracer, -8_000);

        assertThat(result).hasValue(-DriftTracer.MAX_DRIFT_MICROS);
        assertThat(tracer.drift()).isEqualTo(-8_000 + DriftTracer.MAX_DRIFT_MICROS);
    }

    @Test
    void accumulatorResetsAfterEachCompletedSpan() {
        DriftTracer tracer = new DriftTracer();
        fillSpan(tracer, 3_000);

        // A fresh span of 1_000us samples should yield exactly 1_000, not
        // anything blended with the previous span's leftover accumulator.
        OptionalLong result = fillSpan(tracer, 1_000);

        assertThat(result).isEmpty();
        assertThat(tracer.drift()).isEqualTo(1_000);
    }

    private static OptionalLong fillSpan(DriftTracer tracer, long sampleMicros) {
        OptionalLong result = OptionalLong.empty();
        for (int i = 0; i < DriftTracer.MAX_SAMPLES; i++) {
            result = tracer.update(sampleMicros);
        }
        return result;
    }
}
