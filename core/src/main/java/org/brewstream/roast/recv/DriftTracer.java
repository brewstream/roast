package org.brewstream.roast.recv;

import java.util.OptionalLong;

/**
 * Median-based clock-drift estimator for TSBPD, ported from libsrt's generic
 * {@code DriftTracer<MAX_SPAN, MAX_DRIFT, CLEAR_ON_UPDATE=true>} template
 * ({@code srtcore/utilities.h}) and specialized directly to libsrt's own
 * constants ({@code TSBPD_DRIFT_MAX_SAMPLES} = 1000, {@code
 * TSBPD_DRIFT_MAX_VALUE} = 5000us) — this codebase only ever needs one
 * instantiation, so the template generality isn't ported.
 *
 * <p>Accumulates samples via {@link #update}; every {@value #MAX_SAMPLES}
 * samples it computes their average, clamps it to +/-{@value
 * #MAX_DRIFT_MICROS}us (banking any excess as "overdrift"), and resets the
 * accumulator. libsrt exposes the excess via a separate {@code overdrift()}
 * accessor that's only meaningful if read immediately after an {@code
 * update()} call that returned true — a fragile two-step protocol. This class
 * collapses that into a single {@link #update} return value carrying the same
 * information, a deliberate API simplification with no behavioral difference
 * from libsrt's {@code CLEAR_ON_UPDATE=true} semantics.
 *
 * <p>Not thread-safe, same as this package's other receive-side state.
 */
public final class DriftTracer {

    static final int MAX_SAMPLES = 1000;
    static final long MAX_DRIFT_MICROS = 5000;

    private long driftMicros;
    private long driftSumMicros;
    private int sampleCount;

    /**
     * Records one drift sample. Returns the timebase adjustment ("overdrift")
     * to apply exactly when this call completes a {@value #MAX_SAMPLES}-sample
     * span whose average exceeds +/-{@value #MAX_DRIFT_MICROS}us; empty
     * otherwise (including every call that doesn't complete a span).
     */
    public OptionalLong update(long sampleMicros) {
        driftSumMicros += sampleMicros;
        sampleCount++;

        if (sampleCount < MAX_SAMPLES) {
            return OptionalLong.empty();
        }

        driftMicros = driftSumMicros / sampleCount;
        driftSumMicros = 0;
        sampleCount = 0;

        if (Math.abs(driftMicros) > MAX_DRIFT_MICROS) {
            long overdriftMicros = driftMicros < 0 ? -MAX_DRIFT_MICROS : MAX_DRIFT_MICROS;
            driftMicros -= overdriftMicros;
            return OptionalLong.of(overdriftMicros);
        }

        return OptionalLong.empty();
    }

    /** The current running (post-clamp) drift estimate, in microseconds. */
    public long drift() {
        return driftMicros;
    }
}
