package org.brewstream.roast.util;

import java.util.Objects;

/**
 * A counter that wraps from {@code max} back to {@code 0} — SRT's 31-bit packet
 * sequence numbers and 32-bit timestamps/ACK numbers all behave this way on the
 * wire, so naive integer comparison breaks near the wrap point. Two circular
 * numbers only make sense to compare when they share the same {@code max} (the
 * same "domain"); comparisons across domains throw.
 *
 * <p>The trick, and its limitation: the maximum distance two circular numbers can
 * be apart and still be ordered sensibly is half of {@code max}. Given only two
 * values with no history of how they got there, "5 is less than max-3" could mean
 * 5 is behind (hasn't wrapped) or 5 is ahead (wrapped past max-3) — that's why this
 * type resolves it by treating whichever value is closer as authoritative and only
 * gives a well-defined answer within that half-range; comparisons of three or more
 * values spread further apart than that aren't guaranteed transitive, which is why
 * this class deliberately does not implement {@link Comparable}.
 *
 * <p>Ported from gosrt's {@code circular.Number} (github.com/datarhei/gosrt, MIT
 * licensed; see {@code roast/references/gosrt/circular/circular.go}).
 */
public final class CircularNumber {

    private final long max;
    private final long threshold;
    private final long value;

    private CircularNumber(long value, long max) {
        this.value = value;
        this.max = max;
        this.threshold = max / 2;
    }

    /**
     * Creates a circular number in the domain {@code [0, max]}. If {@code x} itself
     * exceeds {@code max}, it's treated as a step count added to zero (i.e. it wraps),
     * matching gosrt's {@code New}.
     */
    public static CircularNumber of(long x, long max) {
        if (x < 0) {
            throw new IllegalArgumentException("value must not be negative: " + x);
        }
        if (max <= 0) {
            throw new IllegalArgumentException("max must be positive: " + max);
        }
        return x > max ? new CircularNumber(0, max).add(x) : new CircularNumber(x, max);
    }

    public long value() {
        return value;
    }

    public long max() {
        return max;
    }

    /** Distance between two numbers in the same domain, always in {@code [0, max/2]}. */
    public long distance(CircularNumber other) {
        requireSameDomain(other);
        if (value == other.value) {
            return 0;
        }
        long d = Math.abs(value - other.value);
        return d >= threshold ? max - d + 1 : d;
    }

    public boolean lessThan(CircularNumber other) {
        requireSameDomain(other);
        if (value == other.value) {
            return false;
        }
        boolean thisIsSmallerRaw = value < other.value;
        long d = thisIsSmallerRaw ? other.value - value : value - other.value;
        return d < threshold ? thisIsSmallerRaw : !thisIsSmallerRaw;
    }

    public boolean lessThanOrEqual(CircularNumber other) {
        return value == other.value || lessThan(other);
    }

    public boolean greaterThan(CircularNumber other) {
        return !lessThanOrEqual(other);
    }

    public boolean greaterThanOrEqual(CircularNumber other) {
        return !lessThan(other);
    }

    public CircularNumber inc() {
        return new CircularNumber(value == max ? 0 : value + 1, max);
    }

    public CircularNumber dec() {
        return new CircularNumber(value == 0 ? max : value - 1, max);
    }

    public CircularNumber add(long n) {
        long remaining = max - value;
        long newValue = n <= remaining ? value + n : n - remaining - 1;
        return new CircularNumber(newValue, max);
    }

    public CircularNumber subtract(long n) {
        long newValue = n <= value ? value - n : max - (n - value) + 1;
        return new CircularNumber(newValue, max);
    }

    private void requireSameDomain(CircularNumber other) {
        if (max != other.max) {
            throw new IllegalArgumentException(
                    "circular numbers from different domains (max " + max + " vs " + other.max + ")");
        }
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof CircularNumber other && max == other.max && value == other.value;
    }

    @Override
    public int hashCode() {
        return Objects.hash(max, value);
    }

    @Override
    public String toString() {
        return "CircularNumber(" + value + "/" + max + ")";
    }
}
