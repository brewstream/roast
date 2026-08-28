package org.brewstream.roast.util;

import org.brewstream.roast.packet.SrtPacket;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Boundary cases mirror gosrt's {@code circular_test.go}, plus SRT's actual wire
 * domains ({@link SrtPacket#MAX_SEQUENCE_NUMBER}, {@code MAX_TIMESTAMP}) rather than
 * only the generic 32-bit max gosrt's own tests use.
 */
class CircularNumberTest {

    private static final long MAX = 0xFFFF_FFFFL;

    @Test
    void incWithoutWrap() {
        CircularNumber a = CircularNumber.of(42, MAX);
        assertThat(a.inc().value()).isEqualTo(43);
    }

    @Test
    void incWraps() {
        CircularNumber a = CircularNumber.of(MAX - 1, MAX);
        assertThat(a.inc().value()).isEqualTo(MAX);
        assertThat(a.inc().inc().value()).isEqualTo(0);
    }

    @Test
    void decWithoutWrap() {
        CircularNumber a = CircularNumber.of(42, MAX);
        assertThat(a.dec().value()).isEqualTo(41);
    }

    @Test
    void decWraps() {
        CircularNumber a = CircularNumber.of(0, MAX);
        assertThat(a.dec().value()).isEqualTo(MAX);
        assertThat(a.dec().dec().value()).isEqualTo(MAX - 1);
    }

    @Test
    void distanceWithoutWrap() {
        CircularNumber a = CircularNumber.of(42, MAX);
        CircularNumber b = CircularNumber.of(50, MAX);
        assertThat(a.distance(b)).isEqualTo(8);
        assertThat(b.distance(a)).isEqualTo(8);
    }

    @Test
    void distanceAcrossWrap() {
        CircularNumber a = CircularNumber.of(2, MAX);
        CircularNumber b = CircularNumber.of(MAX - 2, MAX);
        assertThat(a.distance(b)).isEqualTo(5);
        assertThat(b.distance(a)).isEqualTo(5);
    }

    @Test
    void lessThan() {
        CircularNumber a = CircularNumber.of(42, MAX);
        CircularNumber b = CircularNumber.of(50, MAX);
        CircularNumber c = CircularNumber.of(MAX - 10, MAX);

        assertThat(a.lessThan(b)).isTrue();
        assertThat(b.lessThan(a)).isFalse();
        // c is "behind" a once wraparound is accounted for, even though its raw value is larger.
        assertThat(a.lessThan(c)).isFalse();
        assertThat(c.lessThan(a)).isTrue();
    }

    @Test
    void greaterThan() {
        CircularNumber a = CircularNumber.of(42, MAX);
        CircularNumber b = CircularNumber.of(50, MAX);
        CircularNumber c = CircularNumber.of(MAX - 10, MAX);

        assertThat(a.greaterThan(b)).isFalse();
        assertThat(b.greaterThan(a)).isTrue();
        assertThat(a.greaterThan(c)).isTrue();
        assertThat(c.greaterThan(a)).isFalse();
    }

    @Test
    void lessThanOrEqualAndGreaterThanOrEqualAgreeOnEquality() {
        CircularNumber a = CircularNumber.of(42, MAX);
        CircularNumber b = CircularNumber.of(42, MAX);

        assertThat(a.lessThanOrEqual(b)).isTrue();
        assertThat(a.greaterThanOrEqual(b)).isTrue();
        assertThat(a.lessThan(b)).isFalse();
        assertThat(a.greaterThan(b)).isFalse();
    }

    @Test
    void addWraps() {
        CircularNumber a = CircularNumber.of(MAX - 42, MAX);
        a = a.add(42);
        assertThat(a.value()).isEqualTo(MAX);
        a = a.add(1);
        assertThat(a.value()).isEqualTo(0);
    }

    @Test
    void subtractWraps() {
        CircularNumber a = CircularNumber.of(42, MAX);
        a = a.subtract(42);
        assertThat(a.value()).isEqualTo(0);
        a = a.subtract(1);
        assertThat(a.value()).isEqualTo(MAX);
    }

    @Test
    void ofNormalizesAValueLargerThanMaxByWrapping() {
        // Mirrors gosrt's New(): a value beyond max is treated as a step count from 0.
        CircularNumber a = CircularNumber.of(SrtPacket.MAX_SEQUENCE_NUMBER + 5, SrtPacket.MAX_SEQUENCE_NUMBER);
        assertThat(a.value()).isEqualTo(4);
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, -100})
    void ofRejectsNegativeValues(long negative) {
        assertThatIllegalArgumentException().isThrownBy(() -> CircularNumber.of(negative, MAX));
    }

    @Test
    void comparingAcrossDomainsThrows() {
        CircularNumber seq = CircularNumber.of(1, SrtPacket.MAX_SEQUENCE_NUMBER);
        CircularNumber timestamp = CircularNumber.of(1, SrtPacket.MAX_TIMESTAMP);

        assertThatIllegalArgumentException().isThrownBy(() -> seq.lessThan(timestamp));
    }

    @Test
    void sequenceNumberWrapBoundaryBehavesCorrectly() {
        long max = SrtPacket.MAX_SEQUENCE_NUMBER;
        CircularNumber justBeforeWrap = CircularNumber.of(max, max);
        CircularNumber justAfterWrap = justBeforeWrap.inc();

        assertThat(justAfterWrap.value()).isEqualTo(0);
        assertThat(justBeforeWrap.lessThan(justAfterWrap)).isTrue();
        assertThat(justAfterWrap.greaterThan(justBeforeWrap)).isTrue();
        assertThat(justBeforeWrap.distance(justAfterWrap)).isEqualTo(1);
    }

    @Test
    void equalsAndHashCodeConsiderBothValueAndDomain() {
        CircularNumber a = CircularNumber.of(5, MAX);
        CircularNumber sameDomain = CircularNumber.of(5, MAX);
        CircularNumber differentDomain = CircularNumber.of(5, SrtPacket.MAX_SEQUENCE_NUMBER);

        assertThat(a).isEqualTo(sameDomain).hasSameHashCodeAs(sameDomain);
        assertThat(a).isNotEqualTo(differentDomain);
    }
}
