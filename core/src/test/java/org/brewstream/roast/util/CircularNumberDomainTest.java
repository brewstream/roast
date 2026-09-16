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

package org.brewstream.roast.util;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The domain invariant: a CircularNumber's value is always in [0, max].
 *
 * <p>Split out from {@code CircularNumberTest}, which covers ordinary use. These
 * cases came from an external review and are about arithmetic larger than one
 * turn of the circle — which no call site performs today, every one of them
 * stepping by small deltas. That is exactly why the old implementation survived:
 * it handled a single wrap correctly and broke on the second. A public utility
 * whose stated contract is a bounded domain should hold that contract for any
 * input it accepts, not only the inputs it currently gets.
 */
class CircularNumberDomainTest {

    private static final long MAX = 0x7FFF_FFFFL;

    private static void assertInDomain(CircularNumber n) {
        assertThat(n.value()).isBetween(0L, MAX);
    }

    @Test
    void addingMoreThanOneFullWrapStaysInDomain() {
        CircularNumber start = CircularNumber.of(10, MAX);

        assertInDomain(start.add(2 * MAX + 5));
        assertInDomain(start.add(MAX + 1));
        assertInDomain(start.add(Long.MAX_VALUE / 2));
    }

    @Test
    void subtractingMoreThanTheValueStaysInDomain() {
        CircularNumber start = CircularNumber.of(10, MAX);

        assertInDomain(start.subtract(3 * MAX));
        assertInDomain(start.subtract(MAX + 1));
        assertInDomain(start.subtract(Long.MAX_VALUE / 2));
    }

    @Test
    void ofAcceptsValuesBeyondMaxAndFoldsThemIntoDomain() {
        assertInDomain(CircularNumber.of(2 * MAX + 5, MAX));
        assertInDomain(CircularNumber.of(MAX + 1, MAX));
    }

    /** Exactly one full turn returns where it started. */
    @Test
    void addingAFullWrapIsIdentity() {
        CircularNumber start = CircularNumber.of(12345, MAX);

        assertThat(start.add(MAX + 1)).isEqualTo(start);
        assertThat(start.subtract(MAX + 1)).isEqualTo(start);
    }

    /** add and subtract remain inverses across wraps. */
    @Test
    void addAndSubtractAreInversesAcrossWraps() {
        CircularNumber start = CircularNumber.of(7, MAX);

        for (long n : new long[]{1, MAX - 1, MAX, MAX + 1, 2 * MAX + 5}) {
            assertThat(start.add(n).subtract(n)).as("delta %d", n).isEqualTo(start);
        }
    }

    /** The behaviour everything already relies on must not change. */
    @Test
    void singleStepWrappingIsUnchanged() {
        assertThat(CircularNumber.of(MAX, MAX).add(1).value()).isZero();
        assertThat(CircularNumber.of(0, MAX).subtract(1).value()).isEqualTo(MAX);
        assertThat(CircularNumber.of(5, MAX).add(3).value()).isEqualTo(8);
        assertThat(CircularNumber.of(8, MAX).subtract(3).value()).isEqualTo(5);
    }
}