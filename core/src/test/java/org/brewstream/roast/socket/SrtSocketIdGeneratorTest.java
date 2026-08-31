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

package org.brewstream.roast.socket;

import org.brewstream.roast.packet.SrtSocketId;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.PrimitiveIterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class SrtSocketIdGeneratorTest {

    private static SrtSocketIdGenerator withFixedSequence(int... values) {
        PrimitiveIterator.OfInt it = Arrays.stream(values).iterator();
        return new SrtSocketIdGenerator(it::nextInt);
    }

    @Test
    void skipsZeroSinceItIsReservedForInduction() {
        SrtSocketIdGenerator generator = withFixedSequence(0, 0, 42);

        SrtSocketId id = generator.generate();

        assertThat(id).isEqualTo(SrtSocketId.of(42));
        assertThat(id.isZero()).isFalse();
    }

    @Test
    void retriesOnCollisionWithAnAlreadyReservedId() {
        SrtSocketIdGenerator generator = withFixedSequence(7, 7, 7, 9);
        SrtSocketId first = generator.generate(); // consumes the first "7"

        SrtSocketId second = generator.generate(); // "7", "7" collide, then "9" succeeds

        assertThat(first).isEqualTo(SrtSocketId.of(7));
        assertThat(second).isEqualTo(SrtSocketId.of(9));
    }

    @Test
    void throwsAfterExhaustingAttemptsAgainstAnAlreadyReservedId() {
        SrtSocketIdGenerator generator = new SrtSocketIdGenerator(() -> 5);
        generator.generate(); // reserves 5

        // Every retry yields 5 again, which is now already in use.
        assertThatIllegalStateException().isThrownBy(generator::generate);
    }

    @Test
    void releaseAllowsTheIdToBeReusedAfterwards() {
        SrtSocketIdGenerator generator = withFixedSequence(11, 11);
        SrtSocketId id = generator.generate();
        assertThat(generator.isInUse(id)).isTrue();

        generator.release(id);

        assertThat(generator.isInUse(id)).isFalse();
        assertThat(generator.generate()).isEqualTo(id);
    }

    @Test
    void isInUseReflectsGeneratedAndUnreservedIds() {
        SrtSocketIdGenerator generator = withFixedSequence(21);
        SrtSocketId id = generator.generate();

        assertThat(generator.isInUse(id)).isTrue();
        assertThat(generator.isInUse(SrtSocketId.of(999))).isFalse();
    }

    @Test
    void defaultConstructorProducesDistinctNonZeroIds() {
        SrtSocketIdGenerator generator = new SrtSocketIdGenerator();
        Set<SrtSocketId> generated = new HashSet<>();

        for (int i = 0; i < 1000; i++) {
            SrtSocketId id = generator.generate();
            assertThat(id.isZero()).isFalse();
            generated.add(id);
        }

        assertThat(generated).hasSize(1000);
    }
}