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

package org.brewstream.roast.crypto;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The reusable cipher must produce byte-identical output to the one-shot form.
 *
 * <p>That equivalence is the whole safety argument for reusing a {@code Cipher}:
 * the static path is what was verified against gosrt's golden vectors, so if the
 * session agrees with it on every input, the session is verified too. Checked
 * across payload sizes, key lengths, and — most importantly — repeated use of
 * one session, since a stale IV or a dirty scratch buffer would only show from
 * the second packet onward.
 */
class PayloadCipherSessionTest {

    private static byte[] bytes(int length, long seed) {
        byte[] out = new byte[length];
        new Random(seed).nextBytes(out);
        return out;
    }

    @Test
    void aSessionMatchesTheOneShotFormForEveryKeyLength() {
        byte[] salt = bytes(16, 1);
        for (int keyLength : new int[]{16, 24, 32}) {
            byte[] sek = bytes(keyLength, keyLength);
            byte[] plain = bytes(1316, 7);

            byte[] viaStatic = plain.clone();
            PayloadCipher.encryptOrDecrypt(viaStatic, sek, salt, 42);

            byte[] viaSession = plain.clone();
            new PayloadCipher.Session(sek).apply(viaSession, salt, 42);

            assertThat(viaSession).as("key length %d", keyLength).isEqualTo(viaStatic);
        }
    }

    /**
     * The case reuse could plausibly break: a session used repeatedly must
     * re-derive the IV per packet, not carry the previous one forward.
     */
    @Test
    void repeatedUseOfOneSessionMatchesFreshOneShotCallsEachTime() {
        byte[] salt = bytes(16, 2);
        byte[] sek = bytes(16, 3);
        PayloadCipher.Session session = new PayloadCipher.Session(sek);

        for (int sequenceNumber = 0; sequenceNumber < 50; sequenceNumber++) {
            byte[] plain = bytes(1316, sequenceNumber);

            byte[] expected = plain.clone();
            PayloadCipher.encryptOrDecrypt(expected, sek, salt, sequenceNumber);

            byte[] actual = plain.clone();
            session.apply(actual, salt, sequenceNumber);

            assertThat(actual).as("packet %d", sequenceNumber).isEqualTo(expected);
        }
    }

    /** Varying payload sizes, since the scratch buffer is reused and grows. */
    @Test
    void theScratchBufferIsReusedWithoutLeakingBetweenPackets() {
        byte[] salt = bytes(16, 4);
        byte[] sek = bytes(32, 5);
        PayloadCipher.Session session = new PayloadCipher.Session(sek);

        // Large first, then small: a stale tail from the longer payload would
        // corrupt the shorter one if the scratch buffer were used carelessly.
        for (int length : new int[]{1456, 16, 1316, 1, 188}) {
            byte[] plain = bytes(length, length);

            byte[] expected = plain.clone();
            PayloadCipher.encryptOrDecrypt(expected, sek, salt, length);

            ByteBuf buffer = Unpooled.buffer().writeBytes(plain);
            session.apply(buffer, salt, length);
            byte[] actual = new byte[length];
            buffer.getBytes(buffer.readerIndex(), actual);

            assertThat(actual).as("payload of %d bytes", length).isEqualTo(expected);
            assertThat(buffer.readableBytes()).as("indices untouched").isEqualTo(length);
        }
    }

    @Test
    void encryptingThenDecryptingWithTwoSessionsReturnsTheOriginal() {
        byte[] salt = bytes(16, 6);
        byte[] sek = bytes(24, 7);
        byte[] original = bytes(1316, 8);

        byte[] payload = original.clone();
        new PayloadCipher.Session(sek).apply(payload, salt, 99);
        assertThat(payload).isNotEqualTo(original);

        new PayloadCipher.Session(sek).apply(payload, salt, 99);
        assertThat(payload).isEqualTo(original);
    }
}