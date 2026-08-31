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

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;
import java.util.Arrays;

/**
 * Turns a passphrase into the Key Encrypting Key (KEK), and uses it to wrap and
 * unwrap the Stream Encrypting Key(s) that travel in a
 * {@link org.brewstream.roast.packet.cif.KeyMaterialCif}'s
 * {@code wrap} field — draft-sharabayko-srt.md §6.1.4 ("Key Encrypting Key")
 * and §6.1.5 ("Stream Encrypting Key"). This is the step that makes that field
 * meaningful; the CIF codec itself deliberately treats it as opaque bytes.
 *
 * <p>Two details here are easy to get wrong and are the whole reason this is
 * verified against a reference rather than reasoned about:
 * <ul>
 *   <li><b>Only the last 8 bytes of the salt feed PBKDF2.</b> The KM message
 *       carries a 16-byte salt, but the KEK derivation uses {@code salt[8:]}
 *       (gosrt's {@code calculateKEK}; libsrt does the same). Passing the whole
 *       salt produces a perfectly plausible-looking key that no other
 *       implementation agrees with.</li>
 *   <li><b>The derived KEK is as long as the SEK it protects</b> — 16, 24, or
 *       32 bytes — not a fixed width. 2048 iterations, HMAC-SHA-1.</li>
 * </ul>
 *
 * <p>Both primitives are JDK built-ins ({@code PBKDF2WithHmacSHA1} and the
 * {@code AESWrap} cipher, i.e. the AES Key Wrap of RFC 3394), so unlike gosrt —
 * which pulls in an external keywrap package — this needs no third-party
 * dependency. AES Key Wrap adds 8 bytes of integrity check value, which is why
 * a wrapped key is always {@code n * keyLength + 8} bytes; a wrong passphrase
 * fails that check on unwrap rather than silently yielding garbage, which is
 * exactly how a peer distinguishes "bad secret" from "no secret".
 *
 * <p>Verified against gosrt's own {@code crypto_test.go} golden vectors
 * ({@code TestMarshal}/{@code TestUnmarshal}) for all three key lengths and all
 * three key selections — real cross-implementation proof. Note those tests
 * exercise KEK derivation and key wrapping <em>together</em>; there is no
 * isolated PBKDF2 vector in either reference, which is why this class covers
 * both operations rather than splitting them.
 *
 * <p>Stateless and thread-safe: every method takes everything it needs.
 */
public final class StreamKeyWrapper {

    /** draft-sharabayko-srt.md §6.1.4: "The PBKDF2 iteration count is 2048." */
    private static final int PBKDF2_ITERATIONS = 2048;

    /** The KM salt is 16 bytes; only this many trailing bytes are fed to PBKDF2. */
    private static final int PBKDF2_SALT_BYTES = 8;

    /** AES Key Wrap (RFC 3394) appends an 8-byte integrity check value. */
    public static final int WRAP_OVERHEAD = 8;

    /** RFC 3394 operates on 64-bit blocks, so a wrap is always a multiple of this. */
    private static final int WRAP_BLOCK_BYTES = 8;

    private StreamKeyWrapper() {
    }

    /**
     * Derives the KEK from {@code passphrase} and the KM message's {@code salt}.
     * {@code keyLength} is the SEK length in bytes (16, 24, or 32) — the KEK
     * matches it.
     *
     * @throws IllegalArgumentException if the salt is shorter than the 8 bytes
     *         the derivation actually consumes, or the key length isn't a legal
     *         AES size
     */
    public static byte[] deriveKeyEncryptingKey(String passphrase, byte[] salt, int keyLength) {
        return deriveKeyEncryptingKey(passphrase.toCharArray(), salt, keyLength);
    }

    /**
     * As {@link #deriveKeyEncryptingKey(String, byte[], int)}, but taking the
     * passphrase as a {@code char[]} the caller can zero afterwards — a
     * {@code String} would sit in the heap, immutable and un-clearable, for as
     * long as the JVM felt like keeping it. This is the overload
     * {@code EncryptionContext} uses; the {@code String} one exists for tests
     * and for callers that already have one.
     */
    public static byte[] deriveKeyEncryptingKey(char[] passphrase, byte[] salt, int keyLength) {
        requireLegalKeyLength(keyLength);
        if (salt.length < PBKDF2_SALT_BYTES) {
            throw new IllegalArgumentException(
                    "salt must be at least " + PBKDF2_SALT_BYTES + " bytes, got " + salt.length);
        }

        // Only the trailing 8 bytes - see the class javadoc.
        byte[] pbkdf2Salt = Arrays.copyOfRange(salt, salt.length - PBKDF2_SALT_BYTES, salt.length);
        PBEKeySpec spec = new PBEKeySpec(
                passphrase, pbkdf2Salt, PBKDF2_ITERATIONS, keyLength * Byte.SIZE);
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1");
            return factory.generateSecret(spec).getEncoded();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("PBKDF2WithHmacSHA1 unavailable", e);
        } finally {
            spec.clearPassword();
        }
    }

    /**
     * Wraps one or two SEKs with the KEK. Pass the keys already concatenated
     * (even then odd) when wrapping both, matching how they sit in the KM
     * message. The result is {@code keys.length + } {@value #WRAP_OVERHEAD}
     * bytes.
     */
    public static byte[] wrap(byte[] keyEncryptingKey, byte[] keys) {
        try {
            Cipher cipher = Cipher.getInstance("AESWrap");
            cipher.init(Cipher.WRAP_MODE, new SecretKeySpec(keyEncryptingKey, "AES"));
            return cipher.wrap(new SecretKeySpec(keys, "AES"));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES key wrap failed", e);
        }
    }

    /**
     * Reverses {@link #wrap}. Returns {@code null} — rather than throwing — when
     * the integrity check fails, which is the expected outcome for a wrong
     * passphrase and something a peer must answer with a "bad secret" rejection
     * rather than treat as a crash. A structurally impossible input (a wrap that
     * isn't a whole number of 8-byte blocks, say) also returns {@code null},
     * matching this codebase's "drop what arrived malformed" contract for
     * anything read off the wire.
     */
    public static byte[] unwrap(byte[] keyEncryptingKey, byte[] wrapped) {
        if (wrapped.length <= WRAP_OVERHEAD || wrapped.length % WRAP_BLOCK_BYTES != 0) {
            return null;
        }
        try {
            Cipher cipher = Cipher.getInstance("AESWrap");
            cipher.init(Cipher.UNWRAP_MODE, new SecretKeySpec(keyEncryptingKey, "AES"));
            SecretKey unwrapped = (SecretKey) cipher.unwrap(wrapped, "AES", Cipher.SECRET_KEY);
            return unwrapped.getEncoded();
        } catch (GeneralSecurityException e) {
            return null; // wrong passphrase, or a corrupted wrap
        }
    }

    /**
     * Convenience for the common path: derive the KEK from the passphrase and
     * unwrap in one step. Returns {@code null} if the passphrase is wrong.
     */
    public static byte[] deriveAndUnwrap(String passphrase, byte[] salt, int keyLength, byte[] wrapped) {
        return unwrap(deriveKeyEncryptingKey(passphrase, salt, keyLength), wrapped);
    }

    private static void requireLegalKeyLength(int keyLength) {
        if (keyLength != 16 && keyLength != 24 && keyLength != 32) {
            throw new IllegalArgumentException("key length must be 16, 24, or 32 bytes, got " + keyLength);
        }
    }
}