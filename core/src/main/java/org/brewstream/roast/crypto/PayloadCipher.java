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

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;

/**
 * AES-CTR encryption and decryption of a DATA packet's payload —
 * draft-sharabayko-srt.md §6.1.2 ("AES Counter"), §6.2.2 and §6.3.2. One method
 * does both directions, because CTR mode is its own inverse: it produces a
 * keystream from the counter and XORs it with whatever it's given.
 *
 * <p><b>The counter is the whole trick</b>, and it's what a reference test is
 * worth having for. Per §6.1.2 it is 16 bytes:
 * <pre>
 *   bytes  0..13 : the salt's 14 most significant bytes (the 112-bit nonce)
 *   bytes 10..13 : XORed with the packet sequence number, big-endian
 *   bytes 14..15 : the block counter, always zero here
 * </pre>
 * So the sequence number overlaps the tail of the nonce rather than sitting
 * beside it — every packet gets a distinct keystream without any per-packet
 * state, which is exactly what a lossy live protocol needs (a receiver can
 * decrypt packet N without having seen N-1). Note the last two salt bytes are
 * deliberately unused.
 *
 * <p><b>Deliberately stateless about keys.</b> The caller passes the Stream
 * Encrypting Key to use; choosing between the even and odd SEK, and rotating
 * them, is connection-level key management that doesn't exist yet and doesn't
 * belong here. gosrt's equivalent reaches into its own {@code evenSEK}/{@code
 * oddSEK} fields and takes a key-selector argument instead — the split here
 * keeps this class a pure function.
 *
 * <p>Verified against gosrt's own {@code crypto_test.go}
 * {@code TestEncode}/{@code TestDecode} golden vectors — a real 1316-byte
 * MPEG-TS payload, all three key lengths, both keys, checked in both
 * directions. The vectors live in {@code src/test/resources/crypto/} rather
 * than inline, since they run to ~19KB of hex.
 */
public final class PayloadCipher {

    /** The counter's nonce is the salt's 14 most significant bytes (112 bits). */
    private static final int NONCE_BYTES = 14;

    /** Where the packet sequence number is XORed into the counter. */
    private static final int SEQUENCE_NUMBER_OFFSET = 10;

    private static final int COUNTER_BYTES = 16;
    private static final int SALT_BYTES = 16;

    private PayloadCipher() {
    }

    /**
     * Encrypts or decrypts {@code payload} in place — the same call does both.
     * {@code sek} is the Stream Encrypting Key (16, 24, or 32 bytes, unwrapped
     * from a Key Material message by {@link StreamKeyWrapper}); {@code salt} is
     * that message's 16-byte salt; {@code packetSequenceNumber} is the DATA
     * packet's own sequence number, which is what makes each packet's keystream
     * distinct.
     *
     * @throws IllegalArgumentException if the salt isn't 16 bytes or the key
     *         isn't a legal AES size
     */
    public static void encryptOrDecrypt(byte[] payload, byte[] sek, byte[] salt, int packetSequenceNumber) {
        if (salt.length != SALT_BYTES) {
            throw new IllegalArgumentException("salt must be " + SALT_BYTES + " bytes, got " + salt.length);
        }
        if (sek.length != 16 && sek.length != 24 && sek.length != 32) {
            throw new IllegalArgumentException("key length must be 16, 24, or 32 bytes, got " + sek.length);
        }

        byte[] counter = counterFor(salt, packetSequenceNumber);
        try {
            Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");
            cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(sek, "AES"), new IvParameterSpec(counter));
            // CTR is a stream cipher: output length equals input length, so this
            // fills the array it was given rather than needing a fresh one.
            cipher.doFinal(payload, 0, payload.length, payload, 0);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("AES-CTR failed", e);
        }
    }

    /**
     * As {@link #encryptOrDecrypt(byte[], byte[], byte[], int)}, but operating on
     * a packet payload in place. The buffer's reader and writer indices are
     * unchanged — only the bytes between them are rewritten.
     */
    public static void encryptOrDecrypt(ByteBuf payload, byte[] sek, byte[] salt, int packetSequenceNumber) {
        int length = payload.readableBytes();
        if (length == 0) {
            return;
        }
        byte[] bytes = new byte[length];
        payload.getBytes(payload.readerIndex(), bytes);
        encryptOrDecrypt(bytes, sek, salt, packetSequenceNumber);
        payload.setBytes(payload.readerIndex(), bytes);
    }

    /**
     * A reusable cipher bound to one key, for the per-packet path.
     *
     * <p>The static methods above call {@code Cipher.getInstance} per call, which
     * means a JCA provider lookup, a fresh {@code Cipher}, a {@code
     * SecretKeySpec} and an {@code IvParameterSpec} for every packet — at live
     * rates, tens of thousands of provider lookups a second on the event loop.
     * The original design notes warned against exactly this. A session holds the
     * cipher and the key, and re-initialises only the IV, which is all that
     * changes between packets.
     *
     * <p>Only the IV can be reused safely this way: AES-CTR's security depends on
     * never repeating a counter under one key, and the counter here is derived
     * from the packet sequence number, so it varies per packet by construction.
     * Reusing the {@code Cipher} object changes nothing about that.
     *
     * <p>Not thread-safe — one per connection, on its event loop, like the rest
     * of this package's state.
     */
    public static final class Session {

        private final Cipher cipher;
        private final SecretKeySpec key;
        private byte[] scratch = EMPTY;

        /**
         * @param sek the stream encrypting key, 16, 24 or 32 bytes
         */
        public Session(byte[] sek) {
            if (sek.length != 16 && sek.length != 24 && sek.length != 32) {
                throw new IllegalArgumentException("key length must be 16, 24, or 32 bytes, got " + sek.length);
            }
            try {
                this.cipher = Cipher.getInstance("AES/CTR/NoPadding");
            } catch (GeneralSecurityException e) {
                throw new IllegalStateException("AES-CTR unavailable", e);
            }
            this.key = new SecretKeySpec(sek, "AES");
        }

        /** Transforms {@code payload} in place. Encryption and decryption are the same operation in CTR. */
        public void apply(byte[] payload, byte[] salt, int packetSequenceNumber) {
            if (salt.length != SALT_BYTES) {
                throw new IllegalArgumentException("salt must be " + SALT_BYTES + " bytes, got " + salt.length);
            }
            try {
                cipher.init(Cipher.ENCRYPT_MODE, key,
                        new IvParameterSpec(counterFor(salt, packetSequenceNumber)));
                cipher.doFinal(payload, 0, payload.length, payload, 0);
            } catch (GeneralSecurityException e) {
                throw new IllegalStateException("AES-CTR failed", e);
            }
        }

        /**
         * As {@link #apply(byte[], byte[], int)}, for a {@link ByteBuf}. Reuses one
         * growable scratch array rather than allocating per packet; the buffer's
         * indices are unchanged.
         */
        public void apply(ByteBuf payload, byte[] salt, int packetSequenceNumber) {
            int length = payload.readableBytes();
            if (length == 0) {
                return;
            }
            if (scratch.length < length) {
                scratch = new byte[length];
            }
            payload.getBytes(payload.readerIndex(), scratch, 0, length);
            try {
                cipher.init(Cipher.ENCRYPT_MODE, key,
                        new IvParameterSpec(counterFor(salt, packetSequenceNumber)));
                cipher.doFinal(scratch, 0, length, scratch, 0);
            } catch (GeneralSecurityException e) {
                throw new IllegalStateException("AES-CTR failed", e);
            }
            payload.setBytes(payload.readerIndex(), scratch, 0, length);
        }
    }

    private static final byte[] EMPTY = new byte[0];

    /** Visible for testing the counter construction directly — see the class javadoc for the layout. */
    static byte[] counterFor(byte[] salt, int packetSequenceNumber) {
        byte[] counter = new byte[COUNTER_BYTES];

        counter[SEQUENCE_NUMBER_OFFSET] = (byte) (packetSequenceNumber >>> 24);
        counter[SEQUENCE_NUMBER_OFFSET + 1] = (byte) (packetSequenceNumber >>> 16);
        counter[SEQUENCE_NUMBER_OFFSET + 2] = (byte) (packetSequenceNumber >>> 8);
        counter[SEQUENCE_NUMBER_OFFSET + 3] = (byte) packetSequenceNumber;

        for (int i = 0; i < NONCE_BYTES; i++) {
            counter[i] ^= salt[i];
        }
        // The final two bytes stay zero: that's the block counter, which AES-CTR
        // increments itself as it walks the payload.
        return counter;
    }
}