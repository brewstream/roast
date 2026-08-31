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

package org.brewstream.roast.packet.cif;

import io.netty.buffer.ByteBuf;

import java.util.Arrays;

/**
 * The Key Material message carried by the KMREQ/KMRSP handshake extensions
 * (draft-sharabayko-srt.md §3.2.2 "Key Material"): a 16-byte fixed header, an
 * optional 16-byte salt, then the wrapped Stream Encrypting Key(s). This is the
 * <em>wire format only</em> — deriving the key-encrypting key from a passphrase
 * (PBKDF2), wrapping/unwrapping the SEKs (AES key wrap), and encrypting payloads
 * (AES-CTR) are all Phase 5 work this class deliberately does not do. It carries
 * {@link #wrap()} as opaque bytes exactly as it found them.
 *
 * <p><b>Two shapes on the wire.</b> A normal message is at least 16 bytes; a
 * <em>rejection</em> is exactly 4 bytes carrying only an error code. Rather than
 * a separate type, this follows the same shape {@link HandshakeCif} already uses
 * for its rejections — one record, with {@link #isError()}/{@link #errorCode()}
 * distinguishing the two — so callers handle both the way they already handle a
 * rejected handshake. The only two legal codes are
 * {@link #ERROR_NO_SECRET} and {@link #ERROR_BAD_SECRET} (gosrt's
 * {@code KM_NOSECRET}/{@code KM_BADSECRET}); {@link #decode} rejects any other.
 *
 * <p><b>Fixed fields aren't carried.</b> S, Version, Packet Type, the {@code
 * HAI} signature, the KEK index, Stream Encapsulation, and the three reserved
 * fields each have exactly one legal value, so {@link #decode} validates them
 * and {@link #encodeTo} writes them back as constants instead of round-tripping
 * them through the record — carrying a field that can only ever hold one value
 * adds nothing for a caller to inspect. This is the same call {@link AckCif}
 * makes about its own always-derived fields, and differs from gosrt, whose
 * {@code CIFKeyMaterialExtension} struct keeps every one of them as a field.
 *
 * <p>Verified byte-for-byte against gosrt's own {@code TestKM} golden vector
 * ({@code packet/handshake_test.go}) — real cross-implementation proof, the same
 * tier as {@link LossListCodec}/{@link HandshakeCif}/{@link AckCif}.
 *
 * <p><b>Note on {@code equals}</b>: this record holds {@code byte[]} fields, so
 * the generated {@code equals}/{@code hashCode} compare array <em>identity</em>,
 * not contents. Compare {@link #salt()}/{@link #wrap()} explicitly (e.g.
 * AssertJ's {@code isEqualTo} on the arrays) rather than comparing two records.
 */
public record KeyMaterialCif(
        int errorCode,
        KeyEncryption keyEncryption,
        int cipher,
        int authentication,
        byte[] salt,
        byte[] wrap) {

    /** The peer has no passphrase configured while we asked for encryption (gosrt's {@code KM_NOSECRET}). */
    public static final int ERROR_NO_SECRET = 3;

    /** The peer's passphrase doesn't match ours (gosrt's {@code KM_BADSECRET}). */
    public static final int ERROR_BAD_SECRET = 4;

    /** AES-CTR — the only cipher SRT defines for payload encryption ({@code 0} means "none"). */
    public static final int CIPHER_AES_CTR = 2;

    private static final int HEADER_LENGTH = 16;
    private static final int ERROR_LENGTH = 4;
    private static final int SALT_LENGTH = 16;
    /** The wrap is the key(s) plus an 8-byte AES-key-wrap integrity check value. */
    private static final int WRAP_OVERHEAD = 8;

    private static final int VERSION = 1;
    private static final int PACKET_TYPE_KEY_MATERIAL = 2;
    private static final int SIGNATURE_HAI = 0x2029;
    private static final int STREAM_ENCAPSULATION_MPEGTS_SRT = 2;

    /** A 4-byte error response — see {@link #isError()}. */
    public static KeyMaterialCif error(int errorCode) {
        return new KeyMaterialCif(errorCode, null, 0, 0, new byte[0], new byte[0]);
    }

    /** True if this is the 4-byte rejection form, in which case every other field is unset. */
    public boolean isError() {
        return errorCode != 0;
    }

    /** The single key length in bytes, derived from {@link #wrap()} — 16, 24, or 32. */
    public int keyLength() {
        return (wrap.length - WRAP_OVERHEAD) / keyEncryption.keyCount();
    }

    /**
     * Decodes a Key Material message. Returns {@code null} for anything
     * malformed — a truncated buffer, a reserved field carrying an unexpected
     * value, an unusable KK/key length, or an unrecognized error code — matching
     * this package's established "drop it, don't throw" contract for data that
     * arrived off the wire ({@link HandshakeCif#decode}, {@code SrtPacket.decode}).
     * Consumes the bytes it reads.
     */
    public static KeyMaterialCif decode(ByteBuf in) {
        int readable = in.readableBytes();

        if (readable == ERROR_LENGTH) {
            // gosrt reads this one little-endian, unlike every other field here.
            int errorCode = in.readIntLE();
            if (errorCode != ERROR_NO_SECRET && errorCode != ERROR_BAD_SECRET) {
                return null;
            }
            return error(errorCode);
        }

        if (readable < HEADER_LENGTH) {
            return null;
        }

        int first = in.readUnsignedByte();
        if ((first & 0b1000_0000) != 0                                  // S
                || ((first & 0b0111_0000) >>> 4) != VERSION
                || (first & 0b0000_1111) != PACKET_TYPE_KEY_MATERIAL) {
            return null;
        }
        if (in.readUnsignedShort() != SIGNATURE_HAI) {
            return null;
        }

        int fourth = in.readUnsignedByte();
        if ((fourth & 0b1111_1100) != 0) { // Resv1
            return null;
        }
        KeyEncryption keyEncryption = KeyEncryption.fromCode(fourth & 0b0000_0011);
        if (keyEncryption == null) {
            return null;
        }

        if (in.readInt() != 0) { // KEK index: 0 is the stream's default key, the only value in use
            return null;
        }

        int cipher = in.readUnsignedByte();
        int authentication = in.readUnsignedByte();
        if (in.readUnsignedByte() != STREAM_ENCAPSULATION_MPEGTS_SRT) {
            return null;
        }
        if (in.readUnsignedByte() != 0 || in.readUnsignedShort() != 0) { // Resv2, Resv3
            return null;
        }

        int saltLength = in.readUnsignedByte() * 4;
        int keyLength = in.readUnsignedByte() * 4;
        if (keyLength != 16 && keyLength != 24 && keyLength != 32) {
            return null;
        }
        if (saltLength != 0 && saltLength != SALT_LENGTH) { // "The only valid length of salt defined is 128 bits."
            return null;
        }

        byte[] salt = new byte[saltLength];
        if (in.readableBytes() < saltLength) {
            return null;
        }
        in.readBytes(salt);

        int wrapLength = keyEncryption.keyCount() * keyLength + WRAP_OVERHEAD;
        if (in.readableBytes() < wrapLength) {
            return null;
        }
        byte[] wrap = new byte[wrapLength];
        in.readBytes(wrap);

        return new KeyMaterialCif(0, keyEncryption, cipher, authentication, salt, wrap);
    }

    /** Encodes this message into {@code out} at its current writer index. */
    public void encodeTo(ByteBuf out) {
        if (isError()) {
            out.writeIntLE(errorCode);
            return;
        }

        out.writeByte((VERSION << 4) | PACKET_TYPE_KEY_MATERIAL); // S = 0
        out.writeShort(SIGNATURE_HAI);
        out.writeByte(keyEncryption.code()); // Resv1 = 0
        out.writeInt(0); // KEK index
        out.writeByte(cipher);
        out.writeByte(authentication);
        out.writeByte(STREAM_ENCAPSULATION_MPEGTS_SRT);
        out.writeByte(0); // Resv2
        out.writeShort(0); // Resv3
        out.writeByte(salt.length / 4);
        out.writeByte(keyLength() / 4);
        out.writeBytes(salt);
        out.writeBytes(wrap);
    }

    @Override
    public String toString() {
        if (isError()) {
            return "KeyMaterialCif[error=" + errorCode + "]";
        }
        return "KeyMaterialCif[" + keyEncryption + ", cipher=" + cipher
                + ", authentication=" + authentication + ", saltLength=" + salt.length
                + ", keyLength=" + keyLength() + "]";
    }

    /** Contents-based, unlike the record's generated {@code equals} — see the class javadoc. */
    public boolean hasSameContentAs(KeyMaterialCif other) {
        return other != null
                && errorCode == other.errorCode
                && keyEncryption == other.keyEncryption
                && cipher == other.cipher
                && authentication == other.authentication
                && Arrays.equals(salt, other.salt)
                && Arrays.equals(wrap, other.wrap);
    }
}