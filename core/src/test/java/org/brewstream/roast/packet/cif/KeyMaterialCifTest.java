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
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The golden vector is gosrt's own TestKM (packet/handshake_test.go) - real
 * cross-implementation proof, same tier as LossListCodecTest/HandshakeCifTest/
 * AckCifTest rather than a self-designed scenario.
 */
class KeyMaterialCifTest {

    /** gosrt's TestKM asserts exactly this hex for the CIF built below. */
    private static final String GOSRT_GOLDEN_HEX =
            "122029010000000002000200000004040102030405060708090a0b0c0d0e0f10"
                    + "f0f1f2f3f4f5f6f71112131415161718191a1b1c1d1e1f20";

    private static final byte[] SALT = hex("0102030405060708090a0b0c0d0e0f10");
    private static final byte[] WRAP = hex("f0f1f2f3f4f5f6f71112131415161718191a1b1c1d1e1f20");

    private static byte[] hex(String s) {
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    private static String toHex(ByteBuf buf) {
        StringBuilder sb = new StringBuilder();
        for (int i = buf.readerIndex(); i < buf.writerIndex(); i++) {
            sb.append(String.format("%02x", buf.getByte(i)));
        }
        return sb.toString();
    }

    private static KeyMaterialCif goldenCif() {
        return new KeyMaterialCif(0, KeyEncryption.EVEN, KeyMaterialCif.CIPHER_AES_CTR, 0, SALT, WRAP);
    }

    @Test
    void encodesToGosrtsGoldenVector() {
        ByteBuf out = Unpooled.buffer();

        goldenCif().encodeTo(out);

        assertThat(toHex(out)).isEqualTo(GOSRT_GOLDEN_HEX);
        out.release();
    }

    @Test
    void decodesGosrtsGoldenVector() {
        ByteBuf in = Unpooled.wrappedBuffer(hex(GOSRT_GOLDEN_HEX));

        KeyMaterialCif decoded = KeyMaterialCif.decode(in);

        assertThat(decoded).isNotNull();
        assertThat(decoded.isError()).isFalse();
        assertThat(decoded.keyEncryption()).isEqualTo(KeyEncryption.EVEN);
        assertThat(decoded.cipher()).isEqualTo(KeyMaterialCif.CIPHER_AES_CTR);
        assertThat(decoded.authentication()).isZero();
        assertThat(decoded.salt()).isEqualTo(SALT);
        assertThat(decoded.wrap()).isEqualTo(WRAP);
        assertThat(decoded.keyLength()).isEqualTo(16);
        in.release();
    }

    /** gosrt's TestKM round-trips the same way after asserting the hex. */
    @Test
    void roundTripsWithoutLoss() {
        ByteBuf buf = Unpooled.buffer();
        goldenCif().encodeTo(buf);

        KeyMaterialCif decoded = KeyMaterialCif.decode(buf);

        assertThat(decoded.hasSameContentAs(goldenCif())).isTrue();
        buf.release();
    }

    @Test
    void bothKeysCarryTwoWrappedKeysAndReportTheSingleKeyLength() {
        byte[] doubleWrap = new byte[2 * 32 + 8];
        KeyMaterialCif both = new KeyMaterialCif(
                0, KeyEncryption.BOTH, KeyMaterialCif.CIPHER_AES_CTR, 0, SALT, doubleWrap);
        ByteBuf buf = Unpooled.buffer();
        both.encodeTo(buf);

        KeyMaterialCif decoded = KeyMaterialCif.decode(buf);

        assertThat(decoded).isNotNull();
        assertThat(decoded.keyEncryption()).isEqualTo(KeyEncryption.BOTH);
        assertThat(decoded.keyLength()).isEqualTo(32); // one key, not the combined wrap
        assertThat(decoded.wrap()).hasSize(doubleWrap.length);
        buf.release();
    }

    @Test
    void saltMayBeOmitted() {
        KeyMaterialCif noSalt = new KeyMaterialCif(
                0, KeyEncryption.ODD, KeyMaterialCif.CIPHER_AES_CTR, 0, new byte[0], WRAP);
        ByteBuf buf = Unpooled.buffer();
        noSalt.encodeTo(buf);

        KeyMaterialCif decoded = KeyMaterialCif.decode(buf);

        assertThat(decoded).isNotNull();
        assertThat(decoded.salt()).isEmpty();
        assertThat(decoded.wrap()).isEqualTo(WRAP);
        buf.release();
    }

    @Test
    void roundTripsTheFourByteErrorForm() {
        for (int code : new int[]{KeyMaterialCif.ERROR_NO_SECRET, KeyMaterialCif.ERROR_BAD_SECRET}) {
            ByteBuf buf = Unpooled.buffer();
            KeyMaterialCif.error(code).encodeTo(buf);
            assertThat(buf.readableBytes()).isEqualTo(4);

            KeyMaterialCif decoded = KeyMaterialCif.decode(buf);

            assertThat(decoded).isNotNull();
            assertThat(decoded.isError()).isTrue();
            assertThat(decoded.errorCode()).isEqualTo(code);
            buf.release();
        }
    }

    // --- malformed input is dropped, not thrown (this package's established contract)

    @Test
    void rejectsAnUnknownErrorCode() {
        ByteBuf in = Unpooled.buffer().writeIntLE(99);

        assertThat(KeyMaterialCif.decode(in)).isNull();
        in.release();
    }

    @Test
    void rejectsKkOfZeroWhichIsInvalidForAKeyMaterialMessage() {
        ByteBuf in = Unpooled.wrappedBuffer(hex(GOSRT_GOLDEN_HEX));
        in.setByte(3, 0); // KK = 0

        assertThat(KeyMaterialCif.decode(in)).isNull();
        in.release();
    }

    @Test
    void rejectsAWrongSignature() {
        ByteBuf in = Unpooled.wrappedBuffer(hex(GOSRT_GOLDEN_HEX));
        in.setShort(1, 0x1234);

        assertThat(KeyMaterialCif.decode(in)).isNull();
        in.release();
    }

    @Test
    void rejectsAnUnsupportedKeyLength() {
        ByteBuf in = Unpooled.wrappedBuffer(hex(GOSRT_GOLDEN_HEX));
        in.setByte(15, 5); // 5 * 4 = 20 bytes, not 16/24/32

        assertThat(KeyMaterialCif.decode(in)).isNull();
        in.release();
    }

    @Test
    void rejectsATruncatedWrap() {
        byte[] full = hex(GOSRT_GOLDEN_HEX);
        ByteBuf in = Unpooled.wrappedBuffer(full, 0, full.length - 4);

        assertThat(KeyMaterialCif.decode(in)).isNull();
        in.release();
    }

    @Test
    void rejectsAnUndersizedBuffer() {
        ByteBuf in = Unpooled.buffer().writeByte(0x12).writeByte(0x20);

        assertThat(KeyMaterialCif.decode(in)).isNull();
        in.release();
    }
}