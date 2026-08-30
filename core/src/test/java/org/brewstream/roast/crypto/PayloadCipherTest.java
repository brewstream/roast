package org.brewstream.roast.crypto;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Golden vectors are gosrt's own crypto_test.go TestEncode/TestDecode data - a
 * real 1316-byte MPEG-TS payload, all three key lengths, both the even and odd
 * key. They live in src/test/resources/crypto/aes-ctr-vectors.txt rather than
 * inline because they run to roughly 19KB of hex; the file was extracted
 * verbatim from gosrt rather than retyped.
 */
class PayloadCipherTest {

    private record Vector(int keyLength, byte[] salt, byte[] evenSek, byte[] oddSek,
            byte[] evenEncrypted, byte[] oddEncrypted) {

        @Override
        public String toString() {
            return keyLength + "-byte keys";
        }
    }

    private static int packetSequenceNumber;
    private static byte[] plaintext;
    private static final List<Vector> VECTORS = load();

    private static byte[] hex(String s) {
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    private static List<Vector> load() {
        List<Vector> vectors = new ArrayList<>();
        try (InputStream in = PayloadCipherTest.class.getResourceAsStream("/crypto/aes-ctr-vectors.txt");
                BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))) {

            int keyLength = 0;
            byte[] salt = null, evenSek = null, oddSek = null, evenEncrypted = null;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                if (line.equals("vector")) {
                    continue;
                }
                int eq = line.indexOf('=');
                String key = line.substring(0, eq);
                String value = line.substring(eq + 1);
                switch (key) {
                    case "psn" -> packetSequenceNumber = (int) Long.parseLong(value.substring(2), 16);
                    case "plaintext" -> plaintext = hex(value);
                    case "keylength" -> keyLength = Integer.parseInt(value);
                    case "salt" -> salt = hex(value);
                    case "evenSek" -> evenSek = hex(value);
                    case "oddSek" -> oddSek = hex(value);
                    case "passphrase" -> { /* only needed by the key-wrapping tests */ }
                    case "evenEncrypted" -> evenEncrypted = hex(value);
                    case "oddEncrypted" -> vectors.add(
                            new Vector(keyLength, salt, evenSek, oddSek, evenEncrypted, hex(value)));
                    default -> throw new IllegalStateException("unexpected key in vectors file: " + key);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("could not read the AES-CTR golden vectors", e);
        }
        return vectors;
    }

    static List<Vector> vectors() {
        return VECTORS;
    }

    @Test
    void theVectorsFileLoaded() {
        assertThat(VECTORS).hasSize(3);
        assertThat(plaintext).hasSize(1316);
        assertThat(packetSequenceNumber).isEqualTo(0x79ee189e);
    }

    /** Ported from gosrt's TestDecode: their ciphertext must decrypt to their plaintext. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("vectors")
    void decryptsGosrtGoldenVectors(Vector vector) {
        byte[] even = vector.evenEncrypted().clone();
        PayloadCipher.encryptOrDecrypt(even, vector.evenSek(), vector.salt(), packetSequenceNumber);
        assertThat(even).isEqualTo(plaintext);

        byte[] odd = vector.oddEncrypted().clone();
        PayloadCipher.encryptOrDecrypt(odd, vector.oddSek(), vector.salt(), packetSequenceNumber);
        assertThat(odd).isEqualTo(plaintext);
    }

    /** Ported from gosrt's TestEncode: their plaintext must encrypt to their ciphertext. */
    @ParameterizedTest(name = "{0}")
    @MethodSource("vectors")
    void encryptsToGosrtGoldenVectors(Vector vector) {
        byte[] even = plaintext.clone();
        PayloadCipher.encryptOrDecrypt(even, vector.evenSek(), vector.salt(), packetSequenceNumber);
        assertThat(even).isEqualTo(vector.evenEncrypted());

        byte[] odd = plaintext.clone();
        PayloadCipher.encryptOrDecrypt(odd, vector.oddSek(), vector.salt(), packetSequenceNumber);
        assertThat(odd).isEqualTo(vector.oddEncrypted());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("vectors")
    void theSameCallInBothDirectionsRoundTrips(Vector vector) {
        byte[] data = plaintext.clone();

        PayloadCipher.encryptOrDecrypt(data, vector.evenSek(), vector.salt(), packetSequenceNumber);
        assertThat(data).isNotEqualTo(plaintext); // actually did something
        PayloadCipher.encryptOrDecrypt(data, vector.evenSek(), vector.salt(), packetSequenceNumber);

        assertThat(data).isEqualTo(plaintext);
    }

    /**
     * The point of folding the sequence number into the counter: the same key
     * and salt must not produce the same keystream twice, or two packets would
     * XOR against identical bytes.
     */
    @Test
    void adjacentSequenceNumbersProduceDifferentCiphertext() {
        Vector vector = VECTORS.get(0);
        byte[] first = plaintext.clone();
        byte[] second = plaintext.clone();

        PayloadCipher.encryptOrDecrypt(first, vector.evenSek(), vector.salt(), packetSequenceNumber);
        PayloadCipher.encryptOrDecrypt(second, vector.evenSek(), vector.salt(), packetSequenceNumber + 1);

        assertThat(first).isNotEqualTo(second);
    }

    @Test
    void theEvenAndOddKeysProduceDifferentCiphertext() {
        Vector vector = VECTORS.get(0);

        assertThat(vector.evenEncrypted()).isNotEqualTo(vector.oddEncrypted());
    }

    // --- counter construction, asserted directly rather than only implied by the vectors

    @Test
    void theCounterIsTheNonceWithTheSequenceNumberXoredIntoBytesTenToThirteen() {
        byte[] salt = hex("000102030405060708090a0b0c0d0e0f");

        byte[] counter = PayloadCipher.counterFor(salt, 0x11223344);

        // Bytes 0..9: the salt untouched.
        assertThat(counter[0]).isEqualTo((byte) 0x00);
        assertThat(counter[9]).isEqualTo((byte) 0x09);
        // Bytes 10..13: salt XOR the big-endian sequence number.
        assertThat(counter[10]).isEqualTo((byte) (0x0a ^ 0x11));
        assertThat(counter[11]).isEqualTo((byte) (0x0b ^ 0x22));
        assertThat(counter[12]).isEqualTo((byte) (0x0c ^ 0x33));
        assertThat(counter[13]).isEqualTo((byte) (0x0d ^ 0x44));
        // Bytes 14..15: the block counter, zero - and NOT the salt's last two bytes.
        assertThat(counter[14]).isZero();
        assertThat(counter[15]).isZero();
    }

    @Test
    void theSaltsLastTwoBytesAreDeliberatelyUnused() {
        byte[] salt = hex("000102030405060708090a0b0c0d0e0f");
        byte[] differentTail = salt.clone();
        differentTail[14] ^= 0xFF;
        differentTail[15] ^= 0xFF;

        assertThat(PayloadCipher.counterFor(differentTail, 1))
                .isEqualTo(PayloadCipher.counterFor(salt, 1));
    }

    @Test
    void aWrongSizedSaltIsRejected() {
        assertThatThrownBy(() -> PayloadCipher.encryptOrDecrypt(
                new byte[16], new byte[16], new byte[8], 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("salt must be 16 bytes");
    }

    @Test
    void anIllegalKeyLengthIsRejected() {
        assertThatThrownBy(() -> PayloadCipher.encryptOrDecrypt(
                new byte[16], new byte[20], new byte[16], 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16, 24, or 32");
    }

    /** A payload that isn't a whole number of AES blocks still works - CTR is a stream cipher. */
    @Test
    void handlesAPayloadThatIsNotABlockMultiple() {
        Vector vector = VECTORS.get(0);
        byte[] data = new byte["not a block multiple".length()];
        System.arraycopy("not a block multiple".getBytes(StandardCharsets.US_ASCII), 0, data, 0, data.length);
        byte[] original = data.clone();

        PayloadCipher.encryptOrDecrypt(data, vector.evenSek(), vector.salt(), 7);
        assertThat(data).isNotEqualTo(original);
        PayloadCipher.encryptOrDecrypt(data, vector.evenSek(), vector.salt(), 7);

        assertThat(data).isEqualTo(original);
    }
}
