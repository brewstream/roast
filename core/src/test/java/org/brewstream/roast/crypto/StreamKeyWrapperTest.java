package org.brewstream.roast.crypto;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Golden vectors are gosrt's own crypto_test.go TestMarshal/TestUnmarshal data,
 * covering all three key lengths and all three key selections - real
 * cross-implementation proof that KEK derivation and key wrapping together match
 * an independent implementation byte for byte.
 *
 * <p>Worth noting what these vectors do and don't pin down: they exercise KEK
 * derivation and wrapping as a pair, because that is how gosrt tests them and
 * there is no isolated PBKDF2 vector in either reference. A wrong PBKDF2
 * parameterization (the whole salt instead of its trailing 8 bytes, say) is
 * still caught - it produces a different KEK and therefore a different wrap.
 */
class StreamKeyWrapperTest {

    private static final String PASSPHRASE = "foobarfoobar";

    private static byte[] hex(String s) {
        byte[] out = new byte[s.length() / 2];
        for (int i = 0; i < out.length; i++) {
            out[i] = (byte) Integer.parseInt(s.substring(i * 2, i * 2 + 2), 16);
        }
        return out;
    }

    /** keyLength, salt, evenSEK, oddSEK, evenWrap, oddWrap, evenOddWrap - verbatim from gosrt. */
    private static Stream<Arguments> gosrtVectors() {
        return Stream.of(
                Arguments.of(16,
                        "6c438852715a4d26e0e810b3132ca61f",
                        "047dc22e7f000be55a25ba56ae2e9180",
                        "240c8e76ccf3637641af473edaf15aaf",
                        "699ab4eac6b7c66c3a9fa0d6836326c2b294a10764233356",
                        "ca4decaaf8d7b5c38288e84c8796929c84b7c139f1f769d5",
                        "5b901889bd106609ca8a83264b12ed1bfab3f02812bad65784ac396b1f57eb16c53e1020d3a3250b"),
                Arguments.of(24,
                        "e636259ccc41e73611b9363bb58586b1",
                        "4dca0ad088da64fdc8e98002d141bc46fed4fa0167b931c8",
                        "2b2bbb64ee3942cfa31bfe58efd1d2102c40b7bc028f8946",
                        "8c6502d6a83e0ab894a43cb5b37b71c2755afc64a682bed9d46912138b60f384",
                        "1fe56a4475759636674a7c5e44f1cdfb365a9f11d8fe74536e8df6b97eecf1c9",
                        "7360357d363ebec384885b10c8120528889d1be05624bfc381c5fa090f00f9ecef5d6427f7542a58"
                                + "be144f4aeb07452beca546874a68197d"),
                Arguments.of(32,
                        "3825bb4163f7d5cf2804ec0b31a7370f",
                        "53a088d93431181075f8a9bc4876359afe48967308120c93f97bbd823d8de62a",
                        "7893e88b6296ffcc5a2eab5f53d48efd7adaeced8cb3a851d4f8e2dbda8db17a",
                        "7d1578458e41680dd997d1a185c75753f3344c6711542b35833f881f7c480304cbe9bdbe76035914",
                        "cc1af097af558fa25b925417c4e6e9e1adacd8b96916b4ac4fac8e6ecdc3b5c48c01134e92e9e5f6",
                        "f7373def4e9f61f6cd6a22e78916aa07cac8e5f07669d556ec8a15b7631fa9c631e9d98a3f92dbe1"
                                + "87f434569ec71b9e2a53171feafd909a5560233fe02ed0301e576d4992b10c86"));
    }

    /** Ported from gosrt's TestMarshal: wrapping a known SEK must produce their exact bytes. */
    @ParameterizedTest(name = "{0}-byte keys")
    @MethodSource("gosrtVectors")
    void wrapMatchesGosrtGoldenVectors(int keyLength, String salt, String evenSek, String oddSek,
            String evenWrap, String oddWrap, String evenOddWrap) {
        byte[] kek = StreamKeyWrapper.deriveKeyEncryptingKey(PASSPHRASE, hex(salt), keyLength);

        assertThat(StreamKeyWrapper.wrap(kek, hex(evenSek))).isEqualTo(hex(evenWrap));
        assertThat(StreamKeyWrapper.wrap(kek, hex(oddSek))).isEqualTo(hex(oddWrap));

        byte[] both = new byte[keyLength * 2];
        System.arraycopy(hex(evenSek), 0, both, 0, keyLength);
        System.arraycopy(hex(oddSek), 0, both, keyLength, keyLength);
        assertThat(StreamKeyWrapper.wrap(kek, both)).isEqualTo(hex(evenOddWrap));
    }

    /** Ported from gosrt's TestUnmarshal: their wraps must unwrap back to the original keys. */
    @ParameterizedTest(name = "{0}-byte keys")
    @MethodSource("gosrtVectors")
    void unwrapRecoversTheOriginalKeysFromGosrtGoldenVectors(int keyLength, String salt, String evenSek,
            String oddSek, String evenWrap, String oddWrap, String evenOddWrap) {
        assertThat(StreamKeyWrapper.deriveAndUnwrap(PASSPHRASE, hex(salt), keyLength, hex(evenWrap)))
                .isEqualTo(hex(evenSek));
        assertThat(StreamKeyWrapper.deriveAndUnwrap(PASSPHRASE, hex(salt), keyLength, hex(oddWrap)))
                .isEqualTo(hex(oddSek));

        byte[] both = StreamKeyWrapper.deriveAndUnwrap(PASSPHRASE, hex(salt), keyLength, hex(evenOddWrap));
        assertThat(both).hasSize(keyLength * 2);
        assertThat(Arrays.copyOfRange(both, 0, keyLength)).isEqualTo(hex(evenSek));
        assertThat(Arrays.copyOfRange(both, keyLength, keyLength * 2)).isEqualTo(hex(oddSek));
    }

    @ParameterizedTest(name = "{0}-byte keys")
    @MethodSource("gosrtVectors")
    void aWrappedKeyIsEightBytesLongerThanWhatWentIn(int keyLength, String salt, String evenSek,
            String oddSek, String evenWrap, String oddWrap, String evenOddWrap) {
        assertThat(hex(evenWrap)).hasSize(keyLength + StreamKeyWrapper.WRAP_OVERHEAD);
        assertThat(hex(evenOddWrap)).hasSize(keyLength * 2 + StreamKeyWrapper.WRAP_OVERHEAD);
    }

    /**
     * The derivation consumes only the salt's trailing 8 bytes, so changing the
     * leading half must NOT change the key. This is the specific mistake the
     * golden vectors above would catch, asserted directly so the reason is
     * visible rather than implied.
     */
    @Test
    void onlyTheTrailingEightSaltBytesAffectTheDerivedKey() {
        byte[] salt = hex("6c438852715a4d26e0e810b3132ca61f");
        byte[] differentLeadingHalf = salt.clone();
        differentLeadingHalf[0] ^= 0xFF;
        differentLeadingHalf[7] ^= 0xFF;

        assertThat(StreamKeyWrapper.deriveKeyEncryptingKey(PASSPHRASE, differentLeadingHalf, 16))
                .isEqualTo(StreamKeyWrapper.deriveKeyEncryptingKey(PASSPHRASE, salt, 16));

        byte[] differentTrailingHalf = salt.clone();
        differentTrailingHalf[8] ^= 0xFF;

        assertThat(StreamKeyWrapper.deriveKeyEncryptingKey(PASSPHRASE, differentTrailingHalf, 16))
                .isNotEqualTo(StreamKeyWrapper.deriveKeyEncryptingKey(PASSPHRASE, salt, 16));
    }

    @Test
    void theDerivedKeyMatchesTheRequestedKeyLength() {
        byte[] salt = hex("6c438852715a4d26e0e810b3132ca61f");

        assertThat(StreamKeyWrapper.deriveKeyEncryptingKey(PASSPHRASE, salt, 16)).hasSize(16);
        assertThat(StreamKeyWrapper.deriveKeyEncryptingKey(PASSPHRASE, salt, 24)).hasSize(24);
        assertThat(StreamKeyWrapper.deriveKeyEncryptingKey(PASSPHRASE, salt, 32)).hasSize(32);
    }

    /** The whole point of the integrity check value: a wrong passphrase is detected, not silently wrong. */
    @Test
    void aWrongPassphraseFailsTheIntegrityCheckInsteadOfYieldingGarbage() {
        byte[] salt = hex("6c438852715a4d26e0e810b3132ca61f");
        byte[] wrapped = hex("699ab4eac6b7c66c3a9fa0d6836326c2b294a10764233356");

        assertThat(StreamKeyWrapper.deriveAndUnwrap("wrong-passphrase", salt, 16, wrapped)).isNull();
    }

    @Test
    void aCorruptedWrapIsRejected() {
        byte[] salt = hex("6c438852715a4d26e0e810b3132ca61f");
        byte[] wrapped = hex("699ab4eac6b7c66c3a9fa0d6836326c2b294a10764233356");
        wrapped[3] ^= 0xFF;

        assertThat(StreamKeyWrapper.deriveAndUnwrap(PASSPHRASE, salt, 16, wrapped)).isNull();
    }

    @Test
    void aStructurallyImpossibleWrapIsRejectedWithoutThrowing() {
        byte[] kek = StreamKeyWrapper.deriveKeyEncryptingKey(
                PASSPHRASE, hex("6c438852715a4d26e0e810b3132ca61f"), 16);

        assertThat(StreamKeyWrapper.unwrap(kek, new byte[0])).isNull();
        assertThat(StreamKeyWrapper.unwrap(kek, new byte[8])).isNull(); // overhead only, no key
        assertThat(StreamKeyWrapper.unwrap(kek, new byte[20])).isNull(); // not a whole 8-byte multiple
    }

    @Test
    void anIllegalKeyLengthIsRejectedUpFront() {
        byte[] salt = hex("6c438852715a4d26e0e810b3132ca61f");

        assertThatThrownBy(() -> StreamKeyWrapper.deriveKeyEncryptingKey(PASSPHRASE, salt, 20))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16, 24, or 32");
    }

    @Test
    void aTooShortSaltIsRejectedRatherThanSilentlyDerivingADifferentKey() {
        assertThatThrownBy(() -> StreamKeyWrapper.deriveKeyEncryptingKey(PASSPHRASE, new byte[4], 16))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("at least 8 bytes");
    }
}
