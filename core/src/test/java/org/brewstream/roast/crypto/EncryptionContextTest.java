package org.brewstream.roast.crypto;

import org.brewstream.roast.packet.cif.KeyEncryption;
import org.brewstream.roast.packet.cif.KeyMaterialCif;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Self-designed against gosrt's UnmarshalKM/MarshalKM and connection.go's key
 * handling. gosrt has no unit tests for this layer - crypto_test.go covers the
 * primitives (already ported in StreamKeyWrapperTest/PayloadCipherTest) and the
 * key-management state lives in connection.go, which has no tests at all - so
 * this is the same rigor tier as the RTT/drift/wraparound pieces rather than
 * the ported-vector tier.
 *
 * <p>The strongest assertions here are the two-context round trips: a sender
 * and a receiver built independently must agree, which is the property that
 * actually matters and the one a real peer will exercise.
 */
class EncryptionContextTest {

    private static final String PASSPHRASE = "foobarfoobar";

    private static char[] passphrase() {
        return PASSPHRASE.toCharArray();
    }

    private static byte[] payload() {
        return "some payload bytes that will be encrypted".getBytes(StandardCharsets.US_ASCII);
    }

    @ParameterizedTest
    @ValueSource(ints = {16, 24, 32})
    void aReceiverAdoptingASendersKeyMaterialCanDecryptWhatTheSenderEncrypts(int keyLength) {
        EncryptionContext sender = EncryptionContext.generating(passphrase(), keyLength);
        EncryptionContext receiver = EncryptionContext.awaitingPeerKeys(passphrase(), keyLength);

        assertThat(receiver.adopt(sender.keyMaterial(KeyEncryption.BOTH))).isTrue();

        byte[] original = payload();
        byte[] wire = original.clone();
        sender.encrypt(wire, 12345);
        assertThat(wire).isNotEqualTo(original);

        assertThat(receiver.decrypt(wire, 12345, sender.activeKey())).isTrue();
        assertThat(wire).isEqualTo(original);
    }


    /**
     * A peer may announce only the key it is currently using rather than both,
     * and real libsrt does exactly that. This is the case that must work
     * end-to-end: adopting a single key has to leave the context able to
     * encrypt, or nothing gets encrypted at all and the peer drops every packet
     * as unexpectedly-cleartext. Found against real libsrt, which reported
     * "Packet not encrypted ... dropped" while we happily sent plaintext.
     */
    @ParameterizedTest
    @ValueSource(ints = {16, 24, 32})
    void adoptingOnlyOneAnnouncedKeyStillAllowsEncryption(int keyLength) {
        EncryptionContext sender = EncryptionContext.generating(passphrase(), keyLength);
        EncryptionContext receiver = EncryptionContext.awaitingPeerKeys(passphrase(), keyLength);

        // Only the even key is announced - the odd one is never sent.
        assertThat(receiver.adopt(sender.keyMaterial(KeyEncryption.EVEN))).isTrue();
        assertThat(receiver.hasKeys()).isTrue();

        byte[] original = payload();
        byte[] wire = original.clone();
        receiver.encrypt(wire, 77);
        assertThat(wire).isNotEqualTo(original);

        assertThat(sender.decrypt(wire, 77, receiver.activeKey())).isTrue();
        assertThat(wire).isEqualTo(original);
    }

    /** A key we were never given can't decrypt, even though the other one is usable. */
    @Test
    void aPacketNamingAKeyWeWereNeverGivenIsRejected() {
        EncryptionContext sender = EncryptionContext.generating(passphrase(), 16);
        EncryptionContext receiver = EncryptionContext.awaitingPeerKeys(passphrase(), 16);
        receiver.adopt(sender.keyMaterial(KeyEncryption.EVEN)); // even only

        assertThat(receiver.decrypt(payload(), 1, KeyEncryption.EVEN)).isTrue();
        assertThat(receiver.decrypt(payload(), 1, KeyEncryption.ODD)).isFalse();
    }

    @Test
    void aReceiverWithTheWrongPassphraseRejectsTheKeyMaterialAndKeepsNoKeys() {
        EncryptionContext sender = EncryptionContext.generating(passphrase(), 16);
        EncryptionContext receiver = EncryptionContext.awaitingPeerKeys("not-the-passphrase".toCharArray(), 16);

        assertThat(receiver.adopt(sender.keyMaterial(KeyEncryption.BOTH))).isFalse();
        assertThat(receiver.hasKeys()).isFalse();
    }

    /**
     * A receiving side does not choose the key length - the peer generates the
     * keys and announces their size. Stronger than asked for is fine.
     */
    @Test
    void aPeerAnnouncingLongerKeysThanAskedForIsAccepted() {
        EncryptionContext sender = EncryptionContext.generating(passphrase(), 32);
        EncryptionContext receiver = EncryptionContext.awaitingPeerKeys(passphrase(), 16);

        assertThat(receiver.adopt(sender.keyMaterial(KeyEncryption.BOTH))).isTrue();
        assertThat(receiver.keyLength()).isEqualTo(32);

        byte[] original = payload();
        byte[] wire = original.clone();
        sender.encrypt(wire, 3);
        assertThat(receiver.decrypt(wire, 3, sender.activeKey())).isTrue();
        assertThat(wire).isEqualTo(original);
    }

    /** Shorter than asked for is a silent downgrade, and must not be accepted. */
    @Test
    void aPeerAnnouncingShorterKeysThanAskedForIsRefused() {
        EncryptionContext sender = EncryptionContext.generating(passphrase(), 16);
        EncryptionContext receiver = EncryptionContext.awaitingPeerKeys(passphrase(), 32);

        assertThat(receiver.adopt(sender.keyMaterial(KeyEncryption.BOTH))).isFalse();
        assertThat(receiver.hasKeys()).isFalse();
    }

    /** Once keys are held, a rotation may replace them but must not change their size. */
    @Test
    void aMidStreamKeyLengthChangeIsRefused() {
        EncryptionContext sender = EncryptionContext.generating(passphrase(), 16);
        EncryptionContext receiver = EncryptionContext.awaitingPeerKeys(passphrase(), 16);
        assertThat(receiver.adopt(sender.keyMaterial(KeyEncryption.BOTH))).isTrue();

        EncryptionContext longerKeys = EncryptionContext.generating(passphrase(), 32);
        assertThat(receiver.adopt(longerKeys.keyMaterial(KeyEncryption.ODD))).isFalse();
        assertThat(receiver.keyLength()).isEqualTo(16);
    }

    @Test
    void anErrorKeyMaterialMessageIsRejected() {
        EncryptionContext receiver = EncryptionContext.awaitingPeerKeys(passphrase(), 16);

        assertThat(receiver.adopt(KeyMaterialCif.error(KeyMaterialCif.ERROR_BAD_SECRET))).isFalse();
        assertThat(receiver.adopt(null)).isFalse();
        assertThat(receiver.hasKeys()).isFalse();
    }

    /** Adopting must take the peer's salt, not keep our own - otherwise the KEK is derived from the wrong bytes. */
    @Test
    void adoptingUsesThePeersSaltNotOurOwn() {
        EncryptionContext sender = EncryptionContext.generating(passphrase(), 16);
        // A receiver that already has its own (different) salt and keys.
        EncryptionContext receiver = EncryptionContext.generating(passphrase(), 16);

        KeyMaterialCif km = sender.keyMaterial(KeyEncryption.BOTH);
        assertThat(receiver.adopt(km)).isTrue();

        byte[] original = payload();
        byte[] wire = original.clone();
        sender.encrypt(wire, 7);

        assertThat(receiver.decrypt(wire, 7, sender.activeKey())).isTrue();
        assertThat(wire).isEqualTo(original);
    }

    /**
     * Adopting BOTH must install both keys, not just the active one - the peer
     * can switch to the other at any time without re-announcing, so a receiver
     * that only kept one would break at the first rotation.
     */
    @Test
    void adoptingBothInstallsBothKeysSoEitherCanDecrypt() {
        EncryptionContext sender = EncryptionContext.generating(passphrase(), 16);
        EncryptionContext receiver = EncryptionContext.awaitingPeerKeys(passphrase(), 16);
        receiver.adopt(sender.keyMaterial(KeyEncryption.BOTH));

        byte[] original = payload();

        byte[] withEven = original.clone();
        sender.encrypt(withEven, 10); // active key is EVEN
        assertThat(receiver.decrypt(withEven, 10, KeyEncryption.EVEN)).isTrue();
        assertThat(withEven).isEqualTo(original);

        sender.switchActiveKey();
        byte[] withOdd = original.clone();
        sender.encrypt(withOdd, 11);
        assertThat(receiver.decrypt(withOdd, 11, KeyEncryption.ODD)).isTrue();
        assertThat(withOdd).isEqualTo(original);
    }

    /**
     * A single-key announcement carries a salt too, and the salt is shared by
     * both keys - so adopting one from a *different* session also moves the
     * salt, which is what makes the previously-held key unusable. Worth pinning
     * down because it looks like a bug until you notice real key rotation keeps
     * the salt fixed and only rolls a SEK. gosrt's UnmarshalKM behaves the same
     * way (it copies km.Salt whenever the message carries one).
     */
    @Test
    void adoptingASingleKeyFromAnotherSessionAlsoMovesTheSalt() {
        EncryptionContext first = EncryptionContext.generating(passphrase(), 16);
        EncryptionContext receiver = EncryptionContext.awaitingPeerKeys(passphrase(), 16);
        receiver.adopt(first.keyMaterial(KeyEncryption.BOTH));

        EncryptionContext second = EncryptionContext.generating(passphrase(), 16);
        assertThat(receiver.adopt(second.keyMaterial(KeyEncryption.ODD))).isTrue();

        // The second sender's odd key now works...
        second.switchActiveKey();
        byte[] original = payload();
        byte[] wire = original.clone();
        second.encrypt(wire, 5);
        assertThat(receiver.decrypt(wire, 5, KeyEncryption.ODD)).isTrue();
        assertThat(wire).isEqualTo(original);

        // ...while the first sender's even key no longer round-trips, because
        // the salt moved out from under it.
        byte[] stale = original.clone();
        first.encrypt(stale, 6);
        receiver.decrypt(stale, 6, KeyEncryption.EVEN);
        assertThat(stale).isNotEqualTo(original);
    }

    @Test
    void announcingASingleKeyMakesItTheActiveOne() {
        EncryptionContext sender = EncryptionContext.generating(passphrase(), 16);
        EncryptionContext receiver = EncryptionContext.awaitingPeerKeys(passphrase(), 16);

        receiver.adopt(sender.keyMaterial(KeyEncryption.ODD));

        assertThat(receiver.activeKey()).isEqualTo(KeyEncryption.ODD);
    }

    @Test
    void announcingBothKeysLeavesTheActiveKeyAlone() {
        EncryptionContext sender = EncryptionContext.generating(passphrase(), 16);
        EncryptionContext receiver = EncryptionContext.awaitingPeerKeys(passphrase(), 16);
        KeyEncryption before = receiver.activeKey();

        receiver.adopt(sender.keyMaterial(KeyEncryption.BOTH));

        assertThat(receiver.activeKey()).isEqualTo(before);
    }

    @Test
    void switchingTheActiveKeyChangesWhichKeyEncrypts() {
        EncryptionContext sender = EncryptionContext.generating(passphrase(), 16);
        EncryptionContext receiver = EncryptionContext.awaitingPeerKeys(passphrase(), 16);
        receiver.adopt(sender.keyMaterial(KeyEncryption.BOTH));

        assertThat(sender.activeKey()).isEqualTo(KeyEncryption.EVEN);
        sender.switchActiveKey();
        assertThat(sender.activeKey()).isEqualTo(KeyEncryption.ODD);

        byte[] original = payload();
        byte[] wire = original.clone();
        sender.encrypt(wire, 42);

        // Decrypting with the key the header would now name works...
        byte[] correct = wire.clone();
        assertThat(receiver.decrypt(correct, 42, KeyEncryption.ODD)).isTrue();
        assertThat(correct).isEqualTo(original);
        // ...and with the other key it does not.
        byte[] wrong = wire.clone();
        receiver.decrypt(wrong, 42, KeyEncryption.EVEN);
        assertThat(wrong).isNotEqualTo(original);
    }

    @Test
    void decryptingBeforeAnyKeysExistFails() {
        EncryptionContext receiver = EncryptionContext.awaitingPeerKeys(passphrase(), 16);

        assertThat(receiver.decrypt(payload(), 1, KeyEncryption.EVEN)).isFalse();
    }

    @Test
    void bothIsNotAUsableDecryptionKey() {
        EncryptionContext sender = EncryptionContext.generating(passphrase(), 16);

        assertThat(sender.decrypt(payload(), 1, KeyEncryption.BOTH)).isFalse();
        assertThat(sender.decrypt(payload(), 1, null)).isFalse();
    }

    @Test
    void encryptingBeforeAnyKeysExistIsAProgrammingErrorNotAPeerCondition() {
        EncryptionContext context = EncryptionContext.awaitingPeerKeys(passphrase(), 16);

        assertThatThrownBy(() -> context.encrypt(payload(), 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("no keys yet");
        assertThatThrownBy(() -> context.keyMaterial(KeyEncryption.BOTH))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void anIllegalKeyLengthIsRejectedUpFront() {
        assertThatThrownBy(() -> EncryptionContext.generating(passphrase(), 20))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("16, 24, or 32");
    }

    @Test
    void twoContextsGenerateDifferentKeys() {
        EncryptionContext first = EncryptionContext.generating(passphrase(), 16);
        EncryptionContext second = EncryptionContext.generating(passphrase(), 16);

        // Same passphrase, but independently generated salt and SEKs, so the
        // announced key material must differ.
        assertThat(first.keyMaterial(KeyEncryption.BOTH).wrap())
                .isNotEqualTo(second.keyMaterial(KeyEncryption.BOTH).wrap());
    }

    @Test
    void theAnnouncedKeyMaterialIsAWellFormedMessage() {
        EncryptionContext sender = EncryptionContext.generating(passphrase(), 24);

        KeyMaterialCif km = sender.keyMaterial(KeyEncryption.BOTH);

        assertThat(km.isError()).isFalse();
        assertThat(km.keyEncryption()).isEqualTo(KeyEncryption.BOTH);
        assertThat(km.cipher()).isEqualTo(KeyMaterialCif.CIPHER_AES_CTR);
        assertThat(km.salt()).hasSize(16);
        assertThat(km.keyLength()).isEqualTo(24);
        assertThat(km.wrap()).hasSize(24 * 2 + StreamKeyWrapper.WRAP_OVERHEAD);
    }


    // --- key rotation schedule (gosrt's pop(); no reference test exists)

    private static final long REFRESH = 100;
    private static final long PRE_ANNOUNCE = 30;

    private static EncryptionContext rotating() {
        return EncryptionContext.generating(passphrase(), 16, REFRESH, PRE_ANNOUNCE);
    }

    /** Nothing is announced until the schedule says so. */
    @Test
    void noKeyIsAnnouncedEarlyInTheCycle() {
        EncryptionContext context = rotating();

        for (int i = 0; i < REFRESH - PRE_ANNOUNCE - 1; i++) {
            assertThat(context.onPacketEncrypted()).isEmpty();
        }
    }

    /** The announcement names the key about to become active, not the one in use. */
    @Test
    void theOppositeKeyIsAnnouncedAtThePreAnnouncePoint() {
        EncryptionContext context = rotating();
        assertThat(context.activeKey()).isEqualTo(KeyEncryption.EVEN);

        Optional<KeyEncryption> announced = Optional.empty();
        for (int i = 0; i < REFRESH - PRE_ANNOUNCE; i++) {
            Optional<KeyEncryption> a = context.onPacketEncrypted();
            if (a.isPresent()) {
                announced = a;
            }
        }

        assertThat(announced).contains(KeyEncryption.ODD);
        assertThat(context.activeKey()).isEqualTo(KeyEncryption.EVEN); // not switched yet
    }

    /** Until the peer confirms, the announcement repeats rather than being sent once and forgotten. */
    @Test
    void theAnnouncementRepeatsUntilConfirmed() {
        EncryptionContext context = rotating();
        int announcements = 0;
        for (int i = 0; i < REFRESH - PRE_ANNOUNCE + 20; i++) {
            if (context.onPacketEncrypted().isPresent()) {
                announcements++;
            }
        }

        assertThat(announcements).isGreaterThan(1);
    }

    @Test
    void confirmingStopsTheReAnnouncements() {
        EncryptionContext context = rotating();
        for (int i = 0; i < REFRESH - PRE_ANNOUNCE; i++) {
            context.onPacketEncrypted();
        }
        context.confirmKeyMaterial();
        assertThat(context.isKeyMaterialConfirmed()).isTrue();

        int announcements = 0;
        for (int i = 0; i < 20; i++) {
            if (context.onPacketEncrypted().isPresent()) {
                announcements++;
            }
        }

        assertThat(announcements).isZero();
    }

    @Test
    void theActiveKeySwitchesAtTheRefreshPoint() {
        EncryptionContext context = rotating();

        for (int i = 0; i < REFRESH - 1; i++) {
            context.onPacketEncrypted();
        }
        assertThat(context.activeKey()).isEqualTo(KeyEncryption.EVEN);

        context.onPacketEncrypted(); // the refresh packet
        assertThat(context.activeKey()).isEqualTo(KeyEncryption.ODD);
        assertThat(context.isKeyMaterialConfirmed()).isFalse(); // cleared for the next cycle
    }

    @Test
    void keysAlternateAcrossSuccessiveCycles() {
        EncryptionContext context = rotating();

        for (int i = 0; i < REFRESH; i++) {
            context.onPacketEncrypted();
        }
        assertThat(context.activeKey()).isEqualTo(KeyEncryption.ODD);

        for (int i = 0; i < REFRESH; i++) {
            context.onPacketEncrypted();
        }
        assertThat(context.activeKey()).isEqualTo(KeyEncryption.EVEN);
    }

    /**
     * The key just rotated away from must stay intact for a while - packets
     * encrypted with it may still be in flight or awaiting retransmission - and
     * is only replaced preAnnounce packets after the switch. Asserted by
     * checking the *sender's* retired key against a peer holding the original,
     * which is the only thing the timing actually changes.
     */
    @Test
    void theRetiredKeyIsRegeneratedOnlyWellAfterTheSwitch() {
        EncryptionContext sender = rotating();
        EncryptionContext peer = EncryptionContext.awaitingPeerKeys(passphrase(), 16);
        peer.adopt(sender.keyMaterial(KeyEncryption.BOTH));

        for (int i = 0; i < REFRESH; i++) {
            sender.onPacketEncrypted();
        }
        assertThat(sender.activeKey()).isEqualTo(KeyEncryption.ODD);

        // Immediately after the switch the retired EVEN key is untouched.
        assertThat(peerCanStillReadWhatSenderEncryptsWith(sender, peer, KeyEncryption.EVEN)).isTrue();

        // preAnnounce packets later it has been replaced, so the peer's copy is stale.
        for (int i = 0; i < PRE_ANNOUNCE; i++) {
            sender.onPacketEncrypted();
        }
        assertThat(peerCanStillReadWhatSenderEncryptsWith(sender, peer, KeyEncryption.EVEN)).isFalse();
    }

    /** Encrypts with a specific key without disturbing the rotation schedule's view of "active". */
    private static boolean peerCanStillReadWhatSenderEncryptsWith(EncryptionContext sender,
            EncryptionContext peer, KeyEncryption key) {
        KeyEncryption restore = sender.activeKey();
        if (restore != key) {
            sender.switchActiveKey();
        }
        byte[] original = payload();
        byte[] wire = original.clone();
        sender.encrypt(wire, 5);
        if (sender.activeKey() != restore) {
            sender.switchActiveKey();
        }

        peer.decrypt(wire, 5, key);
        return java.util.Arrays.equals(wire, original);
    }

    @Test
    void aPreAnnounceThatIsNotBeforeTheRefreshIsRejected() {
        assertThatThrownBy(() -> EncryptionContext.generating(passphrase(), 16, 100, 100))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("pre-announce");
    }

    @Test
    void destroyClearsTheKeysAndTheSecretStaysOutOfToString() {
        EncryptionContext context = EncryptionContext.generating(passphrase(), 16);
        assertThat(context.toString()).doesNotContain(PASSPHRASE).contains("hasKeys=true");

        context.destroy();

        assertThat(context.hasKeys()).isFalse();
        assertThat(context.toString()).doesNotContain(PASSPHRASE);
    }
}
