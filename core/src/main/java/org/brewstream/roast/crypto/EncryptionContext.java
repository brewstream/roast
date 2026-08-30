package org.brewstream.roast.crypto;

import io.netty.buffer.ByteBuf;
import org.brewstream.roast.packet.cif.KeyEncryption;
import org.brewstream.roast.packet.cif.KeyMaterialCif;

import java.security.SecureRandom;
import java.util.Arrays;

/**
 * One connection's live encryption state: the salt, the even and odd Stream
 * Encrypting Keys, and which of the two is currently in use. This is the piece
 * that turns the stateless primitives ({@link StreamKeyWrapper},
 * {@link PayloadCipher}) into something a connection can actually hold.
 *
 * <p><b>Why this is a context and not configuration.</b> The passphrase and key
 * length are static inputs an application supplies once. Everything here is
 * derived from them and then <em>changes over the life of the connection</em> —
 * keys get generated, replaced wholesale when a peer's Key Material message
 * arrives, and eventually rotated. Both references draw the same line: gosrt
 * keeps {@code Passphrase}/{@code PBKeyLen} in its {@code Config} but the
 * {@code crypto} object and {@code keyBaseEncryption} on {@code srtConn};
 * libsrt has socket options on one side and {@code CCryptoControl} on the
 * other. Conflating them would put mutable per-connection state into something
 * an application reasonably expects to be shareable and immutable.
 *
 * <p><b>Secret handling.</b> The passphrase is held as a {@code char[]} and can
 * be cleared with {@link #destroy()}; a {@code String} would be immutable and
 * un-clearable. {@link #toString()} deliberately reveals nothing but whether
 * keys are present — this object is exactly the kind of thing that ends up in a
 * log line by accident.
 *
 * <p><b>Who generates keys.</b> In SRT the <em>sender</em> generates the salt
 * and SEKs and announces them in a KM message; the receiver adopts them. A
 * connection that only receives never calls {@link #generateKeys()} — it starts
 * empty and gets everything from {@link #adopt}. Adopting takes the peer's salt
 * <em>first</em> and derives the key-encrypting key from that, then replaces
 * only the key(s) the message's KK field names (ported from gosrt's
 * {@code UnmarshalKM} — getting the order wrong derives the KEK from the wrong
 * salt and every unwrap fails).
 *
 * <p><b>Not here yet</b>: the rotation <em>policy</em> — gosrt's
 * {@code kmPreAnnounce}/{@code kmRefreshRate} countdowns, which decide when to
 * switch keys and when to re-announce. Those are driven by packet flow, so they
 * belong with the connection wiring; {@link #switchActiveKey()} is the
 * mechanism they'll drive. Neither reference has unit tests for that policy
 * (gosrt's lives in `connection.go`, which has none), so it is deliberately not
 * guessed at here.
 *
 * <p>Not thread-safe: like this codebase's other per-connection state, it
 * assumes the connection's single event-loop thread. (gosrt needs a
 * {@code cryptoLock} because its sender and receiver are separate goroutines.)
 */
public final class EncryptionContext {

    /** draft-sharabayko-srt.md §3.2.2: "The only valid length of salt defined is 128 bits." */
    private static final int SALT_BYTES = 16;

    private static final SecureRandom RANDOM = new SecureRandom();

    private final char[] passphrase;
    private final int keyLength;

    private byte[] salt;
    private byte[] evenSek;
    private byte[] oddSek;
    private KeyEncryption activeKey = KeyEncryption.EVEN;

    private EncryptionContext(char[] passphrase, int keyLength) {
        if (keyLength != 16 && keyLength != 24 && keyLength != 32) {
            throw new IllegalArgumentException("key length must be 16, 24, or 32 bytes, got " + keyLength);
        }
        this.passphrase = passphrase.clone();
        this.keyLength = keyLength;
    }

    /**
     * A context that will receive its keys from the peer's Key Material message
     * — the receiving side of a stream. Holds no keys until {@link #adopt}
     * succeeds.
     */
    public static EncryptionContext awaitingPeerKeys(char[] passphrase, int keyLength) {
        return new EncryptionContext(passphrase, keyLength);
    }

    /**
     * A context with a freshly generated salt and both SEKs — the sending side,
     * which announces them to the peer via {@link #keyMaterial}.
     */
    public static EncryptionContext generating(char[] passphrase, int keyLength) {
        EncryptionContext context = new EncryptionContext(passphrase, keyLength);
        context.generateKeys();
        return context;
    }

    /** Generates a new salt and both SEKs, discarding anything held before. */
    public void generateKeys() {
        salt = randomBytes(SALT_BYTES);
        evenSek = randomBytes(keyLength);
        oddSek = randomBytes(keyLength);
    }

    /**
     * True once this context can encrypt: it has a salt and the
     * {@linkplain #activeKey() active} key. Deliberately <em>not</em> "has both
     * keys" — a peer is free to announce only the one it is currently using,
     * and real libsrt does exactly that. Requiring both meant a context keyed
     * from such a peer reported no keys, so nothing was ever encrypted and the
     * peer dropped every packet as unexpectedly-cleartext (found against real
     * libsrt, which logs it as "Packet not encrypted ... dropped").
     */
    public boolean hasKeys() {
        return salt != null && keyFor(activeKey) != null;
    }

    /** Which key {@link #encrypt} currently uses, and which a peer should expect in a DATA header's KK field. */
    public KeyEncryption activeKey() {
        return activeKey;
    }

    /**
     * Flips the active key between even and odd. The <em>mechanism</em> for
     * rotation; the policy that decides when to call it doesn't exist yet — see
     * the class javadoc.
     */
    public void switchActiveKey() {
        activeKey = activeKey == KeyEncryption.EVEN ? KeyEncryption.ODD : KeyEncryption.EVEN;
    }

    /**
     * Builds the Key Material message announcing {@code which} of our keys, for
     * a KMREQ/KMRSP handshake extension. {@link KeyEncryption#BOTH} wraps the
     * even key followed by the odd one, matching how a peer unwraps them.
     *
     * @throws IllegalStateException if no keys have been generated or adopted
     */
    public KeyMaterialCif keyMaterial(KeyEncryption which) {
        requireKeys();
        byte[] keys = switch (which) {
            case EVEN -> evenSek.clone();
            case ODD -> oddSek.clone();
            case BOTH -> concat(evenSek, oddSek);
        };

        byte[] kek = StreamKeyWrapper.deriveKeyEncryptingKey(passphrase, salt, keyLength);
        try {
            byte[] wrap = StreamKeyWrapper.wrap(kek, keys);
            return new KeyMaterialCif(0, which, KeyMaterialCif.CIPHER_AES_CTR, 0, salt.clone(), wrap);
        } finally {
            Arrays.fill(kek, (byte) 0);
            Arrays.fill(keys, (byte) 0);
        }
    }

    /**
     * Adopts the keys a peer announced. Returns {@code false} — rather than
     * throwing — if the passphrase doesn't match, the message is an error
     * response, or its key length disagrees with ours; all three are peer
     * conditions to answer with a rejection, not faults. On {@code false}
     * nothing here changes.
     */
    public boolean adopt(KeyMaterialCif km) {
        if (km == null || km.isError() || km.keyEncryption() == null || km.keyLength() != keyLength) {
            return false;
        }

        // The peer's salt takes effect BEFORE the KEK is derived - see the class javadoc.
        byte[] effectiveSalt = km.salt().length == SALT_BYTES ? km.salt().clone() : salt;
        if (effectiveSalt == null) {
            return false; // no salt from the peer and none of our own to fall back on
        }

        byte[] kek = StreamKeyWrapper.deriveKeyEncryptingKey(passphrase, effectiveSalt, keyLength);
        byte[] unwrapped;
        try {
            unwrapped = StreamKeyWrapper.unwrap(kek, km.wrap());
        } finally {
            Arrays.fill(kek, (byte) 0);
        }
        if (unwrapped == null || unwrapped.length != keyLength * km.keyEncryption().keyCount()) {
            return false;
        }

        salt = effectiveSalt;
        switch (km.keyEncryption()) {
            case EVEN -> evenSek = unwrapped;
            case ODD -> oddSek = unwrapped;
            case BOTH -> {
                evenSek = Arrays.copyOfRange(unwrapped, 0, keyLength);
                oddSek = Arrays.copyOfRange(unwrapped, keyLength, keyLength * 2);
            }
        }
        // A peer that sends only one key is telling us that is the one in use.
        if (km.keyEncryption() != KeyEncryption.BOTH) {
            activeKey = km.keyEncryption();
        }
        return true;
    }

    /**
     * Encrypts a payload in place with the {@linkplain #activeKey() active key}.
     * The caller must put that key in the DATA packet's KK field so the peer
     * knows which one to decrypt with.
     */
    public void encrypt(byte[] payload, int packetSequenceNumber) {
        requireKeys();
        PayloadCipher.encryptOrDecrypt(payload, keyFor(activeKey), salt, packetSequenceNumber);
    }

    /** As {@link #encrypt(byte[], int)}, for a packet payload held in a {@link ByteBuf}. */
    public void encrypt(ByteBuf payload, int packetSequenceNumber) {
        requireKeys();
        PayloadCipher.encryptOrDecrypt(payload, keyFor(activeKey), salt, packetSequenceNumber);
    }

    /**
     * Decrypts a payload in place using the key the DATA header's KK field
     * names. Returns {@code false} if that key isn't held (a packet encrypted
     * with a key we were never told about — droppable, not fatal).
     */
    public boolean decrypt(byte[] payload, int packetSequenceNumber, KeyEncryption key) {
        if (!canUse(key)) {
            return false;
        }
        PayloadCipher.encryptOrDecrypt(payload, keyFor(key), salt, packetSequenceNumber);
        return true;
    }

    /** As {@link #decrypt(byte[], int, KeyEncryption)}, for a packet payload held in a {@link ByteBuf}. */
    public boolean decrypt(ByteBuf payload, int packetSequenceNumber, KeyEncryption key) {
        if (!canUse(key)) {
            return false;
        }
        PayloadCipher.encryptOrDecrypt(payload, keyFor(key), salt, packetSequenceNumber);
        return true;
    }

    /** Zeroes the passphrase and both keys. Call on connection teardown. */
    public void destroy() {
        Arrays.fill(passphrase, '\0');
        if (evenSek != null) {
            Arrays.fill(evenSek, (byte) 0);
        }
        if (oddSek != null) {
            Arrays.fill(oddSek, (byte) 0);
        }
        evenSek = null;
        oddSek = null;
        salt = null;
    }

    /** Deliberately reveals no key material — see the class javadoc. */
    @Override
    public String toString() {
        return "EncryptionContext[keyLength=" + keyLength + ", hasKeys=" + hasKeys()
                + ", activeKey=" + activeKey + "]";
    }

    /**
     * Whether a specific key is usable for decryption: it must name one real key
     * (not {@link KeyEncryption#BOTH}) that we actually hold. Checked per packet
     * rather than via {@link #hasKeys()}, since a peer may send using a key we
     * were never given even while our own active key is fine.
     */
    private boolean canUse(KeyEncryption key) {
        return salt != null && key != null && key != KeyEncryption.BOTH && keyFor(key) != null;
    }

    private byte[] keyFor(KeyEncryption key) {
        return key == KeyEncryption.EVEN ? evenSek : oddSek;
    }

    private void requireKeys() {
        if (!hasKeys()) {
            throw new IllegalStateException("no keys yet - generate them or adopt a peer's Key Material first");
        }
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] out = new byte[first.length + second.length];
        System.arraycopy(first, 0, out, 0, first.length);
        System.arraycopy(second, 0, out, first.length, second.length);
        return out;
    }

    private static byte[] randomBytes(int length) {
        byte[] bytes = new byte[length];
        RANDOM.nextBytes(bytes);
        return bytes;
    }
}
