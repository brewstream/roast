package org.brewstream.roast.packet.cif;

/**
 * The KK field: which Stream Encrypting Key(s) a message refers to. Two bits
 * wide, appearing both in a DATA packet's header (which key that packet's
 * payload was encrypted with) and in a {@link KeyMaterialCif} (which key(s) that
 * message carries).
 *
 * <p>{@code 00} — "no key / unencrypted" — is deliberately <em>not</em> a
 * constant here. It's a legal value in a DATA packet header, where
 * {@link org.brewstream.roast.packet.DataPacket#kk()} keeps it as a raw int, but
 * it is explicitly invalid in a Key Material message (gosrt's
 * {@code CIFKeyMaterialExtension.Unmarshal}: "invalid extension format (KK must
 * not be 0)"), which is this enum's only current use. Modelling it would make
 * every consumer handle a case the wire format forbids.
 */
public enum KeyEncryption {

    /** {@code 01} — the even key only. */
    EVEN(1),

    /** {@code 10} — the odd key only. */
    ODD(2),

    /** {@code 11} — both keys, wrapped back to back in that order. */
    BOTH(3);

    private final int code;

    KeyEncryption(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Returns {@code null} for an unrecognized code (including {@code 0}) rather than throwing. */
    public static KeyEncryption fromCode(int code) {
        return switch (code) {
            case 1 -> EVEN;
            case 2 -> ODD;
            case 3 -> BOTH;
            default -> null;
        };
    }

    /** How many wrapped keys a message with this KK carries — {@link #BOTH} carries two. */
    public int keyCount() {
        return this == BOTH ? 2 : 1;
    }
}
