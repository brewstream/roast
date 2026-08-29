package org.brewstream.roast.packet.cif;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The handshake CIF's Handshake Type field (draft-sharabayko-srt.md handshake
 * section). {@link #DONE}/{@link #AGREEMENT}/{@link #CONCLUSION} are encoded as the
 * top three values below {@code 0}, i.e. negative when read as a signed 32-bit int
 * — that's not a bug, it's the wire encoding.
 */
public enum HandshakeType {
    DONE(0xFFFFFFFD),
    AGREEMENT(0xFFFFFFFE),
    CONCLUSION(0xFFFFFFFF),
    WAVEHAND(0x0000_0000),
    INDUCTION(0x0000_0001);

    private static final Map<Integer, HandshakeType> BY_CODE =
            Stream.of(values()).collect(Collectors.toMap(t -> t.code, t -> t));

    private final int code;

    HandshakeType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Returns null for an unrecognized code, rather than throwing. */
    public static HandshakeType fromCode(int code) {
        return BY_CODE.get(code);
    }
}
