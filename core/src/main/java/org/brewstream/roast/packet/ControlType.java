package org.brewstream.roast.packet;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** SRT control packet types, encoded in bits 30-16 of the common header. */
public enum ControlType {
    HANDSHAKE(0x0),
    KEEPALIVE(0x1),
    ACK(0x2),
    NAK(0x3),
    CONGESTION_WARNING(0x4),
    SHUTDOWN(0x5),
    ACKACK(0x6),
    DROPREQ(0x7),
    PEERERROR(0x8),
    USER_DEFINED(0x7FFF);

    private static final Map<Integer, ControlType> BY_CODE = Stream.of(values())
            .collect(Collectors.toMap(t -> t.code, t -> t));

    private final int code;

    ControlType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Returns null for a code with no known mapping, rather than throwing. */
    public static ControlType fromCode(int code) {
        return BY_CODE.get(code);
    }
}
