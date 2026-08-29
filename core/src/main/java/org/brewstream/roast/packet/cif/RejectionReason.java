package org.brewstream.roast.packet.cif;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Named SRT connection-rejection reasons. A rejection isn't a separate CIF field —
 * it's signaled by putting one of these values (or any other numeric value that
 * isn't a real {@link HandshakeType}) directly into the handshake CIF's Handshake
 * Type field in place of a normal progression value; see
 * {@link HandshakeCif#isRejection()}. Values per gosrt's listen.go
 * (github.com/datarhei/gosrt) {@code REJ_} and {@code REJX_} constants.
 */
public enum RejectionReason {
    UNKNOWN(1000),
    SYSTEM(1001),
    PEER(1002),
    RESOURCE(1003),
    ROGUE(1004),
    BACKLOG(1005),
    IPE(1006),
    CLOSE(1007),
    VERSION(1008),
    RDVCOOKIE(1009),
    BADSECRET(1010),
    UNSECURE(1011),
    MESSAGEAPI(1012),
    CONGESTION(1013),
    FILTER(1014),
    GROUP(1015),
    BAD_REQUEST(1400),
    UNAUTHORIZED(1401),
    OVERLOAD(1402),
    FORBIDDEN(1403),
    NOTFOUND(1404),
    BAD_MODE(1405),
    UNACCEPTABLE(1406),
    CONFLICT(1407),
    NOTSUP_MEDIA(1415),
    LOCKED(1423),
    FAILED_DEPEND(1424),
    ISE(1500),
    UNIMPLEMENTED(1501),
    GW(1502),
    DOWN(1503),
    VERSION_UNSUPPORTED(1505),
    NOROOM(1507);

    private static final Map<Integer, RejectionReason> BY_CODE =
            Stream.of(values()).collect(Collectors.toMap(r -> r.code, r -> r));

    private final int code;

    RejectionReason(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Returns null for a rejection code with no name here — still a valid rejection. */
    public static RejectionReason fromCode(int code) {
        return BY_CODE.get(code);
    }
}
