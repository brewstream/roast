package org.brewstream.roast.packet.cif;

/**
 * The HSREQ/HSRSP extension's capability flags (draft-sharabayko-srt.md
 * "Handshake Extension Message Flags"). {@code crypt} and {@code retransmissionFlag}
 * are legacy flags both peers MUST set; {@code streamMode} true means buffer mode,
 * false means message mode.
 */
public record HandshakeExtensionFlags(
        boolean tsbpdSend,
        boolean tsbpdReceive,
        boolean crypt,
        boolean tooLatePacketDrop,
        boolean periodicNak,
        boolean retransmissionFlag,
        boolean streamMode,
        boolean packetFilter) {

    private static final int TSBPDSND = 1;
    private static final int TSBPDRCV = 1 << 1;
    private static final int CRYPT = 1 << 2;
    private static final int TLPKTDROP = 1 << 3;
    private static final int PERIODICNAK = 1 << 4;
    private static final int REXMITFLG = 1 << 5;
    private static final int STREAM = 1 << 6;
    private static final int PACKET_FILTER = 1 << 7;

    static HandshakeExtensionFlags decode(int bits) {
        return new HandshakeExtensionFlags(
                (bits & TSBPDSND) != 0,
                (bits & TSBPDRCV) != 0,
                (bits & CRYPT) != 0,
                (bits & TLPKTDROP) != 0,
                (bits & PERIODICNAK) != 0,
                (bits & REXMITFLG) != 0,
                (bits & STREAM) != 0,
                (bits & PACKET_FILTER) != 0);
    }

    int encode() {
        int bits = 0;
        bits |= tsbpdSend ? TSBPDSND : 0;
        bits |= tsbpdReceive ? TSBPDRCV : 0;
        bits |= crypt ? CRYPT : 0;
        bits |= tooLatePacketDrop ? TLPKTDROP : 0;
        bits |= periodicNak ? PERIODICNAK : 0;
        bits |= retransmissionFlag ? REXMITFLG : 0;
        bits |= streamMode ? STREAM : 0;
        bits |= packetFilter ? PACKET_FILTER : 0;
        return bits;
    }
}
