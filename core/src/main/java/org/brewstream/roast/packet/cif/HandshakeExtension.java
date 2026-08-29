package org.brewstream.roast.packet.cif;

import io.netty.buffer.ByteBuf;

/**
 * The HSREQ/HSRSP extension payload (draft-sharabayko-srt.md "Handshake Extension
 * Message"). Always 12 bytes on the wire: SRT version, capability flags, then the
 * receive/send TSBPD latency in milliseconds.
 */
public record HandshakeExtension(
        int srtVersion,
        HandshakeExtensionFlags flags,
        int receiveTsbpdDelayMillis,
        int sendTsbpdDelayMillis) {

    static final int LENGTH = 12;

    static HandshakeExtension decode(ByteBuf in) {
        int srtVersion = in.readInt();
        HandshakeExtensionFlags flags = HandshakeExtensionFlags.decode(in.readInt());
        int receiveTsbpdDelayMillis = in.readUnsignedShort();
        int sendTsbpdDelayMillis = in.readUnsignedShort();
        return new HandshakeExtension(srtVersion, flags, receiveTsbpdDelayMillis, sendTsbpdDelayMillis);
    }

    void encodeTo(ByteBuf out) {
        out.writeInt(srtVersion);
        out.writeInt(flags.encode());
        out.writeShort(receiveTsbpdDelayMillis);
        out.writeShort(sendTsbpdDelayMillis);
    }
}
