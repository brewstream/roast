package org.brewstream.roast.packet;

import io.netty.buffer.ByteBuf;

/**
 * F-bit = 1. {@code typeSpecificInfo} and {@code cif} (the control information
 * field) both have per-control-type meaning that Phase 2/3 handshake and ARQ
 * code will interpret; this layer only frames them.
 */
public record ControlPacket(
        ControlType type,
        int typeSpecificInfo,
        int timestamp,
        SrtSocketId destination,
        ByteBuf cif,
        int subtype) implements SrtPacket {

    /**
     * Without a subtype — every control type except {@link ControlType#USER_DEFINED}
     * leaves those 16 bits zero. Last component rather than beside {@code type}
     * (its wire position) so this shortened form can exist without touching
     * every construction site; same trade already made in {@code HandshakeCif}.
     */
    public ControlPacket(ControlType type, int typeSpecificInfo, int timestamp,
            SrtSocketId destination, ByteBuf cif) {
        this(type, typeSpecificInfo, timestamp, destination, cif, 0);
    }

    @Override
    public ByteBuf body() {
        return cif;
    }

    @Override
    public void encodeTo(ByteBuf out) {
        int word0 = 0x8000_0000 | ((type.code() & 0x7FFF) << 16) | (subtype & 0xFFFF);
        out.writeInt(word0);
        out.writeInt(typeSpecificInfo);
        out.writeInt(timestamp);
        out.writeInt(destination.value());
        out.writeBytes(cif);
        cif.release();
    }
}
