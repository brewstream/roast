package org.brewstream.roast.packet;

import io.netty.buffer.ByteBuf;

/**
 * F-bit = 0. {@code pp} is the 2-bit packet-position field (10 = first, 01 =
 * last, 11 = single, 00 = middle); {@code kk} is the 2-bit encryption key flag.
 */
public record DataPacket(
        int sequenceNumber,
        int pp,
        boolean inOrder,
        int kk,
        boolean retransmitted,
        int messageNumber,
        int timestamp,
        SrtSocketId destination,
        ByteBuf payload) implements SrtPacket {

    @Override
    public ByteBuf body() {
        return payload;
    }

    @Override
    public void encodeTo(ByteBuf out) {
        int word0 = sequenceNumber & 0x7FFF_FFFF;
        int word1 = ((pp & 0x3) << 30)
                | ((inOrder ? 1 : 0) << 29)
                | ((kk & 0x3) << 27)
                | ((retransmitted ? 1 : 0) << 26)
                | (messageNumber & 0x03FF_FFFF);
        out.writeInt(word0);
        out.writeInt(word1);
        out.writeInt(timestamp);
        out.writeInt(destination.value());
        out.writeBytes(payload);
        payload.release();
    }
}
