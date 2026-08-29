package org.brewstream.roast.recv;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import org.brewstream.roast.packet.ControlPacket;
import org.brewstream.roast.packet.ControlType;
import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.packet.cif.LossListCodec;
import org.brewstream.roast.packet.cif.LossRange;

import java.util.List;

/** Builds a wire-ready NAK control packet from lost-packet ranges, via {@link LossListCodec}. */
public final class NakGenerator {

    private NakGenerator() {
    }

    public static ControlPacket build(List<LossRange> lostRanges, int timestamp, SrtSocketId destination) {
        ByteBuf cif = ByteBufAllocator.DEFAULT.buffer();
        LossListCodec.encode(lostRanges, cif);
        return new ControlPacket(ControlType.NAK, 0, timestamp, destination, cif);
    }
}
