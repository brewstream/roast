package org.brewstream.roast.packet.cif;

import io.netty.buffer.ByteBuf;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.util.CircularNumber;

/**
 * The ACK control packet's CIF (draft-sharabayko-srt.md "ACK (Acknowledgment)"):
 * delivery status up to a sequence number, plus optional RTT/buffer/bandwidth
 * estimates. The packet header's own Type-specific Information field (not part of
 * this CIF) carries the separate "Acknowledgement Number" — a sequential counter
 * identifying this Full ACK message for the peer's ACKACK reply; 0 for Light/Small.
 */
public record AckCif(
        AckVariant variant,
        CircularNumber lastAckPacketSequenceNumber,
        int rtt,
        int rttVar,
        int availableBufferSize,
        int packetsReceivingRate,
        int estimatedLinkCapacity,
        int receivingRate) {

    public static final int LITE_LENGTH = 4;
    public static final int SMALL_LENGTH = 16;
    public static final int FULL_LENGTH = 28;

    public static AckCif lite(CircularNumber lastAckPacketSequenceNumber) {
        return new AckCif(AckVariant.LITE, lastAckPacketSequenceNumber, 0, 0, 0, 0, 0, 0);
    }

    public static AckCif small(CircularNumber lastAckPacketSequenceNumber, int rtt, int rttVar, int availableBufferSize) {
        return new AckCif(AckVariant.SMALL, lastAckPacketSequenceNumber, rtt, rttVar, availableBufferSize, 0, 0, 0);
    }

    /** Returns {@code null} for a length that's neither 4, 16, nor at least 28 bytes. */
    public static AckCif decode(ByteBuf in) {
        int readable = in.readableBytes();
        if (readable != LITE_LENGTH && readable != SMALL_LENGTH && readable < FULL_LENGTH) {
            return null;
        }

        CircularNumber lastAck = CircularNumber.of(in.readInt() & 0x7FFF_FFFF, SrtPacket.MAX_SEQUENCE_NUMBER);
        if (readable == LITE_LENGTH) {
            return lite(lastAck);
        }

        int rtt = in.readInt();
        int rttVar = in.readInt();
        int availableBufferSize = in.readInt();
        if (readable == SMALL_LENGTH) {
            return small(lastAck, rtt, rttVar, availableBufferSize);
        }

        int packetsReceivingRate = in.readInt();
        int estimatedLinkCapacity = in.readInt();
        int receivingRate = in.readInt();
        return new AckCif(AckVariant.FULL, lastAck, rtt, rttVar, availableBufferSize,
                packetsReceivingRate, estimatedLinkCapacity, receivingRate);
    }

    public void encodeTo(ByteBuf out) {
        out.writeInt((int) lastAckPacketSequenceNumber.value());
        if (variant == AckVariant.LITE) {
            return;
        }

        out.writeInt(rtt);
        out.writeInt(rttVar);
        out.writeInt(availableBufferSize);
        if (variant == AckVariant.SMALL) {
            return;
        }

        out.writeInt(packetsReceivingRate);
        out.writeInt(estimatedLinkCapacity);
        out.writeInt(receivingRate);
    }
}
