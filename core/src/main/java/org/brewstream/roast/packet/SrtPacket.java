package org.brewstream.roast.packet;

import io.netty.buffer.ByteBuf;

/**
 * An SRT packet's fixed 16-byte common header, decoded into one of the two
 * packet families the header's F-bit distinguishes. Variable-length bodies
 * (payload / control information field) are carried as a raw {@link ByteBuf} —
 * per-control-type CIF field parsing is not this layer's job.
 */
public sealed interface SrtPacket permits DataPacket, ControlPacket {

    int HEADER_LENGTH = 16;

    SrtSocketId destination();

    int timestamp();

    /** The variable-length payload (data packets) or CIF (control packets). */
    ByteBuf body();

    /**
     * Encodes this packet's header and body into {@code out} at its current writer
     * index. Consumes (releases) {@link #body()} in the process — callers that need
     * to send the same packet again (e.g. ARQ retransmission) must
     * {@code body().retainedDuplicate()} into a fresh {@link SrtPacket} first.
     */
    void encodeTo(ByteBuf out);

    /**
     * Decodes one SRT packet's common header from {@code in}, consuming the header
     * and the remainder of the buffer as the body. Returns {@code null} if {@code in}
     * is shorter than {@link #HEADER_LENGTH} or names an unrecognized control type —
     * callers should drop such datagrams rather than treat them as a protocol error,
     * since arbitrary UDP noise can land on the port.
     */
    static SrtPacket decode(ByteBuf in) {
        if (in.readableBytes() < HEADER_LENGTH) {
            return null;
        }
        int word0 = in.readInt();
        boolean isControl = (word0 & 0x8000_0000) != 0;
        int word1 = in.readInt();
        int timestamp = in.readInt();
        SrtSocketId destination = SrtSocketId.of(in.readInt());
        ByteBuf body = in.readRetainedSlice(in.readableBytes());

        if (isControl) {
            ControlType type = ControlType.fromCode((word0 >>> 16) & 0x7FFF);
            if (type == null) {
                body.release();
                return null;
            }
            return new ControlPacket(type, word1, timestamp, destination, body);
        }

        int sequenceNumber = word0 & 0x7FFF_FFFF;
        int pp = (word1 >>> 30) & 0x3;
        boolean inOrder = ((word1 >>> 29) & 0x1) != 0;
        int kk = (word1 >>> 27) & 0x3;
        boolean retransmitted = ((word1 >>> 26) & 0x1) != 0;
        int messageNumber = word1 & 0x03FF_FFFF;
        return new DataPacket(sequenceNumber, pp, inOrder, kk, retransmitted, messageNumber,
                timestamp, destination, body);
    }
}
