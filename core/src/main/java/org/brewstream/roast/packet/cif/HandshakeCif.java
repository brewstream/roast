package org.brewstream.roast.packet.cif;

import io.netty.buffer.ByteBuf;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.util.CircularNumber;

import java.net.InetAddress;
import java.nio.charset.StandardCharsets;

/**
 * The handshake control packet's CIF (draft-sharabayko-srt.md handshake section):
 * a fixed 48-byte base structure, followed — for a CONCLUSION message that declares
 * a nonzero extension field — by a run of TLV extension blocks. Only the HSREQ/HSRSP
 * capability extension and the Stream ID extension are parsed; KMREQ/KMRSP
 * (encryption, Phase 5) and Congestion Control blocks are recognized but skipped by
 * their declared length. {@code isRequest} isn't wire data — it's which side is
 * sending, needed to pick the HSREQ vs. HSRSP extension tag on encode.
 */
public record HandshakeCif(
        boolean isRequest,
        int version,
        int encryptionField,
        int extensionField,
        CircularNumber initialPacketSequenceNumber,
        int maxTransmissionUnitSize,
        int maxFlowWindowSize,
        HandshakeType handshakeType,
        SrtSocketId srtSocketId,
        int synCookie,
        InetAddress peerAddress,
        HandshakeExtension handshakeExtension,
        String streamId) {

    public static final int BASE_LENGTH = 48;

    /**
     * Decodes a handshake CIF. Returns {@code null} for anything malformed: fewer
     * than {@link #BASE_LENGTH} bytes, an unrecognized handshake type or peer
     * address, or a truncated/mis-sized extension block — a corrupt handshake
     * should be dropped like any other bad datagram, not crash the pipeline.
     */
    public static HandshakeCif decode(ByteBuf in, boolean isRequest) {
        if (in.readableBytes() < BASE_LENGTH) {
            return null;
        }

        int version = in.readInt();
        int encryptionField = in.readUnsignedShort();
        int extensionField = in.readUnsignedShort();
        CircularNumber initialPacketSequenceNumber =
                CircularNumber.of(in.readInt() & 0x7FFF_FFFF, SrtPacket.MAX_SEQUENCE_NUMBER);
        int maxTransmissionUnitSize = in.readInt();
        int maxFlowWindowSize = in.readInt();
        HandshakeType handshakeType = HandshakeType.fromCode(in.readInt());
        if (handshakeType == null) {
            return null;
        }
        SrtSocketId srtSocketId = SrtSocketId.of(in.readInt());
        int synCookie = in.readInt();
        InetAddress peerAddress = PeerAddressCodec.decode(in);
        if (peerAddress == null) {
            return null;
        }

        if (handshakeType != HandshakeType.CONCLUSION || extensionField == 0 || !in.isReadable()) {
            return new HandshakeCif(isRequest, version, encryptionField, extensionField,
                    initialPacketSequenceNumber, maxTransmissionUnitSize, maxFlowWindowSize, handshakeType,
                    srtSocketId, synCookie, peerAddress, null, null);
        }

        HandshakeExtension handshakeExtension = null;
        String streamId = null;

        while (in.readableBytes() >= 4) {
            ExtensionType type = ExtensionType.fromCode(in.readUnsignedShort());
            int extensionLength = in.readUnsignedShort() * 4;
            if (in.readableBytes() < extensionLength) {
                return null;
            }

            if (type == ExtensionType.HSREQ || type == ExtensionType.HSRSP) {
                if (extensionLength != HandshakeExtension.LENGTH) {
                    return null;
                }
                handshakeExtension = HandshakeExtension.decode(in);
            } else if (type == ExtensionType.SID) {
                streamId = decodeStreamId(in, extensionLength);
            } else {
                in.skipBytes(extensionLength);
            }
        }

        return new HandshakeCif(isRequest, version, encryptionField, extensionField,
                initialPacketSequenceNumber, maxTransmissionUnitSize, maxFlowWindowSize, handshakeType,
                srtSocketId, synCookie, peerAddress, handshakeExtension, streamId);
    }

    public void encodeTo(ByteBuf out) {
        out.writeInt(version);
        out.writeShort(encryptionField);
        out.writeShort(extensionField);
        out.writeInt((int) initialPacketSequenceNumber.value());
        out.writeInt(maxTransmissionUnitSize);
        out.writeInt(maxFlowWindowSize);
        out.writeInt(handshakeType.code());
        out.writeInt(srtSocketId.value());
        out.writeInt(synCookie);
        PeerAddressCodec.encode(peerAddress, out);

        if (handshakeExtension != null) {
            out.writeShort((isRequest ? ExtensionType.HSREQ : ExtensionType.HSRSP).code());
            out.writeShort(HandshakeExtension.LENGTH / 4);
            handshakeExtension.encodeTo(out);
        }

        if (streamId != null && !streamId.isEmpty()) {
            byte[] padded = padTo4ByteWords(streamId.getBytes(StandardCharsets.US_ASCII));
            out.writeShort(ExtensionType.SID.code());
            out.writeShort(padded.length / 4);
            writeByteSwappedWords(padded, out);
        }
    }

    /**
     * Stream ID (and, unimplemented here, Congestion Control) extension text is
     * stored 4 bytes at a time with each 4-byte word byte-reversed on the wire —
     * the same quirk as {@link PeerAddressCodec}, just word-sized instead of
     * whole-field. {@code length} is already validated {@code <=} readable bytes.
     */
    private static String decodeStreamId(ByteBuf in, int length) {
        byte[] text = new byte[length];
        int base = in.readerIndex();
        for (int i = 0; i < length; i += 4) {
            text[i] = in.getByte(base + i + 3);
            text[i + 1] = in.getByte(base + i + 2);
            text[i + 2] = in.getByte(base + i + 1);
            text[i + 3] = in.getByte(base + i);
        }
        in.skipBytes(length);

        int end = text.length;
        while (end > 0 && text[end - 1] == 0) {
            end--;
        }
        return new String(text, 0, end, StandardCharsets.US_ASCII);
    }

    private static byte[] padTo4ByteWords(byte[] raw) {
        int paddedLength = ((raw.length + 3) / 4) * 4;
        byte[] padded = new byte[paddedLength];
        System.arraycopy(raw, 0, padded, 0, raw.length);
        return padded;
    }

    private static void writeByteSwappedWords(byte[] padded, ByteBuf out) {
        for (int i = 0; i < padded.length; i += 4) {
            out.writeByte(padded[i + 3]);
            out.writeByte(padded[i + 2]);
            out.writeByte(padded[i + 1]);
            out.writeByte(padded[i]);
        }
    }
}
