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
 * a nonzero extension field — by a run of TLV extension blocks. The HSREQ/HSRSP
 * capability extension, the Stream ID, and the KMREQ/KMRSP key material (see
 * {@link KeyMaterialCif}) are parsed; Congestion Control blocks are recognized but
 * skipped by their declared length. {@code isRequest} isn't wire data — it's which
 * side is sending, needed to pick the HSREQ vs. HSRSP and KMREQ vs. KMRSP extension
 * tags on encode.
 *
 * <p>{@code handshakeTypeCode} is the raw wire value rather than a {@link HandshakeType}
 * because a rejection isn't a separate field — it's signaled by putting a
 * {@link RejectionReason} code (or any other value outside the 5 known progression
 * values) directly into this field. Use {@link #handshakeType()} for the normal case
 * and {@link #isRejection()}/{@link #rejectionReason()} for the rejection case.
 */
public record HandshakeCif(
        boolean isRequest,
        int version,
        int encryptionField,
        int extensionField,
        CircularNumber initialPacketSequenceNumber,
        int maxTransmissionUnitSize,
        int maxFlowWindowSize,
        int handshakeTypeCode,
        SrtSocketId srtSocketId,
        int synCookie,
        InetAddress peerAddress,
        HandshakeExtension handshakeExtension,
        String streamId,
        KeyMaterialCif keyMaterial) {

    public static final int BASE_LENGTH = 48;

    /**
     * Without key material — the common case, and what every caller predating
     * encryption support used. {@code keyMaterial} is last rather than in wire
     * order (it sits between HSREQ/HSRSP and SID on the wire) precisely so this
     * overload can exist: a record's canonical constructor can't be shortened,
     * but a prefix of it makes a natural convenience constructor, and that beat
     * churning two dozen call sites to thread a {@code null} through.
     */
    public HandshakeCif(boolean isRequest, int version, int encryptionField, int extensionField,
            CircularNumber initialPacketSequenceNumber, int maxTransmissionUnitSize, int maxFlowWindowSize,
            int handshakeTypeCode, SrtSocketId srtSocketId, int synCookie, InetAddress peerAddress,
            HandshakeExtension handshakeExtension, String streamId) {
        this(isRequest, version, encryptionField, extensionField, initialPacketSequenceNumber,
                maxTransmissionUnitSize, maxFlowWindowSize, handshakeTypeCode, srtSocketId, synCookie,
                peerAddress, handshakeExtension, streamId, null);
    }

    /** Null if {@link #isRejection()} — this field holds a rejection reason instead. */
    public HandshakeType handshakeType() {
        return HandshakeType.fromCode(handshakeTypeCode);
    }

    /** Per gosrt's {@code IsHandshake}/{@code IsRejection}: anything not a known progression value is a rejection. */
    public boolean isRejection() {
        return handshakeType() == null;
    }

    /** Null if {@link #isRejection()} is false, or if it's a rejection code with no name in {@link RejectionReason}. */
    public RejectionReason rejectionReason() {
        return RejectionReason.fromCode(handshakeTypeCode);
    }

    /**
     * Decodes a handshake CIF. Returns {@code null} for anything malformed: fewer
     * than {@link #BASE_LENGTH} bytes, an unrecognized peer address, or a
     * truncated/mis-sized extension block — a corrupt handshake should be dropped
     * like any other bad datagram, not crash the pipeline. An unrecognized
     * handshake-type code is NOT treated as malformed — see {@link #isRejection()}.
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
        int handshakeTypeCode = in.readInt();
        SrtSocketId srtSocketId = SrtSocketId.of(in.readInt());
        int synCookie = in.readInt();
        InetAddress peerAddress = PeerAddressCodec.decode(in);
        if (peerAddress == null) {
            return null;
        }

        if (handshakeTypeCode != HandshakeType.CONCLUSION.code() || extensionField == 0 || !in.isReadable()) {
            return new HandshakeCif(isRequest, version, encryptionField, extensionField,
                    initialPacketSequenceNumber, maxTransmissionUnitSize, maxFlowWindowSize, handshakeTypeCode,
                    srtSocketId, synCookie, peerAddress, null, null);
        }

        HandshakeExtension handshakeExtension = null;
        String streamId = null;
        KeyMaterialCif keyMaterial = null;

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
            } else if (type == ExtensionType.KMREQ || type == ExtensionType.KMRSP) {
                // Hand the KM codec exactly its own extension and no more, so a
                // 4-byte rejection is seen as such rather than as a truncated message.
                keyMaterial = KeyMaterialCif.decode(in.readSlice(extensionLength));
                if (keyMaterial == null) {
                    return null;
                }
            } else {
                in.skipBytes(extensionLength);
            }
        }

        return new HandshakeCif(isRequest, version, encryptionField, extensionField,
                initialPacketSequenceNumber, maxTransmissionUnitSize, maxFlowWindowSize, handshakeTypeCode,
                srtSocketId, synCookie, peerAddress, handshakeExtension, streamId, keyMaterial);
    }

    public void encodeTo(ByteBuf out) {
        out.writeInt(version);
        out.writeShort(encryptionField);
        out.writeShort(extensionField);
        out.writeInt((int) initialPacketSequenceNumber.value());
        out.writeInt(maxTransmissionUnitSize);
        out.writeInt(maxFlowWindowSize);
        out.writeInt(handshakeTypeCode);
        out.writeInt(srtSocketId.value());
        out.writeInt(synCookie);
        PeerAddressCodec.encode(peerAddress, out);

        if (handshakeExtension != null) {
            out.writeShort((isRequest ? ExtensionType.HSREQ : ExtensionType.HSRSP).code());
            out.writeShort(HandshakeExtension.LENGTH / 4);
            handshakeExtension.encodeTo(out);
        }

        if (keyMaterial != null) {
            // Between the capability extension and the Stream ID, matching the
            // order a real peer emits (gosrt's Marshal, and its V5 golden vector).
            out.writeShort((isRequest ? ExtensionType.KMREQ : ExtensionType.KMRSP).code());
            // The length is in 4-byte words and isn't known until the body is
            // written, so reserve it and backfill.
            int lengthIndex = out.writerIndex();
            out.writeShort(0);
            int bodyStart = out.writerIndex();
            keyMaterial.encodeTo(out);
            out.setShort(lengthIndex, (out.writerIndex() - bodyStart) / 4);
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
