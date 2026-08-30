package org.brewstream.roast.handshake;

import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.packet.cif.HandshakeCif;
import org.brewstream.roast.packet.cif.HandshakeExtension;
import org.brewstream.roast.packet.cif.HandshakeExtensionFlags;
import org.brewstream.roast.packet.cif.HandshakeType;
import org.brewstream.roast.packet.cif.KeyMaterialCif;
import org.brewstream.roast.util.CircularNumber;

import java.net.InetAddress;

/**
 * The caller side of the HSv5 induction→conclusion exchange (draft-sharabayko-srt.md
 * §4.3.1), the mirror image of {@link ListenerHandshake} — pure decision logic, no
 * socket I/O, takes a decoded {@link HandshakeCif} reply and produces the next
 * {@link HandshakeCif} to send, or a verdict on the final reply. Wiring this into a
 * live channel and constructing the resulting connection is left to
 * {@code SrtCaller}.
 *
 * <p>Traced directly from gosrt's {@code dial.go} (its {@code dialer.handleHandshake})
 * rather than assumed: the initial sequence number is chosen by the caller and
 * echoed back unchanged by the listener throughout — not derived from the peer (see
 * {@link ListenerHandshake}, which already echoes it the same way from the other
 * side); the induction reply's MTU/flow-window values carry straight through into
 * the conclusion request unchanged (gosrt never overwrites them — it reuses the same
 * in-memory packet for both messages); the SYN cookie the listener hands out at
 * induction is echoed back unchanged in the conclusion request. Only HSv5 is
 * supported — no HSv4 fallback.
 */
public final class CallerHandshake {

    /** The magic value SRT uses in an induction reply's Extension Field to advertise SRT support (vs. plain UDT). */
    private static final int SRT_MAGIC_CODE = 0x4A17;
    private static final int MAX_MSS_SIZE = 1500;
    private static final int DEFAULT_FLOW_WINDOW_SIZE = 8192;

    private final int flowWindowSize;

    public CallerHandshake() {
        this(DEFAULT_FLOW_WINDOW_SIZE);
    }

    /** @param flowWindowSize the receive window advertised in the induction request (see {@code SrtConfig}) */
    public CallerHandshake(int flowWindowSize) {
        this.flowWindowSize = flowWindowSize;
    }

    /** The fixed induction request: version 4, zero ISN/cookie, no extensions — matches gosrt's {@code sendInduction} exactly. */
    public HandshakeCif buildInductionRequest(SrtSocketId ownSocketId, InetAddress ownAddress) {
        return new HandshakeCif(
                true, 4, 0, 2,
                CircularNumber.of(0, SrtPacket.MAX_SEQUENCE_NUMBER), MAX_MSS_SIZE, flowWindowSize,
                HandshakeType.INDUCTION.code(),
                ownSocketId, 0, ownAddress, null, null);
    }

    /** Only HSv5 is supported — a v4 (or otherwise unrecognized) reply means the peer doesn't speak it. */
    public boolean isSupportedInductionReply(HandshakeCif reply) {
        return reply.version() == 5 && reply.extensionField() == SRT_MAGIC_CODE;
    }

    /**
     * Builds the conclusion request from a validated induction reply: echoes the
     * cookie and MTU/flow-window unchanged, carries our own initial sequence number
     * (see the class javadoc), and attaches the HSREQ + (if non-empty) SID
     * extensions. {@code extensionField} uses the same bit convention
     * {@link ListenerHandshake#buildAcceptResponse} already does.
     */
    public HandshakeCif buildConclusionRequest(HandshakeCif inductionReply, SrtSocketId ownSocketId,
            InetAddress ownAddress, CircularNumber ownInitialSequenceNumber, int srtVersion,
            int receiveLatencyMillis, int sendLatencyMillis, String streamId) {
        return buildConclusionRequest(inductionReply, ownSocketId, ownAddress, ownInitialSequenceNumber,
                srtVersion, receiveLatencyMillis, sendLatencyMillis, streamId, null);
    }

    /**
     * As above, additionally offering {@code keyMaterial} as a KMREQ extension.
     * The <em>caller</em> is the side that generates the keys: it announces them
     * here, and a listener that can unwrap them with the same passphrase echoes
     * the identical message back as KMRSP. Pass {@code null} for an unencrypted
     * connection.
     *
     * <p>The Encryption Field advertises the cipher family and key size
     * (draft-sharabayko-srt.md Table 2: {@code 2} = AES-128 / 16 bytes,
     * {@code 3} = 24, {@code 4} = 32 — the key length in eight-byte units).
     */
    public HandshakeCif buildConclusionRequest(HandshakeCif inductionReply, SrtSocketId ownSocketId,
            InetAddress ownAddress, CircularNumber ownInitialSequenceNumber, int srtVersion,
            int receiveLatencyMillis, int sendLatencyMillis, String streamId, KeyMaterialCif keyMaterial) {
        boolean hasStreamId = streamId != null && !streamId.isEmpty();
        int extensionField = 1 | (keyMaterial != null ? 2 : 0) | (hasStreamId ? 4 : 0);
        int encryptionField = keyMaterial != null ? keyMaterial.keyLength() / 8 : 0;

        HandshakeExtensionFlags flags = new HandshakeExtensionFlags(true, true, true, true, true, true, false, false);
        HandshakeExtension extension = new HandshakeExtension(srtVersion, flags, receiveLatencyMillis, sendLatencyMillis);

        return new HandshakeCif(
                true, 5, encryptionField, extensionField,
                ownInitialSequenceNumber, inductionReply.maxTransmissionUnitSize(), inductionReply.maxFlowWindowSize(),
                HandshakeType.CONCLUSION.code(),
                ownSocketId, inductionReply.synCookie(), ownAddress, extension, streamId, keyMaterial);
    }

    /**
     * Validates a conclusion reply's protocol-level correctness — the same checklist
     * {@link ListenerHandshake#validateConclusion} runs, from the caller's side:
     * version, required capability flags (TSBPD both directions, TLPKTDROP, periodic
     * NAK, retransmission), live-mode only (no STREAM/message-mode support). A
     * rejection code in the handshake-type field is reported as {@link
     * ConclusionReplyOutcome.Rejected} rather than validated further.
     */
    public ConclusionReplyOutcome validateConclusionReply(HandshakeCif reply, int minSrtVersion,
            int ourReceiveLatencyMillis, int ourSendLatencyMillis) {
        if (reply.isRejection()) {
            return new ConclusionReplyOutcome.Rejected(reply.rejectionReason());
        }
        if (reply.handshakeType() != HandshakeType.CONCLUSION) {
            return new ConclusionReplyOutcome.ProtocolViolation(
                    "expected a CONCLUSION reply, got " + reply.handshakeType());
        }
        if (reply.version() != 5) {
            return new ConclusionReplyOutcome.ProtocolViolation(
                    "unsupported handshake version (" + reply.version() + ")");
        }

        HandshakeExtension extension = reply.handshakeExtension();
        if (extension == null) {
            return new ConclusionReplyOutcome.ProtocolViolation("missing handshake extension");
        }
        if (extension.srtVersion() < minSrtVersion) {
            return new ConclusionReplyOutcome.ProtocolViolation("peer SRT version is not sufficient");
        }

        HandshakeExtensionFlags flags = extension.flags();
        if (!flags.tsbpdSend() || !flags.tsbpdReceive() || !flags.tooLatePacketDrop()
                || !flags.periodicNak() || !flags.retransmissionFlag()) {
            return new ConclusionReplyOutcome.ProtocolViolation("peer doesn't agree on required SRT flags");
        }
        if (flags.streamMode()) {
            return new ConclusionReplyOutcome.ProtocolViolation("peer doesn't support live streaming");
        }

        int receiveLatencyMillis = Math.max(ourReceiveLatencyMillis, extension.sendTsbpdDelayMillis());
        int sendLatencyMillis = Math.max(ourSendLatencyMillis, extension.receiveTsbpdDelayMillis());
        return new ConclusionReplyOutcome.Connected(extension.srtVersion(), receiveLatencyMillis, sendLatencyMillis);
    }
}
