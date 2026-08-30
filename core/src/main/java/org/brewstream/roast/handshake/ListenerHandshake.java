package org.brewstream.roast.handshake;

import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.packet.cif.HandshakeCif;
import org.brewstream.roast.packet.cif.HandshakeExtension;
import org.brewstream.roast.packet.cif.HandshakeExtensionFlags;
import org.brewstream.roast.packet.cif.HandshakeType;
import org.brewstream.roast.packet.cif.KeyMaterialCif;
import org.brewstream.roast.packet.cif.RejectionReason;

import java.net.InetAddress;

/**
 * The listener side of the HSv5 induction→conclusion exchange (draft-sharabayko-srt.md
 * §4.3.1, mirrored against gosrt's {@code newConnRequest}/{@code Accept} in
 * conn_request.go). Pure decision logic — no socket I/O, no connection registry: it
 * takes a decoded {@link HandshakeCif} and produces the {@link HandshakeCif} to send
 * back. Wiring this into a live {@code SrtSocketIdDemultiplexer} acceptor, generating
 * real socket IDs, and running an app-level accept/reject callback on StreamID are
 * all left to a future {@code SrtListener}.
 *
 * <p>Deliberately not replicated here (needs a config object this pass doesn't have):
 * MSS/payload-size negotiation when the peer advertises a smaller MTU, and rejecting
 * on Congestion Control mismatch (we don't parse that extension at all yet).
 */
public final class ListenerHandshake {

    /** The magic value SRT uses in an induction reply's Extension Field to advertise SRT support (vs. plain UDT). */
    private static final int SRT_MAGIC_CODE = 0x4A17;
    private static final int MAX_MSS_SIZE = 1500;

    private final SynCookie cookie;
    private final InetAddress ownAddress;
    private final int srtVersion;

    /**
     * @param srtVersion this listener's own SRT version — advertised in accept
     *                   responses, and used as the minimum version a peer must
     *                   report to avoid a {@link RejectionReason#VERSION} rejection.
     */
    public ListenerHandshake(SynCookie cookie, InetAddress ownAddress, int srtVersion) {
        this.cookie = cookie;
        this.ownAddress = ownAddress;
        this.srtVersion = srtVersion;
    }

    /** An INDUCTION request always gets a reply — there's nothing to reject yet, just a cookie to hand out. */
    public HandshakeCif onInduction(HandshakeCif request, String senderAddress) {
        return new HandshakeCif(
                false, 5, 0, SRT_MAGIC_CODE,
                request.initialPacketSequenceNumber(), request.maxTransmissionUnitSize(),
                request.maxFlowWindowSize(), HandshakeType.INDUCTION.code(),
                request.srtSocketId(), cookie.get(senderAddress), ownAddress,
                request.handshakeExtension(), request.streamId());
    }

    /**
     * Validates a CONCLUSION's protocol-level correctness: the echoed SYN cookie,
     * MTU sanity, SRT version, and the capability flags every real SRT peer must
     * set (TSBPD both directions, TLPKTDROP, periodic NAK, retransmission) — live
     * streaming only, message-mode (STREAM flag) is rejected.
     */
    public ConclusionOutcome validateConclusion(HandshakeCif request, String senderAddress) {
        if (!cookie.verify(request.synCookie(), senderAddress)) {
            return new ConclusionOutcome.Rejected(buildRejectResponse(request, RejectionReason.ROGUE));
        }
        if (request.maxTransmissionUnitSize() > MAX_MSS_SIZE) {
            return new ConclusionOutcome.Rejected(buildRejectResponse(request, RejectionReason.ROGUE));
        }
        if (request.version() != 5) {
            return new ConclusionOutcome.Rejected(buildRejectResponse(request, RejectionReason.ROGUE));
        }

        HandshakeExtension extension = request.handshakeExtension();
        if (extension == null) {
            return new ConclusionOutcome.Rejected(buildRejectResponse(request, RejectionReason.ROGUE));
        }
        if (extension.srtVersion() < srtVersion) {
            return new ConclusionOutcome.Rejected(buildRejectResponse(request, RejectionReason.VERSION));
        }

        HandshakeExtensionFlags flags = extension.flags();
        if (!flags.tsbpdSend() || !flags.tsbpdReceive() || !flags.tooLatePacketDrop()
                || !flags.periodicNak() || !flags.retransmissionFlag()) {
            return new ConclusionOutcome.Rejected(buildRejectResponse(request, RejectionReason.ROGUE));
        }
        if (flags.streamMode()) {
            return new ConclusionOutcome.Rejected(buildRejectResponse(request, RejectionReason.MESSAGEAPI));
        }

        return new ConclusionOutcome.Valid(request);
    }

    /** Builds a rejection response, echoing back the request's fields with the handshake-type field replaced. */
    public HandshakeCif buildRejectResponse(HandshakeCif request, RejectionReason reason) {
        return new HandshakeCif(
                false, request.version(), request.encryptionField(), request.extensionField(),
                request.initialPacketSequenceNumber(), request.maxTransmissionUnitSize(),
                request.maxFlowWindowSize(), reason.code(),
                request.srtSocketId(), request.synCookie(), ownAddress,
                request.handshakeExtension(), request.streamId());
    }

    /**
     * Builds the final CONCLUSION response for an accepted connection. TSBPD delay in
     * each direction is the larger of what we'd ask for and what the peer asked for —
     * the standard SRT negotiation (each side's "receive" delay is compared against
     * the other's "send" delay, since the two are the same logical buffer from
     * opposite ends).
     */
    public HandshakeCif buildAcceptResponse(HandshakeCif request, SrtSocketId assignedSocketId,
            int ourReceiveTsbpdDelayMillis, int ourSendTsbpdDelayMillis) {
        return buildAcceptResponse(request, assignedSocketId, ourReceiveTsbpdDelayMillis,
                ourSendTsbpdDelayMillis, null);
    }

    /**
     * As above, additionally echoing {@code keyMaterial} back as a KMRSP
     * extension. SRT's key exchange is a mirror rather than a fresh
     * announcement: the caller generates the keys and sends them as KMREQ, and
     * the listener — having proved it can unwrap them with the same passphrase —
     * returns the identical message to confirm. Pass {@code null} for an
     * unencrypted connection.
     *
     * <p>The response's Encryption Field carries the agreed key length in
     * 4-byte units (draft-sharabayko-srt.md's Table 2: {@code 2} = 16 bytes,
     * {@code 3} = 24, {@code 4} = 32), which is how a peer learns the size
     * without parsing the key material itself.
     */
    public HandshakeCif buildAcceptResponse(HandshakeCif request, SrtSocketId assignedSocketId,
            int ourReceiveTsbpdDelayMillis, int ourSendTsbpdDelayMillis, KeyMaterialCif keyMaterial) {
        HandshakeExtension requested = request.handshakeExtension();
        int receiveDelay = Math.max(ourReceiveTsbpdDelayMillis, requested.sendTsbpdDelayMillis());
        int sendDelay = Math.max(ourSendTsbpdDelayMillis, requested.receiveTsbpdDelayMillis());

        HandshakeExtensionFlags flags = new HandshakeExtensionFlags(true, true, true, true, true, true, false, false);
        HandshakeExtension responseExtension = new HandshakeExtension(srtVersion, flags, receiveDelay, sendDelay);

        boolean hasStreamId = request.streamId() != null && !request.streamId().isEmpty();
        int extensionField = 1 | (keyMaterial != null ? 2 : 0) | (hasStreamId ? 4 : 0);
        int encryptionField = keyMaterial != null ? keyMaterial.keyLength() / 4 : 0;

        return new HandshakeCif(
                false, 5, encryptionField, extensionField,
                request.initialPacketSequenceNumber(), request.maxTransmissionUnitSize(),
                request.maxFlowWindowSize(), HandshakeType.CONCLUSION.code(),
                assignedSocketId, 0, ownAddress, responseExtension, request.streamId(), keyMaterial);
    }
}
