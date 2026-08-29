package org.brewstream.roast.handshake;

import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.packet.cif.HandshakeCif;
import org.brewstream.roast.packet.cif.HandshakeExtension;
import org.brewstream.roast.packet.cif.HandshakeExtensionFlags;
import org.brewstream.roast.packet.cif.HandshakeType;
import org.brewstream.roast.packet.cif.RejectionReason;
import org.brewstream.roast.util.CircularNumber;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

class ListenerHandshakeTest {

    private static final String SENDER_ADDRESS = "203.0.113.5:4000";
    private static final int SRT_VERSION = 0x010402;

    private static CircularNumber seq(long value) {
        return CircularNumber.of(value, SrtPacket.MAX_SEQUENCE_NUMBER);
    }

    private static InetAddress ownAddress() throws UnknownHostException {
        return InetAddress.getByName("10.0.0.1");
    }

    private static InetAddress callerAddress() throws UnknownHostException {
        return InetAddress.getByName("203.0.113.5");
    }

    private static SynCookie deterministicCookie() {
        return new SynCookie(
                "dl2INvNSQTZ5zQu9MxNmGyAVmNkB33io", "nwj2qrsh3xyC8OmCp1gObD0iOtQNQsLi", "10.0.0.1:9710", () -> 0L);
    }

    private static ListenerHandshake listener() {
        return new ListenerHandshake(deterministicCookie(), ownAddressUnchecked(), SRT_VERSION);
    }

    private static InetAddress ownAddressUnchecked() {
        try {
            return ownAddress();
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }

    private static HandshakeExtension validExtension() {
        return new HandshakeExtension(
                SRT_VERSION, new HandshakeExtensionFlags(true, true, true, true, true, true, false, false), 120, 120);
    }

    private static HandshakeCif validConclusionRequest() throws UnknownHostException {
        int cookie = deterministicCookie().get(SENDER_ADDRESS);
        return new HandshakeCif(
                true, 5, 0, 1, seq(42), 1500, 8192, HandshakeType.CONCLUSION.code(),
                SrtSocketId.of(0xABCDEF), cookie, callerAddress(), validExtension(), "live/test");
    }

    @Test
    void onInductionEchoesRequestAndAttachesFreshCookie() throws UnknownHostException {
        HandshakeCif request = new HandshakeCif(
                true, 5, 0, 0, seq(7), 1500, 8192, HandshakeType.INDUCTION.code(),
                SrtSocketId.of(0x112233), 0, callerAddress(), null, null);

        HandshakeCif response = listener().onInduction(request, SENDER_ADDRESS);

        assertThat(response.version()).isEqualTo(5);
        assertThat(response.encryptionField()).isZero();
        assertThat(response.extensionField()).isEqualTo(0x4A17);
        assertThat(response.handshakeType()).isEqualTo(HandshakeType.INDUCTION);
        assertThat(response.srtSocketId()).isEqualTo(request.srtSocketId());
        assertThat(response.synCookie()).isEqualTo(deterministicCookie().get(SENDER_ADDRESS));
        assertThat(response.peerAddress()).isEqualTo(ownAddress());
        assertThat(response.initialPacketSequenceNumber()).isEqualTo(request.initialPacketSequenceNumber());
        assertThat(response.maxTransmissionUnitSize()).isEqualTo(request.maxTransmissionUnitSize());
        assertThat(response.maxFlowWindowSize()).isEqualTo(request.maxFlowWindowSize());
        assertThat(response.handshakeExtension()).isNull();
        assertThat(response.streamId()).isNull();
    }

    @Test
    void validateConclusionAcceptsAWellFormedRequest() throws UnknownHostException {
        ConclusionOutcome outcome = listener().validateConclusion(validConclusionRequest(), SENDER_ADDRESS);

        assertThat(outcome).isInstanceOf(ConclusionOutcome.Valid.class);
        assertThat(((ConclusionOutcome.Valid) outcome).request()).isEqualTo(validConclusionRequest());
    }

    @Test
    void validateConclusionRejectsAnInvalidCookie() throws UnknownHostException {
        HandshakeCif request = validConclusionRequest();
        HandshakeCif tampered = new HandshakeCif(
                request.isRequest(), request.version(), request.encryptionField(), request.extensionField(),
                request.initialPacketSequenceNumber(), request.maxTransmissionUnitSize(),
                request.maxFlowWindowSize(), request.handshakeTypeCode(), request.srtSocketId(),
                request.synCookie() + 1, request.peerAddress(), request.handshakeExtension(), request.streamId());

        ConclusionOutcome outcome = listener().validateConclusion(tampered, SENDER_ADDRESS);

        assertThat(outcome).isInstanceOf(ConclusionOutcome.Rejected.class);
        HandshakeCif response = ((ConclusionOutcome.Rejected) outcome).response();
        assertThat(response.isRejection()).isTrue();
        assertThat(response.rejectionReason()).isEqualTo(RejectionReason.ROGUE);
    }

    @Test
    void validateConclusionRejectsAnOversizedMtu() throws UnknownHostException {
        HandshakeCif request = withMtu(validConclusionRequest(), 2000);

        ConclusionOutcome outcome = listener().validateConclusion(request, SENDER_ADDRESS);

        assertRejectedWith(outcome, RejectionReason.ROGUE);
    }

    @Test
    void validateConclusionRejectsHandshakeVersionFour() throws UnknownHostException {
        HandshakeCif request = validConclusionRequest();
        HandshakeCif v4 = new HandshakeCif(
                request.isRequest(), 4, request.encryptionField(), request.extensionField(),
                request.initialPacketSequenceNumber(), request.maxTransmissionUnitSize(),
                request.maxFlowWindowSize(), request.handshakeTypeCode(), request.srtSocketId(),
                request.synCookie(), request.peerAddress(), request.handshakeExtension(), request.streamId());

        ConclusionOutcome outcome = listener().validateConclusion(v4, SENDER_ADDRESS);

        assertRejectedWith(outcome, RejectionReason.ROGUE);
    }

    @Test
    void validateConclusionRejectsAMissingHandshakeExtension() throws UnknownHostException {
        HandshakeCif request = validConclusionRequest();
        HandshakeCif withoutExtension = new HandshakeCif(
                request.isRequest(), request.version(), request.encryptionField(), request.extensionField(),
                request.initialPacketSequenceNumber(), request.maxTransmissionUnitSize(),
                request.maxFlowWindowSize(), request.handshakeTypeCode(), request.srtSocketId(),
                request.synCookie(), request.peerAddress(), null, request.streamId());

        ConclusionOutcome outcome = listener().validateConclusion(withoutExtension, SENDER_ADDRESS);

        assertRejectedWith(outcome, RejectionReason.ROGUE);
    }

    @Test
    void validateConclusionRejectsATooOldPeerVersion() throws UnknownHostException {
        HandshakeCif request = withExtension(validConclusionRequest(),
                new HandshakeExtension(SRT_VERSION - 1,
                        new HandshakeExtensionFlags(true, true, true, true, true, true, false, false), 120, 120));

        ConclusionOutcome outcome = listener().validateConclusion(request, SENDER_ADDRESS);

        assertRejectedWith(outcome, RejectionReason.VERSION);
    }

    @Test
    void validateConclusionRejectsAMissingRequiredFlag() throws UnknownHostException {
        HandshakeCif request = withExtension(validConclusionRequest(),
                new HandshakeExtension(SRT_VERSION,
                        new HandshakeExtensionFlags(false, true, true, true, true, true, false, false), 120, 120));

        ConclusionOutcome outcome = listener().validateConclusion(request, SENDER_ADDRESS);

        assertRejectedWith(outcome, RejectionReason.ROGUE);
    }

    @Test
    void validateConclusionRejectsMessageModeStreams() throws UnknownHostException {
        HandshakeCif request = withExtension(validConclusionRequest(),
                new HandshakeExtension(SRT_VERSION,
                        new HandshakeExtensionFlags(true, true, true, true, true, true, true, false), 120, 120));

        ConclusionOutcome outcome = listener().validateConclusion(request, SENDER_ADDRESS);

        assertRejectedWith(outcome, RejectionReason.MESSAGEAPI);
    }

    @Test
    void buildAcceptResponseNegotiatesTheLargerTsbpdDelayEachDirection() throws UnknownHostException {
        HandshakeCif request = withExtension(validConclusionRequest(),
                new HandshakeExtension(SRT_VERSION,
                        new HandshakeExtensionFlags(true, true, true, true, true, true, false, false), 50, 150));
        SrtSocketId assigned = SrtSocketId.of(0x999999);

        HandshakeCif response = listener().buildAcceptResponse(request, assigned, 100, 100);

        assertThat(response.handshakeType()).isEqualTo(HandshakeType.CONCLUSION);
        assertThat(response.srtSocketId()).isEqualTo(assigned);
        assertThat(response.synCookie()).isZero();
        assertThat(response.peerAddress()).isEqualTo(ownAddress());
        assertThat(response.streamId()).isEqualTo("live/test");
        assertThat(response.extensionField()).isEqualTo(5); // HasHS (1) | HasSID (4)

        HandshakeExtension extension = response.handshakeExtension();
        assertThat(extension.srtVersion()).isEqualTo(SRT_VERSION);
        assertThat(extension.receiveTsbpdDelayMillis()).isEqualTo(150); // max(our 100, peer's send 150)
        assertThat(extension.sendTsbpdDelayMillis()).isEqualTo(100); // max(our 100, peer's receive 50)
        assertThat(extension.flags().tsbpdSend()).isTrue();
        assertThat(extension.flags().tsbpdReceive()).isTrue();
        assertThat(extension.flags().crypt()).isTrue();
        assertThat(extension.flags().tooLatePacketDrop()).isTrue();
        assertThat(extension.flags().periodicNak()).isTrue();
        assertThat(extension.flags().retransmissionFlag()).isTrue();
        assertThat(extension.flags().streamMode()).isFalse();
        assertThat(extension.flags().packetFilter()).isFalse();
    }

    @Test
    void buildAcceptResponseOmitsSidExtensionFlagWhenNoStreamId() throws UnknownHostException {
        HandshakeCif request = withStreamId(validConclusionRequest(), null);

        HandshakeCif response = listener().buildAcceptResponse(request, SrtSocketId.of(1), 120, 120);

        assertThat(response.extensionField()).isEqualTo(1); // HasHS only
        assertThat(response.streamId()).isNull();
    }

    @Test
    void buildRejectResponseEchoesRequestFieldsWithReasonSwappedIn() throws UnknownHostException {
        HandshakeCif request = validConclusionRequest();

        HandshakeCif response = listener().buildRejectResponse(request, RejectionReason.PEER);

        assertThat(response.isRejection()).isTrue();
        assertThat(response.rejectionReason()).isEqualTo(RejectionReason.PEER);
        assertThat(response.srtSocketId()).isEqualTo(request.srtSocketId());
        assertThat(response.synCookie()).isEqualTo(request.synCookie());
        assertThat(response.streamId()).isEqualTo(request.streamId());
        assertThat(response.peerAddress()).isEqualTo(ownAddress());
    }

    private static void assertRejectedWith(ConclusionOutcome outcome, RejectionReason reason) {
        assertThat(outcome).isInstanceOf(ConclusionOutcome.Rejected.class);
        HandshakeCif response = ((ConclusionOutcome.Rejected) outcome).response();
        assertThat(response.rejectionReason()).isEqualTo(reason);
    }

    private static HandshakeCif withMtu(HandshakeCif request, int mtu) {
        return new HandshakeCif(
                request.isRequest(), request.version(), request.encryptionField(), request.extensionField(),
                request.initialPacketSequenceNumber(), mtu, request.maxFlowWindowSize(),
                request.handshakeTypeCode(), request.srtSocketId(), request.synCookie(), request.peerAddress(),
                request.handshakeExtension(), request.streamId());
    }

    private static HandshakeCif withExtension(HandshakeCif request, HandshakeExtension extension) {
        return new HandshakeCif(
                request.isRequest(), request.version(), request.encryptionField(), request.extensionField(),
                request.initialPacketSequenceNumber(), request.maxTransmissionUnitSize(),
                request.maxFlowWindowSize(), request.handshakeTypeCode(), request.srtSocketId(),
                request.synCookie(), request.peerAddress(), extension, request.streamId());
    }

    private static HandshakeCif withStreamId(HandshakeCif request, String streamId) {
        return new HandshakeCif(
                request.isRequest(), request.version(), request.encryptionField(), request.extensionField(),
                request.initialPacketSequenceNumber(), request.maxTransmissionUnitSize(),
                request.maxFlowWindowSize(), request.handshakeTypeCode(), request.srtSocketId(),
                request.synCookie(), request.peerAddress(), request.handshakeExtension(), streamId);
    }
}
