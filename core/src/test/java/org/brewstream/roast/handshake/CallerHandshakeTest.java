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

/**
 * No gosrt test file maps to this split the way send_test.go did for
 * SendBuffer - dial.go's own tests (dial_test.go) are integration-style
 * against a hand-rolled fake listener, not narrow unit tests of an isolated
 * piece. Self-designed against dial.go's source, same rigor tier as
 * ListenerHandshake's own tests - see STATUS.md's testing methodology.
 */
class CallerHandshakeTest {

    private static final InetAddress LOCALHOST = loopback();
    private static final SrtSocketId OWN_SOCKET_ID = SrtSocketId.of(0x1111);
    private static final int SRT_VERSION = 0x010401;
    private static final int OWN_LATENCY_MILLIS = 120;

    private final CallerHandshake handshake = new CallerHandshake();

    @Test
    void inductionRequestHasNoIsnOrCookieYet() {
        HandshakeCif request = handshake.buildInductionRequest(OWN_SOCKET_ID, LOCALHOST);

        assertThat(request.isRequest()).isTrue();
        assertThat(request.version()).isEqualTo(4);
        assertThat(request.handshakeType()).isEqualTo(HandshakeType.INDUCTION);
        assertThat(request.initialPacketSequenceNumber().value()).isZero();
        assertThat(request.synCookie()).isZero();
        assertThat(request.srtSocketId()).isEqualTo(OWN_SOCKET_ID);
        assertThat(request.peerAddress()).isEqualTo(LOCALHOST);
        assertThat(request.handshakeExtension()).isNull();
        assertThat(request.streamId()).isNull();
    }

    @Test
    void inductionReplyIsSupportedOnlyForV5WithMagic() {
        HandshakeCif v5WithMagic = inductionReply(5, 0x4A17);
        HandshakeCif v5WrongMagic = inductionReply(5, 0);
        HandshakeCif v4 = inductionReply(4, 2);

        assertThat(handshake.isSupportedInductionReply(v5WithMagic)).isTrue();
        assertThat(handshake.isSupportedInductionReply(v5WrongMagic)).isFalse();
        assertThat(handshake.isSupportedInductionReply(v4)).isFalse();
    }

    @Test
    void conclusionRequestCarriesOwnIsnAndEchoesInductionReplyFields() {
        HandshakeCif inductionReply = inductionReply(5, 0x4A17, seq(1), 1400, 4096, 0xABCD);

        HandshakeCif request = handshake.buildConclusionRequest(
                inductionReply, OWN_SOCKET_ID, LOCALHOST, seq(777), SRT_VERSION,
                OWN_LATENCY_MILLIS, OWN_LATENCY_MILLIS, "live/test");

        assertThat(request.version()).isEqualTo(5);
        assertThat(request.handshakeType()).isEqualTo(HandshakeType.CONCLUSION);
        assertThat(request.initialPacketSequenceNumber()).isEqualTo(seq(777)); // ours, not the reply's
        assertThat(request.maxTransmissionUnitSize()).isEqualTo(1400); // echoed from the induction reply
        assertThat(request.maxFlowWindowSize()).isEqualTo(4096); // echoed from the induction reply
        assertThat(request.synCookie()).isEqualTo(0xABCD); // echoed from the induction reply
        assertThat(request.streamId()).isEqualTo("live/test");
        assertThat(request.handshakeExtension().srtVersion()).isEqualTo(SRT_VERSION);
        assertThat(request.handshakeExtension().receiveTsbpdDelayMillis()).isEqualTo(OWN_LATENCY_MILLIS);
        assertThat(request.extensionField()).isEqualTo(1 | 4); // HSREQ + SID
    }

    @Test
    void conclusionRequestExtensionFieldOmitsSidBitWithoutAStreamId() {
        HandshakeCif inductionReply = inductionReply(5, 0x4A17);

        HandshakeCif request = handshake.buildConclusionRequest(
                inductionReply, OWN_SOCKET_ID, LOCALHOST, seq(1), SRT_VERSION,
                OWN_LATENCY_MILLIS, OWN_LATENCY_MILLIS, "");

        assertThat(request.extensionField()).isEqualTo(1); // HSREQ only
        assertThat(request.streamId()).isEmpty();
    }

    @Test
    void connectedOutcomeNegotiatesTheLargerLatencyInEachDirection() {
        HandshakeCif reply = conclusionReply(5, requiredFlags(), SRT_VERSION, 200, 80);

        ConclusionReplyOutcome outcome = handshake.validateConclusionReply(reply, SRT_VERSION, 120, 120);

        assertThat(outcome).isInstanceOf(ConclusionReplyOutcome.Connected.class);
        ConclusionReplyOutcome.Connected connected = (ConclusionReplyOutcome.Connected) outcome;
        assertThat(connected.srtVersion()).isEqualTo(SRT_VERSION);
        // Our receive delay vs. the peer's send delay (200) - the peer's is larger.
        assertThat(connected.receiveLatencyMillis()).isEqualTo(200);
        // Our send delay (120) vs. the peer's receive delay (80) - ours is larger.
        assertThat(connected.sendLatencyMillis()).isEqualTo(120);
    }

    @Test
    void rejectionCodeIsReportedWithoutFurtherValidation() {
        HandshakeCif reply = new HandshakeCif(
                false, 5, 0, 0, seq(1), 1500, 8192, RejectionReason.ROGUE.code(),
                SrtSocketId.of(2), 0, LOCALHOST, null, null);

        ConclusionReplyOutcome outcome = handshake.validateConclusionReply(reply, SRT_VERSION, 120, 120);

        assertThat(outcome).isEqualTo(new ConclusionReplyOutcome.Rejected(RejectionReason.ROGUE));
    }

    @Test
    void wrongVersionIsAProtocolViolation() {
        HandshakeCif reply = new HandshakeCif(
                false, 4, 0, 0, seq(1), 1500, 8192, HandshakeType.CONCLUSION.code(),
                SrtSocketId.of(2), 0, LOCALHOST, null, null);

        assertThat(handshake.validateConclusionReply(reply, SRT_VERSION, 120, 120))
                .isInstanceOf(ConclusionReplyOutcome.ProtocolViolation.class);
    }

    @Test
    void missingExtensionIsAProtocolViolation() {
        HandshakeCif reply = new HandshakeCif(
                false, 5, 0, 0, seq(1), 1500, 8192, HandshakeType.CONCLUSION.code(),
                SrtSocketId.of(2), 0, LOCALHOST, null, null);

        assertThat(handshake.validateConclusionReply(reply, SRT_VERSION, 120, 120))
                .isInstanceOf(ConclusionReplyOutcome.ProtocolViolation.class);
    }

    @Test
    void tooOldAPeerVersionIsAProtocolViolation() {
        HandshakeCif reply = conclusionReply(5, requiredFlags(), SRT_VERSION - 1, 120, 120);

        assertThat(handshake.validateConclusionReply(reply, SRT_VERSION, 120, 120))
                .isInstanceOf(ConclusionReplyOutcome.ProtocolViolation.class);
    }

    @Test
    void missingARequiredFlagIsAProtocolViolation() {
        HandshakeExtensionFlags missingTlpktdrop =
                new HandshakeExtensionFlags(true, true, true, false, true, true, false, false);
        HandshakeCif reply = conclusionReply(5, missingTlpktdrop, SRT_VERSION, 120, 120);

        assertThat(handshake.validateConclusionReply(reply, SRT_VERSION, 120, 120))
                .isInstanceOf(ConclusionReplyOutcome.ProtocolViolation.class);
    }

    @Test
    void streamModeIsAProtocolViolation() {
        HandshakeExtensionFlags streamMode =
                new HandshakeExtensionFlags(true, true, true, true, true, true, true, false);
        HandshakeCif reply = conclusionReply(5, streamMode, SRT_VERSION, 120, 120);

        assertThat(handshake.validateConclusionReply(reply, SRT_VERSION, 120, 120))
                .isInstanceOf(ConclusionReplyOutcome.ProtocolViolation.class);
    }

    private static HandshakeExtensionFlags requiredFlags() {
        return new HandshakeExtensionFlags(true, true, true, true, true, true, false, false);
    }

    private static HandshakeCif inductionReply(int version, int extensionField) {
        return inductionReply(version, extensionField, seq(0), 1500, 8192, 0);
    }

    private static HandshakeCif inductionReply(int version, int extensionField, CircularNumber isn,
            int mtu, int flowWindow, int synCookie) {
        return new HandshakeCif(
                false, version, 0, extensionField, isn, mtu, flowWindow, HandshakeType.INDUCTION.code(),
                SrtSocketId.of(0x1111), synCookie, LOCALHOST, null, null);
    }

    private static HandshakeCif conclusionReply(int version, HandshakeExtensionFlags flags, int srtVersion,
            int sendDelay, int receiveDelay) {
        HandshakeExtension extension = new HandshakeExtension(srtVersion, flags, receiveDelay, sendDelay);
        return new HandshakeCif(
                false, version, 0, 1, seq(1), 1500, 8192, HandshakeType.CONCLUSION.code(),
                SrtSocketId.of(2), 0, LOCALHOST, extension, null);
    }

    private static CircularNumber seq(long value) {
        return CircularNumber.of(value, SrtPacket.MAX_SEQUENCE_NUMBER);
    }

    private static InetAddress loopback() {
        try {
            return InetAddress.getByName("127.0.0.1");
        } catch (UnknownHostException e) {
            throw new AssertionError(e);
        }
    }
}
