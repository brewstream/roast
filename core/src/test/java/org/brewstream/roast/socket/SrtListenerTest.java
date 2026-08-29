package org.brewstream.roast.socket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.brewstream.roast.packet.ControlPacket;
import org.brewstream.roast.packet.ControlType;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.packet.cif.HandshakeCif;
import org.brewstream.roast.packet.cif.HandshakeExtension;
import org.brewstream.roast.packet.cif.HandshakeExtensionFlags;
import org.brewstream.roast.packet.cif.HandshakeType;
import org.brewstream.roast.packet.cif.RejectionReason;
import org.brewstream.roast.util.CircularNumber;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests: binds a real {@link SrtListener} on loopback and drives it
 * with a plain {@link DatagramSocket} acting as a fake caller, building request
 * bytes through the real codec classes and decoding replies the same way. Unlike
 * the rest of this module's tests, these involve real socket I/O and an
 * asynchronous event-loop thread, so callback-based assertions use a
 * {@link CompletableFuture} rather than a plain synchronous check.
 */
class SrtListenerTest {

    private static final InetAddress LOCALHOST = loopback();
    private static final int TIMEOUT_SECONDS = 5;

    private SrtListener listener;
    private DatagramSocket caller;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (listener != null) {
            listener.close();
        }
        if (caller != null) {
            caller.close();
        }
    }

    @Test
    void inductionGetsAReplyWithAFreshCookieAndSrtMagic() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress(LOCALHOST, 0));
        caller = newCaller();
        SrtSocketId callerSocketId = SrtSocketId.of(0x1000);

        HandshakeCif reply = sendAndReceive(inductionRequest(callerSocketId));

        assertThat(reply.version()).isEqualTo(5);
        assertThat(reply.encryptionField()).isZero();
        assertThat(reply.extensionField()).isEqualTo(0x4A17);
        assertThat(reply.handshakeType()).isEqualTo(HandshakeType.INDUCTION);
        assertThat(reply.srtSocketId()).isEqualTo(callerSocketId);
        assertThat(reply.synCookie()).isNotZero();
    }

    @Test
    void conclusionHappyPathAcceptsAndFiresOnConnection() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress(LOCALHOST, 0));
        CompletableFuture<AcceptedConnection> connected = new CompletableFuture<>();
        CompletableFuture<ConnectionRequest> seenRequest = new CompletableFuture<>();
        listener.setAcceptHandler(request -> {
            seenRequest.complete(request);
            return AcceptDecision.accept();
        });
        listener.onConnection(connected::complete);
        caller = newCaller();
        SrtSocketId callerSocketId = SrtSocketId.of(0x2000);

        HandshakeCif inductionReply = sendAndReceive(inductionRequest(callerSocketId));
        HandshakeCif conclusionReply = sendAndReceive(
                conclusionRequest(callerSocketId, inductionReply.synCookie(), "live/test"));

        assertThat(conclusionReply.handshakeType()).isEqualTo(HandshakeType.CONCLUSION);
        assertThat(conclusionReply.synCookie()).isZero();
        assertThat(conclusionReply.srtSocketId()).isNotEqualTo(callerSocketId);
        assertThat(conclusionReply.srtSocketId().isZero()).isFalse();
        assertThat(conclusionReply.streamId()).isEqualTo("live/test");

        AcceptedConnection connection = connected.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(connection.socketId()).isEqualTo(conclusionReply.srtSocketId());
        assertThat(connection.peerSocketId()).isEqualTo(callerSocketId);
        assertThat(connection.streamId()).isEqualTo("live/test");

        ConnectionRequest request = seenRequest.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(request.streamId()).isEqualTo("live/test");
        assertThat(request.peerSocketId()).isEqualTo(callerSocketId);
    }

    @Test
    void conclusionWithBadCookieIsRejectedProtocolLevel() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress(LOCALHOST, 0));
        CompletableFuture<AcceptedConnection> connected = new CompletableFuture<>();
        listener.onConnection(connected::complete);
        caller = newCaller();
        SrtSocketId callerSocketId = SrtSocketId.of(0x3000);

        HandshakeCif inductionReply = sendAndReceive(inductionRequest(callerSocketId));
        HandshakeCif conclusionReply = sendAndReceive(
                conclusionRequest(callerSocketId, inductionReply.synCookie() + 1, "live/test"));

        assertThat(conclusionReply.isRejection()).isTrue();
        assertThat(conclusionReply.rejectionReason()).isEqualTo(RejectionReason.ROGUE);
        assertThat(connected).isNotDone();
    }

    @Test
    void conclusionWithEmptyStreamIdIsRejectedByDefaultAcceptHandler() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress(LOCALHOST, 0));
        // Default accept handler rejects everything with PEER until setAcceptHandler is called.
        caller = newCaller();
        SrtSocketId callerSocketId = SrtSocketId.of(0x4000);

        HandshakeCif inductionReply = sendAndReceive(inductionRequest(callerSocketId));
        HandshakeCif conclusionReply = sendAndReceive(
                conclusionRequest(callerSocketId, inductionReply.synCookie(), null));

        assertThat(conclusionReply.isRejection()).isTrue();
        assertThat(conclusionReply.rejectionReason()).isEqualTo(RejectionReason.PEER);
    }

    @Test
    void duplicateConclusionInvokesAcceptHandlerOnlyOnce() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress(LOCALHOST, 0));
        AtomicInteger acceptCalls = new AtomicInteger();
        listener.setAcceptHandler(request -> {
            acceptCalls.incrementAndGet();
            return AcceptDecision.accept();
        });
        caller = newCaller();
        SrtSocketId callerSocketId = SrtSocketId.of(0x5000);

        HandshakeCif inductionReply = sendAndReceive(inductionRequest(callerSocketId));
        byte[] conclusion = encode(conclusionRequest(callerSocketId, inductionReply.synCookie(), "live/test"));

        HandshakeCif firstReply = sendAndReceive(conclusion);
        HandshakeCif secondReply = sendAndReceive(conclusion);

        assertThat(acceptCalls.get()).isEqualTo(1);
        assertThat(firstReply.srtSocketId()).isEqualTo(secondReply.srtSocketId());
    }

    private HandshakeCif sendAndReceive(HandshakeCif request) throws IOException {
        return sendAndReceive(encode(request));
    }

    private HandshakeCif sendAndReceive(byte[] requestBytes) throws IOException {
        DatagramPacket outgoing = new DatagramPacket(
                requestBytes, requestBytes.length, listener.localAddress());
        caller.send(outgoing);

        byte[] buffer = new byte[2048];
        DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
        caller.receive(incoming);

        ByteBuf buf = Unpooled.wrappedBuffer(incoming.getData(), 0, incoming.getLength());
        SrtPacket packet = SrtPacket.decode(buf);
        assertThat(packet).isInstanceOf(ControlPacket.class);
        ControlPacket control = (ControlPacket) packet;
        HandshakeCif cif = HandshakeCif.decode(control.body(), false);
        control.body().release();
        assertThat(cif).isNotNull();
        return cif;
    }

    private static byte[] encode(HandshakeCif cif) {
        ByteBuf cifBuf = Unpooled.buffer();
        cif.encodeTo(cifBuf);
        ControlPacket packet = new ControlPacket(ControlType.HANDSHAKE, 0, 0, SrtSocketId.ZERO, cifBuf);
        ByteBuf out = Unpooled.buffer();
        packet.encodeTo(out);
        byte[] bytes = new byte[out.readableBytes()];
        out.readBytes(bytes);
        out.release();
        return bytes;
    }

    private static HandshakeCif inductionRequest(SrtSocketId callerSocketId) {
        return new HandshakeCif(
                true, 5, 0, 0, seq(1), 1500, 8192, HandshakeType.INDUCTION.code(),
                callerSocketId, 0, LOCALHOST, null, null);
    }

    private static HandshakeCif conclusionRequest(SrtSocketId callerSocketId, int synCookie, String streamId) {
        HandshakeExtension extension = new HandshakeExtension(
                0x010401, new HandshakeExtensionFlags(true, true, true, true, true, true, false, false), 120, 120);
        return new HandshakeCif(
                true, 5, 0, 5, seq(1), 1500, 8192, HandshakeType.CONCLUSION.code(),
                callerSocketId, synCookie, LOCALHOST, extension, streamId);
    }

    private static CircularNumber seq(long value) {
        return CircularNumber.of(value, SrtPacket.MAX_SEQUENCE_NUMBER);
    }

    private static DatagramSocket newCaller() throws IOException {
        DatagramSocket socket = new DatagramSocket(new InetSocketAddress(LOCALHOST, 0));
        socket.setSoTimeout(TIMEOUT_SECONDS * 1000);
        return socket;
    }

    private static InetAddress loopback() {
        try {
            return InetAddress.getByName("127.0.0.1");
        } catch (java.net.UnknownHostException e) {
            throw new AssertionError(e);
        }
    }
}
