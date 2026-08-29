package org.brewstream.roast.socket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.brewstream.roast.packet.ControlPacket;
import org.brewstream.roast.packet.ControlType;
import org.brewstream.roast.packet.DataPacket;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.packet.cif.HandshakeCif;
import org.brewstream.roast.packet.cif.HandshakeExtension;
import org.brewstream.roast.packet.cif.HandshakeExtensionFlags;
import org.brewstream.roast.packet.cif.HandshakeType;
import org.brewstream.roast.packet.cif.LossListCodec;
import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.util.CircularNumber;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link SrtConnection}: completes a real handshake (same
 * loopback fake-caller pattern as {@link SrtListenerTest}), then sends DATA
 * packets and observes the connection's hooks and outbound NAK/ACK traffic.
 * Delivery is gated by the negotiated 120ms TSBPD latency, so most assertions
 * here wait on the order of hundreds of milliseconds, not the fast synchronous
 * checks used elsewhere in this module.
 */
class SrtConnectionTest {

    private static final InetAddress LOCALHOST = loopback();
    private static final int TIMEOUT_SECONDS = 5;
    private static final SrtSocketId CALLER_SOCKET_ID = SrtSocketId.of(0x9000);

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
    void inOrderDataPacketIsDeliveredViaOnData() throws Exception {
        SrtConnection connection = connectAndAccept();
        CompletableFuture<String> delivered = new CompletableFuture<>();
        connection.onData(payload -> {
            delivered.complete(payload.toString(StandardCharsets.US_ASCII));
            payload.release();
        });

        sendData(connection.metadata().socketId(), 1, 0, "hello");

        assertThat(delivered.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo("hello");
    }

    @Test
    void gapTriggersImmediateNakAndOnLoss() throws Exception {
        SrtConnection connection = connectAndAccept();
        CompletableFuture<LossRange> lost = new CompletableFuture<>();
        connection.onLoss(lost::complete);

        sendData(connection.metadata().socketId(), 1, 0, "a");
        sendData(connection.metadata().socketId(), 3, 1000, "c"); // skips seq 2

        assertThat(lost.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo(new LossRange(seq(2), seq(2)));
        assertThat(receiveNak()).containsExactly(new LossRange(seq(2), seq(2)));
    }

    @Test
    void recoveringAGapDeliversEverythingInOrder() throws Exception {
        SrtConnection connection = connectAndAccept();
        List<String> delivered = new ArrayList<>();
        CompletableFuture<Void> gotAllThree = new CompletableFuture<>();
        connection.onData(payload -> {
            synchronized (delivered) {
                delivered.add(payload.toString(StandardCharsets.US_ASCII));
                if (delivered.size() == 3) {
                    gotAllThree.complete(null);
                }
            }
            payload.release();
        });

        sendData(connection.metadata().socketId(), 1, 0, "1");
        sendData(connection.metadata().socketId(), 3, 1000, "3");
        sendData(connection.metadata().socketId(), 2, 500, "2");

        gotAllThree.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        synchronized (delivered) {
            assertThat(delivered).containsExactly("1", "2", "3");
        }
    }

    @Test
    void tlpktdropAbandonsAStaleGapAndDeliversPastIt() throws Exception {
        SrtConnection connection = connectAndAccept();
        CompletableFuture<LossRange> abandoned = new CompletableFuture<>();
        List<String> delivered = new ArrayList<>();
        CompletableFuture<Void> gotBoth = new CompletableFuture<>();
        connection.onTlpktDrop(abandoned::complete);
        connection.onData(payload -> {
            synchronized (delivered) {
                delivered.add(payload.toString(StandardCharsets.US_ASCII));
                if (delivered.size() == 2) {
                    gotBoth.complete(null);
                }
            }
            payload.release();
        });

        sendData(connection.metadata().socketId(), 1, 0, "1"); // establishes the receive buffer's time base
        sendData(connection.metadata().socketId(), 3, 1000, "3"); // skips seq 2 - opens the gap

        // Negotiated latency is 120ms; seq 2 is never sent, so once that deadline passes
        // TLPKTDROP must abandon it rather than block delivery of seq 1 and 3 forever.
        assertThat(abandoned.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo(new LossRange(seq(2), seq(2)));

        gotBoth.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        synchronized (delivered) {
            assertThat(delivered).containsExactly("1", "3");
        }
    }

    @Test
    void closeDoesNotThrowWithUndeliveredBufferedPackets() throws Exception {
        SrtConnection connection = connectAndAccept();
        sendData(connection.metadata().socketId(), 1, 0, "buffered-but-not-yet-due");

        connection.close();
    }

    private SrtConnection connectAndAccept() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress(LOCALHOST, 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());
        CompletableFuture<SrtConnection> connected = new CompletableFuture<>();
        listener.onConnection(connected::complete);
        caller = newCaller();

        HandshakeCif inductionReply = sendAndReceiveHandshake(inductionRequest());
        sendAndReceiveHandshake(conclusionRequest(inductionReply.synCookie()));

        return connected.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private void sendData(SrtSocketId destination, int seq, int timestamp, String payload) throws IOException {
        DataPacket packet = new DataPacket(seq, 0b11, true, 0, false, 1, timestamp, destination,
                Unpooled.wrappedBuffer(payload.getBytes(StandardCharsets.US_ASCII)));
        var out = Unpooled.buffer();
        packet.encodeTo(out);
        byte[] bytes = new byte[out.readableBytes()];
        out.readBytes(bytes);
        out.release();

        caller.send(new DatagramPacket(bytes, bytes.length, listener.localAddress()));
    }

    /** Reads incoming packets until a NAK is found (skipping periodic ACKs), decoded into loss ranges. */
    private List<LossRange> receiveNak() throws IOException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline) {
            byte[] buffer = new byte[2048];
            DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
            caller.receive(incoming);

            ByteBuf buf = Unpooled.wrappedBuffer(incoming.getData(), 0, incoming.getLength());
            SrtPacket packet = SrtPacket.decode(buf);
            if (packet instanceof ControlPacket control && control.type() == ControlType.NAK) {
                List<LossRange> ranges = LossListCodec.decode(control.body());
                control.body().release();
                return ranges;
            }
            if (packet != null) {
                packet.body().release();
            }
        }
        throw new AssertionError("No NAK received within " + TIMEOUT_SECONDS + "s");
    }

    private HandshakeCif sendAndReceiveHandshake(HandshakeCif request) throws IOException {
        byte[] requestBytes = encodeHandshake(request);
        caller.send(new DatagramPacket(requestBytes, requestBytes.length, listener.localAddress()));

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

    private static byte[] encodeHandshake(HandshakeCif cif) {
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

    private static HandshakeCif inductionRequest() {
        return new HandshakeCif(
                true, 5, 0, 0, seq(1), 1500, 8192, HandshakeType.INDUCTION.code(),
                CALLER_SOCKET_ID, 0, LOCALHOST, null, null);
    }

    private static HandshakeCif conclusionRequest(int synCookie) {
        HandshakeExtension extension = new HandshakeExtension(
                0x010401, new HandshakeExtensionFlags(true, true, true, true, true, true, false, false), 120, 120);
        return new HandshakeCif(
                true, 5, 0, 5, seq(1), 1500, 8192, HandshakeType.CONCLUSION.code(),
                CALLER_SOCKET_ID, synCookie, LOCALHOST, extension, "live/test");
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
