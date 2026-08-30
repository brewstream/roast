package org.brewstream.roast.socket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.brewstream.roast.packet.ControlPacket;
import org.brewstream.roast.packet.ControlType;
import org.brewstream.roast.packet.DataPacket;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.packet.cif.AckCif;
import org.brewstream.roast.packet.cif.AckVariant;
import org.brewstream.roast.packet.cif.HandshakeCif;
import org.brewstream.roast.packet.cif.HandshakeExtension;
import org.brewstream.roast.packet.cif.HandshakeExtensionFlags;
import org.brewstream.roast.packet.cif.HandshakeType;
import org.brewstream.roast.packet.cif.ExtensionType;
import org.brewstream.roast.packet.cif.KeyEncryption;
import org.brewstream.roast.packet.cif.KeyMaterialCif;
import org.brewstream.roast.packet.cif.RejectionReason;
import org.brewstream.roast.packet.cif.LossListCodec;
import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.crypto.EncryptionContext;
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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
    /** What a caller advertises unless a test needs a distinguishable value. */
    private static final int DEFAULT_FLOW_WINDOW = 8192;
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

    @Test
    void keepAliveIsEchoedBack() throws Exception {
        SrtConnection connection = connectAndAccept();

        sendControl(connection.metadata().socketId(), ControlType.KEEPALIVE);

        receiveControl(ControlType.KEEPALIVE).body().release();
    }

    @Test
    void shutdownClosesTheConnectionAndRepliesWithOurOwnShutdown() throws Exception {
        SrtConnection connection = connectAndAccept();
        CompletableFuture<Void> closed = new CompletableFuture<>();
        connection.onClose(() -> closed.complete(null));

        sendControl(connection.metadata().socketId(), ControlType.SHUTDOWN);

        receiveControl(ControlType.SHUTDOWN).body().release();
        closed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    void ackAckUpdatesRttReportedInSubsequentFullAcks() throws Exception {
        SrtConnection connection = connectAndAccept();

        FullAck first = receiveFullAck();
        assertThat(first.cif().rtt()).isEqualTo(100_000); // unconfirmed yet - gosrt's own initial default

        sendAckAck(connection.metadata().socketId(), first.ackNumber());

        // Keep reading Full ACKs until the RTT actually changes - one or two more
        // might already have been in flight before our ACKACK was processed.
        FullAck updated = receiveFullAckWhereRttDiffersFrom(first.cif().rtt());
        assertThat(updated.cif().rtt()).isLessThan(first.cif().rtt());
    }

    /**
     * Regression test for a real, expensive bug: this figure used to be
     * hardcoded to 0. A peer's sender reads it as our flow-control window, so
     * advertising 0 tells it we can't accept anything - real libsrt responded
     * by collapsing its window to 0 and dropping ~42% of a published live
     * stream before it ever hit the wire (confirmed via its own pktSndDrop/
     * pktFlowWindow counters). Every receive-side loss counter stayed clean
     * throughout, because the packets were never sent at all. See STATUS.md.
     */
    @Test
    void fullAckAdvertisesRealReceiveWindowNotZero() throws Exception {
        connectAndAccept();

        AckCif cif = receiveFullAck().cif();

        // An empty receive buffer means the whole window is available.
        assertThat(cif.availableBufferSize()).isEqualTo(DEFAULT_FLOW_WINDOW);
    }

    /**
     * The window reported is the one actually agreed during the handshake, not a
     * fixed constant - negotiating something other than the built-in fallback is
     * the only way to tell those two apart.
     */
    @Test
    void fullAckAdvertisesTheNegotiatedFlowWindowNotTheFallback() throws Exception {
        int negotiated = 4096; // deliberately != the 8192 fallback

        connectAndAccept(negotiated);

        assertThat(receiveFullAck().cif().availableBufferSize()).isEqualTo(negotiated);
    }

    @Test
    void ackAckWithUnknownAckNumberIsIgnoredWithoutCrashing() throws Exception {
        SrtConnection connection = connectAndAccept();
        receiveFullAck(); // let one go by, ignored

        sendAckAck(connection.metadata().socketId(), 999_999); // matches no pending ack

        FullAck next = receiveFullAck();
        assertThat(next.cif().rtt()).isEqualTo(100_000); // untouched - the bogus ACKACK matched nothing
    }

    @Test
    void writeSendsDataPacketToPeer() throws Exception {
        SrtConnection connection = connectAndAccept();

        connection.write(Unpooled.wrappedBuffer("hello".getBytes(StandardCharsets.US_ASCII)));

        DataPacket received = receiveData();
        assertThat(received.sequenceNumber()).isEqualTo(1); // shared initial sequence number from the handshake
        assertThat(received.payload().toString(StandardCharsets.US_ASCII)).isEqualTo("hello");
        received.payload().release();
    }

    @Test
    void ackedPacketIsNotRetransmittedOnSubsequentNak() throws Exception {
        SrtConnection connection = connectAndAccept();
        CompletableFuture<DataPacket> retransmitted = new CompletableFuture<>();
        connection.onRetransmit(retransmitted::complete);

        connection.write(Unpooled.wrappedBuffer("payload".getBytes(StandardCharsets.US_ASCII)));
        DataPacket original = receiveData();
        int sentSeq = original.sequenceNumber();
        original.payload().release();

        sendFullAck(connection.metadata().socketId(), 1, 50_000, 25_000, seq(sentSeq + 1));
        receiveControl(ControlType.ACKACK).body().release(); // drain our reply before the negative check below

        sendNak(connection.metadata().socketId(), List.of(new LossRange(seq(sentSeq), seq(sentSeq))));

        assertThatThrownBy(() -> retransmitted.get(500, TimeUnit.MILLISECONDS))
                .isInstanceOf(java.util.concurrent.TimeoutException.class);
    }

    @Test
    void nakOnUnacknowledgedPacketTriggersRetransmit() throws Exception {
        SrtConnection connection = connectAndAccept();
        CompletableFuture<DataPacket> retransmitted = new CompletableFuture<>();
        connection.onRetransmit(retransmitted::complete);

        connection.write(Unpooled.wrappedBuffer("payload".getBytes(StandardCharsets.US_ASCII)));
        DataPacket original = receiveData();
        int sentSeq = original.sequenceNumber();
        original.payload().release();

        sendNak(connection.metadata().socketId(), List.of(new LossRange(seq(sentSeq), seq(sentSeq))));

        DataPacket hookPacket = retransmitted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(hookPacket.sequenceNumber()).isEqualTo(sentSeq);
        assertThat(hookPacket.retransmitted()).isTrue();
        // Not released here - onRetransmit doesn't own the payload (unlike onData);
        // it's consumed by the send that immediately follows, see the hook's javadoc.

        DataPacket resent = receiveData();
        assertThat(resent.sequenceNumber()).isEqualTo(sentSeq);
        assertThat(resent.retransmitted()).isTrue();
        resent.payload().release();
    }

    @Test
    void fullAckTriggersAckAckReplyAndUpdatesSharedRtt() throws Exception {
        SrtConnection connection = connectAndAccept();

        sendFullAck(connection.metadata().socketId(), 77, 40_000, 20_000, seq(1));

        ControlPacket ackAck = receiveControl(ControlType.ACKACK);
        assertThat(ackAck.typeSpecificInfo()).isEqualTo(77);
        ackAck.body().release();

        // RTT is one shared estimate (see SrtConnection's "Sending" javadoc) - a
        // peer-reported RTT from an inbound ACK shows up in our own subsequent
        // outgoing Full ACKs about data we've received.
        FullAck updated = receiveFullAckWhereRttDiffersFrom(100_000);
        assertThat(updated.cif().rtt()).isLessThan(100_000);
    }

    @Test
    void closeReleasesUnacknowledgedWrittenPayload() throws Exception {
        SrtConnection connection = connectAndAccept();

        connection.write(Unpooled.wrappedBuffer("not-yet-acked".getBytes(StandardCharsets.US_ASCII)));
        receiveData().payload().release(); // let it actually get sent first

        connection.close();
    }


    // --- encryption (Phase 5): a real handshake with key material, then encrypted data
    // both ways. The peer here is the raw DatagramSocket driving its own
    // EncryptionContext, so both sides of the negotiation are exercised for real.

    private static final char[] TEST_PASSPHRASE = "foobarfoobar".toCharArray();

    private EncryptionContext connectAndAcceptEncrypted(CompletableFuture<SrtConnection> connected,
            char[] listenerPassphrase) throws Exception {
        listener = SrtListener.bind(new InetSocketAddress(LOCALHOST, 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept(listenerPassphrase, 16));
        listener.onConnection(connected::complete);
        caller = newCaller();

        EncryptionContext peer = EncryptionContext.generating(TEST_PASSPHRASE, 16);
        HandshakeCif inductionReply = sendAndReceiveHandshake(inductionRequest());
        HandshakeCif conclusion = new HandshakeCif(
                true, 5, 2, 7, seq(1), 1500, DEFAULT_FLOW_WINDOW, HandshakeType.CONCLUSION.code(),
                CALLER_SOCKET_ID, inductionReply.synCookie(), LOCALHOST,
                new HandshakeExtension(0x010401,
                        new HandshakeExtensionFlags(true, true, true, true, true, true, false, false), 120, 120),
                "live/test", peer.keyMaterial(KeyEncryption.BOTH));
        HandshakeCif reply = sendAndReceiveHandshake(conclusion);

        assertThat(reply.handshakeTypeCode()).isEqualTo(HandshakeType.CONCLUSION.code());
        assertThat(reply.keyMaterial()).isNotNull();
        return peer;
    }

    @Test
    void anEncryptedPeersDataIsDecryptedBeforeReachingOnData() throws Exception {
        CompletableFuture<SrtConnection> connected = new CompletableFuture<>();
        EncryptionContext peer = connectAndAcceptEncrypted(connected, TEST_PASSPHRASE);
        SrtConnection connection = connected.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        CompletableFuture<String> received = new CompletableFuture<>();
        connection.onData(payload -> {
            received.complete(payload.toString(StandardCharsets.US_ASCII));
            payload.release();
        });

        byte[] plaintext = "encrypted-hello".getBytes(StandardCharsets.US_ASCII);
        byte[] ciphertext = plaintext.clone();
        peer.encrypt(ciphertext, 1);
        assertThat(ciphertext).isNotEqualTo(plaintext); // genuinely encrypted on the wire

        sendDataRaw(connection.metadata().socketId(), 1, 1000, ciphertext, peer.activeKey().code());

        assertThat(received.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo("encrypted-hello");
    }

    @Test
    void whatWeSendOnAnEncryptedConnectionIsCiphertextThePeerCanDecrypt() throws Exception {
        CompletableFuture<SrtConnection> connected = new CompletableFuture<>();
        EncryptionContext peer = connectAndAcceptEncrypted(connected, TEST_PASSPHRASE);
        SrtConnection connection = connected.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        connection.write(Unpooled.wrappedBuffer("secret-payload".getBytes(StandardCharsets.US_ASCII)));

        DataPacket sent = receiveData();
        byte[] onWire = new byte[sent.body().readableBytes()];
        sent.body().getBytes(sent.body().readerIndex(), onWire);
        sent.body().release();

        assertThat(new String(onWire, StandardCharsets.US_ASCII)).isNotEqualTo("secret-payload");
        assertThat(sent.kk()).isNotZero(); // the header names the key used

        assertThat(peer.decrypt(onWire, sent.sequenceNumber(), KeyEncryption.fromCode(sent.kk()))).isTrue();
        assertThat(new String(onWire, StandardCharsets.US_ASCII)).isEqualTo("secret-payload");
    }

    @Test
    void aPeerWithTheWrongPassphraseIsRejectedWithBadSecret() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress(LOCALHOST, 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept("the-right-one".toCharArray(), 16));
        caller = newCaller();

        EncryptionContext peer = EncryptionContext.generating("the-wrong-one".toCharArray(), 16);
        HandshakeCif inductionReply = sendAndReceiveHandshake(inductionRequest());
        HandshakeCif conclusion = new HandshakeCif(
                true, 5, 2, 7, seq(1), 1500, DEFAULT_FLOW_WINDOW, HandshakeType.CONCLUSION.code(),
                CALLER_SOCKET_ID, inductionReply.synCookie(), LOCALHOST,
                new HandshakeExtension(0x010401,
                        new HandshakeExtensionFlags(true, true, true, true, true, true, false, false), 120, 120),
                "live/test", peer.keyMaterial(KeyEncryption.BOTH));

        HandshakeCif reply = sendAndReceiveHandshake(conclusion);

        assertThat(reply.isRejection()).isTrue();
        assertThat(reply.rejectionReason()).isEqualTo(RejectionReason.BADSECRET);
    }

    @Test
    void aPeerOfferingNoKeyMaterialIsRejectedWhenAPassphraseIsRequired() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress(LOCALHOST, 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept(TEST_PASSPHRASE, 16));
        caller = newCaller();

        HandshakeCif inductionReply = sendAndReceiveHandshake(inductionRequest());
        HandshakeCif reply = sendAndReceiveHandshake(conclusionRequest(inductionReply.synCookie()));

        assertThat(reply.isRejection()).isTrue();
        assertThat(reply.rejectionReason()).isEqualTo(RejectionReason.UNSECURE);
    }

    /** An unencrypted connection must keep behaving exactly as before. */
    @Test
    void anUnencryptedConnectionStillPassesDataThroughUntouched() throws Exception {
        SrtConnection connection = connectAndAccept();
        CompletableFuture<String> received = new CompletableFuture<>();
        connection.onData(payload -> {
            received.complete(payload.toString(StandardCharsets.US_ASCII));
            payload.release();
        });

        sendData(connection.metadata().socketId(), 1, 1000, "plain");

        assertThat(received.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo("plain");
    }

    private void sendDataRaw(SrtSocketId destination, int seq, int timestamp, byte[] payload, int kk)
            throws IOException {
        DataPacket packet = new DataPacket(seq, 0b11, true, kk, false, 1, timestamp, destination,
                Unpooled.wrappedBuffer(payload));
        var out = Unpooled.buffer();
        packet.encodeTo(out);
        byte[] bytes = new byte[out.readableBytes()];
        out.readBytes(bytes);
        out.release();
        caller.send(new DatagramPacket(bytes, bytes.length, listener.localAddress()));
    }


    /**
     * The retransmission case the encryptIfConfigured javadoc warns about: because
     * SendBuffer retains the plaintext and hands out a fresh duplicate per
     * delivery, a retransmit arrives at the send path as plaintext and must be
     * encrypted again. gosrt skips re-encrypting retransmissions (it encrypts its
     * one retained object in place), and porting that guard here would put
     * retransmitted payloads on the wire in the clear. Same sequence number means
     * the same keystream, so the retransmit is byte-identical to the original.
     */
    @Test
    void aRetransmissionOnAnEncryptedConnectionIsAlsoEncrypted() throws Exception {
        CompletableFuture<SrtConnection> connected = new CompletableFuture<>();
        EncryptionContext peer = connectAndAcceptEncrypted(connected, TEST_PASSPHRASE);
        SrtConnection connection = connected.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        connection.write(Unpooled.wrappedBuffer("resend-me".getBytes(StandardCharsets.US_ASCII)));
        DataPacket first = receiveData();
        byte[] firstBytes = new byte[first.body().readableBytes()];
        first.body().getBytes(first.body().readerIndex(), firstBytes);
        first.body().release();

        sendNak(connection.metadata().socketId(),
                List.of(new LossRange(seq(first.sequenceNumber()), seq(first.sequenceNumber()))));

        DataPacket resent = receiveData();
        byte[] resentBytes = new byte[resent.body().readableBytes()];
        resent.body().getBytes(resent.body().readerIndex(), resentBytes);
        resent.body().release();

        assertThat(resent.retransmitted()).isTrue();
        assertThat(resent.kk()).isNotZero();
        // Not plaintext...
        assertThat(new String(resentBytes, StandardCharsets.US_ASCII)).isNotEqualTo("resend-me");
        // ...identical to the original, since the sequence number drives the keystream...
        assertThat(resentBytes).isEqualTo(firstBytes);
        // ...and it decrypts.
        assertThat(peer.decrypt(resentBytes, resent.sequenceNumber(), KeyEncryption.fromCode(resent.kk()))).isTrue();
        assertThat(new String(resentBytes, StandardCharsets.US_ASCII)).isEqualTo("resend-me");
    }


    /**
     * A peer rotating its key mid-stream: it announces the incoming key in a
     * USER_DEFINED control packet (subtype KMREQ), we adopt it and acknowledge
     * with KMRSP, and data encrypted with the new key then decrypts. This is the
     * inbound half of rotation; the outbound schedule that decides *when* to
     * announce is unit-tested in EncryptionContextTest, and shares the same
     * sendKeyMaterial path exercised here by the KMRSP reply.
     */
    @Test
    void aPeersMidStreamKeyRotationIsAdoptedAndAcknowledged() throws Exception {
        CompletableFuture<SrtConnection> connected = new CompletableFuture<>();
        EncryptionContext peer = connectAndAcceptEncrypted(connected, TEST_PASSPHRASE);
        SrtConnection connection = connected.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        CompletableFuture<String> received = new CompletableFuture<>();
        connection.onData(payload -> {
            received.complete(payload.toString(StandardCharsets.US_ASCII));
            payload.release();
        });

        // The peer rolls to its odd key and announces it.
        peer.generateSek(KeyEncryption.ODD);
        peer.switchActiveKey();
        assertThat(peer.activeKey()).isEqualTo(KeyEncryption.ODD);
        sendKeyMaterial(connection.metadata().socketId(), peer.keyMaterial(KeyEncryption.ODD),
                ExtensionType.KMREQ.code());

        // We must acknowledge it.
        ControlPacket response = receiveControl(ControlType.USER_DEFINED);
        assertThat(response.subtype()).isEqualTo(ExtensionType.KMRSP.code());
        KeyMaterialCif echoed = KeyMaterialCif.decode(response.body());
        response.body().release();
        assertThat(echoed).isNotNull();
        assertThat(echoed.isError()).isFalse();
        assertThat(echoed.keyEncryption()).isEqualTo(KeyEncryption.ODD);

        // ...and data under the new key must now decrypt.
        byte[] ciphertext = "rotated-payload".getBytes(StandardCharsets.US_ASCII);
        peer.encrypt(ciphertext, 1);
        sendDataRaw(connection.metadata().socketId(), 1, 1000, ciphertext, peer.activeKey().code());

        assertThat(received.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo("rotated-payload");
    }

    /** A mid-stream rotation we can't unwrap is answered with an error, not silently ignored. */
    @Test
    void aMidStreamRotationWithTheWrongPassphraseIsRefused() throws Exception {
        CompletableFuture<SrtConnection> connected = new CompletableFuture<>();
        connectAndAcceptEncrypted(connected, TEST_PASSPHRASE);
        SrtConnection connection = connected.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        EncryptionContext impostor = EncryptionContext.generating("a-totally-different-one".toCharArray(), 16);
        sendKeyMaterial(connection.metadata().socketId(), impostor.keyMaterial(KeyEncryption.ODD),
                ExtensionType.KMREQ.code());

        ControlPacket response = receiveControl(ControlType.USER_DEFINED);
        assertThat(response.subtype()).isEqualTo(ExtensionType.KMRSP.code());
        KeyMaterialCif reply = KeyMaterialCif.decode(response.body());
        response.body().release();
        assertThat(reply).isNotNull();
        assertThat(reply.isError()).isTrue();
        assertThat(reply.errorCode()).isEqualTo(KeyMaterialCif.ERROR_BAD_SECRET);
    }

    private void sendKeyMaterial(SrtSocketId destination, KeyMaterialCif keyMaterial, int subtype)
            throws IOException {
        ByteBuf cif = Unpooled.buffer();
        keyMaterial.encodeTo(cif);
        ControlPacket packet = new ControlPacket(
                ControlType.USER_DEFINED, 0, 0, destination, cif, subtype);
        var out = Unpooled.buffer();
        packet.encodeTo(out);
        byte[] bytes = new byte[out.readableBytes()];
        out.readBytes(bytes);
        out.release();
        caller.send(new DatagramPacket(bytes, bytes.length, listener.localAddress()));
    }


    /**
     * SRT live mode sends exactly one packet per write and does not split
     * messages, so an oversized payload has nowhere to go. It used to be handed
     * straight to the encoder, producing a datagram larger than the MTU with no
     * error at all — the caller had no way to learn its data went nowhere.
     * libsrt rejects the same case rather than fragmenting.
     */
    @Test
    void writingMoreThanOnePacketWorthIsRejectedRatherThanSilentlyOversized() throws Exception {
        SrtConnection connection = connectAndAccept();
        int limit = connection.metadata().maxPayloadSize();
        assertThat(limit).isEqualTo(1456); // 1500 MTU - 28 (IP+UDP) - 16 (SRT)

        ByteBuf tooBig = Unpooled.wrappedBuffer(new byte[limit + 1]);

        assertThatThrownBy(() -> connection.write(tooBig))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exceeds")
                .hasMessageContaining("chunk");
        assertThat(tooBig.refCnt()).as("the rejected payload must not leak").isZero();
    }

    @Test
    void aPayloadExactlyAtTheLimitIsAccepted() throws Exception {
        SrtConnection connection = connectAndAccept();
        int limit = connection.metadata().maxPayloadSize();

        connection.write(Unpooled.wrappedBuffer(new byte[limit]));

        DataPacket sent = receiveData();
        assertThat(sent.body().readableBytes()).isEqualTo(limit);
        sent.body().release();
    }

    private SrtConnection connectAndAccept() throws Exception {
        return connectAndAccept(DEFAULT_FLOW_WINDOW);
    }

    private SrtConnection connectAndAccept(int flowWindowSize) throws Exception {
        listener = SrtListener.bind(new InetSocketAddress(LOCALHOST, 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());
        CompletableFuture<SrtConnection> connected = new CompletableFuture<>();
        listener.onConnection(connected::complete);
        caller = newCaller();

        HandshakeCif inductionReply = sendAndReceiveHandshake(inductionRequest(flowWindowSize));
        sendAndReceiveHandshake(conclusionRequest(inductionReply.synCookie(), flowWindowSize));

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

    private void sendControl(SrtSocketId destination, ControlType type) throws IOException {
        sendControl(destination, type, 0);
    }

    private void sendAckAck(SrtSocketId destination, int ackNumber) throws IOException {
        sendControl(destination, ControlType.ACKACK, ackNumber);
    }

    private void sendControl(SrtSocketId destination, ControlType type, int typeSpecificInfo) throws IOException {
        ControlPacket packet = new ControlPacket(type, typeSpecificInfo, 0, destination, Unpooled.buffer(0));
        var out = Unpooled.buffer();
        packet.encodeTo(out);
        byte[] bytes = new byte[out.readableBytes()];
        out.readBytes(bytes);
        out.release();

        caller.send(new DatagramPacket(bytes, bytes.length, listener.localAddress()));
    }

    private List<LossRange> receiveNak() throws IOException {
        ControlPacket control = receiveControl(ControlType.NAK);
        List<LossRange> ranges = LossListCodec.decode(control.body());
        control.body().release();
        return ranges;
    }

    private void sendNak(SrtSocketId destination, List<LossRange> ranges) throws IOException {
        ByteBuf cifBuf = Unpooled.buffer();
        LossListCodec.encode(ranges, cifBuf);
        ControlPacket packet = new ControlPacket(ControlType.NAK, 0, 0, destination, cifBuf);
        var out = Unpooled.buffer();
        packet.encodeTo(out);
        byte[] bytes = new byte[out.readableBytes()];
        out.readBytes(bytes);
        out.release();

        caller.send(new DatagramPacket(bytes, bytes.length, listener.localAddress()));
    }

    private void sendFullAck(SrtSocketId destination, int ackNumber, int rtt, int rttVar,
            CircularNumber lastAckPacketSequenceNumber) throws IOException {
        AckCif cif = new AckCif(AckVariant.FULL, lastAckPacketSequenceNumber, rtt, rttVar, 8192, 1000, 1000, 1000);
        ByteBuf cifBuf = Unpooled.buffer();
        cif.encodeTo(cifBuf);
        ControlPacket packet = new ControlPacket(ControlType.ACK, ackNumber, 0, destination, cifBuf);
        var out = Unpooled.buffer();
        packet.encodeTo(out);
        byte[] bytes = new byte[out.readableBytes()];
        out.readBytes(bytes);
        out.release();

        caller.send(new DatagramPacket(bytes, bytes.length, listener.localAddress()));
    }

    /** Reads incoming packets until a DATA packet is found (skipping control traffic, e.g. periodic ACKs). */
    private DataPacket receiveData() throws IOException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline) {
            byte[] buffer = new byte[2048];
            DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
            caller.receive(incoming);

            ByteBuf buf = Unpooled.wrappedBuffer(incoming.getData(), 0, incoming.getLength());
            SrtPacket packet = SrtPacket.decode(buf);
            if (packet instanceof DataPacket data) {
                return data;
            }
            if (packet != null) {
                packet.body().release();
            }
        }
        throw new AssertionError("No DATA packet received within " + TIMEOUT_SECONDS + "s");
    }

    private record FullAck(int ackNumber, AckCif cif) {
    }

    private FullAck receiveFullAck() throws IOException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline) {
            ControlPacket control = receiveControl(ControlType.ACK);
            AckCif cif = AckCif.decode(control.body());
            int ackNumber = control.typeSpecificInfo();
            control.body().release();
            if (cif != null && cif.variant() == AckVariant.FULL) {
                return new FullAck(ackNumber, cif);
            }
        }
        throw new AssertionError("No Full ACK received within " + TIMEOUT_SECONDS + "s");
    }

    private FullAck receiveFullAckWhereRttDiffersFrom(int rtt) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline) {
            FullAck ack = receiveFullAck();
            if (ack.cif().rtt() != rtt) {
                return ack;
            }
        }
        throw new AssertionError("RTT never changed from " + rtt + " within " + TIMEOUT_SECONDS + "s");
    }

    /** Reads incoming packets until one of the given type is found (skipping others, e.g. periodic ACKs). */
    private ControlPacket receiveControl(ControlType type) throws IOException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline) {
            byte[] buffer = new byte[2048];
            DatagramPacket incoming = new DatagramPacket(buffer, buffer.length);
            caller.receive(incoming);

            ByteBuf buf = Unpooled.wrappedBuffer(incoming.getData(), 0, incoming.getLength());
            SrtPacket packet = SrtPacket.decode(buf);
            if (packet instanceof ControlPacket control && control.type() == type) {
                return control;
            }
            if (packet != null) {
                packet.body().release();
            }
        }
        throw new AssertionError("No " + type + " received within " + TIMEOUT_SECONDS + "s");
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
        return inductionRequest(DEFAULT_FLOW_WINDOW);
    }

    private static HandshakeCif inductionRequest(int flowWindowSize) {
        return new HandshakeCif(
                true, 5, 0, 0, seq(1), 1500, flowWindowSize, HandshakeType.INDUCTION.code(),
                CALLER_SOCKET_ID, 0, LOCALHOST, null, null);
    }

    private static HandshakeCif conclusionRequest(int synCookie) {
        return conclusionRequest(synCookie, DEFAULT_FLOW_WINDOW);
    }

    private static HandshakeCif conclusionRequest(int synCookie, int flowWindowSize) {
        HandshakeExtension extension = new HandshakeExtension(
                0x010401, new HandshakeExtensionFlags(true, true, true, true, true, true, false, false), 120, 120);
        return new HandshakeCif(
                true, 5, 0, 5, seq(1), 1500, flowWindowSize, HandshakeType.CONCLUSION.code(),
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
