package org.brewstream.roast.socket;

import io.netty.buffer.Unpooled;
import org.brewstream.roast.harness.UdpLossProxy;
import org.brewstream.roast.packet.cif.RejectionReason;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Real-socket integration tests for {@link SrtCaller}, against a real {@link
 * SrtListener} in-process rather than a fake — a stronger round trip than
 * gosrt's own dial_test.go, which needs a hand-rolled fake listener since
 * gosrt doesn't have one handy in the same package the way Roast does.
 */
class SrtCallerTest {

    private static final int TIMEOUT_SECONDS = 5;
    private static final String STREAM_ID = "live/test";

    private SrtListener listener;
    private SrtConnection callerSideConnection;
    private SrtConnection listenerSideConnection;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (callerSideConnection != null) {
            callerSideConnection.close();
        }
        if (listenerSideConnection != null) {
            listenerSideConnection.close();
        }
        if (listener != null) {
            listener.close();
        }
    }

    @Test
    void connectSucceedsAndDataFlowsBothWays() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());
        CompletableFuture<SrtConnection> listenerSide = new CompletableFuture<>();
        listener.onConnection(listenerSide::complete);

        callerSideConnection = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), STREAM_ID)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        listenerSideConnection = listenerSide.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(callerSideConnection.metadata().streamId()).isEqualTo(STREAM_ID);
        assertThat(listenerSideConnection.metadata().streamId()).isEqualTo(STREAM_ID);
        assertThat(callerSideConnection.metadata().peerSocketId())
                .isEqualTo(listenerSideConnection.metadata().socketId());
        assertThat(listenerSideConnection.metadata().peerSocketId())
                .isEqualTo(callerSideConnection.metadata().socketId());

        CompletableFuture<String> receivedByListener = new CompletableFuture<>();
        listenerSideConnection.onData(payload -> {
            receivedByListener.complete(payload.toString(StandardCharsets.US_ASCII));
            payload.release();
        });
        CompletableFuture<String> receivedByCaller = new CompletableFuture<>();
        callerSideConnection.onData(payload -> {
            receivedByCaller.complete(payload.toString(StandardCharsets.US_ASCII));
            payload.release();
        });

        callerSideConnection.write(Unpooled.wrappedBuffer("from-caller".getBytes(StandardCharsets.US_ASCII)));
        listenerSideConnection.write(Unpooled.wrappedBuffer("from-listener".getBytes(StandardCharsets.US_ASCII)));

        assertThat(receivedByListener.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo("from-caller");
        assertThat(receivedByCaller.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo("from-listener");
    }

    @Test
    void connectFailsWhenListenerRejects() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.reject(RejectionReason.FORBIDDEN));

        CompletableFuture<SrtConnection> connecting = SrtCaller.connect(
                new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), STREAM_ID);

        Throwable thrown = catchThrowable(() -> connecting.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        assertThat(thrown).isInstanceOf(ExecutionException.class);
        assertThat(thrown.getCause()).hasMessageContaining("FORBIDDEN");
    }


    private static final char[] PASSPHRASE = "roast-caller-secret".toCharArray();

    /**
     * The caller side of encryption, which until now did not exist: Roast could
     * be an encrypted listener but never an encrypted caller. Unlike the
     * listener, the caller *generates* the keys and announces them in its
     * CONCLUSION, so this exercises the opposite half of the key exchange.
     */
    @Test
    void anEncryptedCallerAndListenerExchangeDataBothWays() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept(PASSPHRASE.clone(), 16));
        CompletableFuture<SrtConnection> listenerSide = new CompletableFuture<>();
        listener.onConnection(listenerSide::complete);

        callerSideConnection = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()),
                        STREAM_ID, PASSPHRASE.clone(), 16)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        listenerSideConnection = listenerSide.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        CompletableFuture<String> receivedByListener = new CompletableFuture<>();
        listenerSideConnection.onData(payload -> {
            receivedByListener.complete(payload.toString(StandardCharsets.US_ASCII));
            payload.release();
        });
        CompletableFuture<String> receivedByCaller = new CompletableFuture<>();
        callerSideConnection.onData(payload -> {
            receivedByCaller.complete(payload.toString(StandardCharsets.US_ASCII));
            payload.release();
        });

        callerSideConnection.write(Unpooled.wrappedBuffer("caller-to-listener".getBytes(StandardCharsets.US_ASCII)));
        listenerSideConnection.write(Unpooled.wrappedBuffer("listener-to-caller".getBytes(StandardCharsets.US_ASCII)));

        assertThat(receivedByListener.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo("caller-to-listener");
        assertThat(receivedByCaller.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo("listener-to-caller");
    }

    /** A listener with a different passphrase can't unwrap our keys, so the connect must fail. */
    @Test
    void anEncryptedCallerFailsAgainstAListenerWithADifferentPassphrase() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept("a-different-secret".toCharArray(), 16));

        CompletableFuture<SrtConnection> connecting = SrtCaller.connect(
                new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()),
                STREAM_ID, PASSPHRASE.clone(), 16);

        Throwable thrown = catchThrowable(() -> connecting.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        assertThat(thrown).isInstanceOf(ExecutionException.class);
        assertThat(thrown.getCause()).hasMessageContaining("BADSECRET");
    }

    /** An encrypted caller against a listener expecting cleartext must not silently connect. */
    @Test
    void anEncryptedCallerFailsAgainstAnUnencryptedListener() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        CompletableFuture<SrtConnection> connecting = SrtCaller.connect(
                new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()),
                STREAM_ID, PASSPHRASE.clone(), 16);

        Throwable thrown = catchThrowable(() -> connecting.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));

        assertThat(thrown).isInstanceOf(ExecutionException.class);
        assertThat(thrown.getCause()).hasMessageContaining("key material");
    }


    /**
     * Connecting over a lossy link. Before handshake retry existed this failed
     * outright: a single dropped INDUCTION or CONCLUSION ended the attempt, since
     * SrtCaller sent each step exactly once. Loopback hid it completely — it took
     * putting a loss proxy in the path to see it at all.
     *
     * <p>Repeated, because one run at 20% loss can still get through first try;
     * across several, an unretried handshake is overwhelmingly unlikely to.
     */
    @Test
    void connectSucceedsThroughALossyLinkByRepeatingTheHandshake() throws Exception {
        for (int attempt = 0; attempt < 5; attempt++) {
            SrtListener lossyListener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
            lossyListener.setAcceptHandler(request -> AcceptDecision.accept());
            try (UdpLossProxy lossy = UdpLossProxy.start(
                    new InetSocketAddress("127.0.0.1", lossyListener.localAddress().getPort()), 0.20)) {

                SrtConnection connection = SrtCaller.connect(
                                new InetSocketAddress("127.0.0.1", lossy.localPort()), STREAM_ID)
                        .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

                assertThat(connection.metadata().streamId()).isEqualTo(STREAM_ID);
                connection.close();
            } finally {
                lossyListener.close();
            }
        }
    }

    @Test
    void connectTimesOutWhenNothingIsListening() throws Exception {
        int unusedPort;
        try (DatagramSocket probe = new DatagramSocket(0)) {
            unusedPort = probe.getLocalPort();
        }

        CompletableFuture<SrtConnection> connecting =
                SrtCaller.connect(new InetSocketAddress("127.0.0.1", unusedPort), STREAM_ID);

        Throwable thrown = catchThrowable(() -> connecting.get(10, TimeUnit.SECONDS));

        assertThat(thrown).isInstanceOf(ExecutionException.class);
        assertThat(thrown.getCause()).isInstanceOf(TimeoutException.class);
    }
}
