package org.brewstream.roast.socket;

import io.netty.buffer.Unpooled;
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
