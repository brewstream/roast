package org.brewstream.roast.socket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.brewstream.roast.harness.UdpLossProxy;
import org.brewstream.roast.packet.cif.LossRange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The observability surface that is Roast's actual value over a libsrt
 * binding: events something can subscribe to, and stats something can poll.
 * Exercised over real sockets, since the threading contract is most of the
 * design.
 */
class ObservabilityTest {

    private static final int TIMEOUT_SECONDS = 20;
    private static final String STREAM_ID = "live/observed";

    private SrtListener listener;
    private SrtConnection caller;
    private UdpLossProxy proxy;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (caller != null) {
            caller.close();
        }
        if (listener != null) {
            listener.close();
        }
        if (proxy != null) {
            proxy.close();
        }
    }

    /** A listener registered on the SrtListener sees connections it never wired up itself. */
    @Test
    void aListenerLevelSubscriberSeesConnectAndDisconnect() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        CompletableFuture<SrtConnection> connected = new CompletableFuture<>();
        CountDownLatch disconnected = new CountDownLatch(1);
        listener.addEventListener(new SrtConnectionListener() {
            @Override
            public void onConnected(SrtConnection connection) {
                connected.complete(connection);
            }

            @Override
            public void onDisconnected(SrtConnection connection) {
                disconnected.countDown();
            }
        });

        caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), STREAM_ID)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(connected.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).metadata().streamId()).isEqualTo(STREAM_ID);

        connected.get().close();
        assertThat(disconnected.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    /** Several subscribers must all be delivered to — the old single-slot hooks clobbered each other. */
    @Test
    void everySubscriberIsDeliveredToNotJustTheLastRegistered() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        CountDownLatch both = new CountDownLatch(2);
        listener.addEventListener(new SrtConnectionListener() {
            @Override
            public void onConnected(SrtConnection connection) {
                both.countDown();
            }
        });
        listener.addEventListener(new SrtConnectionListener() {
            @Override
            public void onConnected(SrtConnection connection) {
                both.countDown();
            }
        });

        caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), STREAM_ID)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(both.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    /**
     * The whole point of dispatching off the event loop: a listener that blocks
     * must not stop the connection carrying data. A documented "don't block"
     * contract would leave this failing.
     */
    @Test
    void aBlockingSubscriberDoesNotStallTheDataPath() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        CountDownLatch releaseTheBlockedListener = new CountDownLatch(1);
        listener.addEventListener(new SrtConnectionListener() {
            @Override
            public void onConnected(SrtConnection connection) {
                try {
                    releaseTheBlockedListener.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        CompletableFuture<String> received = new CompletableFuture<>();
        listener.onConnection(connection -> connection.onData(payload -> {
            received.complete(payload.toString(StandardCharsets.US_ASCII));
            payload.release();
        }));

        caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), STREAM_ID)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        caller.write(Unpooled.wrappedBuffer("still-flowing".getBytes(StandardCharsets.US_ASCII)));

        // Data arrives while onConnected is still parked inside the listener.
        assertThat(received.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo("still-flowing");
        releaseTheBlockedListener.countDown();
    }

    /** A listener that throws must not prevent the others from seeing the event. */
    @Test
    void aThrowingSubscriberDoesNotSuppressTheOthers() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        CountDownLatch survivorSaw = new CountDownLatch(1);
        listener.addEventListener(new SrtConnectionListener() {
            @Override
            public void onConnected(SrtConnection connection) {
                throw new IllegalStateException("deliberately broken listener");
            }
        });
        listener.addEventListener(new SrtConnectionListener() {
            @Override
            public void onConnected(SrtConnection connection) {
                survivorSaw.countDown();
            }
        });

        caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), STREAM_ID)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(survivorSaw.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void statsCountWhatActuallyFlowed() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        int messages = 20;
        CountDownLatch allArrived = new CountDownLatch(messages);
        CompletableFuture<SrtConnection> listenerSide = new CompletableFuture<>();
        listener.onConnection(connection -> {
            listenerSide.complete(connection);
            connection.onData(payload -> {
                allArrived.countDown();
                payload.release();
            });
        });

        caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), STREAM_ID)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        byte[] chunk = "0123456789".getBytes(StandardCharsets.US_ASCII);
        for (int i = 0; i < messages; i++) {
            caller.write(Unpooled.wrappedBuffer(chunk));
            Thread.sleep(2);
        }
        assertThat(allArrived.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

        ConnectionStats senderStats = caller.stats();
        assertThat(senderStats.packetsSent()).isGreaterThanOrEqualTo(messages);
        assertThat(senderStats.bytesSent()).isGreaterThanOrEqualTo((long) messages * chunk.length);
        assertThat(senderStats.rttMicros()).isPositive();
        assertThat(senderStats.flowWindowPackets()).isPositive();
        assertThat(senderStats.retransmitRate()).isBetween(0.0, 1.0);

        ConnectionStats receiverStats = listenerSide.get().stats();
        assertThat(receiverStats.packetsReceived()).isGreaterThanOrEqualTo(messages);
        assertThat(receiverStats.bytesReceived()).isGreaterThanOrEqualTo((long) messages * chunk.length);
        assertThat(receiverStats.droppedEvents()).isZero();
    }

    /**
     * The send-side bandwidth figures have to survive the trip from
     * {@code SendRateEstimator} through {@code SendBuffer} to {@code stats()} —
     * the estimator's own unit tests can't catch a wiring mistake. Takes over a
     * second by construction: the rate window is 1s, and nothing is reported
     * until one has closed.
     */
    @Test
    void sendRatesReachStatsOnceAWindowHasClosed() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());
        listener.onConnection(connection -> connection.onData(ByteBuf::release));

        caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), STREAM_ID)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(caller.stats().estimatedSentBytesPerSecond())
                .as("nothing should be reported before a window has closed")
                .isZero();

        byte[] chunk = new byte[1000];
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1400);
        while (System.nanoTime() < deadline) {
            caller.write(Unpooled.wrappedBuffer(chunk));
            Thread.sleep(5);
        }

        ConnectionStats stats = caller.stats();
        assertThat(stats.estimatedInputBytesPerSecond())
                .as("the application offered ~200KB/s").isPositive();
        assertThat(stats.estimatedSentBytesPerSecond())
                .as("and it actually went out").isPositive();
        // No induced loss here, so nothing should have been resent.
        assertThat(stats.sendLossRatePercent()).isZero();
    }

    /** Under real loss, the loss and retransmit signals must actually fire and be counted. */
    @Test
    void lossAndRetransmissionAreReportedUnderRealLoss() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        List<LossRange> lossEvents = new CopyOnWriteArrayList<>();
        CompletableFuture<SrtConnection> listenerSide = new CompletableFuture<>();
        listener.addEventListener(new SrtConnectionListener() {
            @Override
            public void onLoss(SrtConnection connection, LossRange range) {
                lossEvents.add(range);
            }
        });

        int messages = 200;
        listener.onConnection(connection -> {
            listenerSide.complete(connection);
            connection.onData(payload -> payload.release());
        });

        proxy = UdpLossProxy.start(
                new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), 0.0);
        caller = SrtCaller.connect(new InetSocketAddress("127.0.0.1", proxy.localPort()), STREAM_ID)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        proxy.setDropRate(0.05);

        // What this test is about is that loss and retransmission are *reported* -
        // that every packet ultimately arrives is ArqUnderLossTest's job. Waiting
        // on full delivery here made this fail under parallel-suite load while
        // passing alone, which is the worst kind of test: it trains people to
        // rerun rather than to look.
        SrtConnection listenerConnection = listenerSide.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        for (int i = 0; i < messages; i++) {
            caller.write(Unpooled.wrappedBuffer(("m" + i).getBytes(StandardCharsets.US_ASCII)));
            Thread.sleep(2);
        }
        while (System.nanoTime() < deadline
                && (lossEvents.isEmpty()
                        || listenerConnection.stats().packetsLost() == 0
                        || caller.stats().packetsRetransmitted() == 0)) {
            Thread.sleep(20);
        }

        assertThat(lossEvents).as("5%% loss should have produced loss events").isNotEmpty();
        assertThat(listenerConnection.stats().packetsLost()).isPositive();
        assertThat(caller.stats().packetsRetransmitted()).isPositive();
        assertThat(caller.stats().retransmitRate()).isPositive();
    }
}
