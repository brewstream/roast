package org.brewstream.roast.socket;

import io.netty.buffer.Unpooled;
import org.brewstream.roast.packet.SrtSocketId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 6's headline feature: many SRT sockets multiplexed onto one UDP port,
 * demultiplexed by socket ID the way a libsrt listener does.
 *
 * <p>The machinery has existed since Phase 2 — {@link SrtSocketIdDemultiplexer}
 * routes by destination socket ID and {@code SrtListener} keeps a connection per
 * ID — and {@code RelayDemo} has exercised it informally the whole time, with a
 * publisher and several players sharing port 9000. Nothing asserted it, though,
 * and "works when I run the demo" is not coverage. The risk in a demux design is
 * cross-talk: a payload delivered to the wrong connection is silent corruption
 * of someone else's stream, so that is what these tests are shaped around.
 */
class MultiplexingTest {

    private static final int TIMEOUT_SECONDS = 20;
    private static final int CONNECTIONS = 6;

    private SrtListener listener;
    private final List<SrtConnection> callers = new CopyOnWriteArrayList<>();

    @AfterEach
    void tearDown() throws InterruptedException {
        callers.forEach(SrtConnection::close);
        if (listener != null) {
            listener.close();
        }
    }

    @Test
    void manyConnectionsShareOnePortWithoutCrossTalk() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        // What each listener-side connection received, keyed by its StreamID.
        Map<String, List<String>> receivedByStream = new ConcurrentHashMap<>();
        CountDownLatch allDelivered = new CountDownLatch(CONNECTIONS);
        listener.onConnection(connection -> {
            String streamId = connection.metadata().streamId();
            List<String> received = receivedByStream.computeIfAbsent(streamId, k -> new CopyOnWriteArrayList<>());
            connection.onData(payload -> {
                received.add(payload.toString(StandardCharsets.US_ASCII));
                payload.release();
                allDelivered.countDown();
            });
        });

        int port = listener.localAddress().getPort();
        for (int i = 0; i < CONNECTIONS; i++) {
            callers.add(SrtCaller.connect(new InetSocketAddress("127.0.0.1", port), "live/stream-" + i)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }

        // Every connection sends a payload only it should ever produce.
        for (int i = 0; i < CONNECTIONS; i++) {
            callers.get(i).write(Unpooled.wrappedBuffer(("payload-" + i).getBytes(StandardCharsets.US_ASCII)));
        }

        assertThat(allDelivered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

        // Each stream got exactly its own payload and nobody else's.
        assertThat(receivedByStream).hasSize(CONNECTIONS);
        for (int i = 0; i < CONNECTIONS; i++) {
            assertThat(receivedByStream.get("live/stream-" + i))
                    .as("stream %d should see only its own payload", i)
                    .containsExactly("payload-" + i);
        }
    }

    @Test
    void everyConnectionOnThePortGetsItsOwnSocketId() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        List<SrtSocketId> assigned = new CopyOnWriteArrayList<>();
        CountDownLatch accepted = new CountDownLatch(CONNECTIONS);
        listener.onConnection(connection -> {
            assigned.add(connection.metadata().socketId());
            accepted.countDown();
        });

        int port = listener.localAddress().getPort();
        for (int i = 0; i < CONNECTIONS; i++) {
            callers.add(SrtCaller.connect(new InetSocketAddress("127.0.0.1", port), "live/id-" + i)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }

        assertThat(accepted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(assigned).hasSize(CONNECTIONS).doesNotHaveDuplicates();
        assertThat(listener.connections()).hasSize(CONNECTIONS);
        // All on the one bound port - that is the whole point.
        assertThat(listener.localAddress().getPort()).isEqualTo(port);
    }

    /** Tearing one connection down must not disturb its neighbours on the same port. */
    @Test
    void closingOneConnectionLeavesTheOthersWorking() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        Map<String, CountDownLatch> delivered = new ConcurrentHashMap<>();
        Map<String, List<String>> received = new ConcurrentHashMap<>();
        listener.onConnection(connection -> {
            String streamId = connection.metadata().streamId();
            List<String> forStream = received.computeIfAbsent(streamId, k -> new CopyOnWriteArrayList<>());
            connection.onData(payload -> {
                forStream.add(payload.toString(StandardCharsets.US_ASCII));
                payload.release();
                delivered.computeIfAbsent(streamId, k -> new CountDownLatch(1)).countDown();
            });
        });

        int port = listener.localAddress().getPort();
        List<SrtConnection> connected = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            delivered.put("live/close-" + i, new CountDownLatch(1));
            connected.add(SrtCaller.connect(new InetSocketAddress("127.0.0.1", port), "live/close-" + i)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }
        callers.addAll(connected);

        // Drop the middle one.
        connected.get(1).close();
        Thread.sleep(200);

        connected.get(0).write(Unpooled.wrappedBuffer("still-here-0".getBytes(StandardCharsets.US_ASCII)));
        connected.get(2).write(Unpooled.wrappedBuffer("still-here-2".getBytes(StandardCharsets.US_ASCII)));

        assertThat(delivered.get("live/close-0").await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(delivered.get("live/close-2").await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(received.get("live/close-0")).containsExactly("still-here-0");
        assertThat(received.get("live/close-2")).containsExactly("still-here-2");
        assertThat(listener.connections()).hasSize(2);
    }


    /**
     * A long-running relay churns through connections, so the listener must
     * forget them as they close. Both its connection map and its
     * handshake-dedup cache used to grow for the life of the process — an
     * unbounded leak, and {@code connections()} reporting long-dead
     * connections to anyone polling stats.
     */
    @Test
    void theListenerForgetsConnectionsAsTheyClose() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());
        int port = listener.localAddress().getPort();

        for (int round = 0; round < 5; round++) {
            SrtConnection connection = SrtCaller.connect(
                            new InetSocketAddress("127.0.0.1", port), "live/churn-" + round)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(listener.connections()).hasSize(1);

            connection.close();
            // The listener side closes on receiving our SHUTDOWN.
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
            while (!listener.connections().isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
            assertThat(listener.connections())
                    .as("round %d: the closed connection should have been forgotten", round)
                    .isEmpty();
        }
    }

    /** Stats are per connection, not per port — a shared counter would be useless for routing decisions. */
    @Test
    void statsAreTrackedPerConnectionNotPerPort() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        Map<String, SrtConnection> byStream = new ConcurrentHashMap<>();
        CountDownLatch accepted = new CountDownLatch(2);
        CountDownLatch delivered = new CountDownLatch(3);
        listener.onConnection(connection -> {
            byStream.put(connection.metadata().streamId(), connection);
            connection.onData(payload -> {
                payload.release();
                delivered.countDown();
            });
            accepted.countDown();
        });

        int port = listener.localAddress().getPort();
        SrtConnection busy = SrtCaller.connect(new InetSocketAddress("127.0.0.1", port), "live/busy")
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        SrtConnection quiet = SrtCaller.connect(new InetSocketAddress("127.0.0.1", port), "live/quiet")
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        callers.add(busy);
        callers.add(quiet);
        assertThat(accepted.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

        for (int i = 0; i < 2; i++) {
            busy.write(Unpooled.wrappedBuffer("busy".getBytes(StandardCharsets.US_ASCII)));
            Thread.sleep(5);
        }
        quiet.write(Unpooled.wrappedBuffer("quiet".getBytes(StandardCharsets.US_ASCII)));
        assertThat(delivered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

        ConnectionStats busyStats = byStream.get("live/busy").stats();
        ConnectionStats quietStats = byStream.get("live/quiet").stats();

        assertThat(busyStats.packetsReceived()).isEqualTo(2);
        assertThat(quietStats.packetsReceived()).isEqualTo(1);
        assertThat(busyStats.bytesReceived()).isGreaterThan(quietStats.bytesReceived());
    }
}
