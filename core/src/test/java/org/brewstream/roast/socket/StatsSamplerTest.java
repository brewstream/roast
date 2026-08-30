package org.brewstream.roast.socket;

import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The metrics seam DESIGN.md §4 asks for: stats pushable to an external sink
 * rather than only readable from a bespoke object. Roast depends on no metrics
 * library, so what is verified here is the plumbing — that samples arrive, that
 * they are attributed to the right connection, that a connection's final numbers
 * are not lost, and that a badly behaved sink cannot damage the transport.
 */
class StatsSamplerTest {

    private static final int TIMEOUT_SECONDS = 20;

    private SrtListener listener;
    private SrtConnection caller;
    private StatsSampler sampler;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (sampler != null) {
            sampler.close();
        }
        if (caller != null) {
            caller.close();
        }
        if (listener != null) {
            listener.close();
        }
    }

    private void bindAndAccept() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());
    }

    @Test
    void samplesArriveOnAScheduleTaggedWithTheirConnection() throws Exception {
        bindAndAccept();
        listener.onConnection(connection -> connection.onData(payload -> payload.release()));

        Map<String, AtomicReference<ConnectionStats>> byStream = new ConcurrentHashMap<>();
        CountDownLatch sampled = new CountDownLatch(2);
        sampler = StatsSampler.start(Duration.ofMillis(100), listener, (connection, stats) -> {
            byStream.computeIfAbsent(connection.metadata().streamId(), k -> new AtomicReference<>()).set(stats);
            sampled.countDown();
        });

        caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), "live/sampled")
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        caller.write(Unpooled.wrappedBuffer("sampled".getBytes(StandardCharsets.US_ASCII)));

        assertThat(sampled.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(byStream).containsKey("live/sampled");
        assertThat(byStream.get("live/sampled").get().packetsReceived()).isPositive();
    }

    /**
     * Polling alone loses the last partial interval, which for a short-lived
     * connection can be most of it. The final snapshot on disconnect is what
     * makes cumulative counters trustworthy.
     */
    @Test
    void aFinalSampleIsEmittedWhenAConnectionCloses() throws Exception {
        bindAndAccept();
        listener.onConnection(connection -> connection.onData(payload -> payload.release()));

        List<ConnectionStats> samples = new CopyOnWriteArrayList<>();
        CountDownLatch closed = new CountDownLatch(1);
        // Deliberately far longer than the connection will live, so any sample at
        // all can only have come from the disconnect path.
        sampler = StatsSampler.start(Duration.ofHours(1), listener, (connection, stats) -> {
            samples.add(stats);
            closed.countDown();
        });

        caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), "live/short")
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        caller.write(Unpooled.wrappedBuffer("brief".getBytes(StandardCharsets.US_ASCII)));
        Thread.sleep(300);
        caller.close();

        assertThat(closed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(samples).isNotEmpty();
        assertThat(samples.get(samples.size() - 1).packetsReceived()).isPositive();
    }

    /** A sink that blocks must not slow the transport — it runs on the sampler's own thread. */
    @Test
    void aBlockingSinkDoesNotStallTheDataPath() throws Exception {
        bindAndAccept();
        CountDownLatch delivered = new CountDownLatch(1);
        listener.onConnection(connection -> connection.onData(payload -> {
            payload.release();
            delivered.countDown();
        }));

        CountDownLatch releaseSink = new CountDownLatch(1);
        sampler = StatsSampler.start(Duration.ofMillis(50), listener, (connection, stats) -> {
            try {
                releaseSink.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), "live/blocked")
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        caller.write(Unpooled.wrappedBuffer("flows-anyway".getBytes(StandardCharsets.US_ASCII)));

        assertThat(delivered.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        releaseSink.countDown();
    }

    /**
     * scheduleAtFixedRate cancels a task permanently once it throws, so an
     * exception escaping the sink would silently stop all metrics — the failure
     * being invisible is what makes it worth guarding.
     */
    @Test
    void aThrowingSinkDoesNotStopLaterSamples() throws Exception {
        bindAndAccept();
        listener.onConnection(connection -> connection.onData(payload -> payload.release()));

        CountDownLatch attempts = new CountDownLatch(3);
        sampler = StatsSampler.start(Duration.ofMillis(50), listener, (connection, stats) -> {
            attempts.countDown();
            throw new IllegalStateException("deliberately broken sink");
        });

        caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), "live/throwing")
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(attempts.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .as("sampling must keep going after a sink throws").isTrue();
    }

    /** The supplier form works without a listener — e.g. a caller-side connection. */
    @Test
    void samplingWorksFromAnArbitrarySourceOfConnections() throws Exception {
        bindAndAccept();
        listener.onConnection(connection -> connection.onData(payload -> payload.release()));

        caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), "live/supplied")
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        CountDownLatch sampled = new CountDownLatch(1);
        AtomicReference<ConnectionStats> seen = new AtomicReference<>();
        sampler = StatsSampler.start(Duration.ofMillis(100), () -> List.of(caller), (connection, stats) -> {
            seen.set(stats);
            sampled.countDown();
        });

        caller.write(Unpooled.wrappedBuffer("from-supplier".getBytes(StandardCharsets.US_ASCII)));

        assertThat(sampled.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(seen.get().packetsSent()).isPositive();
    }
}
