package org.brewstream.roast.socket;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Polls connections on a schedule and hands each snapshot to a
 * {@link ConnectionStatsSink}.
 *
 * <p>Everything here is doable by hand — {@link SrtListener#connections()} plus
 * {@link SrtConnection#stats()} on a timer — but everyone would write the same
 * loop, and the two details that are easy to get wrong are exactly the ones a
 * shared implementation should own: not sampling on an event loop, and not
 * losing a connection's final numbers.
 *
 * <p><b>Its own thread.</b> Sampling and sink callbacks never touch a
 * connection's event loop, so a sink that blocks on an HTTP push cannot slow
 * packet processing — the same reasoning as {@link SrtConnectionListener}'s
 * dispatch, for the same reason.
 *
 * <p><b>A final sample on disconnect.</b> Polling alone loses whatever happened
 * in the last partial interval, and for a short-lived connection that can be
 * most of it — a stream that ran for two seconds under a one-second poll might
 * report almost nothing. When constructed against an {@link SrtListener} this
 * registers for {@link SrtConnectionListener#onDisconnected} and emits one last
 * snapshot, so cumulative counters are always complete.
 */
public final class StatsSampler implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(StatsSampler.class.getName());

    private final ScheduledExecutorService executor;
    private final Supplier<Collection<SrtConnection>> source;
    private final ConnectionStatsSink sink;

    private StatsSampler(Duration interval, Supplier<Collection<SrtConnection>> source, ConnectionStatsSink sink) {
        this.source = source;
        this.sink = sink;
        this.executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "srt-stats-sampler");
            thread.setDaemon(true);
            return thread;
        });
        long millis = Math.max(1, interval.toMillis());
        executor.scheduleAtFixedRate(this::sampleAll, millis, millis, TimeUnit.MILLISECONDS);
    }

    /**
     * Samples every connection the listener currently holds, and additionally
     * emits one final snapshot as each connection closes so its cumulative
     * counters are never left incomplete.
     */
    public static StatsSampler start(Duration interval, SrtListener listener, ConnectionStatsSink sink) {
        StatsSampler sampler = new StatsSampler(interval, listener::connections, sink);
        listener.addEventListener(new SrtConnectionListener() {
            @Override
            public void onDisconnected(SrtConnection connection) {
                sampler.sample(connection);
            }
        });
        return sampler;
    }

    /**
     * Samples whatever the supplier returns — for a caller-side connection, or a
     * set assembled by the embedder. Periodic only: without a listener to
     * observe, there is no disconnect to hang a final sample off, so a
     * short-lived connection's last interval is lost.
     */
    public static StatsSampler start(Duration interval, Supplier<Collection<SrtConnection>> source,
            ConnectionStatsSink sink) {
        return new StatsSampler(interval, source, sink);
    }

    private void sampleAll() {
        try {
            for (SrtConnection connection : source.get()) {
                sample(connection);
            }
        } catch (RuntimeException e) {
            // Never let one bad round kill the schedule - scheduleAtFixedRate
            // cancels the task permanently if it throws.
            LOG.log(Level.WARNING, "Stats sampling round failed; continuing", e);
        }
    }

    private void sample(SrtConnection connection) {
        try {
            sink.report(connection, connection.stats());
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "Stats sink threw; skipping this sample", e);
        }
    }

    /** Stops sampling. A sample already in progress is interrupted rather than awaited. */
    @Override
    public void close() {
        executor.shutdownNow();
    }
}
