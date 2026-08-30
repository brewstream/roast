package org.brewstream.roast.socket;

import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Delivers {@link SrtConnectionListener} events off the connection's event loop.
 *
 * <p>The point is structural rather than stylistic: a listener that blocks — a
 * synchronous HTTP call to a metrics backend, a slow log append — would
 * otherwise stall packet processing for its connection, and a javadoc contract
 * saying "must not block" only asks people not to cause that. Dispatching
 * elsewhere makes it impossible.
 *
 * <p><b>One thread, deliberately.</b> A pool would deliver a connection's events
 * out of order, which for loss/retransmit observation is worse than useless.
 *
 * <p><b>Bounded queue, dropping on overflow.</b> If listeners can't keep up, an
 * unbounded queue would convert a stall into an out-of-memory failure — strictly
 * worse than the problem being solved. These are observability events, so losing
 * some is acceptable; losing them <em>silently</em> is not, and
 * {@link #droppedEvents()} is surfaced through {@code ConnectionStats}.
 *
 * <p>A listener that throws is logged and skipped: one broken observer must not
 * prevent the others from seeing an event, and must never affect the connection.
 */
final class EventDispatcher implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(EventDispatcher.class.getName());

    /**
     * Deep enough to absorb a burst (a loss episode produces a flurry of events),
     * shallow enough that a wedged listener can't hold much memory hostage.
     */
    private static final int QUEUE_CAPACITY = 1024;

    private final List<SrtConnectionListener> listeners = new CopyOnWriteArrayList<>();
    private final BlockingQueue<Runnable> queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    private final AtomicLong dropped = new AtomicLong();
    private final Thread thread;
    private volatile boolean running = true;

    EventDispatcher(String name) {
        this.thread = new Thread(this::drain, "srt-events-" + name);
        this.thread.setDaemon(true);
        this.thread.start();
    }

    void add(SrtConnectionListener listener) {
        listeners.add(listener);
    }

    void addAll(List<SrtConnectionListener> toAdd) {
        listeners.addAll(toAdd);
    }

    boolean isEmpty() {
        return listeners.isEmpty();
    }

    long droppedEvents() {
        return dropped.get();
    }

    /**
     * Queues {@code event} for delivery to every listener. Returns immediately;
     * called from the event loop, so it must never block or allocate more than
     * necessary — hence the early exit when nobody is listening.
     */
    void fire(Consumer<SrtConnectionListener> event) {
        if (listeners.isEmpty() || !running) {
            return;
        }
        if (!queue.offer(() -> deliver(event))) {
            dropped.incrementAndGet();
        }
    }

    private void deliver(Consumer<SrtConnectionListener> event) {
        for (SrtConnectionListener listener : listeners) {
            try {
                event.accept(listener);
            } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "Event listener threw; continuing with the others", e);
            }
        }
    }

    private void drain() {
        while (running) {
            try {
                Runnable task = queue.poll(100, TimeUnit.MILLISECONDS);
                if (task != null) {
                    task.run();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    @Override
    public void close() {
        running = false;
        thread.interrupt();
    }
}
