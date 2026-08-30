package org.brewstream.roast.socket;

import org.brewstream.roast.packet.cif.LossRange;

/**
 * Observability events for a connection — what happened, for an application to
 * react to, log, or turn into metrics. Register with
 * {@link SrtListener#addEventListener} to observe every connection a listener
 * accepts, or {@link SrtConnection#addEventListener} for one connection.
 *
 * <p><b>Every method is a {@code default} no-op</b>, so implement only what you
 * care about — and so adding an event later doesn't break existing
 * implementations.
 *
 * <p><b>Threading: these never run on a connection's event loop.</b> They are
 * dispatched on a separate single thread, so a slow listener cannot stall packet
 * processing — the failure mode a documented "must not block" contract only
 * asks you to avoid. Two consequences worth knowing:
 * <ul>
 *   <li><b>Events are ordered relative to each other</b> (one dispatcher
 *       thread), but <em>not</em> relative to {@link SrtConnection#onData},
 *       which stays on the event loop for throughput. A payload can reach your
 *       {@code onData} before the {@code onLoss} describing a gap that preceded
 *       it.</li>
 *   <li><b>Events can be dropped.</b> The dispatch queue is bounded; if
 *       listeners fall behind, events are discarded rather than growing memory
 *       without limit — turning a stall into an OOM would be strictly worse.
 *       {@link ConnectionStats#droppedEvents()} counts them, so a consumer that
 *       is falling behind can see that it is.</li>
 * </ul>
 *
 * <p>Deliberately <b>not</b> an event per ACK or per packet: at a few thousand
 * packets a second that is pure garbage and a throughput hazard. High-frequency
 * activity is counted in {@link ConnectionStats} instead — events for notable
 * occurrences, counters for volume.
 *
 * <p>This is <em>not</em> where you attach your data handler.
 * {@link SrtListener#onConnection} stays synchronous on the event loop for
 * exactly that: it runs before the accept response goes out, so a peer's first
 * packets can't arrive before your {@code onData} exists. {@link #onConnected}
 * here is the observability counterpart, and carries no such guarantee.
 */
public interface SrtConnectionListener {

    /** A connection completed its handshake. Observability only — see the class javadoc. */
    default void onConnected(SrtConnection connection) {
    }

    /** A connection finished tearing down, from either side. */
    default void onDisconnected(SrtConnection connection) {
    }

    /** A gap was detected in the received sequence and NAKed. */
    default void onLoss(SrtConnection connection, LossRange range) {
    }

    /** Packets were given up on because their delivery deadline passed (TLPKTDROP). */
    default void onTlpktDrop(SrtConnection connection, LossRange range) {
    }

    /** A packet was retransmitted in response to a peer's NAK. */
    default void onRetransmit(SrtConnection connection, int sequenceNumber) {
    }

    /** The encryption key was rotated mid-stream. */
    default void onKeyRotated(SrtConnection connection) {
    }
}
