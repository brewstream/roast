package org.brewstream.roast.socket;

/**
 * Receives periodic {@link ConnectionStats} samples — the seam between Roast and
 * whatever a deployment actually uses for metrics.
 *
 * <p>DESIGN.md §4 asks for stats "ideally pushable to an external metrics sink
 * (Micrometer or similar), not a bespoke object nobody outside the library can
 * wire up". This is that hook, kept as a one-method interface so it can be a
 * lambda and so Roast depends on no metrics library: a Micrometer binding is a
 * few lines in the embedder's own code, and can live outside this module
 * entirely.
 *
 * <pre>{@code
 * StatsSampler.start(Duration.ofSeconds(1), listener, (connection, stats) -> {
 *     Tags tags = Tags.of("stream", connection.metadata().streamId());
 *     registry.gauge("srt.rtt.micros", tags, stats.rttMicros());
 *     registry.gauge("srt.retransmit.rate", tags, stats.retransmitRate());
 * });
 * }</pre>
 *
 * <p>The {@link SrtConnection} is passed alongside the snapshot so a sink can
 * tag by StreamID, socket ID or peer address — a single aggregate figure across
 * every connection on a port is rarely what anyone wants from a relay.
 *
 * <p>Called on the sampler's own thread, never a connection's event loop, so
 * blocking here — an HTTP push to a metrics backend, say — cannot slow packet
 * processing. A sink that throws is logged and skipped rather than killing the
 * sampler.
 */
@FunctionalInterface
public interface ConnectionStatsSink {

    /**
     * @param connection the connection the sample describes, for tagging
     * @param stats      an immutable snapshot; counters are cumulative for the
     *                   connection's life, so two samples give a rate
     */
    void report(SrtConnection connection, ConnectionStats stats);
}
