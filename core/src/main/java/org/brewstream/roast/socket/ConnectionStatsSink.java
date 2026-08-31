/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brewstream.roast.socket;

/**
 * Receives periodic {@link ConnectionStats} samples — the seam between Roast and
 * whatever a deployment actually uses for metrics.
 *
 * <p>Statistics should be pushable into whatever an embedder already uses —
 * Micrometer or similar — rather than trapped behind a bespoke object nobody
 * outside the library can wire up. Hence a one-method interface: it can be a
 * lambda, and Roast depends on no metrics library. A Micrometer binding is a
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