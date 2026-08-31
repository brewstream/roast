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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * {@link SrtConfig} is deliberately six knobs rather than the sixteen constants
 * that existed before it, so what matters is that each one actually reaches the
 * wire — a setting that silently does nothing is worse than no setting at all.
 */
class SrtConfigTest {

    private static final int TIMEOUT_SECONDS = 20;

    private SrtListener listener;
    private SrtConnection caller;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (caller != null) {
            caller.close();
        }
        if (listener != null) {
            listener.close();
        }
    }

    @Test
    void defaultsMatchWhatWasHardcodedBefore() {
        SrtConfig defaults = SrtConfig.defaults();

        assertThat(defaults.latencyMillis()).isEqualTo(120);
        assertThat(defaults.connectTimeout()).isEqualTo(Duration.ofSeconds(5));
        assertThat(defaults.flowWindowPackets()).isEqualTo(8192);
        assertThat(defaults.srtVersion()).isEqualTo(0x010401);
        assertThat(defaults.maxMss()).isEqualTo(1500);
        assertThat(defaults.keyRefreshPackets()).isEqualTo(1L << 24);
        assertThat(defaults.keyPreAnnouncePackets()).isEqualTo(1L << 12);
    }

    @Test
    void withersLeaveTheOriginalUntouched() {
        SrtConfig defaults = SrtConfig.defaults();

        SrtConfig slower = defaults.withLatency(Duration.ofMillis(400));

        assertThat(slower.latencyMillis()).isEqualTo(400);
        assertThat(defaults.latencyMillis()).isEqualTo(120);
        assertThat(slower.flowWindowPackets()).isEqualTo(defaults.flowWindowPackets());
    }

    /** Latency is negotiated, so a configured value has to show up on the accepted connection. */
    @Test
    void configuredLatencyReachesTheNegotiatedConnection() throws Exception {
        SrtConfig config = SrtConfig.defaults().withLatency(Duration.ofMillis(300));
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0), config);
        listener.setAcceptHandler(request -> AcceptDecision.accept());
        CompletableFuture<SrtConnection> accepted = new CompletableFuture<>();
        listener.onConnection(accepted::complete);

        caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), "live/latency",
                        config)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(accepted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).metadata().receiveLatencyMillis())
                .isEqualTo(300);
        assertThat(caller.metadata().receiveLatencyMillis()).isEqualTo(300);
    }

    @Test
    void configuredFlowWindowIsWhatWeAdvertise() throws Exception {
        SrtConfig config = SrtConfig.defaults().withFlowWindowPackets(4096);
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0), config);
        listener.setAcceptHandler(request -> AcceptDecision.accept());
        CompletableFuture<SrtConnection> accepted = new CompletableFuture<>();
        listener.onConnection(accepted::complete);

        caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), "live/window",
                        config)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        SrtConnection listenerSide = accepted.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(listenerSide.metadata().flowWindowSize()).isEqualTo(4096);
        assertThat(listenerSide.stats().flowWindowPackets()).isEqualTo(4096);
    }

    /** A short connect timeout must actually shorten the wait, not just be stored. */
    @Test
    void configuredConnectTimeoutIsHonoured() throws Exception {
        int unusedPort;
        try (java.net.DatagramSocket probe = new java.net.DatagramSocket(0)) {
            unusedPort = probe.getLocalPort();
        }

        long start = System.nanoTime();
        Throwable thrown = catchThrowable(() -> SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", unusedPort), "live/timeout",
                        SrtConfig.defaults().withConnectTimeout(Duration.ofMillis(600)))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        long elapsedMillis = (System.nanoTime() - start) / 1_000_000;

        assertThat(thrown).isInstanceOf(ExecutionException.class);
        // Comfortably under the 5s default, which is what proves the setting took effect.
        assertThat(elapsedMillis).isLessThan(3_000);
    }

    /**
     * A configured MTU is a ceiling to negotiate down to, not a filter to reject
     * on. This test previously asserted the opposite - that an ordinary
     * 1500-byte caller was refused - which made the setting useless for the case
     * it exists to serve: lowering the MTU for a tunnel would have refused every
     * normal peer rather than accommodating one.
     */
    @Test
    void configuredMaxMssIsNegotiatedDownToRatherThanRejectedOn() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0),
                SrtConfig.defaults().withMaxMss(1000));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        // The caller advertises the standard 1500, above the listener's ceiling.
        SrtConnection connection = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), "live/mss")
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        try {
            assertThat(connection.metadata().maxTransmissionUnitSize()).isEqualTo(1000);
        } finally {
            connection.close();
        }
    }

    // --- validation: a bad value should fail where it is written, not mid-handshake

    @Test
    void aPreAnnounceThatIsNotBeforeTheRefreshIsRejected() {
        assertThatThrownBy(() -> SrtConfig.defaults().withKeyRotation(1000, 1000))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("announced before");
    }

    @Test
    void nonsensicalValuesAreRejectedUpFront() {
        assertThatThrownBy(() -> SrtConfig.defaults().withLatency(Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SrtConfig.defaults().withLatency(Duration.ofMillis(-1)))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SrtConfig.defaults().withFlowWindowPackets(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> SrtConfig.defaults().withMaxMss(400))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("576");
        assertThatThrownBy(() -> SrtConfig.defaults().withMaxMss(9000))
                .isInstanceOf(IllegalArgumentException.class);
    }
}