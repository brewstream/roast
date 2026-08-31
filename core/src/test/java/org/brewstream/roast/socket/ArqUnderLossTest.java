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

import io.netty.buffer.Unpooled;
import org.brewstream.roast.harness.UdpLossProxy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The definition of done for <em>both</em> the receiver and sender paths:
 * under 2% induced loss the output must still be intact, recovered via ARQ. Until
 * now nothing exercised it — {@link UdpLossProxy} was written for exactly this
 * in Phase 0 and then never referenced by a test, so every loss/retransmit path
 * was only ever driven by hand-injected gaps in unit tests, never by packets
 * genuinely going missing on a socket.
 *
 * <p>Roast talks to Roast here rather than to libsrt, deliberately: the point is
 * that <em>nothing is lost</em>, which needs a sender whose exact output is
 * known. ffmpeg's output isn't reproducible, so it can't support a byte-exact
 * claim.
 *
 * <p>Both directions are lossy, so ACKs and NAKs are dropped too, not just data
 * — recovery has to survive losing the recovery signalling itself.
 */
class ArqUnderLossTest {

    private static final int TIMEOUT_SECONDS = 30;
    private static final String STREAM_ID = "live/lossy";
    private static final int MESSAGES = 300;
    /** 2% is the target; 5% is the top of the useful range, so this is the harder end. */
    private static final double DROP_RATE = 0.05;

    private SrtListener listener;
    private SrtConnection callerSide;
    private UdpLossProxy proxy;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (callerSide != null) {
            callerSide.close();
        }
        if (listener != null) {
            listener.close();
        }
        if (proxy != null) {
            proxy.close();
        }
    }

    @Test
    void everyPayloadArrivesInOrderDespiteFivePercentLossInBothDirections() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        List<String> delivered = new ArrayList<>();
        CompletableFuture<Void> allArrived = new CompletableFuture<>();
        listener.onConnection(connection -> connection.onData(payload -> {
            synchronized (delivered) {
                delivered.add(payload.toString(StandardCharsets.US_ASCII));
                if (delivered.size() == MESSAGES) {
                    allArrived.complete(null);
                }
            }
            payload.release();
        }));

        // The caller reaches the listener only through the relay, but loss is
        // switched on after the handshake completes. The caller does retry a
        // lost INDUCTION or CONCLUSION, so connecting through loss would work -
        // it is just a different thing to measure, and letting handshake retry
        // decide this test would tell us nothing about ARQ.
        proxy = UdpLossProxy.start(
                new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), 0.0);
        callerSide = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", proxy.localPort()), STREAM_ID)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        proxy.setDropRate(DROP_RATE);

        // Paced so a lost packet has room for several NAK-driven retries inside
        // the negotiated 120ms TSBPD budget; blasting them would make TLPKTDROP,
        // not ARQ, decide the outcome.
        for (int i = 0; i < MESSAGES; i++) {
            callerSide.write(Unpooled.wrappedBuffer(("msg-" + i).getBytes(StandardCharsets.US_ASCII)));
            Thread.sleep(2);
        }

        allArrived.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        List<String> expected = new ArrayList<>();
        for (int i = 0; i < MESSAGES; i++) {
            expected.add("msg-" + i);
        }
        synchronized (delivered) {
            // Byte-exact and in order: ARQ recovered everything the proxy threw away.
            assertThat(delivered).containsExactlyElementsOf(expected);
        }

        // Without this the test would silently pass on a run that dropped nothing.
        assertThat(proxy.droppedPackets())
                .as("the proxy must actually have dropped packets for this to prove anything")
                .isPositive();
    }
}