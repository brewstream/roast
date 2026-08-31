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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
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
    /**
     * Deliberately far above the 120ms default. The recovery budget, not the
     * timeout, is what this test actually needs: TSBPD gives a lost packet only
     * that long to be NAKed, retransmitted and arrive before TLPKTDROP abandons
     * it, and on a contended two-core CI runner the ~10ms tick slips enough that
     * 120ms stops being sufficient. That made this fail on CI while passing on a
     * fast machine - not a defect, just a test asserting lossless delivery under
     * conditions where dropping is the designed behaviour. A wider budget keeps
     * the strict "nothing is lost" claim honest, and exercises a latency setting
     * nothing else covers.
     */
    private static final Duration LATENCY = Duration.ofMillis(500);
    /**
     * Packets sent after the payloads, purely so that a payload lost at the very
     * end is still followed by something.
     *
     * <p>Loss of the final packets is not recoverable by ARQ alone: the receiver
     * detects loss by seeing a <em>later</em> sequence number arrive, so if the
     * last packets vanish there is no gap to notice, no NAK, and no
     * retransmission. Nothing is late and nothing is dropped - the receiver
     * simply never learns more was coming. libsrt covers this with a sender-side
     * retransmission timer (FASTREXMIT, {@code checkRexmitTimer}); gosrt has no
     * equivalent and neither does Roast, so this test must not assert a
     * guarantee the protocol here does not make. Twenty-five trailers at 5% loss
     * makes "every trailer also lost" vanishingly unlikely.
     */
    private static final int TRAILERS = 25;

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
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0),
                SrtConfig.defaults().withLatency(LATENCY));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        List<String> delivered = new ArrayList<>();
        CountDownLatch allArrived = new CountDownLatch(MESSAGES);
        CompletableFuture<SrtConnection> listenerSide = new CompletableFuture<>();
        listener.onConnection(connection -> {
            listenerSide.complete(connection);
            connection.onData(payload -> {
                String message = payload.toString(StandardCharsets.US_ASCII);
                payload.release();
                if (!message.startsWith("msg-")) {
                    return; // a trailer; see TRAILERS
                }
                synchronized (delivered) {
                    delivered.add(message);
                }
                allArrived.countDown();
            });
        });

        // The caller reaches the listener only through the relay, but loss is
        // switched on after the handshake completes. The caller does retry a
        // lost INDUCTION or CONCLUSION, so connecting through loss would work -
        // it is just a different thing to measure, and letting handshake retry
        // decide this test would tell us nothing about ARQ.
        proxy = UdpLossProxy.start(
                new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()), 0.0);
        callerSide = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", proxy.localPort()), STREAM_ID,
                        SrtConfig.defaults().withLatency(LATENCY))
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        proxy.setDropRate(DROP_RATE);

        // Paced so a lost packet has room for several NAK-driven retries inside
        // the negotiated TSBPD budget; blasting them would make TLPKTDROP, not
        // ARQ, decide the outcome.
        for (int i = 0; i < MESSAGES; i++) {
            callerSide.write(Unpooled.wrappedBuffer(("msg-" + i).getBytes(StandardCharsets.US_ASCII)));
            Thread.sleep(2);
        }
        // See TRAILERS: these exist only to give a lost final payload a
        // later sequence number to be noticed against. They are not asserted on.
        for (int i = 0; i < TRAILERS; i++) {
            callerSide.write(Unpooled.wrappedBuffer(("tail-" + i).getBytes(StandardCharsets.US_ASCII)));
            Thread.sleep(2);
        }

        // Not a bare future.get: a timeout there reports only that time ran out,
        // which is the least useful thing it could say. Waiting on the latch and
        // then asserting lets the failure carry how far delivery actually got,
        // and what the proxy did, which is the difference between "slow" and
        // "permanently lost".
        boolean complete = allArrived.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        int arrived;
        synchronized (delivered) {
            arrived = delivered.size();
        }
        assertThat(complete)
                .as("only %d of %d payloads arrived in %ds (proxy relayed %d, dropped %d); "
                                + "sender stats %s; receiver stats %s",
                        arrived, MESSAGES, TIMEOUT_SECONDS, proxy.relayedPackets(), proxy.droppedPackets(),
                        callerSide.stats(), listenerSide.get().stats())
                .isTrue();

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