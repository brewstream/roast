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

package org.brewstream.roast.send;

/**
 * The send-side rate figures: how fast the application is offering bytes, how
 * fast they are actually going out, and what proportion of the outbound volume
 * is retransmission. Ported from the rate block in gosrt's
 * {@code congestion/live/send.go} ({@code estimatedInputBW}, {@code
 * estimatedSentBW}, {@code pktLossRate}), which keeps them in its one sender
 * struct — the same reason {@link org.brewstream.roast.recv.ReceiveRateEstimator}
 * exists separately on the receive side, and this is deliberately its mirror
 * image.
 *
 * <p><b>Why the two bandwidth figures are worth separating.</b> Input is what
 * {@link SendBuffer#push} was handed; sent is what {@link SendBuffer#tick} and
 * {@link SendBuffer#nak} actually delivered. Equal figures mean the connection
 * is keeping up. Sent running above input means retransmission is consuming
 * real capacity. Input running above sent means the buffer is filling and
 * TLPKTDROP is about to start discarding — which is the interesting one,
 * because by the time it shows up in a drop counter the data is already gone.
 *
 * <p><b>The loss rate is byte-based and a percentage</b>, both faithfully from
 * gosrt: {@code bytesRetrans / bytesSent * 100}. gosrt calls the field
 * {@code pktLossRate}, but it is computed from byte counts, so the name here
 * says what it measures. Note this is a different quantity from {@link
 * org.brewstream.roast.socket.ConnectionStats#retransmitRate()}, which is a
 * cumulative packet-count ratio over the connection's whole life expressed as a
 * fraction — this one is bytes, over the last window, as a percentage. Both are
 * reported; a connection that has recovered from a bad patch shows a low
 * lifetime figure and a low current one, while a connection in trouble right
 * now shows a low lifetime figure and a high current one.
 *
 * <p><b>Window handling matches the receive side exactly</b>: a ~1s window,
 * recomputed on {@link #tick} and then reset, with the rates scaled by the
 * window's real elapsed time so a short or long window still reports a correct
 * per-second figure. Every method takes {@code nowMicros} rather than reading a
 * clock, per this codebase's single-elapsed-time-source rule.
 *
 * <p><b>No reference test exists to ground this against</b> — checked directly:
 * gosrt's {@code congestion/live/send_test.go} has no rate-related cases, so
 * {@code SendRateEstimatorTest} is self-designed against {@code send.go}'s
 * source. Same rigor tier as the RTT, drift, wraparound and receive-rate
 * pieces, rather than the stronger ported-scenario tier. Not thread-safe, same
 * as this codebase's other per-connection send state.
 */
public final class SendRateEstimator {

    private static final long RATE_WINDOW_MICROS = 1_000_000;

    private long windowStartMicros;
    private long bytesPushedInWindow;
    private long bytesSentInWindow;
    private long bytesRetransmittedInWindow;

    private double inputBytesPerSecond;
    private double sentBytesPerSecond;
    private double sendLossRatePercent;

    /** Call for every payload the application offers, from {@link SendBuffer#push}. */
    public void onPushed(int payloadBytes) {
        bytesPushedInWindow += payloadBytes;
    }

    /**
     * Call for every packet actually handed to the wire — first sends and
     * retransmissions alike, since both consume link capacity. gosrt counts a
     * retransmission into <em>both</em> its sent and its retransmitted totals,
     * which is what makes the ratio between them meaningful.
     */
    public void onSent(int payloadBytes, boolean retransmitted) {
        bytesSentInWindow += payloadBytes;
        if (retransmitted) {
            bytesRetransmittedInWindow += payloadBytes;
        }
    }

    /**
     * Recomputes the per-second figures once the window has elapsed, then starts
     * a fresh one. Safe to call every connection tick — a no-op until a full
     * window's worth of time has passed.
     */
    public void tick(long nowMicros) {
        long elapsedMicros = nowMicros - windowStartMicros;
        if (elapsedMicros <= RATE_WINDOW_MICROS) {
            return;
        }

        double elapsedSeconds = elapsedMicros / 1_000_000.0;
        inputBytesPerSecond = bytesPushedInWindow / elapsedSeconds;
        sentBytesPerSecond = bytesSentInWindow / elapsedSeconds;
        // Guarded rather than computed-then-fixed: a window in which nothing was
        // sent has no loss rate, and reporting the previous window's would be a
        // stale figure masquerading as a current one.
        sendLossRatePercent = bytesSentInWindow == 0
                ? 0
                : (double) bytesRetransmittedInWindow / bytesSentInWindow * 100;

        bytesPushedInWindow = 0;
        bytesSentInWindow = 0;
        bytesRetransmittedInWindow = 0;
        windowStartMicros = nowMicros;
    }

    /** Bytes per second the application offered over the last completed window. */
    public int estimatedInputBytesPerSecond() {
        return (int) Math.round(inputBytesPerSecond);
    }

    /** Bytes per second actually sent over the last completed window, retransmissions included. */
    public int estimatedSentBytesPerSecond() {
        return (int) Math.round(sentBytesPerSecond);
    }

    /** Retransmitted bytes as a percentage of all bytes sent, over the last completed window. */
    public double sendLossRatePercent() {
        return sendLossRatePercent;
    }
}