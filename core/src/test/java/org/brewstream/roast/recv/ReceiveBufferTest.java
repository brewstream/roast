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

package org.brewstream.roast.recv;

import io.netty.buffer.Unpooled;
import org.brewstream.roast.packet.DataPacket;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.util.CircularNumber;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ReceiveBufferTest {

    private static CircularNumber seq(long value) {
        return CircularNumber.of(value, SrtPacket.MAX_SEQUENCE_NUMBER);
    }

    private static DataPacket dataPacket(int seq, int timestamp) {
        return new DataPacket(seq, 0, true, 0, false, 1, timestamp, SrtSocketId.of(1), Unpooled.buffer(0));
    }

    private static List<Integer> seqNumbersOf(List<DataPacket> packets) {
        return packets.stream().map(DataPacket::sequenceNumber).toList();
    }

    private static void release(List<DataPacket> packets) {
        packets.forEach(p -> p.body().release());
    }

    /** Matches production usage: compute the ACK boundary, then deliver gated by it. */
    private static DeliveryResult deliverAll(ReceiveBuffer buffer, long nowMicros) {
        AckBoundaryResult ack = buffer.computeAckBoundary(nowMicros);
        return buffer.deliver(ack.lastAckSequenceNumber(), nowMicros);
    }

    @Test
    void inOrderPacketsDueOnTimeAreDeliveredInOrder() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 0);
        buffer.add(dataPacket(0, 0), 0);
        buffer.add(dataPacket(1, 1), 0);

        AckBoundaryResult ack = buffer.computeAckBoundary(1);
        assertThat(ack.abandoned()).isEmpty();
        DeliveryResult result = buffer.deliver(ack.lastAckSequenceNumber(), 1);

        assertThat(seqNumbersOf(result.delivered())).containsExactly(0, 1);
        release(result.delivered());
    }

    @Test
    void packetNotYetDueIsHeldBack() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 5_000);
        buffer.add(dataPacket(0, 0), 0); // tsbpdTime = 0 + 0 + 5000 = 5000

        assertThat(deliverAll(buffer, 1_000).delivered()).isEmpty();

        DeliveryResult result = deliverAll(buffer, 5_000);
        assertThat(seqNumbersOf(result.delivered())).containsExactly(0);
        release(result.delivered());
    }

    @Test
    void outOfOrderArrivalIsBufferedAndDeliveredInSequenceOrder() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 0);
        // Arrival order is scrambled; the time base is set from whichever arrives first (seq 2).
        buffer.add(dataPacket(2, 2), 0); // establishes timeBase = 0 - 2 = -2
        buffer.add(dataPacket(1, 1), 0);
        buffer.add(dataPacket(0, 0), 0);

        DeliveryResult result = deliverAll(buffer, 0);

        assertThat(seqNumbersOf(result.delivered())).containsExactly(0, 1, 2);
        release(result.delivered());
    }

    @Test
    void duplicateOrBelatedArrivalAfterDeliveryIsDroppedAndPayloadReleased() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 0);
        buffer.add(dataPacket(0, 0), 0);
        release(deliverAll(buffer, 0).delivered());

        var duplicatePayload = Unpooled.buffer(0);
        DataPacket duplicate = new DataPacket(0, 0, true, 0, false, 1, 0, SrtSocketId.of(1), duplicatePayload);
        buffer.add(duplicate, 0);

        assertThat(duplicatePayload.refCnt()).isZero();
        assertThat(deliverAll(buffer, 1_000_000).delivered()).isEmpty();
    }

    /**
     * Ported from gosrt's congestion/live/receive_test.go TestSkipTooLate,
     * including the ACK-boundary assertion an earlier version of this test
     * excluded: {@link ReceiveBuffer#computeAckBoundary} now runs ahead of
     * delivery the same way gosrt's periodicACK does (see the class javadoc's
     * "ACK boundary vs. delivery boundary" section), so gosrt's own reported
     * boundary of 13 (seq 12 + 1) is reproduced exactly, not just the
     * delivery outcome.
     */
    @Test
    void matchesGosrtTestSkipTooLate() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 0);
        buffer.add(dataPacket(0, 1), 1); // establishes timeBase = 1 - 1 = 0
        buffer.add(dataPacket(1, 2), 0);
        buffer.add(dataPacket(2, 3), 0);
        buffer.add(dataPacket(3, 4), 0);
        buffer.add(dataPacket(4, 5), 0);

        AckBoundaryResult firstAck = buffer.computeAckBoundary(10);
        assertThat(firstAck.abandoned()).isEmpty();
        assertThat(firstAck.lastAckSequenceNumber()).isEqualTo(seq(4));
        DeliveryResult first = buffer.deliver(firstAck.lastAckSequenceNumber(), 10);
        assertThat(seqNumbersOf(first.delivered())).containsExactly(0, 1, 2, 3, 4);
        release(first.delivered());

        // Skip straight to seq 8-12 (5,6,7 never arrive), with deadlines far in the future.
        buffer.add(dataPacket(8, 19), 0);
        buffer.add(dataPacket(9, 20), 0);
        buffer.add(dataPacket(10, 21), 0);
        buffer.add(dataPacket(11, 22), 0);
        buffer.add(dataPacket(12, 23), 0);

        AckBoundaryResult secondAck = buffer.computeAckBoundary(20);
        assertThat(secondAck.abandoned()).containsExactly(new LossRange(seq(5), seq(7)));
        // gosrt's own "13": seq 9's deadline (20) has passed, so the walk skips the
        // [5,7] gap to reach it, then keeps advancing through 10/11/12 since they're
        // each contiguous with the last - all in this one call, ahead of delivery.
        assertThat(secondAck.lastAckSequenceNumber()).isEqualTo(seq(12));
        DeliveryResult second = buffer.deliver(secondAck.lastAckSequenceNumber(), 20);

        // Delivery still stops at seq 9 - seq 10's own deadline (21) hasn't passed yet.
        assertThat(seqNumbersOf(second.delivered())).containsExactly(8, 9);
        release(second.delivered());
    }

    /**
     * Ported from gosrt's TestIssue67 - a real historical gosrt bug fix for
     * exactly this failure mode (an ACK boundary that stays frozen behind a
     * gap even once a later, non-contiguous packet's own deadline has
     * passed). This is the identical category of bug a real interop session
     * against libsrt hit independently, before this test existed here.
     */
    @Test
    void matchesGosrtTestIssue67() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 0);
        buffer.add(dataPacket(0, 1), 1); // establishes timeBase = 1 - 1 = 0

        // Nothing new arrives for a while - the boundary just sits at 0.
        for (long now = 10; now <= 90; now += 10) {
            assertThat(buffer.computeAckBoundary(now).lastAckSequenceNumber()).isEqualTo(seq(0));
        }

        buffer.add(dataPacket(12, 121), 0); // opens a big gap; not yet due (deadline 121)
        buffer.add(dataPacket(1, 11), 0); // contiguous with seq 0
        buffer.add(dataPacket(11, 111), 0); // still gapped from seq 1, not yet due at t=100/110

        assertThat(buffer.computeAckBoundary(100).lastAckSequenceNumber()).isEqualTo(seq(1));
        assertThat(buffer.computeAckBoundary(110).lastAckSequenceNumber()).isEqualTo(seq(1));

        // seq 11's deadline (111) has now passed - skip the [2,10] gap to reach it,
        // then keep going since seq 12 is contiguous with it. One call, past the gap.
        AckBoundaryResult ack = buffer.computeAckBoundary(120);
        assertThat(ack.abandoned()).containsExactly(new LossRange(seq(2), seq(10)));
        assertThat(ack.lastAckSequenceNumber()).isEqualTo(seq(12));

        // Idempotent - nothing new to process, the boundary just stays put.
        assertThat(buffer.computeAckBoundary(130).lastAckSequenceNumber()).isEqualTo(seq(12));

        release(buffer.deliver(seq(12), 120).delivered());
    }

    /**
     * Inspired by gosrt's TestRecvDropTooLate: in a clean, gap-free run, the
     * ACK boundary and the delivery boundary converge to the same point -
     * the two-boundary design doesn't diverge unless there's actually a gap
     * to skip past.
     */
    @Test
    void ackAndDeliveryBoundariesConvergeWithNoGap() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 0);
        for (int i = 0; i < 10; i++) {
            buffer.add(dataPacket(i, i + 1), 0);
        }

        AckBoundaryResult ack = buffer.computeAckBoundary(10);
        assertThat(ack.abandoned()).isEmpty();
        assertThat(ack.lastAckSequenceNumber()).isEqualTo(seq(9));
        DeliveryResult result = buffer.deliver(ack.lastAckSequenceNumber(), 10);
        assertThat(seqNumbersOf(result.delivered())).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
        release(result.delivered());

        // A late re-arrival of an already-delivered sequence number is dropped.
        var latePayload = Unpooled.buffer(0);
        buffer.add(new DataPacket(3, 0, true, 0, false, 1, 4, SrtSocketId.of(1), latePayload), 0);
        assertThat(latePayload.refCnt()).isZero();
    }

    @Test
    void driftSamplesBeforeTimeBaseEstablishedAreNoOps() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 10_000);

        for (int i = 0; i < DriftTracer.MAX_SAMPLES; i++) {
            buffer.addDriftSample(0, 8_000, 100_000);
        }

        buffer.add(dataPacket(0, 0), 0); // establishes timeBase = 0 - 0 = 0

        // If the pre-timeBase samples above had silently accumulated, the
        // completed span would already have banked an overdrift and shifted
        // this deadline past 10_000 (latency alone).
        assertThat(deliverAll(buffer, 9_999).delivered()).isEmpty();
        DeliveryResult result = deliverAll(buffer, 10_000);
        assertThat(seqNumbersOf(result.delivered())).containsExactly(0);
        release(result.delivered());
    }

    @Test
    void overdriftShiftsAlreadyBufferedPacketsEffectiveDeadline() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 10_000);
        buffer.add(dataPacket(0, 0), 0); // establishes timeBase = 0 - 0 = 0; deadline = 10_000 pre-drift

        assertThat(deliverAll(buffer, 9_999).delivered()).isEmpty();

        // A consistent 8_000us drift sample (same RTT every time, so the
        // RTT-delta term stays 0) exceeds the 5_000us clamp once the span
        // completes, banking a 5_000us overdrift into the time base and
        // leaving a running drift() of 3_000us - shifting this already-
        // buffered packet's deadline from 10_000 to 18_000.
        for (int i = 0; i < DriftTracer.MAX_SAMPLES; i++) {
            buffer.addDriftSample(0, 8_000, 100_000);
        }

        assertThat(deliverAll(buffer, 17_999).delivered()).isEmpty();
        DeliveryResult result = deliverAll(buffer, 18_000);
        assertThat(seqNumbersOf(result.delivered())).containsExactly(0);
        release(result.delivered());
    }

    // --- 32-bit wire-timestamp wraparound (no reference test exists in gosrt
    // or libsrt to port - self-designed against both sources' source code).
    // All timestamps below are unsigned
    // 32-bit microsecond values; ts(long) reinterprets one as the signed int
    // DataPacket.timestamp() actually stores on the wire.

    private static final long WRAP_PERIOD_MICROS = 30_000_000L;
    private static final long JUST_PAST_WRAP_THRESHOLD = SrtPacket.MAX_TIMESTAMP - WRAP_PERIOD_MICROS + 1;

    private static int ts(long unsignedValue) {
        return (int) unsignedValue;
    }

    @Test
    void packetPastWrapThresholdEntersWrapPeriodButOwnDeadlineIsUnaffected() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 0);
        // Establishes timeBase = 0; this packet's own timestamp is nowhere near
        // small enough for carryover (its unsigned value is > 2*WRAP_PERIOD),
        // so entering the wrap period doesn't touch its own deadline.
        buffer.add(dataPacket(0, ts(JUST_PAST_WRAP_THRESHOLD)), JUST_PAST_WRAP_THRESHOLD);

        assertThat(deliverAll(buffer, JUST_PAST_WRAP_THRESHOLD - 1).delivered()).isEmpty();
        DeliveryResult result = deliverAll(buffer, JUST_PAST_WRAP_THRESHOLD);
        assertThat(seqNumbersOf(result.delivered())).containsExactly(0);
        release(result.delivered());
    }

    @Test
    void smallTimestampDuringSuspectedWrapGetsProvisionalCarryover() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 0);
        buffer.add(dataPacket(0, ts(JUST_PAST_WRAP_THRESHOLD)), JUST_PAST_WRAP_THRESHOLD);
        release(deliverAll(buffer, JUST_PAST_WRAP_THRESHOLD).delivered()); // out of the way; wrap period stays entered

        buffer.add(dataPacket(1, 5_000_000), 0); // small ts, still within the wrap-suspect window (<= 60s)

        long expectedDeadline = (SrtPacket.MAX_TIMESTAMP + 1) + 5_000_000; // provisional carryover applied
        assertThat(deliverAll(buffer, expectedDeadline - 1).delivered()).isEmpty();
        DeliveryResult result = deliverAll(buffer, expectedDeadline);
        assertThat(seqNumbersOf(result.delivered())).containsExactly(1);
        release(result.delivered());
    }

    @Test
    void wrapConfirmationCommitsOffsetConsistentlyForNewPackets() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 0);
        buffer.add(dataPacket(0, ts(JUST_PAST_WRAP_THRESHOLD)), JUST_PAST_WRAP_THRESHOLD);
        release(deliverAll(buffer, JUST_PAST_WRAP_THRESHOLD).delivered());

        buffer.add(dataPacket(1, 45_000_000), 0); // lands in [30s, 60s] - confirms and commits the wrap

        long expectedDeadline = (SrtPacket.MAX_TIMESTAMP + 1) + 45_000_000; // timeBase now includes the cycle
        assertThat(deliverAll(buffer, expectedDeadline - 1).delivered()).isEmpty();
        DeliveryResult result = deliverAll(buffer, expectedDeadline);
        assertThat(seqNumbersOf(result.delivered())).containsExactly(1);
        release(result.delivered());
    }

    /**
     * The key correctness property: a packet buffered while the wrap was only
     * suspected (provisional carryover) must land on the exact same deadline
     * once a later packet confirms and permanently commits the wrap - no
     * discontinuity from the provisional-to-committed transition.
     */
    @Test
    void bufferedPacketDeadlineIsUnchangedAcrossWrapConfirmation() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 0);
        buffer.add(dataPacket(0, ts(JUST_PAST_WRAP_THRESHOLD)), JUST_PAST_WRAP_THRESHOLD);
        release(deliverAll(buffer, JUST_PAST_WRAP_THRESHOLD).delivered());

        buffer.add(dataPacket(1, 5_000_000), 0); // provisional carryover, not yet confirmed
        assertThat(deliverAll(buffer, 0).delivered()).isEmpty();

        buffer.add(dataPacket(2, 45_000_000), 0); // confirms the wrap, commits the offset

        long expectedSeq1Deadline = (SrtPacket.MAX_TIMESTAMP + 1) + 5_000_000; // unchanged from before confirmation
        assertThat(deliverAll(buffer, expectedSeq1Deadline - 1).delivered()).isEmpty();
        DeliveryResult result = deliverAll(buffer, expectedSeq1Deadline);
        assertThat(seqNumbersOf(result.delivered())).containsExactly(1); // seq 2 not due yet, stays buffered
        release(result.delivered());
    }
}