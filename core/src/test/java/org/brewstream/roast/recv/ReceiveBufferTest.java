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

    @Test
    void inOrderPacketsDueOnTimeAreDeliveredInOrder() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 0);
        buffer.add(dataPacket(0, 0), 0);
        buffer.add(dataPacket(1, 1), 0);

        DeliveryResult result = buffer.deliver(1);

        assertThat(seqNumbersOf(result.delivered())).containsExactly(0, 1);
        assertThat(result.abandoned()).isEmpty();
        release(result.delivered());
    }

    @Test
    void packetNotYetDueIsHeldBack() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 5_000);
        buffer.add(dataPacket(0, 0), 0); // tsbpdTime = 0 + 0 + 5000 = 5000

        assertThat(buffer.deliver(1_000).delivered()).isEmpty();

        DeliveryResult result = buffer.deliver(5_000);
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

        DeliveryResult result = buffer.deliver(0);

        assertThat(seqNumbersOf(result.delivered())).containsExactly(0, 1, 2);
        release(result.delivered());
    }

    @Test
    void duplicateOrBelatedArrivalAfterDeliveryIsDroppedAndPayloadReleased() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 0);
        buffer.add(dataPacket(0, 0), 0);
        release(buffer.deliver(0).delivered());

        var duplicatePayload = Unpooled.buffer(0);
        DataPacket duplicate = new DataPacket(0, 0, true, 0, false, 1, 0, SrtSocketId.of(1), duplicatePayload);
        buffer.add(duplicate, 0);

        assertThat(duplicatePayload.refCnt()).isZero();
        assertThat(buffer.deliver(1_000_000).delivered()).isEmpty();
    }

    /**
     * Ported from gosrt's congestion/live/receive_test.go TestSkipTooLate, the
     * delivery-sequence assertions only. gosrt's test also asserts an ACK
     * boundary of 13 after the second tick, but that value depends on gosrt's
     * ACK boundary running ahead of delivery for still-buffered in-order packets
     * (seq 10, 11 there aren't due for delivery yet but are counted as
     * "acknowledged" anyway) - a distinction this class deliberately doesn't
     * make (see its class-level javadoc). The delivery outcome - which packets
     * actually get handed over, and which gap gets abandoned - matches exactly.
     */
    @Test
    void matchesGosrtTestSkipTooLateDeliveryOutcome() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 0);
        buffer.add(dataPacket(0, 1), 1); // establishes timeBase = 1 - 1 = 0
        buffer.add(dataPacket(1, 2), 0);
        buffer.add(dataPacket(2, 3), 0);
        buffer.add(dataPacket(3, 4), 0);
        buffer.add(dataPacket(4, 5), 0);

        DeliveryResult first = buffer.deliver(10);
        assertThat(seqNumbersOf(first.delivered())).containsExactly(0, 1, 2, 3, 4);
        assertThat(first.abandoned()).isEmpty();
        release(first.delivered());

        // Skip straight to seq 8-12 (5,6,7 never arrive), with deadlines far in the future.
        buffer.add(dataPacket(8, 19), 0);
        buffer.add(dataPacket(9, 20), 0);
        buffer.add(dataPacket(10, 21), 0);
        buffer.add(dataPacket(11, 22), 0);
        buffer.add(dataPacket(12, 23), 0);

        DeliveryResult second = buffer.deliver(20);

        assertThat(seqNumbersOf(second.delivered())).containsExactly(8, 9);
        assertThat(second.abandoned()).containsExactly(new LossRange(seq(5), seq(7)));
        release(second.delivered());
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
        assertThat(buffer.deliver(9_999).delivered()).isEmpty();
        DeliveryResult result = buffer.deliver(10_000);
        assertThat(seqNumbersOf(result.delivered())).containsExactly(0);
        release(result.delivered());
    }

    @Test
    void overdriftShiftsAlreadyBufferedPacketsEffectiveDeadline() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 10_000);
        buffer.add(dataPacket(0, 0), 0); // establishes timeBase = 0 - 0 = 0; deadline = 10_000 pre-drift

        assertThat(buffer.deliver(9_999).delivered()).isEmpty();

        // A consistent 8_000us drift sample (same RTT every time, so the
        // RTT-delta term stays 0) exceeds the 5_000us clamp once the span
        // completes, banking a 5_000us overdrift into the time base and
        // leaving a running drift() of 3_000us - shifting this already-
        // buffered packet's deadline from 10_000 to 18_000.
        for (int i = 0; i < DriftTracer.MAX_SAMPLES; i++) {
            buffer.addDriftSample(0, 8_000, 100_000);
        }

        assertThat(buffer.deliver(17_999).delivered()).isEmpty();
        DeliveryResult result = buffer.deliver(18_000);
        assertThat(seqNumbersOf(result.delivered())).containsExactly(0);
        release(result.delivered());
    }

    // --- 32-bit wire-timestamp wraparound (no reference test exists in gosrt
    // or libsrt to port - self-designed against both sources' source code;
    // see STATUS.md's testing methodology). All timestamps below are unsigned
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

        assertThat(buffer.deliver(JUST_PAST_WRAP_THRESHOLD - 1).delivered()).isEmpty();
        DeliveryResult result = buffer.deliver(JUST_PAST_WRAP_THRESHOLD);
        assertThat(seqNumbersOf(result.delivered())).containsExactly(0);
        release(result.delivered());
    }

    @Test
    void smallTimestampDuringSuspectedWrapGetsProvisionalCarryover() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 0);
        buffer.add(dataPacket(0, ts(JUST_PAST_WRAP_THRESHOLD)), JUST_PAST_WRAP_THRESHOLD);
        release(buffer.deliver(JUST_PAST_WRAP_THRESHOLD).delivered()); // out of the way; wrap period stays entered

        buffer.add(dataPacket(1, 5_000_000), 0); // small ts, still within the wrap-suspect window (<= 60s)

        long expectedDeadline = (SrtPacket.MAX_TIMESTAMP + 1) + 5_000_000; // provisional carryover applied
        assertThat(buffer.deliver(expectedDeadline - 1).delivered()).isEmpty();
        DeliveryResult result = buffer.deliver(expectedDeadline);
        assertThat(seqNumbersOf(result.delivered())).containsExactly(1);
        release(result.delivered());
    }

    @Test
    void wrapConfirmationCommitsOffsetConsistentlyForNewPackets() {
        ReceiveBuffer buffer = new ReceiveBuffer(seq(0), 0);
        buffer.add(dataPacket(0, ts(JUST_PAST_WRAP_THRESHOLD)), JUST_PAST_WRAP_THRESHOLD);
        release(buffer.deliver(JUST_PAST_WRAP_THRESHOLD).delivered());

        buffer.add(dataPacket(1, 45_000_000), 0); // lands in [30s, 60s] - confirms and commits the wrap

        long expectedDeadline = (SrtPacket.MAX_TIMESTAMP + 1) + 45_000_000; // timeBase now includes the cycle
        assertThat(buffer.deliver(expectedDeadline - 1).delivered()).isEmpty();
        DeliveryResult result = buffer.deliver(expectedDeadline);
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
        release(buffer.deliver(JUST_PAST_WRAP_THRESHOLD).delivered());

        buffer.add(dataPacket(1, 5_000_000), 0); // provisional carryover, not yet confirmed
        assertThat(buffer.deliver(0).delivered()).isEmpty();

        buffer.add(dataPacket(2, 45_000_000), 0); // confirms the wrap, commits the offset

        long expectedSeq1Deadline = (SrtPacket.MAX_TIMESTAMP + 1) + 5_000_000; // unchanged from before confirmation
        assertThat(buffer.deliver(expectedSeq1Deadline - 1).delivered()).isEmpty();
        DeliveryResult result = buffer.deliver(expectedSeq1Deadline);
        assertThat(seqNumbersOf(result.delivered())).containsExactly(1); // seq 2 not due yet, stays buffered
        release(result.delivered());
    }
}
