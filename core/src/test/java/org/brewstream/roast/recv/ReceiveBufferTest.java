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
}
