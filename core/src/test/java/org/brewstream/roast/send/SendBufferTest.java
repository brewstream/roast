package org.brewstream.roast.send;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.brewstream.roast.packet.DataPacket;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.util.CircularNumber;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ported directly from gosrt's congestion/live/send_test.go - real reference
 * tests exist for this piece (unlike RTT/drift/wraparound, where none did).
 * gosrt's own tests access lossList/packetList lengths directly since the
 * test file shares the production package (Go's whitebox-testing idiom) -
 * {@link SendBuffer#packetListSize()}/{@link SendBuffer#lossListSize()} are
 * the direct Java equivalent, package-private and test-support only.
 */
class SendBufferTest {

    private static final SrtSocketId DESTINATION = SrtSocketId.of(1);
    private static final long DROP_THRESHOLD = 10;

    private static CircularNumber seq(long value) {
        return CircularNumber.of(value, SrtPacket.MAX_SEQUENCE_NUMBER);
    }

    private static SendBuffer sendBuffer(java.util.function.Consumer<DataPacket> deliver) {
        return new SendBuffer(seq(0), DESTINATION, DROP_THRESHOLD, deliver);
    }

    /** Ported from gosrt's TestSendSequence. */
    @Test
    void deliversInOrderAsScheduledTimeComesDue() {
        List<Integer> delivered = new ArrayList<>();
        SendBuffer buffer = sendBuffer(p -> {
            delivered.add(p.sequenceNumber());
            p.payload().release(); // each delivery is now a duplicate the consumer owns - see SendBuffer's javadoc
        });

        for (int i = 0; i < 10; i++) {
            buffer.push(Unpooled.buffer(0), i + 1);
        }

        buffer.tick(5);
        assertThat(delivered).containsExactly(0, 1, 2, 3, 4);

        buffer.tick(10);
        assertThat(delivered).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        buffer.flush();
    }

    /** Ported from gosrt's TestSendLossListACK. */
    @Test
    void ackPrunesLossListInOrder() {
        SendBuffer buffer = sendBuffer(p -> p.payload().release());

        for (int i = 0; i < 10; i++) {
            buffer.push(Unpooled.buffer(0), i + 1);
        }
        buffer.tick(10);
        assertThat(buffer.lossListSize()).isEqualTo(10);

        for (int i = 0; i < 10; i++) {
            buffer.ack(seq(i + 1));
            assertThat(buffer.lossListSize()).isEqualTo(10 - (i + 1));
        }
    }

    /** Ported from gosrt's TestSendRetransmit. */
    @Test
    void nakRetransmitsMatchingPackets() {
        List<DataPacket> delivered = new ArrayList<>();
        SendBuffer buffer = sendBuffer(delivered::add);

        for (int i = 0; i < 10; i++) {
            buffer.push(Unpooled.buffer(0), i + 1);
        }
        buffer.tick(10);
        assertThat(retransmitCount(delivered)).isZero();

        buffer.nak(List.of(new LossRange(seq(2), seq(2))));
        assertThat(retransmitCount(delivered)).isEqualTo(1);

        buffer.nak(List.of(new LossRange(seq(5), seq(7))));
        assertThat(retransmitCount(delivered)).isEqualTo(4);

        // Every delivery - first send and retransmit alike - is a duplicate
        // the consumer owns (see SendBuffer's javadoc); the originals stay in
        // lossList and are released separately by flush().
        delivered.forEach(p -> p.payload().release());
        buffer.flush();
    }

    /**
     * Regression test for a real bug caught while wiring this into a live
     * connection: tick()'s first delivery used to hand out the same packet
     * object retained in lossList. Once a real consumer (an encoder, or here,
     * a callback that releases immediately - simulating one) actually
     * releases what it's given, a later nak() retransmit calling
     * retainedDuplicate() on that same already-released buffer would throw
     * IllegalReferenceCountException. Fixed by always duplicating on delivery.
     */
    @Test
    void retransmitAfterARealSendDoesNotReuseAnAlreadyReleasedBuffer() {
        List<DataPacket> delivered = new ArrayList<>();
        SendBuffer buffer = sendBuffer(p -> {
            delivered.add(p);
            p.payload().release(); // simulates a real encoder consuming the buffer on send
        });

        buffer.push(Unpooled.buffer(0), 1);
        buffer.tick(1);

        buffer.nak(List.of(new LossRange(seq(0), seq(0))));

        // Both deliveries were already released by the callback above (simulating
        // a real encoder) - if nak() had reused the first delivery's already-
        // released buffer instead of duplicating the still-valid original held
        // in lossList, this would have thrown IllegalReferenceCountException.
        assertThat(delivered).hasSize(2); // original send + retransmit
        buffer.flush();
    }

    private static long retransmitCount(List<DataPacket> delivered) {
        return delivered.stream().filter(DataPacket::retransmitted).count();
    }

    /** Ported from gosrt's TestSendDrop. */
    @Test
    void tickDropsStaleUnacknowledgedPackets() {
        SendBuffer buffer = sendBuffer(p -> p.payload().release());

        for (int i = 0; i < 10; i++) {
            buffer.push(Unpooled.buffer(0), i + 1);
        }
        buffer.tick(10);
        assertThat(buffer.lossListSize()).isEqualTo(10);

        buffer.tick(20);
        assertThat(buffer.lossListSize()).isZero();
    }

    /** Ported from gosrt's TestSendFlush. */
    @Test
    void flushClearsBothQueues() {
        SendBuffer buffer = sendBuffer(p -> p.payload().release());

        for (int i = 0; i < 10; i++) {
            buffer.push(Unpooled.buffer(0), i + 1);
        }
        assertThat(buffer.packetListSize()).isEqualTo(10);
        assertThat(buffer.lossListSize()).isZero();

        buffer.tick(5);
        assertThat(buffer.packetListSize()).isEqualTo(5);
        assertThat(buffer.lossListSize()).isEqualTo(5);

        buffer.flush();
        assertThat(buffer.packetListSize()).isZero();
        assertThat(buffer.lossListSize()).isZero();
    }

    /**
     * No gosrt test covers the 16th/17th-packet probe trick (checked directly -
     * send_test.go has no probe-related cases), so this is self-designed
     * against gosrt's Push source. A fresh buffer starts at seq(0), and 0 % 16
     * == 0 / 1 % 16 == 1, so the very first two pushes already form a probe
     * pair - no need to push 16 packets first.
     */
    @Test
    void seventeenthPacketRidesOutOnSixteenthPacketsSchedule() {
        List<DataPacket> delivered = new ArrayList<>();
        SendBuffer buffer = sendBuffer(delivered::add);

        buffer.push(Unpooled.buffer(0), 100); // seq 0, probe0
        buffer.push(Unpooled.buffer(0), 500); // seq 1, probe1 - naturally not due yet at t=100
        buffer.push(Unpooled.buffer(0), 200); // seq 2, not a probe packet - unaffected

        buffer.tick(100);

        // Both probe-pair packets are delivered together, even though seq 1's own
        // schedule (500) hasn't come due - it rode out on seq 0's schedule instead.
        assertThat(delivered).extracting(DataPacket::sequenceNumber).containsExactly(0, 1);
        // The wire timestamp is untouched - only the internal delivery scheduling was
        // overridden, matching gosrt's own comment on why this is safe to do in-place.
        assertThat(delivered.get(1).timestamp()).isEqualTo(500);

        buffer.tick(200);
        assertThat(delivered).extracting(DataPacket::sequenceNumber).containsExactly(0, 1, 2);

        delivered.forEach(p -> p.payload().release());
        buffer.flush();
    }

    // --- ByteBuf-leak checks (no gosrt equivalent needed - Go's GC handles
    // this; Roast needs explicit release(), so it's worth verifying directly).

    @Test
    void ackReleasesPrunedPayload() {
        ByteBuf payload = Unpooled.buffer(0);
        SendBuffer buffer = sendBuffer(p -> p.payload().release());
        buffer.push(payload, 1);
        buffer.tick(1);

        buffer.ack(seq(1));

        assertThat(payload.refCnt()).isZero();
    }

    @Test
    void tickDropReleasesStalePayload() {
        ByteBuf payload = Unpooled.buffer(0);
        SendBuffer buffer = sendBuffer(p -> p.payload().release());
        buffer.push(payload, 1);
        buffer.tick(1);

        buffer.tick(1 + DROP_THRESHOLD);

        assertThat(payload.refCnt()).isZero();
    }

    @Test
    void flushReleasesEverythingRegardlessOfState() {
        ByteBuf delivered = Unpooled.buffer(0); // will be moved to lossList
        ByteBuf pending = Unpooled.buffer(0); // stays in packetList, not yet due
        SendBuffer buffer = sendBuffer(p -> p.payload().release());
        buffer.push(delivered, 1);
        buffer.tick(1);
        buffer.push(pending, 100);

        buffer.flush();

        assertThat(delivered.refCnt()).isZero();
        assertThat(pending.refCnt()).isZero();
    }
}
