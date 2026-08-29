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
        SendBuffer buffer = sendBuffer(p -> delivered.add(p.sequenceNumber()));

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
        SendBuffer buffer = sendBuffer(p -> { });

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

        // Retransmit duplicates are separate ByteBuf instances the buffer
        // doesn't track once handed off - the test owns releasing those.
        // Originals are still held by the buffer's lossList; flush() releases them.
        delivered.stream().filter(DataPacket::retransmitted).forEach(p -> p.payload().release());
        buffer.flush();
    }

    private static long retransmitCount(List<DataPacket> delivered) {
        return delivered.stream().filter(DataPacket::retransmitted).count();
    }

    /** Ported from gosrt's TestSendDrop. */
    @Test
    void tickDropsStaleUnacknowledgedPackets() {
        SendBuffer buffer = sendBuffer(p -> { });

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
        SendBuffer buffer = sendBuffer(p -> { });

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

    // --- ByteBuf-leak checks (no gosrt equivalent needed - Go's GC handles
    // this; Roast needs explicit release(), so it's worth verifying directly).

    @Test
    void ackReleasesPrunedPayload() {
        ByteBuf payload = Unpooled.buffer(0);
        SendBuffer buffer = sendBuffer(p -> { });
        buffer.push(payload, 1);
        buffer.tick(1);

        buffer.ack(seq(1));

        assertThat(payload.refCnt()).isZero();
    }

    @Test
    void tickDropReleasesStalePayload() {
        ByteBuf payload = Unpooled.buffer(0);
        SendBuffer buffer = sendBuffer(p -> { });
        buffer.push(payload, 1);
        buffer.tick(1);

        buffer.tick(1 + DROP_THRESHOLD);

        assertThat(payload.refCnt()).isZero();
    }

    @Test
    void flushReleasesEverythingRegardlessOfState() {
        ByteBuf delivered = Unpooled.buffer(0); // will be moved to lossList
        ByteBuf pending = Unpooled.buffer(0); // stays in packetList, not yet due
        SendBuffer buffer = sendBuffer(p -> { });
        buffer.push(delivered, 1);
        buffer.tick(1);
        buffer.push(pending, 100);

        buffer.flush();

        assertThat(delivered.refCnt()).isZero();
        assertThat(pending.refCnt()).isZero();
    }
}
