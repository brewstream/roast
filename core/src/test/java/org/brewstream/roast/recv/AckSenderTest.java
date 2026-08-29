package org.brewstream.roast.recv;

import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.cif.AckCif;
import org.brewstream.roast.packet.cif.AckVariant;
import org.brewstream.roast.util.CircularNumber;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link AckSender} no longer computes the "last acknowledged" boundary
 * itself — that moved to {@link ReceiveBuffer#computeAckBoundary} (see its
 * javadoc for why: a real interop session found the old LossList-only
 * computation stayed frozen at the oldest unresolved gap, starving a real
 * peer's send buffer). These tests just pass a fixed sequence number in and
 * check the timing/variant decision, which is all this class does now.
 */
class AckSenderTest {

    private static final int RTT = 1, RTT_VAR = 2, BUF = 3, RATE = 4, CAPACITY = 5, RECV_RATE = 6;

    private static CircularNumber seq(long value) {
        return CircularNumber.of(value, SrtPacket.MAX_SEQUENCE_NUMBER);
    }

    private static Optional<AckCif> tick(AckSender sender, long nowMicros, CircularNumber lastAck) {
        return sender.tick(nowMicros, lastAck, RTT, RTT_VAR, BUF, RATE, CAPACITY, RECV_RATE);
    }

    @Test
    void noAckBeforeTheFirstIntervalElapses() {
        AckSender sender = new AckSender();

        assertThat(tick(sender, 0, seq(1))).isEmpty();
        assertThat(tick(sender, 5_000, seq(1))).isEmpty();
    }

    @Test
    void fullAckFiresOnceTheFirstIntervalElapses() {
        AckSender sender = new AckSender();

        Optional<AckCif> ack = tick(sender, 10_000, seq(100));

        assertThat(ack).isPresent();
        assertThat(ack.get().variant()).isEqualTo(AckVariant.FULL);
        assertThat(ack.get().lastAckPacketSequenceNumber()).isEqualTo(seq(100));
        assertThat(ack.get().rtt()).isEqualTo(RTT);
        assertThat(ack.get().receivingRate()).isEqualTo(RECV_RATE);
    }

    @Test
    void fullAckFiresAgainOnceTheNextIntervalElapses() {
        AckSender sender = new AckSender();
        tick(sender, 10_000, seq(1));

        assertThat(tick(sender, 15_000, seq(1))).isEmpty();
        assertThat(tick(sender, 20_000, seq(1))).isPresent();
    }

    @Test
    void lightAckFiresAfter64PacketsBeforeTheIntervalElapses() {
        AckSender sender = new AckSender();

        for (int i = 0; i < 64; i++) {
            sender.onPacketReceived();
        }

        Optional<AckCif> ack = tick(sender, 5_000, seq(65)); // well before the first 10ms full-ACK interval
        assertThat(ack).isPresent();
        assertThat(ack.get().variant()).isEqualTo(AckVariant.LITE);
        assertThat(ack.get().lastAckPacketSequenceNumber()).isEqualTo(seq(65));
    }

    @Test
    void lightAckDoesNotResetTheFullAckTimer() {
        AckSender sender = new AckSender();
        for (int i = 0; i < 64; i++) {
            sender.onPacketReceived();
        }
        tick(sender, 5_000, seq(1)); // the light ACK

        assertThat(tick(sender, 9_999, seq(1))).isEmpty();
        assertThat(tick(sender, 10_000, seq(1))).isPresent();
    }

    /**
     * Ported from gosrt's congestion/live/receive_test.go TestRecvPeriodicACKLite:
     * pushing 100 packets before the first tick, with no time elapsed yet, still
     * gets a Light ACK immediately (the 64-packet threshold overrides the interval
     * wait) — not a Full ACK, since not enough time has passed for one of those.
     */
    @Test
    void matchesGosrtTestRecvPeriodicAckLite() {
        AckSender sender = new AckSender();

        for (int i = 0; i < 100; i++) {
            sender.onPacketReceived();
        }

        Optional<AckCif> ack = tick(sender, 1, seq(100)); // gosrt's test ticks at t=1, far short of a 10ms interval
        assertThat(ack).isPresent();
        assertThat(ack.get().variant()).isEqualTo(AckVariant.LITE);
    }
}
