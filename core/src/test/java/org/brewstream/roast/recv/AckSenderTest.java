package org.brewstream.roast.recv;

import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.cif.AckCif;
import org.brewstream.roast.packet.cif.AckVariant;
import org.brewstream.roast.util.CircularNumber;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class AckSenderTest {

    private static final int RTT = 1, RTT_VAR = 2, BUF = 3, RATE = 4, CAPACITY = 5, RECV_RATE = 6;

    private static CircularNumber seq(long value) {
        return CircularNumber.of(value, SrtPacket.MAX_SEQUENCE_NUMBER);
    }

    private static Optional<AckCif> tick(AckSender sender, long nowMicros) {
        return sender.tick(nowMicros, RTT, RTT_VAR, BUF, RATE, CAPACITY, RECV_RATE);
    }

    @Test
    void noAckBeforeTheFirstIntervalElapses() {
        LossList lossList = new LossList(seq(1));
        AckSender sender = new AckSender(lossList);

        assertThat(tick(sender, 0)).isEmpty();
        assertThat(tick(sender, 5_000)).isEmpty();
    }

    @Test
    void fullAckFiresOnceTheFirstIntervalElapses() {
        LossList lossList = new LossList(seq(100));
        AckSender sender = new AckSender(lossList);

        Optional<AckCif> ack = tick(sender, 10_000);

        assertThat(ack).isPresent();
        assertThat(ack.get().variant()).isEqualTo(AckVariant.FULL);
        assertThat(ack.get().lastAckPacketSequenceNumber()).isEqualTo(seq(100));
        assertThat(ack.get().rtt()).isEqualTo(RTT);
        assertThat(ack.get().receivingRate()).isEqualTo(RECV_RATE);
    }

    @Test
    void fullAckFiresAgainOnceTheNextIntervalElapses() {
        LossList lossList = new LossList(seq(1));
        AckSender sender = new AckSender(lossList);
        tick(sender, 10_000);

        assertThat(tick(sender, 15_000)).isEmpty();
        assertThat(tick(sender, 20_000)).isPresent();
    }

    @Test
    void lightAckFiresAfter64PacketsBeforeTheIntervalElapses() {
        LossList lossList = new LossList(seq(1));
        AckSender sender = new AckSender(lossList);

        for (long i = 1; i <= 64; i++) {
            lossList.onPacketReceived(seq(i));
            sender.onPacketReceived();
        }

        Optional<AckCif> ack = tick(sender, 5_000); // well before the first 10ms full-ACK interval
        assertThat(ack).isPresent();
        assertThat(ack.get().variant()).isEqualTo(AckVariant.LITE);
        assertThat(ack.get().lastAckPacketSequenceNumber()).isEqualTo(seq(65));
    }

    @Test
    void lightAckDoesNotResetTheFullAckTimer() {
        LossList lossList = new LossList(seq(1));
        AckSender sender = new AckSender(lossList);
        for (int i = 0; i < 64; i++) {
            sender.onPacketReceived();
        }
        tick(sender, 5_000); // the light ACK

        assertThat(tick(sender, 9_999)).isEmpty();
        assertThat(tick(sender, 10_000)).isPresent();
    }

    @Test
    void reportsTheFirstMissingSequenceNumberWhenThereIsAGap() {
        LossList lossList = new LossList(seq(5));
        lossList.onPacketReceived(seq(5));
        lossList.onPacketReceived(seq(10)); // opens gap [6,9]
        AckSender sender = new AckSender(lossList);

        Optional<AckCif> ack = tick(sender, 10_000);

        assertThat(ack.get().lastAckPacketSequenceNumber()).isEqualTo(seq(6));
    }

    /**
     * Ported from gosrt's congestion/live/receive_test.go TestRecvPeriodicACKLite:
     * pushing 100 packets before the first tick, with no time elapsed yet, still
     * gets a Light ACK immediately (the 64-packet threshold overrides the interval
     * wait) — not a Full ACK, since not enough time has passed for one of those.
     */
    @Test
    void matchesGosrtTestRecvPeriodicAckLite() {
        LossList lossList = new LossList(seq(0));
        AckSender sender = new AckSender(lossList);

        for (long i = 0; i < 100; i++) {
            lossList.onPacketReceived(seq(i));
            sender.onPacketReceived();
        }

        Optional<AckCif> ack = tick(sender, 1); // gosrt's test ticks at t=1, far short of a 10ms interval
        assertThat(ack).isPresent();
        assertThat(ack.get().variant()).isEqualTo(AckVariant.LITE);
    }

    /**
     * Ported from gosrt's TestRecvPeriodicNAK, the ACK-side assertions only (the
     * NAK-side assertions are ported into LossListTest). In that test none of the
     * packets' TSBPD delivery deadlines have passed by the ticks under test, so
     * gosrt's receiver reports the ACK boundary pinned to the first missing
     * sequence number (5) across repeated ticks — exactly what this class always
     * does, since it has no TSBPD-deadline-driven skip-ahead at all (that's
     * TLPKTDROP, a separate not-yet-built mechanism — see LossListTest for where
     * this stops matching gosrt once a deadline-driven skip is involved).
     */
    @Test
    void matchesGosrtTestRecvPeriodicNakAckBoundary() {
        LossList lossList = new LossList(seq(0));
        for (long i = 0; i <= 4; i++) {
            lossList.onPacketReceived(seq(i));
        }
        for (long i = 7; i <= 9; i++) {
            lossList.onPacketReceived(seq(i)); // opens gap [5,6]
        }
        AckSender sender = new AckSender(lossList);

        assertThat(tick(sender, 10_000).get().lastAckPacketSequenceNumber()).isEqualTo(seq(5));
        assertThat(tick(sender, 20_000).get().lastAckPacketSequenceNumber()).isEqualTo(seq(5));
    }
}
