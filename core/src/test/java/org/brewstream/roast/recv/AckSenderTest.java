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
    void firstTickAlwaysReturnsAFullAck() {
        LossList lossList = new LossList(seq(100));
        AckSender sender = new AckSender(lossList);

        Optional<AckCif> ack = tick(sender, 0);

        assertThat(ack).isPresent();
        assertThat(ack.get().variant()).isEqualTo(AckVariant.FULL);
        assertThat(ack.get().lastAckPacketSequenceNumber()).isEqualTo(seq(100));
        assertThat(ack.get().rtt()).isEqualTo(RTT);
        assertThat(ack.get().receivingRate()).isEqualTo(RECV_RATE);
    }

    @Test
    void secondTickWithinIntervalAndFewPacketsReturnsEmpty() {
        LossList lossList = new LossList(seq(1));
        AckSender sender = new AckSender(lossList);
        tick(sender, 0);

        assertThat(tick(sender, 5_000)).isEmpty();
    }

    @Test
    void fullAckFiresAgainOnceTheIntervalElapses() {
        LossList lossList = new LossList(seq(1));
        AckSender sender = new AckSender(lossList);
        tick(sender, 0);

        Optional<AckCif> ack = tick(sender, 10_000);

        assertThat(ack).isPresent();
        assertThat(ack.get().variant()).isEqualTo(AckVariant.FULL);
    }

    @Test
    void lightAckFiresAfter64PacketsWithinTheInterval() {
        LossList lossList = new LossList(seq(1));
        AckSender sender = new AckSender(lossList);
        tick(sender, 0); // consumes the always-due first full ACK

        for (long i = 1; i <= 64; i++) {
            lossList.onPacketReceived(seq(i));
            sender.onPacketReceived();
        }

        Optional<AckCif> ack = tick(sender, 5_000);

        assertThat(ack).isPresent();
        assertThat(ack.get().variant()).isEqualTo(AckVariant.LITE);
        assertThat(ack.get().lastAckPacketSequenceNumber()).isEqualTo(seq(65));
    }

    @Test
    void lightAckDoesNotResetTheFullAckTimer() {
        LossList lossList = new LossList(seq(1));
        AckSender sender = new AckSender(lossList);
        tick(sender, 0);
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

        Optional<AckCif> ack = tick(sender, 0);

        assertThat(ack.get().lastAckPacketSequenceNumber()).isEqualTo(seq(6));
    }
}
