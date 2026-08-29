package org.brewstream.roast.recv;

import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.util.CircularNumber;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class LossListTest {

    private static CircularNumber seq(long value) {
        return CircularNumber.of(value, SrtPacket.MAX_SEQUENCE_NUMBER);
    }

    @Test
    void inOrderArrivalsReportNoLoss() {
        LossList lossList = new LossList(seq(1));

        assertThat(lossList.onPacketReceived(seq(1))).isEmpty();
        assertThat(lossList.onPacketReceived(seq(2))).isEmpty();
        assertThat(lossList.onPacketReceived(seq(3))).isEmpty();
        assertThat(lossList.outstanding()).isEmpty();
    }

    @Test
    void gapOpensAnImmediateNakAndTracksItAsOutstanding() {
        LossList lossList = new LossList(seq(5));
        lossList.onPacketReceived(seq(5));

        List<LossRange> immediate = lossList.onPacketReceived(seq(10));

        assertThat(immediate).containsExactly(new LossRange(seq(6), seq(9)));
        assertThat(lossList.outstanding()).containsExactly(new LossRange(seq(6), seq(9)));
    }

    @Test
    void firstPacketMatchingInitialSequenceNumberReportsNoLoss() {
        LossList lossList = new LossList(seq(100));

        assertThat(lossList.onPacketReceived(seq(100))).isEmpty();
        assertThat(lossList.outstanding()).isEmpty();
    }

    @Test
    void fullyRecoveringAGapClearsIt() {
        LossList lossList = new LossList(seq(5));
        lossList.onPacketReceived(seq(5));
        lossList.onPacketReceived(seq(10)); // opens gap [6,9]

        assertThat(lossList.onPacketReceived(seq(6))).isEmpty();
        assertThat(lossList.onPacketReceived(seq(7))).isEmpty();
        assertThat(lossList.onPacketReceived(seq(8))).isEmpty();
        assertThat(lossList.onPacketReceived(seq(9))).isEmpty();

        assertThat(lossList.outstanding()).isEmpty();
    }

    @Test
    void recoveringTheFrontOfAGapShrinksItFromTheStart() {
        LossList lossList = new LossList(seq(5));
        lossList.onPacketReceived(seq(5));
        lossList.onPacketReceived(seq(10)); // gap [6,9]

        lossList.onPacketReceived(seq(6));

        assertThat(lossList.outstanding()).containsExactly(new LossRange(seq(7), seq(9)));
    }

    @Test
    void recoveringTheEndOfAGapShrinksItFromTheEnd() {
        LossList lossList = new LossList(seq(5));
        lossList.onPacketReceived(seq(5));
        lossList.onPacketReceived(seq(10)); // gap [6,9]

        lossList.onPacketReceived(seq(9));

        assertThat(lossList.outstanding()).containsExactly(new LossRange(seq(6), seq(8)));
    }

    @Test
    void recoveringTheMiddleOfAGapSplitsItInTwo() {
        LossList lossList = new LossList(seq(5));
        lossList.onPacketReceived(seq(5));
        lossList.onPacketReceived(seq(10)); // gap [6,9]

        lossList.onPacketReceived(seq(7));

        assertThat(lossList.outstanding()).containsExactly(
                new LossRange(seq(6), seq(6)),
                new LossRange(seq(8), seq(9)));
    }

    @Test
    void recoveringASingleMissingPacketRemovesItEntirely() {
        LossList lossList = new LossList(seq(5));
        lossList.onPacketReceived(seq(5));
        lossList.onPacketReceived(seq(7)); // gap [6,6]

        lossList.onPacketReceived(seq(6));

        assertThat(lossList.outstanding()).isEmpty();
    }

    @Test
    void duplicateOfAnAlreadyInOrderPacketIsIgnored() {
        LossList lossList = new LossList(seq(1));
        lossList.onPacketReceived(seq(1));
        lossList.onPacketReceived(seq(2));

        assertThat(lossList.onPacketReceived(seq(1))).isEmpty();
        assertThat(lossList.outstanding()).isEmpty();
    }

    @Test
    void duplicateOfAnAlreadyRecoveredPacketIsIgnored() {
        LossList lossList = new LossList(seq(5));
        lossList.onPacketReceived(seq(5));
        lossList.onPacketReceived(seq(10)); // gap [6,9]
        lossList.onPacketReceived(seq(7)); // splits into [6,6] and [8,9]

        List<LossRange> result = lossList.onPacketReceived(seq(7)); // duplicate of the recovered one

        assertThat(result).isEmpty();
        assertThat(lossList.outstanding()).containsExactly(
                new LossRange(seq(6), seq(6)),
                new LossRange(seq(8), seq(9)));
    }

    @Test
    void separateGapsStayIndependentAfterTheFirstIsFullyRecovered() {
        LossList lossList = new LossList(seq(5));
        lossList.onPacketReceived(seq(5));
        lossList.onPacketReceived(seq(10)); // gap [6,9]
        for (long i = 6; i <= 9; i++) {
            lossList.onPacketReceived(seq(i));
        }
        assertThat(lossList.outstanding()).isEmpty();

        lossList.onPacketReceived(seq(20)); // new gap [11,19]

        assertThat(lossList.outstanding()).containsExactly(new LossRange(seq(11), seq(19)));
    }

    @Test
    void gapSpanningTheSequenceNumberWrapIsHandledCorrectly() {
        long max = SrtPacket.MAX_SEQUENCE_NUMBER;
        LossList lossList = new LossList(seq(max - 2));
        lossList.onPacketReceived(seq(max - 2));
        lossList.onPacketReceived(seq(max - 1));
        // seq(max) then wraps to 0, 1, 2 - simulate a gap spanning the wrap: skip straight to 2.

        List<LossRange> immediate = lossList.onPacketReceived(seq(2));

        assertThat(immediate).containsExactly(new LossRange(seq(max), seq(1)));
        assertThat(lossList.outstanding()).containsExactly(new LossRange(seq(max), seq(1)));

        lossList.onPacketReceived(seq(max));
        lossList.onPacketReceived(seq(0));
        lossList.onPacketReceived(seq(1));

        assertThat(lossList.outstanding()).isEmpty();
    }
}
