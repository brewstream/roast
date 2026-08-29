package org.brewstream.roast.recv;

import org.brewstream.roast.packet.ControlPacket;
import org.brewstream.roast.packet.ControlType;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.packet.cif.LossListCodec;
import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.util.CircularNumber;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class NakGeneratorTest {

    private static CircularNumber seq(long value) {
        return CircularNumber.of(value, SrtPacket.MAX_SEQUENCE_NUMBER);
    }

    @Test
    void buildsAWireReadyNakControlPacket() {
        List<LossRange> lost = List.of(LossRange.single(seq(42)), new LossRange(seq(45), seq(49)));
        SrtSocketId destination = SrtSocketId.of(0x1234);

        ControlPacket packet = NakGenerator.build(lost, 999, destination);

        assertThat(packet.type()).isEqualTo(ControlType.NAK);
        assertThat(packet.timestamp()).isEqualTo(999);
        assertThat(packet.destination()).isEqualTo(destination);

        List<LossRange> decoded = LossListCodec.decode(packet.body());
        assertThat(decoded).isEqualTo(lost);
        packet.body().release();
    }
}
