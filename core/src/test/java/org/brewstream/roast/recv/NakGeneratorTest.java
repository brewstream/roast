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