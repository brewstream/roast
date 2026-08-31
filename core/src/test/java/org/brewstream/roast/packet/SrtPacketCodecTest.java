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

package org.brewstream.roast.packet;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

class SrtPacketCodecTest {

    @Test
    void dataPacketRoundTrips() {
        byte[] payload = {1, 2, 3, 4, 5};
        DataPacket original = new DataPacket(
                0x1234_5678, 0b10, true, 0b01, false, 0x03FF_FFFF,
                0x0A0B_0C0D, SrtSocketId.of(42), Unpooled.wrappedBuffer(payload));

        var buf = ByteBufAllocator.DEFAULT.buffer();
        original.encodeTo(buf);

        SrtPacket decoded = SrtPacket.decode(buf);
        assertThat(decoded).isInstanceOf(DataPacket.class);
        DataPacket data = (DataPacket) decoded;

        assertThat(data.sequenceNumber()).isEqualTo(0x1234_5678);
        assertThat(data.pp()).isEqualTo(0b10);
        assertThat(data.inOrder()).isTrue();
        assertThat(data.kk()).isEqualTo(0b01);
        assertThat(data.retransmitted()).isFalse();
        assertThat(data.messageNumber()).isEqualTo(0x03FF_FFFF);
        assertThat(data.timestamp()).isEqualTo(0x0A0B_0C0D);
        assertThat(data.destination()).isEqualTo(SrtSocketId.of(42));

        byte[] outPayload = new byte[data.body().readableBytes()];
        data.body().readBytes(outPayload);
        assertThat(outPayload).isEqualTo(payload);

        data.body().release();
        buf.release();
    }

    @ParameterizedTest
    @EnumSource(ControlType.class)
    void controlPacketRoundTrips(ControlType type) {
        byte[] cif = {9, 8, 7};
        ControlPacket original = new ControlPacket(
                type, 0x1111_2222, 0x3333_4444, SrtSocketId.of(7), Unpooled.wrappedBuffer(cif));

        var buf = ByteBufAllocator.DEFAULT.buffer();
        original.encodeTo(buf);

        SrtPacket decoded = SrtPacket.decode(buf);
        assertThat(decoded).isInstanceOf(ControlPacket.class);
        ControlPacket control = (ControlPacket) decoded;

        assertThat(control.type()).isEqualTo(type);
        assertThat(control.typeSpecificInfo()).isEqualTo(0x1111_2222);
        assertThat(control.timestamp()).isEqualTo(0x3333_4444);
        assertThat(control.destination()).isEqualTo(SrtSocketId.of(7));

        byte[] outCif = new byte[control.body().readableBytes()];
        control.body().readBytes(outCif);
        assertThat(outCif).isEqualTo(cif);

        control.body().release();
        buf.release();
    }

    @Test
    void decodeReturnsNullForShortBuffer() {
        var buf = Unpooled.wrappedBuffer(new byte[]{1, 2, 3});
        assertThat(SrtPacket.decode(buf)).isNull();
        buf.release();
    }

    @Test
    void decodeReturnsNullForUnknownControlType() {
        var buf = ByteBufAllocator.DEFAULT.buffer();
        // F=1, control type = 0x2A (unassigned), rest zero.
        buf.writeInt(0x8000_0000 | (0x2A << 16));
        buf.writeInt(0);
        buf.writeInt(0);
        buf.writeInt(0);

        assertThat(SrtPacket.decode(buf)).isNull();
        buf.release();
    }
}