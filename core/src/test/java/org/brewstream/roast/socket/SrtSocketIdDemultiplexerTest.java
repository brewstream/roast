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

package org.brewstream.roast.socket;

import io.netty.buffer.Unpooled;
import io.netty.channel.AddressedEnvelope;
import io.netty.channel.DefaultAddressedEnvelope;
import io.netty.channel.embedded.EmbeddedChannel;
import org.brewstream.roast.packet.ControlPacket;
import org.brewstream.roast.packet.ControlType;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.net.InetSocketAddress;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class SrtSocketIdDemultiplexerTest {

    private static final InetSocketAddress PEER = new InetSocketAddress("127.0.0.1", 9000);

    private static AddressedEnvelope<SrtPacket, InetSocketAddress> envelopeFor(SrtSocketId destination) {
        ControlPacket packet = new ControlPacket(ControlType.KEEPALIVE, 0, 0, destination,
                Unpooled.wrappedBuffer(new byte[0]));
        return new DefaultAddressedEnvelope<>(packet, null, PEER);
    }

    @Test
    void routesToRegisteredConnection() {
        SrtSocketIdDemultiplexer demux = new SrtSocketIdDemultiplexer();
        SrtPacketSink sink = mock(SrtPacketSink.class);
        SrtSocketId id = SrtSocketId.of(123);
        demux.register(id, sink);

        EmbeddedChannel channel = new EmbeddedChannel(demux);
        var envelope = envelopeFor(id);
        channel.writeInbound(envelope);

        verify(sink).onPacket(envelope);
        channel.finishAndReleaseAll();
    }

    @Test
    void routesSocketIdZeroToAcceptor() {
        SrtSocketIdDemultiplexer demux = new SrtSocketIdDemultiplexer();
        SrtPacketSink acceptor = mock(SrtPacketSink.class);
        demux.setAcceptor(acceptor);

        EmbeddedChannel channel = new EmbeddedChannel(demux);
        var envelope = envelopeFor(SrtSocketId.ZERO);
        channel.writeInbound(envelope);

        verify(acceptor).onPacket(envelope);
        channel.finishAndReleaseAll();
    }

    @Test
    void dropsPacketForUnregisteredSocketId() {
        SrtSocketIdDemultiplexer demux = new SrtSocketIdDemultiplexer();
        SrtPacketSink sink = mock(SrtPacketSink.class);
        demux.register(SrtSocketId.of(1), sink);

        EmbeddedChannel channel = new EmbeddedChannel(demux);
        channel.writeInbound(envelopeFor(SrtSocketId.of(999)));

        verifyNoInteractions(sink);
        channel.finishAndReleaseAll();
    }
}