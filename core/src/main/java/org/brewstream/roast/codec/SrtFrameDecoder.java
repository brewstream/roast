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

package org.brewstream.roast.codec;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.DefaultAddressedEnvelope;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.brewstream.roast.packet.SrtPacket;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Decodes inbound Netty {@link DatagramPacket}s into {@link SrtPacket}s, wrapped
 * with their sender address so downstream handlers (socket-ID demux, handshake)
 * know who sent them.
 */
public final class SrtFrameDecoder extends MessageToMessageDecoder<DatagramPacket> {

    private static final Logger LOG = Logger.getLogger(SrtFrameDecoder.class.getName());

    @Override
    protected void decode(ChannelHandlerContext ctx, DatagramPacket msg, List<Object> out) {
        SrtPacket packet = SrtPacket.decode(msg.content());
        if (packet == null) {
            LOG.log(Level.FINE, "Dropping malformed/undersized datagram from {0}", msg.sender());
            return;
        }
        out.add(new DefaultAddressedEnvelope<>(packet, (InetSocketAddress) msg.recipient(),
                (InetSocketAddress) msg.sender()));
    }
}