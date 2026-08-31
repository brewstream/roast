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

import io.netty.buffer.ByteBuf;
import io.netty.channel.AddressedEnvelope;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.socket.DatagramPacket;
import io.netty.handler.codec.MessageToMessageEncoder;
import org.brewstream.roast.packet.SrtPacket;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * Encodes an {@link SrtPacket} addressed to a recipient into a Netty
 * {@link DatagramPacket} ready to write to a {@code NioDatagramChannel}.
 */
public final class SrtFrameEncoder
        extends MessageToMessageEncoder<AddressedEnvelope<SrtPacket, InetSocketAddress>> {

    @Override
    protected void encode(ChannelHandlerContext ctx, AddressedEnvelope<SrtPacket, InetSocketAddress> msg,
            List<Object> out) {
        SrtPacket packet = msg.content();
        ByteBuf buf = ctx.alloc().buffer(SrtPacket.HEADER_LENGTH + packet.body().readableBytes());
        packet.encodeTo(buf);
        out.add(new DatagramPacket(buf, msg.recipient()));
    }
}