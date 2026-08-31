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
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Running on an application's own Netty resources rather than resources Roast
 * creates. The contract worth testing is ownership: a group we were lent must
 * still be usable after the listener or connection that borrowed it is closed,
 * because the application that lent it is very likely still using it.
 */
class SrtTransportTest {

    private static final int TIMEOUT_SECONDS = 10;

    private EventLoopGroup group;
    private SrtListener listener;
    private final List<SrtConnection> connections = new ArrayList<>();

    @AfterEach
    void tearDown() throws InterruptedException {
        connections.forEach(SrtConnection::close);
        if (listener != null) {
            listener.close();
        }
        if (group != null) {
            group.shutdownGracefully().sync();
        }
    }

    private SrtTransport sharedTransport() {
        group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        return SrtTransport.shared(group, NioDatagramChannel.class);
    }

    @Test
    void aListenerAndCallerOnASharedGroupCarryDataNormally() throws Exception {
        SrtTransport transport = sharedTransport();

        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0), SrtConfig.defaults(), transport);
        listener.setAcceptHandler(request -> AcceptDecision.accept());
        CompletableFuture<String> received = new CompletableFuture<>();
        listener.onConnection(connection -> connection.onData(payload -> {
            received.complete(payload.toString(StandardCharsets.US_ASCII));
            payload.release();
        }));

        SrtConnection caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()),
                        "live/shared", null, 0, SrtConfig.defaults(), transport)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        connections.add(caller);
        caller.write(Unpooled.wrappedBuffer("on-your-loops".getBytes(StandardCharsets.US_ASCII)));

        assertThat(received.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo("on-your-loops");
    }

    /**
     * The point of the whole feature. Closing a listener that borrowed a group
     * must not shut that group down — an application whose other traffic shares
     * it would find its own event loops terminated by our teardown.
     */
    @Test
    void closingAListenerLeavesABorrowedGroupRunning() throws Exception {
        SrtTransport transport = sharedTransport();
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0), SrtConfig.defaults(), transport);
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        listener.close();
        listener = null;

        assertThat(group.isShuttingDown()).as("a borrowed group must survive its borrower").isFalse();
        assertThat(group.isShutdown()).isFalse();
        // Still usable: bind a second listener on the same group.
        SrtListener second = SrtListener.bind(
                new InetSocketAddress("127.0.0.1", 0), SrtConfig.defaults(), transport);
        second.close();
        assertThat(group.isShutdown()).isFalse();
    }

    /** Same contract on the caller side, where the default is one group per connection. */
    @Test
    void closingACallerLeavesABorrowedGroupRunning() throws Exception {
        SrtTransport transport = sharedTransport();
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0), SrtConfig.defaults(), transport);
        listener.setAcceptHandler(request -> AcceptDecision.accept());
        listener.onConnection(connection -> connection.onData(payload -> payload.release()));

        SrtConnection caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()),
                        "live/borrowed", null, 0, SrtConfig.defaults(), transport)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        caller.close();

        assertThat(group.isShuttingDown()).isFalse();
        assertThat(group.isShutdown()).isFalse();
    }

    /**
     * Many connections on one group is the reason to reach for this: the default
     * gives each caller its own group, and so its own thread.
     */
    @Test
    void manyConnectionsShareOneGroup() throws Exception {
        SrtTransport transport = sharedTransport();
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0), SrtConfig.defaults(), transport);
        listener.setAcceptHandler(request -> AcceptDecision.accept());
        listener.onConnection(connection -> connection.onData(payload -> payload.release()));

        for (int i = 0; i < 5; i++) {
            connections.add(SrtCaller.connect(
                            new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()),
                            "live/many-" + i, null, 0, SrtConfig.defaults(), transport)
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        }

        assertThat(listener.connections()).hasSize(5);
        assertThat(group.isShutdown()).isFalse();
    }

    /** A Roast-created transport is still Roast's to clean up. */
    @Test
    void aDefaultTransportIsStillShutDownByItsOwner() throws Exception {
        SrtListener owned = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        owned.setAcceptHandler(request -> AcceptDecision.accept());
        int port = owned.localAddress().getPort();

        owned.close();

        // The port is released, which it would not be if the group were left running.
        SrtListener rebound = SrtListener.bind(new InetSocketAddress("127.0.0.1", port));
        rebound.close();
    }
}