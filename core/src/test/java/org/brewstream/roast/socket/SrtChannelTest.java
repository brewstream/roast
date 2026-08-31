package org.brewstream.roast.socket;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.WriteBufferWaterMark;
import io.netty.handler.codec.MessageToMessageDecoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The connection as a Netty channel: an application's own handlers on the data
 * path, and backpressure that is a real signal rather than an unbounded queue.
 * Exercised over real sockets, since the point is how it behaves under a live
 * connection rather than in isolation.
 */
class SrtChannelTest {

    private static final int TIMEOUT_SECONDS = 20;
    /** A deliberately small flow window, so backpressure is reached in packets rather than thousands. */
    private static final int WINDOW = 64;

    private SrtListener listener;
    private SrtConnection caller;

    @AfterEach
    void tearDown() throws InterruptedException {
        if (caller != null) {
            caller.close();
        }
        if (listener != null) {
            listener.close();
        }
    }

    private SrtConnection connect(CompletableFuture<SrtConnection> listenerSide) throws Exception {
        return connect(listenerSide, SrtConfig.defaults());
    }

    private SrtConnection connect(CompletableFuture<SrtConnection> listenerSide, SrtConfig callerConfig)
            throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());
        listener.onConnection(listenerSide::complete);
        caller = SrtCaller.connect(
                        new InetSocketAddress("127.0.0.1", listener.localAddress().getPort()),
                        "live/channel", null, 0, callerConfig)
                .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        return listenerSide.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /** The case this exists for: an application's own decoder on the data path. */
    @Test
    void anApplicationsOwnHandlerSeesDeliveredPayloads() throws Exception {
        CompletableFuture<SrtConnection> listenerSide = new CompletableFuture<>();
        SrtConnection remote = connect(listenerSide);

        CompletableFuture<String> decoded = new CompletableFuture<>();
        remote.pipeline().addLast(new MessageToMessageDecoder<ByteBuf>() {
            @Override
            protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
                // Stands in for a real codec - an MPEG-TS demultiplexer, say.
                out.add(in.toString(StandardCharsets.US_ASCII).toUpperCase());
            }
        });
        remote.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) {
                decoded.complete((String) message);
            }
        });

        caller.write(Unpooled.wrappedBuffer("through-the-pipeline".getBytes(StandardCharsets.US_ASCII)));

        assertThat(decoded.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo("THROUGH-THE-PIPELINE");
    }

    /**
     * A handler added after {@code onData} must still be reachable. This is the
     * ordering that was wrong first time round: the terminal handler backing
     * onData used to be installed at construction, so anything added later sat
     * behind it and never saw a byte.
     */
    @Test
    void handlersAndOnDataFollowOrdinaryPipelineOrdering() throws Exception {
        CompletableFuture<SrtConnection> listenerSide = new CompletableFuture<>();
        SrtConnection remote = connect(listenerSide);

        List<String> order = new CopyOnWriteArrayList<>();
        CountDownLatch seen = new CountDownLatch(1);
        remote.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelRead(ChannelHandlerContext ctx, Object message) {
                order.add("handler");
                ctx.fireChannelRead(message);
            }
        });
        remote.onData(payload -> {
            order.add("onData");
            payload.release();
            seen.countDown();
        });

        caller.write(Unpooled.wrappedBuffer("ordered".getBytes(StandardCharsets.US_ASCII)));

        assertThat(seen.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(order).containsExactly("handler", "onData");
    }

    /** The channel is a real, live Netty channel with the connection's addresses on it. */
    @Test
    void theChannelIsActiveAndCarriesTheConnectionsAddresses() throws Exception {
        CompletableFuture<SrtConnection> listenerSide = new CompletableFuture<>();
        SrtConnection remote = connect(listenerSide);

        assertThat(remote.channel().isActive()).isTrue();
        assertThat(remote.channel().isOpen()).isTrue();
        assertThat(remote.channel().remoteAddress()).isEqualTo(remote.metadata().peerAddress());
        assertThat(remote.channel().parent()).isNotNull();
    }

    /** Closing the channel closes the connection, and vice versa — one lifetime, two doors. */
    @Test
    void closingEitherTheChannelOrTheConnectionClosesBoth() throws Exception {
        CompletableFuture<SrtConnection> listenerSide = new CompletableFuture<>();
        SrtConnection remote = connect(listenerSide);
        CountDownLatch closed = new CountDownLatch(1);
        remote.onClose(closed::countDown);

        remote.channel().close().sync();

        assertThat(closed.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();
        assertThat(remote.channel().isActive()).isFalse();
    }

    @Test
    void closingTheConnectionClosesTheChannel() throws Exception {
        CompletableFuture<SrtConnection> listenerSide = new CompletableFuture<>();
        SrtConnection remote = connect(listenerSide);

        remote.close();

        assertThat(remote.channel().isOpen()).isFalse();
    }

    /**
     * Writing through the channel is the same path {@code write} takes, so a
     * payload sent this way arrives normally.
     */
    @Test
    void writingThroughTheChannelDeliversNormally() throws Exception {
        CompletableFuture<SrtConnection> listenerSide = new CompletableFuture<>();
        SrtConnection remote = connect(listenerSide);
        CompletableFuture<String> received = new CompletableFuture<>();
        remote.onData(payload -> {
            received.complete(payload.toString(StandardCharsets.US_ASCII));
            payload.release();
        });

        caller.channel().writeAndFlush(
                Unpooled.wrappedBuffer("written-as-a-channel".getBytes(StandardCharsets.US_ASCII))).sync();

        assertThat(received.get(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isEqualTo("written-as-a-channel");
    }

    /**
     * An oversized payload fails its write future rather than vanishing. Before
     * the channel existed there was no way to learn a write had been refused
     * except the synchronous throw from {@code write}.
     */
    @Test
    void anOversizedWriteFailsItsFuture() throws Exception {
        CompletableFuture<SrtConnection> listenerSide = new CompletableFuture<>();
        connect(listenerSide);

        ByteBuf tooBig = Unpooled.wrappedBuffer(new byte[caller.metadata().maxPayloadSize() + 1]);

        ChannelFuture future = caller.channel().writeAndFlush(tooBig);
        assertThat(future.await(TIMEOUT_SECONDS, TimeUnit.SECONDS)).isTrue();

        assertThat(future.isSuccess()).as("an oversized write must fail, not silently succeed").isFalse();
        assertThat(future.cause()).isInstanceOf(IllegalArgumentException.class);
        assertThat(tooBig.refCnt()).as("a refused payload must not leak").isZero();
        assertThat(caller.channel().isOpen()).as("one bad write must not kill the connection").isTrue();
    }

    /**
     * The signal that did not exist before: writing past the peer's flow window
     * leaves data in the outbound buffer, and the water marks turn that into
     * isWritable() going false. Previously every write was accepted and an
     * application outrunning the link found out when the heap did.
     */
    @Test
    void outrunningTheFlowWindowMakesTheChannelUnwritable() throws Exception {
        CompletableFuture<SrtConnection> listenerSide = new CompletableFuture<>();
        connect(listenerSide, SrtConfig.defaults().withFlowWindowPackets(WINDOW));

        assertThat(caller.metadata().flowWindowSize())
                .as("the small window must actually have been negotiated").isEqualTo(WINDOW);
        caller.channel().config().setWriteBufferWaterMark(new WriteBufferWaterMark(2048, 4096));

        // The loop runs ON the event loop, which is what makes this a test of the
        // flow window rather than of scheduling. Writing from another thread only
        // proves that writes can outrun the loop - the outbound buffer grows
        // because doWrite has not run yet, and the assertion passes even with the
        // window check removed. Verified: that version survived mutation. Here
        // each writeAndFlush runs doWrite inline, so anything left queued is the
        // window refusing it and nothing else.
        CompletableFuture<Boolean> everUnwritable = new CompletableFuture<>();
        caller.channel().eventLoop().execute(() -> {
            boolean seen = false;
            for (int i = 0; i < WINDOW * 4 && !seen; i++) {
                caller.channel().writeAndFlush(Unpooled.wrappedBuffer(new byte[1000]));
                seen = !caller.channel().isWritable();
            }
            everUnwritable.complete(seen);
        });

        assertThat(everUnwritable.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .as("a writer past the flow window must see isWritable() go false")
                .isTrue();
    }
}
