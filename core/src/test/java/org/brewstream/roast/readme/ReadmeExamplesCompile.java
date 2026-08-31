package org.brewstream.roast.readme;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.packet.cif.RejectionReason;
import org.brewstream.roast.socket.AcceptDecision;
import org.brewstream.roast.socket.ConnectionStats;
import org.brewstream.roast.socket.SrtCaller;
import org.brewstream.roast.socket.SrtConfig;
import org.brewstream.roast.socket.SrtConnection;
import org.brewstream.roast.socket.SrtConnectionListener;
import org.brewstream.roast.socket.SrtListener;
import org.brewstream.roast.socket.SrtTransport;
import org.brewstream.roast.socket.StatsSampler;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/** Not a test: a compile-time check that every Java snippet in README.md still type-checks. */
final class ReadmeExamplesCompile {

    private ReadmeExamplesCompile() {
    }

    static void receiving() throws InterruptedException {
        SrtListener listener = SrtListener.bind(new InetSocketAddress(9000));

        listener.setAcceptHandler(request ->
                request.streamId().startsWith("live/")
                        ? AcceptDecision.accept()
                        : AcceptDecision.reject(RejectionReason.FORBIDDEN));

        listener.onConnection(connection -> {
            String streamId = connection.metadata().streamId();

            connection.onData(payload -> {
                try {
                    accept(streamId, payload);
                } finally {
                    payload.release();
                }
            });

            connection.onClose(() -> remove(streamId));
        });
    }

    static void sending(byte[] chunk)
            throws InterruptedException, ExecutionException, TimeoutException {
        SrtConnection connection = SrtCaller
                .connect(new InetSocketAddress("example.com", 9000), "live/camera-1")
                .get(5, TimeUnit.SECONDS);

        connection.write(Unpooled.wrappedBuffer(chunk));

        int max = connection.metadata().maxPayloadSize();
        assert max > 0;
    }

    static void events(SrtListener listener) {
        listener.addEventListener(new SrtConnectionListener() {
            @Override
            public void onConnected(SrtConnection c) {
                log(c.metadata().streamId(), c.metadata().peerAddress());
            }

            @Override
            public void onDisconnected(SrtConnection c) {
                log(c.metadata().streamId(), null);
            }

            @Override
            public void onLoss(SrtConnection c, LossRange range) {
                log(range.start() + ".." + range.end(), null);
            }

            @Override
            public void onTlpktDrop(SrtConnection c, LossRange range) {
                log(c.metadata().streamId(), null);
            }

            @Override
            public void onKeyRotated(SrtConnection c) {
                log(c.metadata().streamId(), null);
            }
        });
    }

    static void statistics(SrtConnection connection, SrtListener listener) {
        ConnectionStats stats = connection.stats();

        long rtt = stats.rttMicros();
        double retransmitting = stats.retransmitRate();
        double rightNow = stats.sendLossRatePercent();

        // Every field named in the README's snapshot table.
        long volume = stats.packetsSent() + stats.packetsReceived() + stats.bytesSent()
                + stats.bytesReceived() + stats.packetsRetransmitted()
                + stats.packetsLost() + stats.packetsDropped() + stats.droppedEvents()
                + stats.rttVarMicros();
        int gauges = stats.receiveRatePacketsPerSecond() + stats.receiveRateBytesPerSecond()
                + stats.estimatedLinkCapacityPacketsPerSecond()
                + stats.estimatedInputBytesPerSecond() + stats.estimatedSentBytesPerSecond()
                + stats.receiveBufferedPackets() + stats.sendBufferedPackets()
                + stats.sendInFlightPackets() + stats.flowWindowPackets();
        assert rtt + volume + gauges >= 0 && retransmitting + rightNow >= 0;

        StatsSampler sampler = StatsSampler.start(Duration.ofSeconds(1), listener,
                (c, s) -> gauge(c.metadata().streamId(), s.rttMicros(), s.retransmitRate(),
                        s.sendLossRatePercent(), s.sendBufferedPackets(), s.packetsDropped()));
        sampler.close();
    }

    static void admissionControl(SrtListener listener, int capacity) {
        listener.setAcceptHandler(request -> {
            if (!authorised(request.streamId(), request.peerAddress().toString())) {
                return AcceptDecision.reject(RejectionReason.UNAUTHORIZED);
            }
            if (listener.connections().size() >= capacity) {
                return AcceptDecision.reject(RejectionReason.OVERLOAD);
            }
            boolean encrypted = request.encryptionRequested();
            int version = request.srtVersion();
            assert version >= 0 || encrypted;
            return AcceptDecision.accept();
        });
    }

    static void encryption(SrtListener listener, InetSocketAddress address, char[] passphrase) {
        listener.setAcceptHandler(request ->
                AcceptDecision.accept(passphraseFor(request.streamId()), 16));

        SrtCaller.connect(address, "live/camera-1", passphrase, 16);
    }

    static void configuration(InetSocketAddress address, String streamId) throws InterruptedException {
        SrtConfig config = SrtConfig.defaults()
                .withLatency(Duration.ofMillis(200))
                .withPeerIdleTimeout(Duration.ofSeconds(30))
                .withMaxMss(1200);

        SrtListener.bind(address, config);
        SrtCaller.connect(address, streamId, config);
    }

    static void lifecycle(SrtListener listener) throws InterruptedException {
        listener.pipeline();
        listener.connections();
        listener.localAddress();
        listener.close();
    }

    static void ownNettyResources(EventLoopGroup existingGroup,
            InetSocketAddress address, String streamId) throws InterruptedException {
        SrtTransport transport = SrtTransport.shared(existingGroup, NioDatagramChannel.class);

        SrtListener.bind(address, SrtConfig.defaults(), transport);
        SrtCaller.connect(address, streamId, null, 0, SrtConfig.defaults(), transport);
    }

    private static void accept(String streamId, ByteBuf payload) {
    }

    private static void remove(String streamId) {
    }

    private static void log(Object a, Object b) {
    }

    private static void gauge(Object... values) {
    }

    private static boolean authorised(String streamId, String peer) {
        return true;
    }

    private static char[] passphraseFor(String streamId) {
        return new char[0];
    }
}
