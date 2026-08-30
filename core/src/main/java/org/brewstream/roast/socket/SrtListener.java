package org.brewstream.roast.socket;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.AddressedEnvelope;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.DefaultAddressedEnvelope;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import org.brewstream.roast.codec.SrtFrameDecoder;
import org.brewstream.roast.codec.SrtFrameEncoder;
import org.brewstream.roast.handshake.ConclusionOutcome;
import org.brewstream.roast.crypto.EncryptionContext;
import org.brewstream.roast.handshake.ListenerHandshake;
import org.brewstream.roast.handshake.SynCookie;
import org.brewstream.roast.packet.ControlPacket;
import org.brewstream.roast.packet.ControlType;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.packet.cif.HandshakeCif;
import org.brewstream.roast.packet.cif.HandshakeExtension;
import org.brewstream.roast.packet.cif.HandshakeType;
import org.brewstream.roast.packet.cif.RejectionReason;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Binds a real Netty {@code NioDatagramChannel} and drives the HSv5 induction→
 * conclusion exchange ({@link ListenerHandshake}) through it. This is Phase 2's
 * "reaches connected" milestone — there is no data-transfer path yet (Phase 3),
 * so a DATA packet for an accepted connection is logged and dropped by
 * {@link SrtSocketIdDemultiplexer}'s existing unknown-socket-id path; that's
 * expected, not a bug here.
 */
public final class SrtListener {

    private static final Logger LOG = Logger.getLogger(SrtListener.class.getName());
    private static final int DEFAULT_LATENCY_MILLIS = 120;
    private static final int DEFAULT_SRT_VERSION = 0x010401;

    private final Channel channel;
    private final EventLoopGroup eventLoopGroup;
    private final SrtSocketIdDemultiplexer demultiplexer;
    private final ListenerHandshake listenerHandshake;
    private final SrtSocketIdGenerator socketIdGenerator;
    private final ConcurrentHashMap<SrtSocketId, HandshakeCif> acceptedByPeerSocketId = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<SrtSocketId, SrtConnection> connections = new ConcurrentHashMap<>();
    private final long startNanos = System.nanoTime();

    private volatile AcceptHandler acceptHandler = request -> AcceptDecision.reject(RejectionReason.PEER);
    private volatile Consumer<SrtConnection> connectionHandler = connection -> {
    };
    private final java.util.List<SrtConnectionListener> eventListeners = new java.util.concurrent.CopyOnWriteArrayList<>();

    private SrtListener(Channel channel, EventLoopGroup eventLoopGroup, SrtSocketIdDemultiplexer demultiplexer,
            ListenerHandshake listenerHandshake, SrtSocketIdGenerator socketIdGenerator) {
        this.channel = channel;
        this.eventLoopGroup = eventLoopGroup;
        this.demultiplexer = demultiplexer;
        this.listenerHandshake = listenerHandshake;
        this.socketIdGenerator = socketIdGenerator;
    }

    public static SrtListener bind(InetSocketAddress localAddress) throws InterruptedException {
        SrtSocketIdDemultiplexer demultiplexer = new SrtSocketIdDemultiplexer();
        EventLoopGroup group = new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory());
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioDatagramChannel.class)
                .handler(new ChannelInitializer<DatagramChannel>() {
                    @Override
                    protected void initChannel(DatagramChannel ch) {
                        ch.pipeline().addLast(new SrtFrameDecoder(), new SrtFrameEncoder(), demultiplexer);
                    }
                });

        Channel channel = bootstrap.bind(localAddress).sync().channel();
        InetSocketAddress boundAddress = (InetSocketAddress) channel.localAddress();

        SynCookie cookie = SynCookie.forListener(boundAddress.toString());
        ListenerHandshake listenerHandshake =
                new ListenerHandshake(cookie, boundAddress.getAddress(), DEFAULT_SRT_VERSION);

        SrtListener listener = new SrtListener(
                channel, group, demultiplexer, listenerHandshake, new SrtSocketIdGenerator());
        demultiplexer.setAcceptor(listener::onHandshakePacket);
        return listener;
    }

    /** Decides whether to accept an incoming connection once it's passed protocol-level validation. */
    public void setAcceptHandler(AcceptHandler handler) {
        this.acceptHandler = handler;
    }

    /**
     * Wiring hook: attach your {@code onData} (and anything else the connection
     * needs before traffic arrives) here. Runs <b>synchronously on the event
     * loop, before the accept response is sent</b>, so a peer's first packets
     * cannot arrive before your handler exists.
     *
     * <p>For observing a connection rather than wiring it — metrics, logging —
     * use {@link #addEventListener}, which is dispatched off the event loop and
     * carries no such ordering guarantee.
     */
    public void onConnection(Consumer<SrtConnection> handler) {
        this.connectionHandler = handler;
    }

    /**
     * Registers an observability listener for <em>every</em> connection this
     * listener accepts, present and future — so a component managing the
     * listener attaches once rather than wiring each connection by hand, and
     * still sees {@link SrtConnectionListener#onConnected}, which a
     * per-connection registration is too late to catch.
     *
     * <p>Events are delivered off the event loop; see
     * {@link SrtConnectionListener} for the full threading contract.
     */
    public void addEventListener(SrtConnectionListener listener) {
        eventListeners.add(listener);
    }

    /**
     * This listener's Netty pipeline, so an embedder can insert its own
     * {@code ChannelHandler}s — custom telemetry, traffic interception or
     * mutation — rather than being limited to the hooks above. DESIGN.md §4
     * asks for this on the grounds that hiding Netty behind a closed API would
     * waste the fact that the transport *is* Netty.
     *
     * <p><b>The granularity is per port, not per connection</b>, which is worth
     * knowing before reaching for it. §4 describes adding handlers to "a
     * connection's pipeline", but Roast multiplexes every connection for a
     * listener onto one {@code NioDatagramChannel} — that is what makes
     * many-sockets-on-one-port work — so there is exactly one pipeline here and
     * a handler added to it sees traffic for <em>all</em> connections,
     * demultiplexed only further along by {@link SrtSocketIdDemultiplexer}. A
     * handler wanting one connection must filter on the destination socket ID
     * itself.
     *
     * <p>Handlers run on the event loop, so the usual rule applies: blocking
     * here stalls packet processing for every connection on this port. The
     * observability hooks ({@link #addEventListener}) are dispatched off it
     * precisely so they cannot; this deliberately is not, because a pipeline
     * handler is part of the data path by definition.
     */
    public io.netty.channel.ChannelPipeline pipeline() {
        return channel.pipeline();
    }

    /** The connections currently accepted and live — for polling {@link SrtConnection#stats()}. */
    public java.util.Collection<SrtConnection> connections() {
        return java.util.Collections.unmodifiableCollection(connections.values());
    }

    public InetSocketAddress localAddress() {
        return (InetSocketAddress) channel.localAddress();
    }

    public void close() throws InterruptedException {
        connections.values().forEach(SrtConnection::close);
        channel.close().sync();
        eventLoopGroup.shutdownGracefully().sync();
    }

    private void onHandshakePacket(AddressedEnvelope<SrtPacket, InetSocketAddress> msg) {
        SrtPacket packet = msg.content();
        if (!(packet instanceof ControlPacket control) || control.type() != ControlType.HANDSHAKE) {
            packet.body().release();
            return;
        }

        HandshakeCif request = HandshakeCif.decode(control.body(), true);
        control.body().release();
        if (request == null) {
            LOG.log(Level.FINE, "Dropping malformed handshake CIF from {0}", msg.sender());
            return;
        }

        String senderAddress = msg.sender().toString();
        HandshakeType type = request.handshakeType();

        if (type == HandshakeType.INDUCTION) {
            send(listenerHandshake.onInduction(request, senderAddress), request.srtSocketId(), msg.sender());
            return;
        }
        if (type != HandshakeType.CONCLUSION) {
            LOG.log(Level.FINE, "Dropping non-progression handshake type from {0}", msg.sender());
            return;
        }

        HandshakeCif cachedResponse = acceptedByPeerSocketId.get(request.srtSocketId());
        if (cachedResponse != null) {
            // A retried CONCLUSION for an already-accepted connection - resend, don't re-run accept logic.
            send(cachedResponse, request.srtSocketId(), msg.sender());
            return;
        }

        ConclusionOutcome outcome = listenerHandshake.validateConclusion(request, senderAddress);
        if (outcome instanceof ConclusionOutcome.Rejected rejected) {
            send(rejected.response(), request.srtSocketId(), msg.sender());
            return;
        }

        AcceptDecision decision = acceptHandler.handle(toConnectionRequest(request, msg.sender()));
        if (decision instanceof AcceptDecision.Reject reject) {
            send(listenerHandshake.buildRejectResponse(request, reject.reason()), request.srtSocketId(), msg.sender());
            return;
        }

        AcceptDecision.Accept accepted = (AcceptDecision.Accept) decision;
        EncryptionContext encryptionContext = null;
        if (accepted.isEncrypted()) {
            if (request.keyMaterial() == null) {
                // The application demanded a passphrase; the peer offered no keys at all.
                send(listenerHandshake.buildRejectResponse(request, RejectionReason.UNSECURE),
                        request.srtSocketId(), msg.sender());
                return;
            }
            // The Encryption Field names the cipher family and key size (Table 2:
            // 2 = AES-128, 3 = AES-192, 4 = AES-256, i.e. the key length in
            // 8-byte units), and the key material states the length again in its
            // own KLen. When the peer advertises a specific method the two must
            // agree - disagreement means malformed, not unauthenticated, so it
            // isn't a BADSECRET case. Zero means "no specific method advertised",
            // which is what gosrt always sends, so there is nothing to check.
            int advertisedKeyLength = request.encryptionField() * 8;
            if (request.encryptionField() != 0 && advertisedKeyLength != request.keyMaterial().keyLength()) {
                LOG.log(Level.FINE, "Rejecting {0}: Encryption Field says {1} bytes, key material says {2}",
                        new Object[]{msg.sender(), advertisedKeyLength, request.keyMaterial().keyLength()});
                send(listenerHandshake.buildRejectResponse(request, RejectionReason.ROGUE),
                        request.srtSocketId(), msg.sender());
                return;
            }

            encryptionContext = EncryptionContext.awaitingPeerKeys(accepted.passphrase(), accepted.keyLength());
            if (!encryptionContext.adopt(request.keyMaterial())) {
                encryptionContext.destroy();
                // A key shorter than the application asked for is a downgrade, not
                // a bad secret - say so, rather than blaming the passphrase.
                RejectionReason reason = request.keyMaterial().keyLength() < accepted.keyLength()
                        ? RejectionReason.UNSECURE
                        : RejectionReason.BADSECRET;
                send(listenerHandshake.buildRejectResponse(request, reason),
                        request.srtSocketId(), msg.sender());
                return;
            }
        }

        SrtSocketId assignedSocketId = socketIdGenerator.generate();
        HandshakeCif response = listenerHandshake.buildAcceptResponse(
                request, assignedSocketId, DEFAULT_LATENCY_MILLIS, DEFAULT_LATENCY_MILLIS,
                encryptionContext != null ? request.keyMaterial() : null);
        acceptedByPeerSocketId.put(request.srtSocketId(), response);

        HandshakeExtension negotiated = response.handshakeExtension();
        AcceptedConnection metadata = new AcceptedConnection(
                assignedSocketId, request.srtSocketId(), msg.sender(), request.streamId(),
                negotiated.receiveTsbpdDelayMillis(), negotiated.sendTsbpdDelayMillis(), negotiated.srtVersion(),
                response.maxFlowWindowSize(), response.maxTransmissionUnitSize());

        // Everything that must be ready for inbound DATA happens BEFORE the accept
        // response goes out: constructing the connection registers it with the
        // demultiplexer, and the application attaches its onData in
        // connectionHandler. A peer may legitimately send its first DATA packet
        // the instant it sees the response, so sending first left a real window
        // in which those packets routed to the acceptor sink (no registration
        // yet) or hit a no-op onData (no handler yet) and were dropped. A steady
        // stream hides this - only the first packets are lost - but a peer that
        // sends one small burst and stops loses all of it.
        // Forget the connection once it closes. Uses the internal
        // channel-owner-close slot rather than the app-facing onClose hook,
        // which is a single overridable slot an application would clobber.
        // Without this both maps grow for the life of the listener: a relay
        // with churning connections leaks every one it ever accepted, and
        // connections() reports long-dead ones.
        SrtSocketId peerSocketId = request.srtSocketId();
        SrtConnection connection = new SrtConnection(
                channel, demultiplexer, metadata, request.initialPacketSequenceNumber(),
                () -> {
                    connections.remove(assignedSocketId);
                    acceptedByPeerSocketId.remove(peerSocketId);
                },
                encryptionContext);
        eventListeners.forEach(connection::addEventListener);
        connections.put(assignedSocketId, connection);
        connectionHandler.accept(connection);
        connection.fireConnected();

        send(response, request.srtSocketId(), msg.sender());
    }

    private static ConnectionRequest toConnectionRequest(HandshakeCif request, InetSocketAddress peerAddress) {
        HandshakeExtension extension = request.handshakeExtension();
        String streamId = request.streamId() == null ? "" : request.streamId();
        return new ConnectionRequest(
                peerAddress, request.srtSocketId(), streamId, extension.srtVersion(),
                request.encryptionField() != 0,
                extension.receiveTsbpdDelayMillis(), extension.sendTsbpdDelayMillis());
    }

    private void send(HandshakeCif responseCif, SrtSocketId packetDestination, InetSocketAddress recipient) {
        ByteBuf cifBuf = channel.alloc().buffer();
        responseCif.encodeTo(cifBuf);
        ControlPacket controlPacket =
                new ControlPacket(ControlType.HANDSHAKE, 0, elapsedMicros(), packetDestination, cifBuf);
        channel.writeAndFlush(new DefaultAddressedEnvelope<>(controlPacket, recipient));
    }

    private int elapsedMicros() {
        return (int) ((System.nanoTime() - startNanos) / 1000);
    }
}
