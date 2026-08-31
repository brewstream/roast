# Roast

A pure-Java implementation of [SRT](https://github.com/Haivision/srt) (Secure
Reliable Transport), built on Netty. Part of **BrewStream**.

Use it anywhere a JVM application needs to speak SRT: accepting contribution
feeds, pulling a stream to record or transcode, feeding an MPEG-TS decoder or an
analyser, relaying between endpoints, or publishing out of a service you already
run. It is a general-purpose SRT stack rather than a component shaped around one
kind of pipeline.

What it adds over a JNI binding to libsrt is visibility. Loss, retransmission,
jitter, buffer occupancy and key rotation are first-class API — observable per
connection, in real time, from Java — where otherwise they are absent or sit
behind a C++ struct you cannot extend without forking. That is as useful to
something measuring stream health as to something forwarding it.

**Status:** v1 is complete. The protocol is verified against real libsrt in both
directions — handshake, ARQ, TSBPD with drift correction, encryption with
mid-stream key rotation, and socket-ID multiplexing. It has not yet been run in
production by anyone.

## Contents

- [Requirements](#requirements) · [Feature matrix](#feature-matrix)
- **Integrating:** [Receiving](#receiving-a-stream) · [Sending](#sending-a-stream) ·
  [Threading and buffer ownership](#threading-and-buffer-ownership) ·
  [Events](#events) · [Statistics](#statistics) ·
  [Admission control](#admission-control) · [Encryption](#encryption) ·
  [Configuration](#configuration) · [Lifecycle](#lifecycle-and-shutdown) ·
  [As a Netty channel](#the-connection-as-a-netty-channel) ·
  [Your own Netty resources](#running-on-your-own-netty-resources)
- [Command line](#command-line) · [Building](#building-and-testing) ·
  [Not implemented](#what-is-not-implemented)

## Requirements

Java 21 or newer. Netty is the only runtime dependency, and it is exposed as
`api` rather than `implementation` — `ByteBuf` and `ChannelPipeline` are part of
Roast's surface, so you get them on your compile classpath.

**Not yet published to any repository**, and the build has no publishing plugin
configured — so for now the way to consume it is a Gradle composite build:

```groovy
// settings.gradle
includeBuild '../roast'

// build.gradle
dependencies {
    implementation 'org.brewstream:core'
}
```

Gradle substitutes the dependency for the local project. Coordinates will change
when this is published properly.

## Feature matrix

Measured against the two reference implementations Roast is developed against:
[gosrt](https://github.com/datarhei/gosrt) (Go, MIT) for structure, and
[libsrt](https://github.com/Haivision/srt) (C++, MPL-2.0) as the interoperability
target.

| Feature | Roast | gosrt | libsrt |
|---|:---:|:---:|:---:|
| Caller–listener handshake (HSv5) | ✅ | ✅ | ✅ |
| Handshake v4 | ❌ | ✅ | ✅ |
| Rendezvous handshake | ❌ | ❌ | ✅ |
| TSBPD delivery | ✅ | ✅ | ✅ |
| Clock drift correction | ✅ | ⚠️ dead code | ✅ |
| Too-late packet drop (TLPKTDROP) | ✅ | ✅ | ✅ |
| NAK and periodic NAK | ✅ | ✅ | ✅ |
| Live congestion control (LiveCC) | ✅ | ✅ | ✅ |
| Rate pacing (enforced send interval) | ❌ | ❌ | ✅ |
| Encryption (AES-128/192/256) | ✅ | ✅ | ✅ |
| Mid-stream key rotation | ✅ | ✅ | ✅ |
| Message mode | ✅ | ✅ | ✅ |
| Socket-ID multiplexing on one port | ✅ | ✅ | ✅ |
| StreamID routing | ✅ | ✅ | ✅ |
| Handshake retry on packet loss | ✅ | ❌ | ✅ |
| Keepalive origination | ✅ | ❌ | ✅ |
| Peer idle timeout | ✅ | ✅ | ✅ |
| Per-connection event hooks | ✅ | ❌ | ❌ |
| Connection as a Netty channel | ✅ | ❌ | n/a |
| Pushable metrics sink | ✅ | ❌ | ❌ |
| Buffer/stream mode | ❌ | ❌ | ✅ |
| File transfer congestion control | ❌ | ❌ | ✅ |
| Connection bonding | ❌ | ❌ | ✅ |

Roast matches gosrt on every feature gosrt supports except HSv4, and goes beyond
it on handshake retry, keepalive origination, live drift correction, and the
whole observability surface. See [What is not
implemented](#what-is-not-implemented) for why the gaps are where they are.

## Receiving a stream

A listener owns one UDP port and multiplexes many connections over it by socket
ID, the same way a libsrt listener does.

```java
SrtListener listener = SrtListener.bind(new InetSocketAddress(9000));

listener.setAcceptHandler(request ->
        request.streamId().startsWith("live/")
                ? AcceptDecision.accept()
                : AcceptDecision.reject(RejectionReason.FORBIDDEN));

listener.onConnection(connection -> {
    String streamId = connection.metadata().streamId();

    connection.onData(payload -> {
        // One MPEG-TS chunk, in order, after TSBPD buffering. Decode it,
        // record it, analyse it, forward it — you own this buffer, so
        // release it when you are done.
        try {
            pipeline.accept(streamId, payload);
        } finally {
            payload.release();
        }
    });

    connection.onClose(() -> pipeline.remove(streamId));
});
```

`onConnection` runs *before* the accept response goes out, so your handlers are
installed before the peer's first packet can arrive. Wire everything the
connection needs there rather than afterwards.

## Sending a stream

```java
SrtConnection connection = SrtCaller
        .connect(new InetSocketAddress("example.com", 9000), "live/camera-1")
        .get(5, TimeUnit.SECONDS);

connection.write(Unpooled.wrappedBuffer(chunk));
```

`connect` returns a `CompletableFuture`, so it composes with whatever async
machinery you already have; it fails the future on rejection or timeout rather
than blocking indefinitely.

**One `write` is one packet.** Live mode does not split messages, so a payload
above `connection.metadata().maxPayloadSize()` (1456 bytes at a 1500-byte MTU) is
rejected with `IllegalArgumentException` rather than silently fragmented. Chunk
to that size yourself — the canonical unit is 1316 bytes, seven 188-byte MPEG-TS
packets.

`write` is safe to call from any thread; it marshals onto the connection's event
loop internally.

## Threading and buffer ownership

The two things most likely to bite you, stated plainly.

**`onData` runs on the connection's Netty event loop.** That thread also drives
ACKs, NAKs, retransmission and TSBPD delivery for *every* connection on the same
listener port. Blocking in `onData` — a synchronous HTTP call, a lock, a slow
disk write — stalls the protocol itself, not just your handler. Hand off to your
own executor if the work is not trivial.

**You own the `ByteBuf` passed to `onData`.** Release it exactly once. If you
hand it to another thread, `retain()` first and release on that side. This is
ordinary Netty reference counting, and a leak here looks like steadily growing
direct memory rather than an immediate failure — run with
`-Dio.netty.leakDetection.level=paranoid` in tests.

**Events do *not* run on the event loop.** `SrtConnectionListener` callbacks are
dispatched on a separate thread per connection, so a listener that blocks cannot
stall packet processing. The trade-offs: events are not ordered against
`onData`, and a listener that falls far behind will have events dropped rather
than growing an unbounded queue. Drops are counted in
`ConnectionStats.droppedEvents()` — a nonzero value means your own observability
is incomplete, which is worth alerting on.

## Events

Things that *happened*, delivered to any number of subscribers. Register on the
listener to see every connection on the port, or on a single connection.

```java
listener.addEventListener(new SrtConnectionListener() {
    @Override
    public void onConnected(SrtConnection c) {
        log.info("connected: {} from {}", c.metadata().streamId(), c.metadata().peerAddress());
    }

    @Override
    public void onDisconnected(SrtConnection c) {
        log.info("disconnected: {}", c.metadata().streamId());
    }

    @Override
    public void onLoss(SrtConnection c, LossRange range) {
        // A gap was detected and NAKed — recovery is in progress.
        log.debug("loss {}..{} on {}", range.start(), range.end(), c.metadata().streamId());
    }

    @Override
    public void onTlpktDrop(SrtConnection c, LossRange range) {
        // Recovery failed inside the latency budget. This is real data loss:
        // the bytes are gone and will not arrive. Usually the signal that
        // matters for alerting.
        meterRegistry.counter("srt.tlpktdrop", "stream", c.metadata().streamId()).increment();
    }

    @Override
    public void onKeyRotated(SrtConnection c) {
        log.info("key rotated: {}", c.metadata().streamId());
    }
});
```

The full set:

| Event | Fires when |
|---|---|
| `onConnected` | handshake completed, connection live |
| `onDisconnected` | connection closed, by either side or by idle timeout |
| `onLoss` | a gap was detected and NAKed — recovery starting |
| `onTlpktDrop` | recovery failed within the latency budget — **real data loss** |
| `onRetransmit` | a packet was resent in response to a peer's NAK |
| `onAckSent` | an acknowledgement went to the peer (~every 10ms) |
| `onAckReceived` | a peer acknowledged our data, with its own RTT estimate |
| `onKeyRotated` | the encryption key changed mid-stream |
| `onPacketReceived` | every DATA packet — see below |

`onPacketReceived` is the one event that fires per packet, a few hundred times a
second at live bitrates. It carries the packet's *shape* — sequence number,
payload size, whether it was a retransmission — not its bytes, because handing a
buffer to an asynchronous listener would mean either a copy per packet or a
use-after-release. Nothing is allocated or queued when no listener is
registered, so it costs nothing unless you ask for it.

All methods are `default`, so implement only what you need.

## Statistics

The pull half of observability: an immutable snapshot, shaped for a periodic
sampler. High-frequency activity is *counted* here rather than announced as an
event — there is no event per ACK.

```java
ConnectionStats stats = connection.stats();

long rtt = stats.rttMicros();
double retransmitting = stats.retransmitRate();       // lifetime fraction
double rightNow = stats.sendLossRatePercent();        // last 1s window
```

`StatsSampler` owns the scheduling loop you would otherwise write yourself,
including a final snapshot on disconnect — polling alone loses the last partial
interval, which for a short-lived connection can be most of it.

```java
StatsSampler sampler = StatsSampler.start(Duration.ofSeconds(1), listener,
        (connection, stats) -> {
            Tags tags = Tags.of("stream", connection.metadata().streamId());
            registry.gauge("srt.rtt.micros", tags, stats.rttMicros());
            registry.gauge("srt.retransmit.rate", tags, stats.retransmitRate());
            registry.gauge("srt.send.loss.percent", tags, stats.sendLossRatePercent());
            registry.gauge("srt.send.buffer.packets", tags, stats.sendBufferedPackets());
            registry.counter("srt.packets.dropped", tags).increment(stats.packetsDropped());
        });
```

Roast depends on no metrics library — the Micrometer binding is the five lines
above, in your code. The sink runs on the sampler's own thread, so blocking in
it (an HTTP push, say) cannot slow packet processing, and a sink that throws is
logged and skipped rather than killing the sampler.

`StatsSampler.start` also accepts a `Supplier<Collection<SrtConnection>>` if you
want to sample caller-side connections, which no listener owns.

**What a snapshot carries.** Counters accumulate over the connection's life, so
two samples give a rate; gauges describe it right now.

| | Field |
|---|---|
| **Volume** | `packetsSent` `packetsReceived` `bytesSent` `bytesReceived` `packetsRetransmitted` |
| **Loss** | `packetsLost` `packetsDropped` `retransmitRate()` `sendLossRatePercent` |
| **Timing** | `rttMicros` `rttVarMicros` |
| **Receive rate** | `receiveRatePacketsPerSecond` `receiveRateBytesPerSecond` `estimatedLinkCapacityPacketsPerSecond` |
| **Send rate** | `estimatedInputBytesPerSecond` `estimatedSentBytesPerSecond` |
| **Buffers** | `receiveBufferedPackets` `sendBufferedPackets` `sendInFlightPackets` `flowWindowPackets` |
| **Self-check** | `droppedEvents` |

Three of these are worth understanding rather than merely graphing:

- **`packetsDropped` is data you lost.** It counts TLPKTDROP — packets given up
  on because their deadline passed. `packetsLost` is only packets that went
  *missing*; most of those are recovered by retransmission.
- **`estimatedInputBytesPerSecond` above `estimatedSentBytesPerSecond`** means
  your application is offering data faster than it is going out, so the send
  buffer is filling and TLPKTDROP is about to start discarding. This is the
  early warning; `packetsDropped` is the damage report.
- **`retransmitRate()` vs `sendLossRatePercent`.** The first is a lifetime
  packet-count fraction, the second a byte-based percentage over the last
  second. A connection that recovered from a bad patch reads low on both; one in
  trouble *right now* reads low on the lifetime figure and high on the current
  one. Alert on the second, report the first.

## Admission control

The accept handler runs before any resources are committed to the connection,
and sees enough of the handshake to decide on more than the StreamID.

```java
listener.setAcceptHandler(request -> {
    if (!authorised(request.streamId(), request.peerAddress())) {
        return AcceptDecision.reject(RejectionReason.UNAUTHORIZED);
    }
    if (activeStreams.size() >= capacity) {
        return AcceptDecision.reject(RejectionReason.OVERLOAD);
    }
    return AcceptDecision.accept();
});
```

`ConnectionRequest` carries `streamId`, `peerAddress`, `peerSocketId`,
`srtVersion`, `encryptionRequested`, and the peer's requested TSBPD delays in
each direction. Rejection reasons map to the real SRT reject codes, so a libsrt
peer reports the cause you chose rather than a generic failure — `UNAUTHORIZED`,
`FORBIDDEN`, `OVERLOAD`, `BAD_REQUEST`, `BACKLOG`, and the rest of the 1000- and
1400-series.

`listener.connections()` gives the live set at any time, which is what a health
endpoint or an admin view wants.

## Encryption

The passphrase is decided **per connection, not per socket**, so one listener can
hold a different secret per stream — deliberately unlike libsrt's
`SRTO_PASSPHRASE` and gosrt's `Config.Passphrase`, both socket-wide. It is also
deliberately *not* on `SrtConfig`: a secret has no business in a settings object
an application might log or share.

```java
// Listener: decide per request, with the StreamID in hand
listener.setAcceptHandler(request ->
        AcceptDecision.accept(passphraseFor(request.streamId()), 16));

// Caller: the side that generates the keys
SrtCaller.connect(address, "live/camera-1", passphrase, 16);
```

Passphrases are `char[]`, not `String`, so they can be zeroed; Roast copies what
you pass, zeroes its copy on close, and never renders one in `toString()`. Key
length is 16, 24 or 32 bytes.

A peer whose passphrase does not match is rejected during the handshake
(`BADSECRET`) rather than left to send data nobody can read, and a peer offering
no keys where you required them gets `UNSECURE`. Keys rotate mid-stream on their
own schedule; `onKeyRotated` tells you when.

## Configuration

```java
SrtConfig config = SrtConfig.defaults()
        .withLatency(Duration.ofMillis(200))
        .withPeerIdleTimeout(Duration.ofSeconds(30))
        .withMaxMss(1200);

SrtListener.bind(address, config);
SrtCaller.connect(address, streamId, config);
```

| Setting | Default | Notes |
|---|---|---|
| `latency` | 120ms | the central trade-off: recovery time against delay. Negotiated — the peer may raise it |
| `connectTimeout` | 5s | how long a caller retries before failing |
| `flowWindowPackets` | 8192 | receive window advertised to the peer; memory against throughput |
| `maxMss` | 1500 | your MTU. Negotiated down to the smaller of the two, not used to reject |
| `peerIdleTimeout` | 5s | silence before a peer is declared dead. Raise it for links with long outages |
| `keyRefreshPackets` / `keyPreAnnouncePackets` | 2²⁴ / 2¹² | key rotation schedule |
| `srtVersion` | 1.4.1 | advertised version, for compatibility testing |

Immutable record with `with*` copies; validated on construction, so a bad value
fails where you wrote it rather than halfway through a handshake.

Protocol timing — the ~10ms tick, NAK interval, handshake retry, SYN-cookie
window — is deliberately not configurable. Those are fixed by the protocol or
derived from measured RTT, and changing them breaks interoperability rather than
tuning anything.

## Lifecycle and shutdown

`SrtListener.close()` closes every connection it owns, gracefully: queued writes
are drained and a SHUTDOWN is sent, so peers learn the stream ended rather than
timing out. `SrtConnection.close()` does the same for one connection and is safe
from any thread.

In a Spring Boot application, the natural shape is a bean whose lifecycle matches
the listener's:

```java
@Component
class SrtIngest implements AutoCloseable {

    private final SrtListener listener;
    private final StatsSampler sampler;

    SrtIngest(IngestPipeline pipeline, MeterRegistry registry) throws InterruptedException {
        this.listener = SrtListener.bind(new InetSocketAddress(9000),
                SrtConfig.defaults().withLatency(Duration.ofMillis(200)));
        listener.setAcceptHandler(request -> pipeline.accepts(request.streamId())
                ? AcceptDecision.accept()
                : AcceptDecision.reject(RejectionReason.FORBIDDEN));
        listener.onConnection(connection -> pipeline.attach(connection));
        this.sampler = StatsSampler.start(Duration.ofSeconds(1), listener,
                new MicrometerSink(registry));
    }

    @Override
    public void close() throws InterruptedException {
        sampler.close();
        listener.close();
    }
}
```

Spring calls `close()` on shutdown for any bean implementing `AutoCloseable`, so
no `@PreDestroy` is needed. Close the sampler first — it holds a reference to the
listener's connections.

**Netty pipeline access.** `listener.pipeline()` exposes the underlying
`ChannelPipeline` if you need to go beyond the hooks above — custom telemetry,
traffic interception. Note it is per *port*, not per connection: every connection
on a listener is multiplexed onto one datagram channel, which is what makes
many-sockets-on-one-port work, so a handler you add sees all of them.

## The connection as a Netty channel

Most applications should use the callbacks above. If you want your own handlers
on the data path — an MPEG-TS demultiplexer, an RTP packetiser, traffic shaping,
logging — every connection is also a Netty channel:

```java
connection.pipeline().addLast(new MpegTsDecoder(), new RtpPacketizer());

Channel channel = connection.channel();
```

`onData` is implemented as a terminal handler on that same pipeline, installed
when you first call it, so there is one delivery path rather than two. Ordering
is ordinary Netty `addLast` ordering: set `onData` and it is the terminal
consumer; add handlers instead and they are.

**Backpressure.** `channel().isWritable()` goes false once the peer's negotiated
flow window is full and Netty's write water marks are exceeded. This is the only
backpressure signal Roast offers — `write()` and `writeAndFlush()` otherwise
accept whatever you give them — so an application that can outrun its link
should consult it:

```java
if (connection.channel().isWritable()) {
    connection.channel().writeAndFlush(chunk);
}
```

`writeAndFlush` also returns a `ChannelFuture`, so a write refused for being
oversized, or dropped because the connection closed, fails observably instead of
vanishing.

**Bridging to another transport.** Because it is a real channel, the standard
Netty proxy idiom works — throttle the SRT side when the downstream backs up:

```java
srtConnection.channel().config().setAutoRead(downstream.isWritable());
```

With `autoRead` off, delivered payloads queue on the channel rather than being
fired at your handlers. Note that queue is local to this process: it does not
yet narrow the receive window advertised to the peer, so backpressure stops here
rather than reaching the sender.

`SrtConnection` deliberately *has* a channel rather than *being* one. Netty's
`write(Object)` means "queue, don't flush"; `SrtConnection.write(ByteBuf)` means
"send this". One class cannot carry both meanings of the same word without
misleading somebody, and keeping them apart leaves the callback API exactly as
simple as it was.

A handler added after the connection is live joins an already-active channel and
will not see `channelActive`; use `handlerAdded` with `isActive()`, as you would
attaching to any live channel.

## Running on your own Netty resources

By default Roast creates and owns an event loop group and uses
`NioDatagramChannel`, which suits an application not otherwise using Netty. If
you already run Netty, hand over your own:

```java
SrtTransport transport = SrtTransport.shared(existingGroup, EpollDatagramChannel.class);

SrtListener.bind(address, SrtConfig.defaults(), transport);
SrtCaller.connect(address, streamId, null, 0, SrtConfig.defaults(), transport);
```

Two reasons this is worth doing. **Threads:** by default every *caller*
connection gets its own group, and Netty starts a thread for each group that
receives a channel — so fifty outbound pulls means fifty threads that could have
been a handful. **Transport:** the default is NIO, while `EpollDatagramChannel`
(or io_uring) supports `SO_REUSEPORT`, which is how UDP receive scales across
cores at live packet rates. The channel type must match the group's transport;
Netty fails the registration if they disagree.

Roast never shuts down a group you lent it — `close()` closes its channels and
leaves your loops alone, since you are probably still using them. A group Roast
created is shut down with whatever created it.

This is deliberately not part of `SrtConfig`: an event loop group is a resource
with a lifecycle, not a setting, and `SrtConfig` is a value object meant to be
safe to log, copy and share.

## Command line

`srt-java-live-transmit` mirrors libsrt's tool of the same name, so command lines
usually move between the two unchanged. Useful for testing an integration
without writing a client.

```sh
./gradlew installDist
export PATH="$PWD/core/build/install/srt-java-live-transmit/bin:$PATH"

# Receive to stdout
srt-java-live-transmit "srt://:9000?mode=listener" file://con | ffplay -

# Send from stdin
ffmpeg -re -i input.ts -c copy -f mpegts - \
  | srt-java-live-transmit file://con "srt://host:9000?streamid=live/x"
```

Diagnostics and `-stats 1` output go to stderr, so stdout stays a clean stream
and the pipes above work as written. (Run it through `./gradlew` and they don't —
Gradle writes its own progress to stdout, which corrupts the media.)

An unknown URI parameter is an error rather than ignored: silently dropping a
misspelled `passphrse` is how someone ends up certain encryption is on when it
isn't.

## Building and testing

```sh
./gradlew test          # 338 tests, no external dependencies
./gradlew interopTest   # against real libsrt and ffmpeg; skips itself if absent
```

The interop suite runs Roast against a locally built `srt-live-transmit` and
against `ffmpeg`, in both directions, encrypted and not, including a mid-stream
key rotation a real libsrt peer has to follow. It skips rather than fails when
those binaries are missing. See `references/` (gitignored) for building libsrt.

## What is not implemented

Each of these is a decision rather than an omission; `STATUS.md` records the
reasoning in full.

- **HSv4.** Only HSv5 is spoken. gosrt supports v4; Roast declines it, because it
  is a second handshake path rather than a fallback branch, and every modern peer
  — libsrt, ffmpeg, OBS — negotiates v5.
- **Rendezvous mode**, **buffer/stream mode**, **file-transfer congestion
  control**, and **connection bonding.** Live streaming only. gosrt implements
  none of these either.
- **Message chunking.** One `write` is one packet, matching gosrt, whose live
  sender also emits only single-packet messages. Multi-packet reassembly would be
  message mode proper.
- **Rate pacing.** `SendBuffer` releases each packet at its scheduled time and
  does not space them further, so a bursty writer bursts onto the wire. This is
  exact parity with gosrt, whose `pktSndPeriod` is computed for statistics and
  never delays a send; libsrt does enforce an interval. Fine for a self-pacing
  source such as a live encoder. If you feed Roast from a file, either consult
  `channel().isWritable()` or put a `ChannelTrafficShapingHandler` on the
  connection's pipeline — being a Netty channel makes rate limiting somebody
  else's already-solved problem.

## Design notes

`STATUS.md` is a running engineering log: what is built, what is deliberately
deferred, how each piece is verified, and the bugs found along the way with what
they cost. The flow-window story in particular is the best argument in this
repository for testing against a real peer rather than against yourself. Worth
reading before changing the connection layer.

Where the two reference implementations disagree, or where Roast deviates from
both, `STATUS.md` records why.

## Licence

See the workspace root.
