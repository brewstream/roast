# Roast

A pure-Java implementation of [SRT](https://github.com/Haivision/srt) (Secure
Reliable Transport), built on Netty. Part of **BrewStream**.

Roast is aimed at applications that need to *route* live streams, not just carry
them: loss, retransmission, jitter and key rotation are all observable through a
first-class API, rather than hidden behind an opaque socket. That is the main
reason to use it over a JNI binding to libsrt.

**Status:** the protocol is complete and verified against real libsrt —
handshake, ARQ, TSBPD delivery with drift correction, encryption with key
rotation, and multiplexing. It has not yet been run in production by anyone.

## Requirements

Java 21 or newer. No runtime dependencies beyond Netty.

## Receiving a stream

```java
SrtListener listener = SrtListener.bind(new InetSocketAddress(9000));

listener.setAcceptHandler(request ->
        request.streamId().startsWith("live/")
                ? AcceptDecision.accept()
                : AcceptDecision.reject(RejectionReason.FORBIDDEN));

listener.onConnection(connection -> connection.onData(payload -> {
    // One MPEG-TS chunk, in order, after TSBPD buffering.
    // The handler owns the buffer.
    handle(payload);
    payload.release();
}));
```

`onConnection` runs before the accept response is sent, so a peer's first
packets cannot arrive before your handler exists.

## Sending a stream

```java
SrtConnection connection = SrtCaller
        .connect(new InetSocketAddress("example.com", 9000), "live/camera-1")
        .get();

connection.write(Unpooled.wrappedBuffer(chunk));
```

One `write` is one packet. Live mode does not split messages, so a payload
larger than `connection.metadata().maxPayloadSize()` (1456 bytes at the usual
MTU) is rejected rather than silently fragmented — chunk it yourself.

## Encryption

The passphrase is decided per connection, not per socket, so a relay can hold a
different secret per stream:

```java
// Listener: decide per request, with the StreamID in hand
listener.setAcceptHandler(request ->
        AcceptDecision.accept(passphraseFor(request.streamId()), 16));

// Caller: the side that generates the keys
SrtCaller.connect(address, "live/camera-1", passphrase, 16);
```

A peer whose passphrase does not match is rejected during the handshake
(`BADSECRET`), not left to send data nobody can read. Keys rotate mid-stream on
their own; `SrtConfig` controls how often.

## Observing a connection

Two mechanisms, because they answer different questions.

**Events** — things that happened, delivered to any number of listeners:

```java
listener.addEventListener(new SrtConnectionListener() {
    @Override public void onConnected(SrtConnection c) { ... }
    @Override public void onLoss(SrtConnection c, LossRange range) { ... }
    @Override public void onTlpktDrop(SrtConnection c, LossRange range) { ... }
});
```

**Statistics** — a snapshot you poll, shaped for a metrics backend:

```java
StatsSampler.start(Duration.ofSeconds(1), listener, (connection, stats) -> {
    Tags tags = Tags.of("stream", connection.metadata().streamId());
    registry.gauge("srt.rtt.micros", tags, stats.rttMicros());
    registry.gauge("srt.retransmit.rate", tags, stats.retransmitRate());
});
```

Roast depends on no metrics library; a Micrometer binding is the few lines
above, in your own code.

Events never run on a connection's event loop — a listener that blocks cannot
stall packet processing. The trade is that events are not ordered against
`onData`, and that a listener falling far behind will have events dropped
(counted in `ConnectionStats.droppedEvents()`). High-frequency activity is
counted rather than announced: there is no event per ACK.

## Configuration

```java
SrtListener.bind(address, SrtConfig.defaults()
        .withLatency(Duration.ofMillis(200))
        .withMaxMss(1200));
```

Six settings: latency, connect timeout, flow window, key-rotation schedule,
advertised SRT version, and max MSS. Protocol timing — tick interval, NAK
interval, handshake retry — is deliberately not configurable: those are fixed by
the protocol or derived from measured RTT, and changing them breaks
interoperability rather than tuning anything.

## Command line

`srt-java-live-transmit` mirrors libsrt's tool of the same name, so command
lines usually move between the two unchanged:

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
and the pipes above work as written. (Run it through `./gradlew` and they don't
— Gradle writes its own progress to stdout, which would corrupt the media.)

## Building and testing

```sh
./gradlew test          # unit and integration tests, no external dependencies
./gradlew interopTest   # against real libsrt and ffmpeg; skips itself if absent
```

The interop suite runs Roast against a locally built `srt-live-transmit` and
against `ffmpeg`, in both directions, encrypted and not. It skips rather than
fails when those binaries are missing. See `references/` (gitignored) for
building libsrt.

## What is not implemented

- **Rendezvous mode**, **HSv4 fallback**, and **message/file transfer modes** —
  live mode only.
- **Message chunking.** One `write` is one packet.
- **Congestion Control extension** parsing, and MSS negotiation down to a
  smaller peer MTU.
- **Bandwidth pacing.** Matches gosrt, whose live congestion control also
  computes rates without enforcing them.

## Design notes

`STATUS.md` is a running engineering log — what is built, what is deliberately
deferred, how each piece is verified, and the bugs found along the way with what
they cost. Worth reading before changing the connection layer.

Roast is developed against two reference implementations:
[gosrt](https://github.com/datarhei/gosrt) (Go, MIT) for structure and golden
vectors, and [libsrt](https://github.com/Haivision/srt) (MPL-2.0) as the
interoperability target. Where the two disagree, or where Roast deviates from
both, `STATUS.md` records why.

## Licence

See the workspace root.
