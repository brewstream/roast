# Roast — status snapshot

Written 2026-08-28, updated 2026-08-29, as a durable checkpoint in case a session
gets lost. See `CLAUDE.md` for conventions (package root, Netty decision, test
stack) and `../DESIGN.md` for the full protocol design/phase plan this follows
(module name predates "Roast" — it says `srt-core` there; also see its
"Extensibility & observability" section, added 2026-08-29 — read that before
touching the connection layer's public API again).

## Where we are

**Phase 2 (handshake) is essentially done** per DESIGN.md §5's phased plan and its
own stated definition of done: a real SRT peer reaches "connected" against
`SrtListener`. Confirmed 2026-08-29 against real **libsrt 1.5.7**
(`srt-live-transmit`, built from source at `roast/references/srt`) — induction,
cookie verification, StreamID/extension parsing, TSBPD negotiation, and the
accept/`onConnection` hooks all checked out correctly against an independent
implementation, not just our own code. This is `LibsrtInteropTest`
(`@Tag("interop")`, run via `./gradlew interopTest`), not the default `test` task
— see "Architecture decisions" below for why. Phase 1 (packet codec) is
functionally done for what's needed so far.

**Not yet tried:** `ffmpeg`'s own `srt://` muxer specifically (only libsrt's own
`srt-live-transmit` tool) — ffmpeg wraps the same libsrt handshake code, so this
is expected to work, but hasn't been literally exercised. Everything else is
still verified only against gosrt's golden vectors and our own round-trip tests.

**Phase 3 (receiver path) is now wired end-to-end**: `SrtConnection` owns
`LossList`/`AckSender`/`ReceiveBuffer` per accepted connection and drives them
over a real ~10ms tick on the connection's own Netty event loop — an accepted
connection can actually receive DATA, NAK a gap, ACK periodically, and TLPKTDROP
a stale gap, all verified over real sockets/timers in `SrtConnectionTest`. This
is also where DESIGN.md's "Extensibility & observability" hooks became real
rather than aspirational: `onData`/`onLoss`/`onTlpktDrop` on `SrtConnection`
(`SrtListener.onConnection` now hands out `SrtConnection`, not bare
`AcceptedConnection` — see "Architecture decisions" for the API-shape note).
KEEPALIVE (echoed back on receipt) and SHUTDOWN (tears down, sends our own
SHUTDOWN back, fires a new `onClose` hook) are now handled too. ACKACK is now
handled as well, unlocking real RTT/RTTVar tracking and an RTT-adaptive periodic
NAK interval (previously a fixed floor) — see "What's built" and "Testing
methodology" below. `ReceiveBuffer` now also does TSBPD clock-drift correction
and 32-bit wire-timestamp wraparound handling, both fed by/tied into the same
ACKACK/delivery-time machinery. **Phase 3 (receiver path) is feature-complete
per DESIGN.md's phased plan.**

**Phase 4 (sender path) is now underway**: `SendBuffer` (new `send` package)
is built, tested against gosrt's own `send_test.go`, and wired into
`SrtConnection` — a connection can now `write(ByteBuf)` data out, get it
retransmitted on an inbound NAK, have it acknowledged/pruned on an inbound
ACK, and reply with ACKACK on a Full ACK (feeding the *same* shared RTT
estimate the receive side already tracks). See "What's built" and "Known
gaps" for what's still missing (real bandwidth pacing beyond gosrt's own
informational model, message chunking/MSS, live stats, caller-side
handshake). Wiring this in surfaced and fixed a real bug in `SendBuffer`
itself — see "Testing methodology." **Confirmed against real libsrt 1.5.7**:
`LibsrtInteropTest.realListenerSendsDataToRealLibsrtCaller` has a real
`srt-live-transmit` call our listener and read data *we* write, asserting
byte-for-byte correctness — the actual DESIGN.md definition-of-done for
Phase 4's interop story, not just our own round-trip tests.

**Caller-side handshake is done**: `SrtCaller.connect(...)` connects *out* to
a peer's listener — new `CallerHandshake` (mirrors `ListenerHandshake`) plus
the Netty wiring. `SrtConnection` needed no protocol changes at all (it was
already role-agnostic); one small addition for channel lifecycle only — see
"What's built". Verified with a real round trip against Roast's own
`SrtListener` (data flowing both directions), plus rejection and timeout
paths. **This closes the last structural gap in the connection lifecycle** —
Roast can now both accept and initiate connections. Real interop confirming
our caller against libsrt/gosrt acting as *listener* hasn't been done yet —
see "Next steps."

**A real, confirmed correctness bug was found via manual interop testing**
(ffmpeg pushing a real, continuous MPEG-TS stream through a Roast listener at
realistic bitrate, via the new `RelayDemo` tool — the first time this
codebase has been exercised under sustained real throughput rather than a
handful of test packets). Root-caused via a `tcpdump` packet capture decoded
with our own `SrtPacket.decode` (see "Testing methodology" for the exact
method): `AckSender`'s ACK boundary stayed frozen at the start of the
*oldest* unresolved gap in `LossList` for the entire ~120ms TLPKTDROP window,
even while dozens of packets arrived cleanly behind it. **This specific bug
is now fixed** — `ReceiveBuffer.computeAckBoundary` decouples the ACK
boundary from the delivery boundary, matching gosrt's real two-boundary
design exactly (verified against `TestIssue67`, a real historical gosrt bug
fix for this identical failure mode — see "Testing methodology"). See "Known
gaps" for the full before/after story.

**However, re-running the `RelayDemo` + real-ffmpeg reproduction after the
fix still shows corruption, essentially unchanged.** A follow-up diagnostic
(decoding `DROPREQ` directly, no `tcpdump` needed) found `DROPREQ` arriving
only ~4-10ms after our *very first* NAK for a fresh loss — too fast to be
explained by ACK-boundary lag (we hadn't had time to send a follow-up ACK
yet). So the ACK-boundary fix was real and correct, but **not the dominant
cause** of the interop symptom — a second, distinct, not-yet-root-caused
issue remains, most likely genuine frequent packet loss on loopback itself
(JVM/event-loop scheduling jitter is the leading suspect; Netty socket-buffer
sizing and this codebase's own blocking I/O were both checked and ruled out
during the same investigation). **Not yet investigated further** — this is
the actual top priority now, ahead of any Phase 5/6 work; see "Next steps."

169 tests passing (128 default + 2 gated interop + 2 ACKACK/RTT + 5
`DriftTracerTest` + 2 `ReceiveBufferTest` drift + 4 `ReceiveBufferTest`
wraparound + 10 `SendBufferTest` + 5 `SrtConnectionTest` send-side + 11
`CallerHandshakeTest` + 3 `SrtCallerTest`), all committed to `main` (no
branches). Every commit so far has been asked-for explicitly by the user, one
narrowly-scoped piece at a time — see git log for the exact sequence and
rationale (commit messages are detailed).

## What's built

**`packet`** — the 16-byte common header (F-bit, data-packet fields, control-type
dispatch), as a sealed `SrtPacket` (`DataPacket`/`ControlPacket`), decode/encode
against a Netty `ByteBuf`. `SrtSocketId` value type. `ControlType` enum (all 9 SRT
control types). CIF bodies are carried as an opaque `ByteBuf` — this layer only
frames the fixed header.

**`packet.cif`** — parses two specific CIFs:
- `LossListCodec`/`LossRange` — NAK loss list (single/range sequence-number
  encoding), verified byte-for-byte against gosrt's `CIFNAK` golden vector.
- `HandshakeCif` (+ `HandshakeType`, `HandshakeExtension`,
  `HandshakeExtensionFlags`, `ExtensionType`, `RejectionReason`, `PeerAddressCodec`)
  — the 48-byte handshake base structure, plus HSREQ/HSRSP and SID extension TLVs.
  KMREQ/KMRSP (encryption) and Congestion Control extensions are recognized but
  skipped by declared length, not parsed. Verified against gosrt's
  `TestHandshakeV4`/`V5` golden vectors (with the KM/Congestion portions of V5
  stripped out, since those aren't parsed).
  `handshakeTypeCode` is a raw int, not a closed enum — any value outside the 5
  known progression types is a legitimate rejection-reason code, not malformed
  data (see `HandshakeCif.isRejection()`/`rejectionReason()`).
- `AckCif`/`AckVariant` — the ACK CIF's three wire variants (Lite 4B / Small 16B /
  Full 28B, determined by encoded length, not a marker field), verified against
  gosrt's `TestFullACK`/`TestSmallACK`/`TestLiteACK` golden vectors.

**`codec`** — `SrtFrameDecoder`/`SrtFrameEncoder`, Netty
`MessageToMessage(De|En)coder`s bridging `DatagramPacket` ↔ `SrtPacket`, preserving
sender/recipient via `AddressedEnvelope`. Malformed/undersized datagrams are
dropped, never thrown.

**`socket`** — the connection layer:
- `SrtSocketIdDemultiplexer` — routes inbound packets by destination socket ID to
  a registered `SrtPacketSink`; socket ID `0` routes to a separate "acceptor" sink.
- `SrtSocketIdGenerator` — random 32-bit socket ID allocation with collision retry
  and release-for-reuse.
- `SrtListener` — binds a real Netty `NioDatagramChannel` (pipeline:
  `SrtFrameDecoder` → `SrtFrameEncoder` → `SrtSocketIdDemultiplexer`) and drives
  `ListenerHandshake` through it. Writes replies by holding the bound `Channel`
  directly (`SrtPacketSink.onPacket` doesn't carry a `ChannelHandlerContext`, so
  changing that shipped interface wasn't worth it for this). Dedupes retried
  CONCLUSIONs (keyed by the peer's advertised socket ID) by resending a cached
  accept response rather than re-running accept logic — more robust than gosrt's
  silent-drop, since it recovers if our own first reply was lost. On accept,
  builds an `SrtConnection` and registers it with the demux for its assigned
  socket ID (used to just drop DATA packets here — that's now `SrtConnection`'s
  job); tracks all live connections and closes them in `SrtListener.close()`.
- `SrtConnection` — a live, accepted connection: owns `LossList`/`AckSender`/
  `ReceiveBuffer` for its lifetime, registered as its socket ID's `SrtPacketSink`,
  driven by a ~10ms tick on the connection's own Netty event loop (no
  synchronization needed — packet arrival and the tick both run on that one
  thread). Handles DATA, KEEPALIVE (echoed back on receipt — see "Known gaps"
  for a real ping-pong risk this inherits from gosrt, verified safe against
  libsrt specifically), and SHUTDOWN (`close()`: sends our own SHUTDOWN back
  unconditionally — mirroring gosrt's symmetric teardown, not a one-sided
  notification — then tears down; idempotent via an `AtomicBoolean` guard,
  since it's reachable both from a peer's SHUTDOWN on the event loop and from
  `SrtListener.close()` on an arbitrary thread). ACKACK is now handled: every
  Full ACK sent is recorded (ack number to send time in `pendingAcks`); the
  matching ACKACK's round trip becomes an RTT sample folded into a running
  RTT/RTTVar estimate via gosrt's exact EWMA (`connection.go`'s `rtt.Recalculate`:
  `rtt = rtt*0.875 + sample*0.125`, `rttVar = rttVar*0.75 + |rtt-sample|*0.25`,
  seeded at gosrt's own defaults 100ms/50ms). This feeds real RTT/RTTVar into
  `AckSender.tick` (previously hardcoded 0s) and makes the periodic NAK interval
  RTT-adaptive (`(rtt + 4*rttVar)/2`, floored at 20ms — gosrt's `NAKInterval()`),
  replacing the old fixed floor. Unknown/stale ACKACKs are logged and ignored,
  matching gosrt's tolerant behavior. Buffer/rate figures fed to `AckSender.tick`
  are still hardcoded to 0 — that needs receive-side stats this codebase doesn't
  track yet. Now also owns a `SendBuffer` (see `send` below) and exposes
  `write(ByteBuf)` — the actual data-send API, marshaled onto the connection's
  own event loop since it's the one method meant to be called from an arbitrary
  application thread (every other method here assumes the single event-loop
  thread). Inbound ACK prunes `SendBuffer`'s retransmit state for every ACK
  variant; a Full ACK additionally feeds its own reported RTT into the *same
  shared* RTT/RTTVar estimate ACKACK round trips update, and triggers an ACKACK
  reply — both gated to Full only, matching gosrt's `handleACK` exactly. Inbound
  NAK triggers retransmission via `SendBuffer.nak`. Both directions share the
  same initial sequence number (HSv5's handshake carries one such field,
  mirrored by both peers — confirmed in gosrt and in `ListenerHandshake`, which
  echoes the peer's own value back rather than generating a fresh one).
  Exposes `onData`/`onLoss`/`onTlpktDrop`/`onRetransmit`/`onClose` — see
  below — and `.metadata()` returning its `AcceptedConnection`. Fully
  role-agnostic: `SrtCaller` (below) constructs the exact same class, unchanged,
  for a dialed-out connection. The one addition made *for* that: a
  package-private 5-arg constructor overload carrying an `onChannelOwnerClose`
  callback, run as part of `close()`'s fixed internal teardown (not the
  app-facing `onClose` hook, which a caller-created connection's dedicated
  channel/event-loop-group cleanup can't safely share with an application's
  own `onClose(...)` call — that would silently clobber it and leak the
  channel). The public 4-arg constructor (still what `SrtListener` uses)
  delegates to it with a no-op.
- `SrtCaller` — connects *out* to a peer's listener: binds a dedicated
  ephemeral-port channel with the *same* pipeline `SrtListener.bind` uses
  (`SrtFrameDecoder`/`SrtFrameEncoder`/`SrtSocketIdDemultiplexer` don't care
  whether a channel talks to one peer or many), drives `CallerHandshake`
  (below) through it, and on success builds a plain `SrtConnection` — no
  protocol-layer changes needed there at all. `connect(InetSocketAddress,
  String)` returns a `CompletableFuture<SrtConnection>`; single-shot, no
  induction/conclusion retry on packet loss (matches gosrt's `dial.go`, which
  doesn't retry either — a known simplification vs. real libsrt, which does
  retry with backoff per spec). No HSv4 fallback (DESIGN.md defers that to
  Phase 7) and no `SrtConfig` yet, matching `SrtListener`'s existing
  hardcoded-defaults precedent.
- `ConnectionRequest` / `AcceptDecision` / `AcceptHandler` / `AcceptedConnection`
  — the extensibility surface added per DESIGN.md's "Extensibility &
  observability" section: rich accept/reject (peer address, StreamID, SRT
  version, requested latency, encryption flag) and a connection-lifecycle hook.
  `AcceptedConnection` stays pure *metadata* (peer info, negotiated latency) —
  `SrtListener.onConnection` hands out the richer `SrtConnection` (wraps it, adds
  the live hooks), not `AcceptedConnection` directly.

**`util`** — `CircularNumber`: wrap-aware comparator/arithmetic for 31-bit sequence
numbers and 32-bit timestamps (SRT wraps these on the wire), ported from gosrt's
`circular.Number`.

**`recv`** — Phase 3's receiver path, now wired to a live connection via
`SrtConnection` above:
- `LossList` — tracks which received-stream sequence numbers are known missing;
  detects newly-opened gaps for immediate NAK, maintains still-missing ranges for
  periodic re-announcement, handles partial recovery (splitting a range from the
  front/back/middle) and the sequence-number wrap boundary. `abandon(upTo)`
  clears a stale gap without it ever having been received — how `ReceiveBuffer`'s
  TLPKTDROP feeds back into loss tracking.
- `NakGenerator` — thin wrapper turning `LossList` output into a wire-ready NAK
  `ControlPacket` via `LossListCodec`.
- `AckSender` — decides when to send an ACK and which variant (Full ~every 10ms,
  Light for every 64 packets in between — gosrt's own receiver never emits Small
  despite the wire format supporting it, neither does this). No longer depends
  on `LossList` at all — the "last acknowledged" sequence number is now
  supplied by the caller on each `tick` (computed by `ReceiveBuffer.computeAckBoundary`,
  below), so this class is purely the timing/variant decision plus CIF
  construction. RTT/RTTVar/buffer/rate figures are also caller-supplied —
  `SrtConnection` currently hardcodes the buffer/rate ones to 0, not measured
  yet. `nowMicros` must be elapsed time since this receiver's own start,
  matching gosrt's `lastPeriodicACK` zero-value-start semantics exactly.
- `ReceiveBuffer` (+ `DeliveryResult`, `AckBoundaryResult`) — holds accepted
  DATA packets sorted by sequence number. Tracks **two separate boundaries**,
  matching gosrt's `lastACKSequenceNumber`/`lastDeliveredSequenceNumber` split
  exactly (a fix landed 2026-08-29 after real interop testing found the old
  single-boundary design starved a real peer's send buffer — see "Known gaps"
  and "Testing methodology"): `computeAckBoundary(nowMicros)` walks the buffer
  from the ACK boundary forward, skipping a packet whose own TSBPD deadline
  has already passed *even across a gap* (the actual TLPKTDROP "give up"
  decision lives here now) or advancing normally through contiguous packets —
  call once per tick, before `deliver`. `deliver(ackBoundary, nowMicros)` is
  now a purely mechanical hand-out, gated by both that boundary and each
  entry's own deadline; it no longer independently discovers gaps. Delivery
  deadlines are computed live at query time (not frozen when a packet is
  buffered) — matches libsrt's `getPktTime()` model, so an already-buffered
  packet's deadline correctly shifts if drift/time-base changes while it's
  still waiting. Also handles 32-bit wire-timestamp wraparound (`updateWrapPeriod`/
  `carryoverMicros`, ported from gosrt's `handlePacket` state machine /
  libsrt's `CTsbpdTime::updateBaseTime`): a full 2^32µs cycle is provisionally
  applied per-query once a packet's timestamp comes within 30s of wrapping,
  then permanently folded into the time base once a later packet confirms the
  wrap actually happened — composes correctly with drift correction since
  both now live in the same per-query formula. `dispose()` releases
  undelivered buffered payloads on connection teardown.
- `DriftTracer` — median-based clock-drift estimator, ported from libsrt's
  generic `DriftTracer<MAX_SPAN, MAX_DRIFT, CLEAR_ON_UPDATE=true>` template
  (`utilities.h`), specialized to libsrt's own constants (1000-sample span,
  ±5000µs clamp). `ReceiveBuffer.addDriftSample(...)` (a faithful port of
  libsrt's `CTsbpdTime::addDriftSample`) feeds it a sample on every ACKACK,
  called from `SrtConnection.handleAckAck` with the ACKACK's own header
  timestamp, local arrival time, and the raw (unsmoothed) RTT sample for that
  exchange; a no-op before `ReceiveBuffer`'s TSBPD time base exists. libsrt's
  `update()`-then-separately-read-`overdrift()` two-step protocol (fragile —
  only valid if read immediately after `update()` returns true) is collapsed
  into a single `OptionalLong` return from `update()` — same information, a
  deliberate documented API simplification, not a behavior change.

**`send`** — Phase 4's sender path, now wired to a live connection via
`SrtConnection` above:
- `SendBuffer` — the sending side's counterpart to `LossList`/`ReceiveBuffer`
  combined, ported directly from gosrt's `congestion/live.sender` (`send.go`).
  `push`/`tick`/`ack`/`nak`/`flush`: queues outgoing DATA packets, delivers
  whatever's due (gosrt's own "pacing" is informational only — nothing spaces
  packets out beyond each one's own scheduled send time, traced directly from
  gosrt's source rather than assumed), prunes retransmit state on ACK,
  retransmits on NAK, and implements sender-side TLPKTDROP (the other half of
  the mechanism — `ReceiveBuffer` implements the receiver's). Every delivery —
  first send or retransmit alike — hands out a `retainedDuplicate()`, never
  the entry actually held in its internal queues, so the original stays valid
  for a possible later retransmit (see "Testing methodology" for the bug this
  fixes). The 16th/17th-packet bandwidth-probe trick and full bandwidth-rate
  statistics (gosrt's `Stats()`) are deliberately not ported — see known gaps.

**`handshake`** — `SynCookie`: MD5-based SYN cookie so a listener can verify an
INDUCTION cookie was echoed back correctly in CONCLUSION without keeping
per-attempt state, verified against gosrt's golden vector.
`ListenerHandshake` (+ `ConclusionOutcome`): the listener side of the
induction→conclusion exchange as **pure decision logic** — given a decoded
`HandshakeCif`, produces the `HandshakeCif` to send back. No socket I/O itself;
`SrtListener` is what actually calls it over a live channel.
`CallerHandshake` (+ `ConclusionReplyOutcome`): the caller-side mirror, same
pure-decision-logic contract, ported from gosrt's `dial.go` — builds the
induction/conclusion requests, validates the induction reply (HSv5 only) and
the conclusion reply against the same capability checklist
`ListenerHandshake.validateConclusion` runs, from the caller's side.
`ConclusionReplyOutcome` adds a third case beyond `ConclusionOutcome`'s
`Valid`/`Rejected`: `ProtocolViolation` — a caller receiving a broken reply
has no universal "send a rejection back" move the way a listener does.
`SrtCaller` is what actually calls it over a live channel.

**`harness`** (test-only) — `UdpLossProxy`: standalone UDP relay that randomly
drops packets in both directions, for exercising ARQ without OS-level netem.

**`cli`** (manual tool, not part of the build's test suite) — `RelayDemo`:
binds one `SrtListener`, treats any connection whose StreamID starts with
`publish/` as the source and every other connection as a player, relaying
whatever the source sends to all currently-connected players as-is. Run via
`./gradlew relayDemo` (`-Pport=<n>` to override the default 9000). This is
what surfaced the ACK-boundary bug above — pushing a real ffmpeg stream
through it at realistic bitrate was the first time this codebase saw
sustained real throughput rather than a handful of test packets. A natural,
minimal early prototype of DESIGN.md's eventual Phase 6
`srt-java-live-transmit` CLI, though not held to this codebase's usual rigor
(no design-note javadoc, no tests, by design — see the class's own doc
comment).

**`interop`** (test-only) — `LibsrtInteropTest`: binds `SrtListener`, launches the
real `srt-live-transmit` binary as a subprocess against it. Two directions:
`realLibsrtCallerReachesConnected` — libsrt pushes data to us, asserts on both
sides independently (our hooks fire with correct fields, *and* the peer's own
stdout confirms `"SRT target connected"`); `realListenerSendsDataToRealLibsrtCaller`
— we push data (`SrtConnection.write`) to libsrt, asserting byte-for-byte on
what it actually received (libsrt's own log/verbose output goes to stderr by
default, stats-to-stdout is opt-in and left off, so stdout redirected straight
to a file is guaranteed clean of anything but the raw bytes — confirmed
directly from libsrt's own source, not assumed). Both skip themselves via
`Assumptions` (build stays green) if the binary isn't found — checks
`$SRT_LIVE_TRANSMIT` env var first, falls back to
`references/srt/build/srt-live-transmit`.

## Architecture decisions in force

- Package root `org.brewstream.roast` (Gradle group `org.brewstream`), not
  `io.github.brewstream` — renamed early on.
- Transport is built on **Netty** (4.2.17.Final), not raw NIO — an explicit
  engineering call, not DESIGN.md's original "Option A vs B" (that section of
  DESIGN.md has since been reworded from an open choice to stating the decision).
  Netty 4.2 deprecates `NioEventLoopGroup` in favor of
  `new MultiThreadIoEventLoopGroup(NioIoHandler.newFactory())` — `SrtListener`
  uses the modern form; watch for this if adding more Netty bootstrap code.
- DESIGN.md's **"Extensibility & observability"** section (§4, added 2026-08-29)
  is now a first-class design constraint on the connection layer, not a Phase 6
  afterthought — this is BrewStream's actual value proposition over a libsrt
  binding (see the connection request/lifecycle hooks above). Read it before
  changing `SrtListener`'s public API or designing the Phase 3 data path's hooks.
- Tests: JUnit 5 + **AssertJ** for assertions (not `Assertions.assertEquals` etc.)
  + **Mockito** for collaborator mocks. `SrtListenerTest` and `LibsrtInteropTest`
  are the exceptions to "plain synchronous assertions" — real socket I/O (and, for
  the latter, a real subprocess) on async threads, so callback/output assertions
  go through a `CompletableFuture` / a joined reader thread.
- **Interop tests are gated separately.** `build.gradle`'s default `test` task
  excludes `@Tag("interop")`; a dedicated `interopTest` task runs those. Don't add
  a test that spawns an external process or depends on a locally-built binary to
  the default `test` task — tag it `interop` instead, and make it skip via
  `Assumptions` (not fail) when its prerequisite isn't present.
- Reference implementations cloned locally at `roast/references/` (gitignored,
  never pushed): `gosrt` (Go, MIT — the primary structural/golden-vector
  reference), `srt-rfc` (Haivision's actively-maintained spec source, supersedes
  the expired 2021 IETF `draft-sharabayko-srt-01` datatracker snapshot — SRT
  never became a formal RFC), and `srt` (Haivision's libsrt, MPL-2.0 — built from
  source, `cmake -DENABLE_APPS=ON .. && make` in `references/srt/build/`, gives
  `srt-live-transmit` for interop testing; needs OpenSSL and a C++ toolchain,
  both present via Homebrew on this machine).
- `SrtPacket.encodeTo`/CIF `encodeTo` methods **consume and release** their
  `ByteBuf` body — a packet that needs sending again (ARQ retransmit) must
  `retainedDuplicate()` into a fresh packet first, not reuse an encoded one.
- **Ground tests against the reference's actual test files, not just its source**
  — reading gosrt's source and designing your own scenarios from that reading is
  not the same guarantee as porting gosrt's own test assertions. See "Testing
  methodology" below for what this caught.

## Testing methodology

- **Wire-format pieces** (`CircularNumber`, `LossListCodec`, `HandshakeCif`,
  `SynCookie`, `AckCif`) are verified byte-for-byte against gosrt's own golden hex
  vectors — real cross-implementation proof, not just internal round-trips.
- **Decision-logic pieces** (`ListenerHandshake`, `SrtSocketIdGenerator`,
  `LossList`, `AckSender`) started out tested only against self-designed scenarios
  based on reading gosrt's source. `LossList`/`AckSender` have since also been
  cross-checked against gosrt's actual `congestion/live/receive_test.go` (see
  `matchesGosrt*` tests in `LossListTest`/`AckSenderTest`) — `ListenerHandshake`
  and `SrtSocketIdGenerator` have **not** been re-checked against a corresponding
  gosrt test file yet (worth doing before trusting them as strongly as the ported
  pieces).
- **Live socket I/O** (`SrtListener`) is verified against real libsrt via
  `LibsrtInteropTest` — independent proof beyond any Go reference.
- **RTT measurement / ACKACK handling** (`SrtConnection.handleAckAck`,
  `recalculateRtt`, `nakIntervalMicros`) has **no gosrt test to ground against** —
  checked deliberately, not an oversight. gosrt's `rtt` struct (`connection.go`)
  is unexported with zero dedicated unit tests; the receive-side congestion layer
  (`congestion/live`, our `LossList`/`AckSender` equivalent) takes
  `PeriodicNAKInterval` as an externally-injected fixed value and never computes
  it (see `TestRecvPeriodicNAK`) — the RTT-adaptive math lives only in
  `connection.go`, gosrt's own connection-wiring layer, our `SrtConnection`'s
  counterpart. The only place it's exercised at all in gosrt is
  `TestLowRateACKOverhead`, a full-duplex `Dial`+`Server` integration test
  measuring ACK-suppression traffic volume over wall-clock time — not portable
  here yet since it needs a working caller/sender side (Phase 4, not built).
  So `ackAckUpdatesRttReportedInSubsequentFullAcks`/
  `ackAckWithUnknownAckNumberIsIgnoredWithoutCrashing` are self-designed against
  the formula/behavior read from `connection.go`'s source — same rigor tier as
  `ListenerHandshake`/`SrtSocketIdGenerator` above, not the stronger
  ported-scenario tier.
- **Drift correction** (`DriftTracer`, `ReceiveBuffer.addDriftSample`) has
  **no reference test to ground against, in either implementation** — checked
  deliberately. gosrt's drift support is dead code (a field declared, never
  assigned — see "Architecture decisions"/"Known gaps"), so there's nothing to
  port from there at all. libsrt is the only real implementation
  (`srtcore/tsbpd_time.{h,cpp}`, `srtcore/utilities.h`'s `DriftTracer`), but
  its own `test/` directory has zero drift- or tsbpd-related unit tests —
  checked directly, not assumed. So `DriftTracerTest` and the two new
  `ReceiveBufferTest` drift cases are self-designed directly against libsrt's
  source, same rigor tier as the ACKACK/RTT work above.
- **32-bit wire-timestamp wraparound** (`ReceiveBuffer.updateWrapPeriod`,
  `carryoverMicros`) also has **no reference test to ground against, in either
  implementation** — checked directly (gosrt's `*_test.go` files, libsrt's
  `test/` directory — nothing wrap/tsbpd-related in either), unlike drift this
  time both references actually *implement* the behavior, just without unit
  coverage. The four new `ReceiveBufferTest` wraparound cases are self-designed
  against both sources; one of them verifies the property most worth
  double-checking directly — a packet buffered while the wrap was only
  suspected lands on the exact same deadline once a later packet confirms it,
  with no discontinuity across that transition.
- **`SendBuffer`** is grounded at the strongest tier this codebase uses: gosrt
  has real dedicated tests (`congestion/live/send_test.go`) and all five are
  ported directly (`SendBufferTest`), unlike RTT/drift/wraparound above, where
  neither reference had any coverage to port from.
- **Wiring `SendBuffer` into `SrtConnection` surfaced a real bug**, not just a
  wiring detail: `tick()`'s first delivery used to hand the `deliver` callback
  the same `DataPacket`/`ByteBuf` object retained internally for
  retransmission — harmless in `SendBufferTest`'s no-op callbacks (nothing
  ever actually encoded/released anything), but once wired to a real Netty
  encoder, that buffer gets consumed/released the moment it's actually sent —
  so a later NAK-triggered retransmit calling `retainedDuplicate()` on that
  already-released buffer would throw `IllegalReferenceCountException`. Fixed
  by having every delivery, first send included, hand out a fresh duplicate
  (symmetric with how retransmits already worked) — see `SendBuffer`'s
  javadoc. `SendBufferTest.retransmitAfterARealSendDoesNotReuseAnAlreadyReleasedBuffer`
  is a regression test that fails without the fix; `SrtConnectionTest`'s
  `nakOnUnacknowledgedPacketTriggersRetransmit` exercises the real path (an
  actual Netty encoder in the loop) end-to-end.
- **`CallerHandshake`** has no gosrt test file to port from — checked
  directly: `dial_test.go`'s tests (`TestDialOK`, `TestDialReject`,
  `TestDialV4`/`V5`/`V5Pre130`/`V5MissingExtension`,
  `TestDialWithContextCancel`) are integration-style against a hand-rolled
  fake listener, not narrow unit tests of an isolated piece the way
  `send_test.go` was for `SendBuffer`. `CallerHandshakeTest` is self-designed
  against `dial.go`'s source — same rigor tier as `ListenerHandshake`'s own
  tests (still not itself re-checked against a gosrt test file either, per
  the entry above). `SrtCallerTest`, by contrast, verifies a real round trip
  against Roast's own `SrtListener` in-process — arguably a stronger
  integration proof than gosrt's own `dial_test.go` gets, since it exercises
  two independently-built real Roast components together rather than one
  real piece plus a fake.
- **A new methodology precedent, 2026-08-29**: the ACK-boundary bug above was
  found and root-caused via manual, sustained-real-throughput interop testing
  (`RelayDemo` + real ffmpeg) — the first time this codebase was exercised
  beyond a handful of test packets — and confirmed at the wire level via a
  `tcpdump -i lo0 -w file.pcap` capture, decoded with a temporary tool that
  reused our own `SrtPacket.decode`/`AckCif.decode`/`LossListCodec.decode`
  directly against the raw captured bytes (not manual hex reading, not a
  third-party SRT dissector). That tool was throwaway and has been removed
  after use, but the technique — capture on loopback, decode with our own
  codec — is worth remembering as the way to get wire-level ground truth
  when app-level logging isn't conclusive enough on its own.
- **`ReceiveBuffer.computeAckBoundary`** (the ACK-boundary fix above) is
  grounded at the strongest tier this codebase has used for a piece this
  architecturally significant: `matchesGosrtTestIssue67` in
  `ReceiveBufferTest` is a direct port of gosrt's `TestIssue67`
  (`congestion/live/receive_test.go`) — a real, historical gosrt bug fix for
  this exact failure category (ack boundary stuck behind a gap), not a
  self-designed scenario. `ackAndDeliveryBoundariesConvergeWithNoGap` is
  inspired by gosrt's `TestRecvDropTooLate`, which asserts
  `lastACKSequenceNumber`/`lastDeliveredSequenceNumber` as genuinely
  distinct fields — confirming the two-boundary model itself, not just one
  bug scenario in it.
- **The user directly challenged a piece of the diagnostic code during this
  investigation** ("did you write bad netty code? are those log lines
  synchronous on the main event loop?"). The honest answer was yes for one
  early version — a first `ReceiveDumpDemo` draft called
  `FileOutputStream.flush()` per packet inside `onData`, a real
  synchronous-disk-I/O-on-the-event-loop anti-pattern — already caught and
  fixed before the question was asked, but the challenge was answered
  precisely rather than deflected, and cross-checked against the fact that
  the *original*, always-blocking-I/O-free `RelayDemo` showed the same
  magnitude of loss, which is what actually rules out the diagnostic code as
  the (sole) cause. Both temporary diagnostic tools built for this
  investigation (`ReceiveDumpDemo`, `PcapAnalyzer`) were thrown away after
  use, per the established "no scope creep beyond what's asked" discipline.

## Known gaps / deliberately deferred

- **MSS/payload-size negotiation** when a peer advertises a smaller MTU than
  ours, and **Congestion Control extension** parsing/mismatch rejection — both
  skipped in `ListenerHandshake` for lack of a config object; noted inline there.
- **No `SrtConfig`** — `SrtListener` hardcodes 120ms latency (both directions)
  and SRT version `0x010401` (matching gosrt's own baseline).
- **Encryption** (KMREQ/KMRSP, PBKDF2, AES-CTR) — Phase 5 in DESIGN.md, untouched.
- ~~Caller-side handshake~~ **Closed** — `SrtCaller`/`CallerHandshake`; see
  "What's built" and "Testing methodology". No HSv4 fallback and no
  induction/conclusion retry-with-backoff, both matching gosrt's own
  `dial.go` (deferred, not gaps introduced beyond the reference).
- ~~The ACK boundary stays frozen behind the oldest unresolved gap for the
  full TLPKTDROP window, starving the peer's send buffer~~ **Closed,
  2026-08-29** — `ReceiveBuffer.computeAckBoundary` now ports gosrt's real
  two-boundary design (separate `lastAcked`/`lastDelivered`, walked every
  tick with TLPKTDROP's "give up across a gap" decision moved into the ACK
  walk itself), verified against `TestIssue67` (a real historical gosrt bug
  fix for this identical failure mode) — see "What's built" and "Testing
  methodology". Root-caused via real interop testing (`RelayDemo` + real
  ffmpeg, `tcpdump` capture decoded with our own codec — see "Where we are").
  **However, re-running the same real-ffmpeg reproduction after this fix
  still shows corruption, essentially unchanged** — this fix was real and
  independently verified, but turned out not to be the dominant cause of the
  interop symptom. See the next entry.
- **NEW, top priority: a second, distinct, not-yet-root-caused source of
  real data loss/corruption under sustained real throughput.** Found
  2026-08-29 while re-verifying the ACK-boundary fix above: pushing a real
  ffmpeg stream through `RelayDemo` still shows MPEG-TS corruption at a
  similar rate to before the fix. A follow-up diagnostic (decoding `DROPREQ`
  control packets directly off the wire, no `tcpdump` needed this time)
  found libsrt replying `DROPREQ` only ~4-10ms after *our very first* NAK
  for a fresh loss — far too fast to be explained by ACK-boundary lag, since
  no follow-up ACK could even have been sent in that window. This rules out
  the just-fixed bug as the (sole) explanation and points at something more
  fundamental: the leading hypothesis is genuine, frequent packet loss on
  loopback itself, caused by JVM/event-loop scheduling jitter (a GC pause or
  a delayed tick leaving the socket's receive buffer to fill and drop under
  real, sustained throughput — never exercised before `RelayDemo`). Already
  checked and ruled out during the same investigation: Netty `SO_RCVBUF`/
  `MAX_MESSAGES_PER_READ` tuning (no effect), and this codebase's own
  diagnostic code introducing blocking I/O on the event loop (checked
  directly per the user's own challenge — see "Testing methodology" — and
  confirmed not the cause, since the original unmodified `RelayDemo` shows
  the same magnitude of loss). **Not yet root-caused** — needs its own
  investigation pass before Phase 5/6 work; see "Next steps."
- ~~No RTT measurement~~ **Closed** — `SrtConnection` now tracks real RTT/RTTVar
  from ACK/ACKACK round trips and feeds them to `AckSender.tick` and the
  periodic NAK interval; see "What's built" and "Testing methodology" (the
  latter for the honest caveat that this piece has no gosrt test to ground
  against). Buffer/rate figures fed to `AckSender.tick` are still hardcoded to 0.
- ~~No drift correction~~ **Closed** — `ReceiveBuffer` now does TSBPD
  clock-drift correction via `DriftTracer`, fed on every ACKACK; see "What's
  built" and "Testing methodology" (the latter for the honest caveat that
  neither reference has test coverage for this piece — gosrt's own drift
  support is dead code).
- ~~No 32-bit wire-timestamp wraparound handling~~ **Closed** — `ReceiveBuffer`
  now handles it (`updateWrapPeriod`/`carryoverMicros`); see "What's built"
  and "Testing methodology" (the latter for the no-reference-test caveat —
  both gosrt and libsrt implement this, but neither has unit coverage for it).
  This closes the last item under `ReceiveBuffer`'s known gaps — Phase 3 (the
  receiver path) is now feature-complete per DESIGN.md.
- **KEEPALIVE's echo-on-receipt has no rate limit** — ported faithfully from
  gosrt's `handleKeepAlive`, which doesn't gate it either, but two peers that
  *both* echo immediately on receipt could in theory tight-loop forever (neither
  gosrt nor the RFC's own KEEPALIVE section impose a limit). Verified safe
  against libsrt (doesn't echo on receipt) — reconsider a rate limit before this
  codebase gets its own keepalive-originating caller/sender side.
- ~~The entire sender-side path (Phase 4)~~ **Underway** — `SrtConnection` now
  handles DATA/KEEPALIVE/SHUTDOWN/ACK/NAK/ACKACK and can `write(...)` data
  out; see "What's built". Remaining pieces of Phase 4, still deferred:
  - **Real bandwidth pacing** — `SendBuffer` matches gosrt's own model, which
    is informational only (nothing spaces packets out beyond each one's own
    scheduled send time); actual rate-limited output, if ever needed, isn't
    implemented by gosrt's "live" congestion control either.
  - **Message chunking/MSS** — `write(ByteBuf)` sends exactly one DATA packet
    per call, no splitting; matches the existing "no MSS negotiation" gap.
  - **Full send-side stats** (gosrt's `Stats()`: `estimatedInputBW`/
    `estimatedSentBW`/`pktLossRate`) and the 16th/17th-packet bandwidth-probe
    trick — both deliberately not ported, see `SendBuffer`'s javadoc.
  - **ACK-sent/received hooks and live pollable stats** generally — still
    just `onRetransmit` added this pass, matching how `onData`/`onLoss`/
    `onTlpktDrop` were rolled out incrementally too.
  ~~Real interop for the send path~~ **Closed** — see `LibsrtInteropTest`'s
  `realListenerSendsDataToRealLibsrtCaller` above; didn't need the
  caller-side handshake first, since Roast only needed to be the *listener*
  here, with libsrt calling in and reading.

## Next steps, in order

1. **Root-cause the second, still-open real-throughput loss/corruption
   issue** (see "Known gaps" — now the top priority, ahead of any Phase 5/6
   work, since the ACK-boundary bug that was previously #1 here is now
   fixed). Leading hypothesis: genuine loopback packet loss from
   JVM/event-loop scheduling jitter under sustained real throughput, not a
   protocol-logic bug like the one just closed. Netty socket-buffer tuning
   and this codebase's own blocking I/O are already ruled out. Needs its own
   investigation pass — likely more `tcpdump`/wire-level diagnosis, or
   instrumenting actual tick-to-tick timing on the event loop to check the
   jitter hypothesis directly — before it can be scoped as a fix the way the
   ACK-boundary bug was.
2. Real interop confirming `SrtCaller` against libsrt/gosrt acting as
   *listener* — needs `srt-live-transmit` launched with `mode=listener` in its
   URI (the existing interop tests always run it as caller). The connection
   lifecycle's structural gaps are otherwise closed; this is proof against an
   independent implementation, matching how every other piece here eventually
   got that treatment.
3. With both `SrtListener`/`SrtCaller` and full send/receive paths in place,
   the natural next major milestone is DESIGN.md's Phase 6 (multiplexing &
   polish) or Phase 5 (encryption) — worth a deliberate choice with the user
   rather than assumed, since both are substantial and neither is blocking
   the other.
4. *(Optional, low-priority)* Try `ffmpeg --enable-libsrt`'s own `srt://` muxer
   against `SrtListener`, for full belt-and-suspenders confidence beyond
   `srt-live-transmit` — not expected to surface anything new, since ffmpeg
   wraps the same libsrt handshake code already exercised.

## How to pick this back up

Read this file, then `CLAUDE.md`, then skim `git log --oneline` (commit messages
carry real rationale, not just "what changed"). `DESIGN.md` at the workspace root
still governs overall phase order and protocol references, including the
extensibility/hooks design constraint added 2026-08-29 — it predates this
implementation work in its original form, but has been actively edited alongside
it since, so treat it as current, not frozen.
