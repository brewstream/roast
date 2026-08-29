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
methodology" below. `ReceiveBuffer` now also does TSBPD clock-drift correction,
fed by the same ACKACK path. Still missing: 32-bit wire-timestamp wraparound
handling and the entire sender-side path (Phase 4) — see "Known gaps."

137 tests passing (128 default + 1 gated interop + 2 ACKACK/RTT + 5 new
`DriftTracerTest` + 2 new `ReceiveBufferTest` drift cases), all committed to
`main` (no branches). Every commit so far has been asked-for explicitly by the
user, one narrowly-scoped piece at a time — see git log for the exact sequence
and rationale (commit messages are detailed).

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
  track yet. Exposes `onData`/`onLoss`/`onTlpktDrop`/`onClose` — see below — and
  `.metadata()` returning its `AcceptedConnection`.
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
  despite the wire format supporting it, neither does this), built directly on
  `LossList`'s state. RTT/RTTVar/buffer/rate figures are caller-supplied per
  `tick` — `SrtConnection` currently hardcodes them to 0, not measured yet.
  `nowMicros` must be elapsed time since this receiver's own start, matching
  gosrt's `lastPeriodicACK` zero-value-start semantics exactly.
- `ReceiveBuffer` (+ `DeliveryResult`) — holds accepted DATA packets sorted by
  sequence number, delivers whatever's contiguous-or-abandoned and past its
  TSBPD deadline (wire timestamp + a per-connection time base + negotiated
  latency + clock drift). Combines what DESIGN.md separately names
  `ReceiveBuffer` and `TsbpdDeliverer` (gosrt keeps them as one struct too).
  Implements TLPKTDROP: gives up on a stale gap once a later packet's deadline
  has passed rather than blocking delivery forever. Delivery deadlines are
  computed live at `deliver()`-check time (not frozen when a packet is
  buffered) — matches libsrt's `getPktTime()` model, so an already-buffered
  packet's deadline correctly shifts if drift/time-base changes while it's
  still waiting. No 32-bit wire-timestamp wraparound handling yet (correct
  under ~71 minutes) — documented in the class javadoc, not silent.
  `dispose()` releases undelivered buffered payloads on connection teardown.
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

**`handshake`** — `SynCookie`: MD5-based SYN cookie so a listener can verify an
INDUCTION cookie was echoed back correctly in CONCLUSION without keeping
per-attempt state, verified against gosrt's golden vector.
`ListenerHandshake` (+ `ConclusionOutcome`): the listener side of the
induction→conclusion exchange as **pure decision logic** — given a decoded
`HandshakeCif`, produces the `HandshakeCif` to send back. No socket I/O itself;
`SrtListener` is what actually calls it over a live channel.

**`harness`** (test-only) — `UdpLossProxy`: standalone UDP relay that randomly
drops packets in both directions, for exercising ARQ without OS-level netem.

**`interop`** (test-only) — `LibsrtInteropTest`: binds `SrtListener`, launches the
real `srt-live-transmit` binary as a subprocess against it, asserts on both sides
independently (our hooks fire with correct fields, *and* the peer's own stdout
confirms `"SRT target connected"`). Skips itself via `Assumptions` (build stays
green) if the binary isn't found — checks `$SRT_LIVE_TRANSMIT` env var first,
falls back to `references/srt/build/srt-live-transmit`.

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

## Known gaps / deliberately deferred

- **MSS/payload-size negotiation** when a peer advertises a smaller MTU than
  ours, and **Congestion Control extension** parsing/mismatch rejection — both
  skipped in `ListenerHandshake` for lack of a config object; noted inline there.
- **No `SrtConfig`** — `SrtListener` hardcodes 120ms latency (both directions)
  and SRT version `0x010401` (matching gosrt's own baseline).
- **Encryption** (KMREQ/KMRSP, PBKDF2, AES-CTR) — Phase 5 in DESIGN.md, untouched.
- **Caller-side handshake** (dial/connect flow) — only the listener side exists.
- ~~TLPKTDROP's interaction with ACK generation~~ **Closed** — `ReceiveBuffer`
  implements TLPKTDROP and feeds abandoned gaps back into `LossList.abandon(...)`
  via `SrtConnection`'s tick. Note it's a deliberate simplification of gosrt's
  exact mechanism, not a byte-for-byte port — see `ReceiveBuffer`'s javadoc and
  `ReceiveBufferTest`'s ported `TestSkipTooLate` for exactly where they diverge
  (gosrt runs a separate ACK-boundary-vs-delivery-boundary computation this
  codebase deliberately unifies into one).
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
- **No 32-bit wire-timestamp wraparound handling** in `ReceiveBuffer` —
  correct for connections under ~71 minutes; documented in its class javadoc,
  not silent. This is the remaining half of what used to be one bundled
  "drift + wraparound" gap.
- **KEEPALIVE's echo-on-receipt has no rate limit** — ported faithfully from
  gosrt's `handleKeepAlive`, which doesn't gate it either, but two peers that
  *both* echo immediately on receipt could in theory tight-loop forever (neither
  gosrt nor the RFC's own KEEPALIVE section impose a limit). Verified safe
  against libsrt (doesn't echo on receipt) — reconsider a rate limit before this
  codebase gets its own keepalive-originating caller/sender side.
- **The entire sender-side path (Phase 4)** — untouched. `SrtConnection`
  now handles DATA/KEEPALIVE/SHUTDOWN/ACKACK; anything else is logged and
  dropped. This is why a connection can currently receive but not send.

## Next steps, in order

1. 32-bit wire-timestamp wraparound handling in `ReceiveBuffer` — matters once
   a connection runs long enough (~71 minutes) to hit it.
2. The sender-side path (Phase 4) — send buffer, live-mode pacing, NAK-driven
   retransmission, ACK handling. Everything so far is receive-only; a connection
   can't send anything back yet.
3. *(Optional, low-priority)* Try `ffmpeg --enable-libsrt`'s own `srt://` muxer
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
