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

**Phase 3 (receiver path) has started**: loss detection/NAK generation and ACK
timing/variant decision logic exist (`recv` package below), grounded not just
against gosrt's source but against its actual test suite
(`congestion/live/receive_test.go`) — see "Known gaps" for what that surfaced.
Nothing in `recv` is wired to a live connection yet; `ReceiveBuffer`/TSBPD
delivery/TLPKTDROP don't exist, which is a real behavioral gap now documented
below, not just an unbuilt feature.

113 tests passing (112 default + 1 gated interop), all committed to `main` (no
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
  silent-drop, since it recovers if our own first reply was lost. **Registers no
  per-connection data sink** — a DATA packet for an accepted connection is logged
  and dropped by the demux's existing unknown-socket-id path, which is *correct*
  Phase 2 behavior (DESIGN.md: "connects... then fails gracefully at data stage"),
  not a bug to chase.
- `ConnectionRequest` / `AcceptDecision` / `AcceptHandler` / `AcceptedConnection`
  — the extensibility surface added per DESIGN.md's "Extensibility &
  observability" section: rich accept/reject (peer address, StreamID, SRT
  version, requested latency, encryption flag) and a connection-lifecycle hook,
  not just a bare accept/reject boolean. `AcceptedConnection` is connection
  *metadata* only — no read/write surface exists yet (needs Phase 3).

**`util`** — `CircularNumber`: wrap-aware comparator/arithmetic for 31-bit sequence
numbers and 32-bit timestamps (SRT wraps these on the wire), ported from gosrt's
`circular.Number`.

**`recv`** — the start of Phase 3, none of it wired to a live connection yet:
- `LossList` — tracks which received-stream sequence numbers are known missing;
  detects newly-opened gaps for immediate NAK, maintains still-missing ranges for
  periodic re-announcement, handles partial recovery (splitting a range from the
  front/back/middle) and the sequence-number wrap boundary.
- `NakGenerator` — thin wrapper turning `LossList` output into a wire-ready NAK
  `ControlPacket` via `LossListCodec`.
- `AckSender` — decides when to send an ACK and which variant (Full ~every 10ms,
  Light for every 64 packets in between — gosrt's own receiver never emits Small
  despite the wire format supporting it, neither does this), built directly on
  `LossList`'s state. RTT/RTTVar/buffer/rate figures are caller-supplied per
  `tick`, not measured here — that needs pieces that don't exist yet.
  `nowMicros` must be elapsed time since this receiver's own start, matching
  gosrt's `lastPeriodicACK` zero-value-start semantics exactly (see "Known gaps").

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

## Known gaps / deliberately deferred

- **MSS/payload-size negotiation** when a peer advertises a smaller MTU than
  ours, and **Congestion Control extension** parsing/mismatch rejection — both
  skipped in `ListenerHandshake` for lack of a config object; noted inline there.
- **No `SrtConfig`** — `SrtListener` hardcodes 120ms latency (both directions)
  and SRT version `0x010401` (matching gosrt's own baseline).
- **Encryption** (KMREQ/KMRSP, PBKDF2, AES-CTR) — Phase 5 in DESIGN.md, untouched.
- **Caller-side handshake** (dial/connect flow) — only the listener side exists.
- **TLPKTDROP's interaction with ACK generation is not implemented.** Found while
  grounding tests against gosrt's `receive_test.go` (`TestIssue67`): real SRT
  forces the ACK boundary to skip past a still-open, unrecovered gap once that
  gap's packets' TSBPD delivery deadline has passed — otherwise a single lost
  packet could stall ACK progress (and the sender's flow control) forever.
  `AckSender` has no TSBPD-deadline awareness at all, so right now `LossList`
  would keep an unrecovered gap outstanding indefinitely instead of the receiver
  eventually giving up on it. Documented in `LossListTest`/`AckSenderTest`
  (`matchesGosrt*` tests) rather than silently unhandled — needs a
  `ReceiveBuffer`/`TsbpdDeliverer` (which track per-packet delivery deadlines) to
  fix, so it's blocked on that piece, not forgotten.
- **ACKACK, TSBPD delivery/drift correction, the actual data path (`ReceiveBuffer`,
  `TsbpdDeliverer`), and the sender-side path entirely** — Phase 3/4, untouched.
  This is why an accepted connection can't yet send/receive anything; `LossList`/
  `AckSender`/`NakGenerator` exist but aren't wired to a live connection.

## Next steps, in order

1. **`ReceiveBuffer`/`TsbpdDeliverer`** — hold out-of-order data packet payloads,
   deliver them once their TSBPD deadline arrives (with drift correction), and
   implement TLPKTDROP so `AckSender`/`LossList` can stop waiting on a gap once
   it's truly too late — closes the known gap above. This is what actually lets
   an accepted connection produce data, not just complete a handshake.
2. Wire `LossList`/`AckSender`/`NakGenerator` and the new `ReceiveBuffer` into a
   live connection (a per-socket-ID `SrtPacketSink` registered with
   `SrtSocketIdDemultiplexer` once `SrtListener` accepts one — currently nothing
   is registered there at all). Design its event hooks against DESIGN.md's
   "Extensibility & observability" list from the start, not retrofitted after.
3. KEEPALIVE, SHUTDOWN, ACKACK.
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
