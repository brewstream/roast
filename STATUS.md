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
SHUTDOWN back, fires a new `onClose` hook) are now handled too, as is
**DROPREQ**: a peer's "I've given up on these packets" immediately advances the
acknowledgement boundary instead of letting the stream stall until those
packets' own deadlines expire. Follows libsrt's `processCtrlDropReq` — gosrt
marks the type unimplemented — **including its validation**, which matters more
than the feature: libsrt's own comment calls a reversed range a DoS primitive
(its buffer walk wraps and clears nearly everything held), and this arrives
unauthenticated from a peer. ACKACK is now
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
Roast can now both accept and initiate connections. **Confirmed against real
libsrt 1.5.7 as of 2026-08-29**: `LibsrtInteropTest.`
`realCallerSendsDataToRealLibsrtListener` dials out to a real
`srt-live-transmit` running `mode=listener` and asserts byte-for-byte on what
it received. That was the last piece verified only against Roast's own
`SrtListener` — i.e. two halves of the same codebase agreeing with each other
— so **both roles are now proven against an independent implementation, in
both data directions**.

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
cause** of the interop symptom — a second, distinct issue remains, and at
this point became the top investigative priority (see below for how that
investigation actually went, and "Next steps" for where it landed).

**2026-08-29, later the same day: a much more thorough elimination pass**,
prompted by direct challenge ("are you sure about them?") to verify every
ruled-out cause with a real measurement rather than a plausible-sounding
guess. Two control experiments first: the identical `ffmpeg` source relayed
through **libsrt's own `srt-live-transmit`** (as a pure two-port relay, no
Roast in the loop) and separately through **gosrt's own `contrib/server`**
(built locally via `brew install go`, a real gosrt-based pub/sub relay, same
architecture as `RelayDemo`) both play back **clean** — ruling out "ffplay is
just being nosy" and ruling out "the no-real-pacing design itself is the
cause" (gosrt's own `congestion/live/send.go` `Tick()` was read directly this
session and confirmed to blast every ripe packet in one pass with no
enforced pacing either — the same design `SendBuffer` is ported from — yet
it doesn't reproduce this). The bug is real and specific to Roast.

From there, every plausible cause was checked with direct evidence, not
assumed, and each is now genuinely ruled out:
- **OS-level network loss/corruption** — `netstat -s -p udp`'s full counter
  set (not just one field) diffed before/after a repro run: `datagrams
  received` and `delivered` matched exactly (+33,272 each), and `bad
  checksum`/`bad data length field`/`dropped due to full socket buffers` all
  stayed flat at baseline. Zero.
- **SRT-protocol-level loss** — both `SrtConnection`'s own bookkeeping
  (receive/deliver/send counters, `LossList.outstanding()`) *and* an
  independent real receiver (`srt-live-transmit` with `-statsout`/`-f`, i.e.
  libsrt's own `pktRcvLoss`/`pktRcvDrop`/`pktRcvRetrans` counters) report
  zero loss, drop, or retransmit for entire repro runs.
- **Content corruption or reordering, end-to-end** — CRC32 cross-checks at
  three points, closing the whole pipeline: (1) what `ReceiveBuffer` delivers
  from the publisher leg vs. what `SendBuffer`/`send()` actually hands off
  for the player leg — every sent packet is byte-identical to something
  genuinely received, at a consistent buffering-delay offset, zero
  mismatches; (2) the raw bytes `srt-live-transmit` wrote to disk, decoded
  independently with `ffmpeg`, still show the same corruption — proving it's
  not an artifact of `ffmpeg`'s own SRT client specifically; (3) **the
  decisive check**: a real `tcpdump -i lo0 -w ... 'udp port 9000'` capture
  (run by the user, root-owned, read-only to us) decoded with our own
  `SrtPacket.decode` and cross-checked against every packet
  `SrtConnection.send()` believed it sent — **zero unmatched**. What Roast's
  own code believes it transmits is exactly what physically leaves the
  interface. The encoder, the socket write, and the wire are all clean.
- **Event-loop scheduling lag** — the original "JVM/event-loop jitter"
  hypothesis was itself checked, not just assumed: instrumented `tick()`
  timing directly (gap since the previous tick, tick duration, packets
  delivered per tick). Tick-to-tick gaps never once exceeded the 20ms
  threshold checked for; ticks fire on schedule. That specific mechanism is
  ruled out, though the broader "something about Roast's real-world timing
  differs from the references" intuition it was chasing turned out to be
  worth keeping — see below.

**Given every layer from decode through the physical wire is now proven
correct, content-wise, loss-wise, and timing-wise (ticks aren't late), the
remaining explanation has to be a protocol-level difference in *what* Roast
negotiates or reports** — something valid-looking on the wire that still
causes the peer's own SRT stack (libsrt, via `ffmpeg`'s client, since this
symptom is specific to *Roast being the sender* and doesn't reproduce with
gosrt/libsrt as the sender) to reconstruct incorrectly, despite receiving a
complete, correctly-ordered, byte-perfect stream.

**Three further candidates were tried, all with real evidence, none of them
the answer:**
- **16th/17th-packet bandwidth-probe trick** — `SendBuffer` didn't implement
  it (a documented gap), and a real libsrt receiver's own bandwidth estimate
  was observed climbing from ~12 to ~350 Mbps over one run for a fixed
  low-bitrate source — a real, measured anomaly consistent with the missing
  probe. **Implemented** (ported directly from gosrt's `Push`, own test —
  see "What's built"/"Testing methodology"), **and empirically re-tested: no
  change** to the corruption rate. Kept anyway — it's a real, correctly-
  grounded fix independent of this bug.
- **MSS/payload-size negotiation** — on inspection, `ListenerHandshake`
  already echoes back whatever MSS the caller declares (matching gosrt's own
  behavior), and every payload actually observed on the wire (≤1316 bytes)
  is safely under any reasonable bound. **Directly confirmed a non-issue** by
  decoding real `CONCLUSION` handshake replies from Roast, a locally-built
  gosrt `contrib/server`, and libsrt's `srt-live-transmit` (via another
  `tcpdump` capture, decoded with `HandshakeCif.decode`'s own field layout):
  MSS (1500), flow window (8192), and both latency values (120ms) are
  **identical** across Roast and gosrt. (libsrt reports different flags and
  its own real SRT version, as expected for a genuinely different, newer
  implementation — not the relevant comparison, since gosrt already matches
  Roast almost exactly here and stays clean.)
- **Gradle daemon CPU contention** — stopped the daemon and ran `RelayDemo`
  as a plain `java` process (no daemon competing for CPU during the repro).
  **No change** to the corruption rate.

**ROOT-CAUSED AND FIXED, same day.** The investigation above was briefly
parked as unsolved; picking it back up, the answer came from asking a
question none of those passes had: *were the sequence numbers arriving at
`handleData` contiguous?* They were not — **579 of 1671 missing, ~35% of the
published stream** — and since `netstat` had already proven zero UDP drops,
those packets were **never transmitted at all**.

**The bug**: a Full ACK's "available buffer size" is, to the peer's sender,
this receiver's flow-control window. `SrtConnection.tick` hardcoded it to 0
(a long-standing, *documented* simplification — "buffer/rate figures are
still hardcoded to 0" appears in this file's own earlier text, and in the
class javadoc, without anyone connecting it to the symptom). Advertising 0
tells the peer "I can accept nothing", so libsrt collapsed its send window
and dropped whatever it judged undeliverable — announcing exactly the
`DROPREQ` seen at the very start of this investigation and misattributed to
ACK-boundary lag. gosrt sets this field to its configured `FC`
(`connection.go`'s `sendACK`); Roast now reports its advertised window minus
what is actually buffered.

**Confirmed by controlled A/B against real libsrt's own sender counters**,
not just by the symptom disappearing:

| | `pktFlowWindow` | `pktSndDrop` |
|---|---|---|
| `availableBufferSize = 0` (the bug) | 0–21 (collapsed) | 624 and climbing (~42%) |
| the fix | 8188 | 0 |

End-to-end, `ffmpeg` → `RelayDemo` → `ffmpeg` went from **53–78 corrupt
packets per run — every run, for the entire investigation — to 0 across
three consecutive runs**, with zero decode errors. `SrtConnectionTest.`
`fullAckAdvertisesRealReceiveWindowNotZero` is the regression test.

**Why every earlier check came back clean** is the real lesson, recorded in
"Testing methodology": all of them — received-vs-delivered counters, CRCs
verified through to captured wire bytes, libsrt's *receiver* stats, tick
timing — verified Roast's **internal consistency**. None could detect
packets a peer decided never to send. The one check that would have found it
immediately (are the received sequence numbers contiguous?) was cheap, and
was not run until last.

305 tests passing (128 default + 7 gated interop + 2 ACKACK/RTT + 5
`DriftTracerTest` + 2 `ReceiveBufferTest` drift + 4 `ReceiveBufferTest`
wraparound + 10 `SendBufferTest` + 1 `SendBufferTest` probe-trick + 5
`SrtConnectionTest` send-side + 2 `SrtConnectionTest` flow-window + 9
`ReceiveRateEstimatorTest` + 12 `KeyMaterialCifTest` + 16 `StreamKeyWrapperTest`
+ 17 `PayloadCipherTest` + 19 `EncryptionContextTest` + 3 `HandshakeCifTest` KM
+ 6 `SrtConnectionTest` encryption + 11 `CallerHandshakeTest`
+ 3 `SrtCallerTest`),
all committed to `main` (no branches). Every commit so far has been asked-for
explicitly by the user, one narrowly-scoped piece at a time — see git log for
the exact sequence and rationale (commit messages are detailed).

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
  KMREQ/KMRSP key material is parsed too (into a `KeyMaterialCif`); only
  Congestion Control is still recognized-but-skipped by declared length.
  Verified against gosrt's
  `TestHandshakeV4`/`V5` golden vectors — V5 now used **in full**, KM
  included, where it previously had to be stripped down; that also exercises
  the congestion-block skip against real bytes.
  `handshakeTypeCode` is a raw int, not a closed enum — any value outside the 5
  known progression types is a legitimate rejection-reason code, not malformed
  data (see `HandshakeCif.isRejection()`/`rejectionReason()`).
- `AckCif`/`AckVariant` — the ACK CIF's three wire variants (Lite 4B / Small 16B /
  Full 28B, determined by encoded length, not a marker field), verified against
  gosrt's `TestFullACK`/`TestSmallACK`/`TestLiteACK` golden vectors.
- `KeyMaterialCif`/`KeyEncryption` — the Key Material message the KMREQ/KMRSP
  extensions carry (16-byte header, optional 16-byte salt, wrapped SEK(s)),
  verified byte-for-byte against gosrt's `TestKM` golden vector. **Wire format
  only** — the wrapped key bytes are opaque here; no PBKDF2, key wrapping, or
  AES-CTR (see "Known gaps"). Two deliberate departures from gosrt's struct,
  both in the javadoc: the 4-byte rejection form folds into the same record via
  `isError()`/`errorCode()` (mirroring `HandshakeCif`'s own rejection handling
  rather than adding a second type), and the nine fields with exactly one legal
  value are validated on decode and written as constants on encode rather than
  round-tripped. `KeyEncryption` (the KK field) deliberately has no constant
  for `00`: legal in a DATA header, explicitly invalid in a Key Material
  message, which is this enum's only use.

`ControlPacket` also carries the header's **subtype** (bits 15-0), meaningful
only for `USER_DEFINED` — which is how SRT carries a mid-stream key update, as
opposed to the handshake's KM extension. Previously decoded and discarded.

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
  matching gosrt's tolerant behavior. Every figure fed to `AckSender.tick` is
  now real: the available buffer size is the negotiated flow window
  (`AcceptedConnection.flowWindowSize()`) minus what's currently buffered, and
  the packet-rate/link-capacity/receiving-rate figures come from
  `ReceiveRateEstimator` — nothing in the ACK CIF is a placeholder any more,
  which matters more than it sounds (see "Known gaps": the buffer figure
  being hardcoded to 0 silently cost ~35-42% of a real stream). Now also owns
  a `SendBuffer` (see `send` below) and exposes
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
- `SrtConnectionListener` / `EventDispatcher` / `ConnectionStats` — the
  observability surface DESIGN.md §4 calls Roast's real value over a libsrt
  binding. **Three mechanisms for three different consumers**, deliberately not
  merged: `onData` stays a single-owner data handler; `SrtConnectionListener`
  is multicast *events* (`onConnected`/`onDisconnected`/`onLoss`/
  `onTlpktDrop`/`onRetransmit`/`onKeyRotated`), registerable on `SrtListener`
  for every connection it accepts or on one `SrtConnection`; `ConnectionStats`
  is a pollable immutable snapshot from `connection.stats()`, shaped for a
  periodic sampler (the Micrometer path later, no dependency now).
  Also `onPacketReceived` (pre-TSBPD arrival), `onAckSent` and `onAckReceived`,
  completing §4's list. `onPacketReceived` carries the packet's *shape* —
  sequence number, size, retransmitted flag — and deliberately **not its
  bytes**: the payload buffer is owned by the receive path and released once
  handled, so handing it to an asynchronous listener would mean a copy per
  packet or a use-after-release. It's also the one per-packet event, but
  nothing is allocated or queued with no listener registered, so a subscriber
  opts into that rate knowingly. Everything higher-frequency stays a counter.
  **Events never run on the event loop.** A "must not block" javadoc contract
  only asks people not to stall packet processing; a separate single dispatcher
  thread makes it impossible. One thread, not a pool, so a connection's events
  stay ordered; a bounded queue that drops on overflow, because unbounded would
  turn a stall into an OOM; and the drops are counted in
  `ConnectionStats.droppedEvents()`, since losing observability silently is its
  own bug. Two ordering facts are documented rather than left to be
  discovered: `onConnection` stays **synchronous** (it runs before the accept
  response and is where `onData` is attached — making it async would
  reintroduce the race fixed 2026-08-30), and events are therefore *not*
  ordered against `onData`. `SrtListener.connections()` enumerates live
  connections for polling.
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
  construction. RTT/RTTVar/buffer/rate figures are also caller-supplied, and
  all of them are now real values rather than placeholders (see
  `SrtConnection` above and `ReceiveRateEstimator` below).
  `nowMicros` must be elapsed time since this receiver's own start,
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
- `ReceiveRateEstimator` — the three rate figures a Full ACK reports back to
  the peer, ported from gosrt's `congestion/live` receiver (`receive.go`'s
  `Push`/`Tick`/`PacketRate`). Packet and byte arrival rates over a ~1s
  sliding window, recomputed and reset on `tick`; plus an estimated link
  capacity derived from the **16th/17th-packet probe pair** — the receive-side
  counterpart to the trick `SendBuffer.push` performs when sending. The
  arrival gap between a consecutive `≡ 0 (mod 16)` / `≡ 1 (mod 16)` pair is
  scaled to what a fully-loaded packet would have taken, then folded into an
  EWMA; a retransmit or a non-consecutive partner disarms the measurement
  instead of feeding it a bogus sample. Lives in its own class because Roast
  splits gosrt's single receiver struct across `LossList`/`AckSender`/
  `ReceiveBuffer` and none of those is a natural home for rate bookkeeping —
  same reasoning that carved out `DriftTracer`. Takes `nowMicros` explicitly
  rather than reading a clock (gosrt mixes wall-clock `time.Now()` for the
  probe with elapsed micros for the rate window; this codebase keeps one
  elapsed-time source per connection).

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
  fixes). The 16th/17th-packet bandwidth-probe trick *is* ported (see
  `push`'s javadoc, and `ReceiveRateEstimator` above for the receive-side half
  that consumes it); full bandwidth-rate statistics (gosrt's `Stats()`) remain
  deliberately not ported — see known gaps.

**`crypto`** — Phase 5's key handling, not yet wired to anything:
- `StreamKeyWrapper` — derives the Key Encrypting Key from a passphrase
  (PBKDF2/HMAC-SHA-1, 2048 iterations) and wraps/unwraps the Stream Encrypting
  Key(s) with it (AES Key Wrap, RFC 3394), which is what fills in a
  `KeyMaterialCif`'s otherwise-opaque `wrap` field. Both are JDK built-ins
  (`PBKDF2WithHmacSHA1`, the `AESWrap` cipher), so unlike gosrt this needs no
  third-party keywrap dependency. Verified against gosrt's `crypto_test.go`
  golden vectors for all three key lengths and all three key selections.
  Two parameterization details worth knowing, both easy to get plausibly wrong
  and both directly tested: **only the salt's trailing 8 bytes** feed PBKDF2
  (gosrt's `salt[8:]` — passing all 16 yields a key no other implementation
  agrees with), and the KEK is **as long as the SEK it wraps**, not a fixed
  width. `unwrap` returns `null` rather than throwing when the integrity check
  fails, since a wrong passphrase is an expected peer condition to answer with
  a "bad secret" rejection.
- `PayloadCipher` — AES-CTR encryption/decryption of a DATA packet's payload;
  one method does both directions, since CTR is its own inverse. The counter
  construction is the subtle part (draft-sharabayko-srt.md §6.1.2): the salt's
  14 most significant bytes, with the big-endian packet sequence number XORed
  over bytes 10-13 and a zero block counter in the last two — so the sequence
  number *overlaps* the nonce's tail rather than sitting beside it, and the
  salt's final two bytes go deliberately unused. That gives every packet a
  distinct keystream with no per-packet state, which is what lets a receiver
  decrypt packet N without having seen N-1. Verified against gosrt's
  `TestEncode`/`TestDecode` golden vectors (a real 1316-byte MPEG-TS payload,
  all three key lengths, both keys, both directions); mutation-checked by
  widening the nonce to all 16 salt bytes, which fails 10 tests. Deliberately
  stateless about keys — the caller passes the SEK, since choosing between
  even/odd and rotating them is connection-level key management that lives in
  `EncryptionContext` instead.
- `EncryptionContext` — one connection's live encryption state: the salt, both
  SEKs, and which is active. Builds the KM message announcing our keys,
  adopts a peer's, and encrypts/decrypts payloads with the key a DATA header's
  KK field names. **This is a context, not configuration** — a distinction
  raised in review and worth keeping: the passphrase and key length are static
  inputs, but everything here is derived from them and then changes over the
  connection's life. Both references split it the same way (gosrt:
  `Passphrase`/`PBKeyLen` in `Config`, the `crypto` object and
  `keyBaseEncryption` on `srtConn`; libsrt: socket options vs
  `CCryptoControl`). Deliberately **no `SrtConfig`** was invented for the
  passphrase — a secret has no business in a general-purpose settings bag, and
  a record's generated `toString` would leak it. It's held as a `char[]` that
  `destroy()` zeroes, and `toString()` reveals only whether keys exist.
  Adopting a peer's KM takes **their salt before deriving the KEK** (ported
  from gosrt's `UnmarshalKM`; the wrong order derives from the wrong bytes and
  every unwrap fails — mutation-checked). `hasKeys()` means "salt plus the
  *active* key", **not** "both keys": a peer may announce only the key it is
  using, and real libsrt does — requiring both meant we silently sent
  plaintext and libsrt dropped all of it (see "Testing methodology").
  **Key rotation** lives here too: `onPacketEncrypted()` advances SRT's
  schedule one sent packet at a time and reports the key to announce, ported
  from gosrt's `pop()` (defaults 1<<24 packets between rotations, announced
  1<<12 ahead; injectable so tests can drive it). Its four conditions are
  order-sensitive — announce the *opposite* key, re-announce until confirmed,
  switch and clear confirmation at the refresh point, and regenerate the
  retired key only `preAnnounce` packets *after* the switch, since packets
  encrypted with it may still be in flight or awaiting retransmission.
  `confirmKeyMaterial()` ignores a response arriving outside the pre-announce
  window, matching gosrt's `handleKMResponse`.

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

**`harness`** (test-only) — `UdpLossProxy`: UDP relay that randomly drops
packets in both directions, for exercising ARQ without OS-level netem. Written
in Phase 0 as a `main`-only tool and, for that reason, **never used by a test
until 2026-08-30** — a test can't drive something that binds a fixed port and
loops forever. Now embeddable (ephemeral port, daemon thread, closeable), with
a mid-flight `setDropRate` and a dropped-packet count so a test can assert loss
actually happened. Drives `ArqUnderLossTest`.

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

**`interop`** (test-only) — `LibsrtInteropTest`: runs the real
`srt-live-transmit` binary as a subprocess and exercises **all three
connection directions**, so both Roast roles are covered against an
independent implementation:
- `realLibsrtCallerReachesConnected` — libsrt calls *us*, pushes data, and we
  assert on both sides independently (our hooks fire with correct fields,
  *and* the peer's own stdout confirms `"SRT target connected"`).
- `realListenerSendsDataToRealLibsrtCaller` — libsrt calls us and *we* push
  data (`SrtConnection.write`), asserting byte-for-byte on what it received.
- `realCallerSendsDataToRealLibsrtListener` — *we* call out (`SrtCaller`) to
  libsrt running `mode=listener`, and push data. This is the one the other
  two structurally can't cover: they both run libsrt as the caller, so before
  this, `CallerHandshake`/`SrtCaller` had only ever been checked against
  Roast's own `SrtListener`. Connects in a retry loop rather than once, since
  libsrt needs a moment to bind and `SrtCaller` is deliberately single-shot
  with no induction retry (see its javadoc) — the retry lives in the test so
  that limitation stays visible rather than hidden behind a sleep.

`FfmpegInteropTest` (separate class, its own `Assumptions` check) covers what
`srt-live-transmit` cannot drive. `aFiveMegabitStreamArrivesByteExact` is
Phase 3's literal definition of done — byte-exact output from a real 5 Mbps
ffmpeg publisher. Byte-exactness against ffmpeg looks impossible since its
output isn't reproducible; the **`tee` muxer** resolves it, writing one encode
simultaneously to a file and to us, so the file is ground truth for that exact
run. (First attempt appeared to receive *more* than was produced: ffmpeg
buffers its file writes and a forced kill loses the tail, so it now exits on
its own timer before the comparison.) The other test covers a real
libsrt-backed sender *encrypting* to us. It asserts MPEG-TS structure rather than exact bytes, since
ffmpeg's output isn't reproducible — every payload must be a whole number of
188-byte packets each starting with `0x47`, which failed decryption would not
produce (mutation-checked).

Byte-for-byte assertions are safe here because libsrt's own log/verbose
output goes to stderr by default, and stats-to-stdout is opt-in and left off,
so stdout redirected straight to a file carries nothing but the raw bytes —
confirmed directly from libsrt's own source, not assumed. All three skip
themselves via `Assumptions` (build stays green) if the binary isn't found —
checks `$SRT_LIVE_TRANSMIT` env var first, falls back to
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
- **Golden vectors go inline; bulk vectors go in `src/test/resources/`.** Every
  other ported vector in this codebase is a short hex literal in the test that
  uses it, which keeps the expected value next to the assertion. The AES-CTR
  vectors are ~19KB (a real 1316-byte MPEG-TS payload times three key lengths
  times two keys), so they live in `core/src/test/resources/crypto/` instead —
  the first test resource here. They were **extracted verbatim from gosrt by
  script, not retyped**, which is the only safe way to move that much hex; do
  the same for any future bulk vector rather than hand-copying.
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
  against Roast's own `SrtListener` in-process — a good integration proof,
  but worth being precise about its limit: both ends are *our* code, so any
  shared misreading of the spec would pass unnoticed. That gap is now closed
  by `LibsrtInteropTest.realCallerSendsDataToRealLibsrtListener`, which dials
  a real libsrt listener — the caller side finally has independent
  confirmation rather than only self-agreement.
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
- **Eliminating a candidate means measuring it, not describing a plausible
  mechanism for it, 2026-08-29.** The "second issue" investigation (see
  "Where we are"/"Known gaps") first floated "JVM/event-loop scheduling
  jitter" as a leading hypothesis based on reasoning about the code, without
  a direct measurement — called out directly by the user ("are you sure
  about them?"), which was the right challenge. Redone properly: `tick()`
  gained temporary timing instrumentation (gap since previous tick, tick
  duration) — no lag was ever found, so that specific hypothesis was
  actually wrong, not confirmed. From there, every remaining candidate was
  checked the same rigorous way: a *full* `netstat -s -p udp` counter diff
  (not just one field) for network-level loss; both Roast's own receive/send
  counters *and* an independent real receiver's own protocol stats
  (`srt-live-transmit -statsout`, libsrt's `pktRcvLoss`/`pktRcvDrop`) for
  SRT-level loss; and CRC32 checksums planted at three points — the
  publisher-receive point, the player-send point, and (via a user-run
  `tcpdump` capture decoded with our own `SrtPacket.decode`, since this
  process can't get raw-socket/BPF permission itself) the actual bytes on
  the wire — to verify content end-to-end rather than assume a passthrough
  relay can't corrupt anything. Each check either genuinely confirmed
  "clean" with a number to point to, or would have caught a real problem;
  none were rhetorical. Two control experiments (libsrt's own
  `srt-live-transmit` and a locally-built gosrt `contrib/server`, each
  relaying the identical source) were run *before* any of this, specifically
  to confirm the symptom is real and Roast-specific rather than an artifact
  of the test setup or the player. This is the same discipline the earlier
  ACK-boundary bug was root-caused with, just applied one level more
  skeptically after an unverified guess slipped through once.
- **…but rigor about *how* you measure doesn't help if you keep measuring the
  wrong side of the boundary — the single most useful lesson from this
  codebase so far.** Every check in the bullet above was sound, and every one
  came back clean, because every one of them verified Roast's own *internal*
  consistency: received-vs-delivered counters, CRCs traced through to the
  captured wire bytes, tick timing, our own loss lists. All of them are
  structurally incapable of seeing a packet that a peer *chose never to
  send*. The actual bug (a Full ACK advertising a 0-byte receive window, so
  libsrt closed its send window and dropped ~35-42% of the stream) was
  invisible to all of them, and was found in one step by a check that costs
  nothing: **are the sequence numbers we received contiguous?** Practical
  rule going forward: when debugging against a real peer, measure what the
  *peer* did (its stats, its dropped/never-sent counters, gaps in what
  reached you) before, or at least alongside, exhaustively re-verifying your
  own pipeline. A clean internal audit is not evidence that the input was
  complete. Corollary, equally important: **a hardcoded placeholder in a
  protocol field is not automatically cosmetic.** This one was documented as
  a known simplification in three places, including in the javadoc of the
  method that sent it, and was still read straight past for the entire
  investigation because "buffer/rate figures are 0" didn't sound like it
  could cost 42% of a stream.
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
- **Comparing real negotiated handshake values across implementations,
  2026-08-29**: to check the MSS-negotiation hypothesis for the still-open
  corruption issue without guessing, a locally-built gosrt `contrib/server`
  (a real gosrt-based pub/sub relay, `go build` via a fresh `brew install
  go`) and libsrt's `srt-live-transmit` were run as listeners on separate
  ports alongside `RelayDemo`, `ffmpeg` connected briefly to all three, and
  a `tcpdump` capture of all three ports was decoded by hand-replicating
  `HandshakeCif`'s exact field layout (base structure + HSREQ/HSRSP
  extension) in a throwaway script, cross-checking the *actual* MSS/flow
  window/latency/flags each implementation put on the wire rather than
  reasoning about what `ListenerHandshake`'s code *should* produce. This is
  the same "capture on loopback, decode with our own field layout" technique
  used earlier for the ACK-boundary bug, applied to a different question
  (comparing implementations, not confirming a bug in isolation) — worth
  remembering as a general technique whenever "does Roast negotiate the same
  thing a reference does" needs a real answer instead of a guess.
- **Check what a reference test actually covers before planning around it,
  2026-08-29.** Phase 5 was planned as PBKDF2 first, then AES key wrap, on the
  stated basis that gosrt's `TestMarshal`/`TestUnmarshal` would ground them.
  Reading those tests before writing any code showed they exercise KEK
  derivation and wrapping *together* and there is no isolated PBKDF2 vector in
  either reference — so the planned first step would have landed a
  parameterization (which salt bytes, how many iterations, what key width)
  that nothing could verify. RFC 6070 vectors wouldn't have helped: they
  confirm the JDK's PBKDF2 works, which was never in question. The two steps
  were merged into one commit for that reason. Same lesson as the earlier
  `KeyMaterialCif` slice, where the golden vector turned out to live in
  `packet/handshake_test.go` rather than `crypto_test.go` as first claimed:
  **name the specific test you intend to port, and open it, before treating a
  piece as grounded.**
- **A test written for a documented edge case found a real bug in it,
  2026-08-30.** `SrtConnection.encryptIfConfigured`'s javadoc explained why
  retransmissions must be re-encrypted, unlike gosrt. Writing the test for that
  claim showed the retransmission going out as *plaintext*:
  `retainedDuplicate()` shares the original's memory, so encrypting in place
  rewrote the plaintext `SendBuffer` retains, and the resend's second
  encryption pass — CTR against a keystream fixed by the sequence number —
  restored the cleartext. Documenting an edge case is not the same as testing
  it; this is the second ByteBuf-aliasing bug in this codebase (see the earlier
  `SendBuffer` one), and both were found only by exercising the retransmission
  path specifically.
- **Read the peer's own log before theorising, 2026-08-30.** Encrypted
  payloads reached real libsrt and produced an empty output file with no
  decryption error — which looked like a cipher or key-exchange mismatch, and
  the leading theories were all about *which* key libsrt expected. Its debug
  log said something else entirely: "Packet not encrypted (seqno ...),
  dropped". The packets were leaving as cleartext, so libsrt rejected them
  before attempting decryption at all. The cause was `hasKeys()` demanding
  both SEKs when libsrt announces only one, so encryption was skipped
  silently. Two lessons: an independent implementation's log will often name
  the failure precisely, and a boolean guard around a security operation
  should fail loudly rather than degrade to the insecure path — sending
  plaintext because "we have no keys" is the worst possible default, and
  nothing in our own tests noticed because both our contexts always
  generated both keys.
- **Chasing an interop failure is worth doing before assuming it's your bug,
  2026-08-30.** An encrypted-payload interop test failed; instrumenting showed
  249 control packets and zero DATA, and `srt-live-transmit`'s *own* stats
  confirmed it never transmits when reading a redirected file. That is a
  property of the tool, not of Roast — and it explains why the pre-existing
  `realLibsrtCallerReachesConnected` only ever asserted connection. The same
  hunt turned up a genuine accept-ordering race in `SrtListener` (see "What's
  built"). Neither would have surfaced without checking where the packets
  actually went.
- **The 16th/17th-packet bandwidth-probe trick** — neither half has a gosrt
  test to ground against (`send_test.go` has no probe cases and
  `receive_test.go` has no rate/capacity cases, both checked directly), so
  the tests for `SendBuffer.push`'s sending half and
  `ReceiveRateEstimatorTest`'s consuming half are both self-designed against
  gosrt's source — same rigor tier as this codebase's RTT/drift/wraparound
  pieces, not the stronger ported-scenario tier.
- **A silent-failure guard is worth a test even when it looks like boilerplate,
  2026-08-30.** `StatsSampler` wraps its sink calls in try/catch, which reads
  as defensive noise until you notice `scheduleAtFixedRate` cancels a task
  *permanently* the first time it throws. Without the guard, one bad sink
  stops every metric for the process's life, with no error anywhere and
  nothing to notice but an absence. Mutation-checked by removing it.
- **Two ordering bugs from one small change, both caught by tests rather than
  by reading, 2026-08-30.** Making `close()` drain instead of discard looked
  trivial. First, teardown ran on the *calling* thread, so a `write()` already
  marshaled onto the event loop hadn't reached the send buffer when the drain
  ran — the data was lost. Moving teardown onto the event loop fixed the
  ordering. Then the write was *still* dropped, because `close()` sets its
  idempotence flag the moment close is **requested**, and the queued write task
  checked that same flag. "Close requested" and "torn down" are genuinely
  different states, and conflating them silently discards in-flight data. Both
  are the kind of async-ordering fault that reads as correct on the page.
- **Writing a test for something that "already works" is worth doing anyway,
  2026-08-30.** Multiplexing had worked since Phase 2 and `RelayDemo`
  exercised it daily, so it felt like coverage. The first real test found an
  unbounded leak: `SrtListener` never removed anything from its connection map
  or handshake-dedup cache, so a churning relay retained every connection it
  ever accepted and reported dead ones to anything polling stats. A demo
  exercising a happy path is not a test of the lifecycle around it.
- **Make the failure mode impossible rather than documented, 2026-08-30.** The
  observability listeners were first designed to run on the connection's event
  loop with a javadoc contract saying they must not block — which is only a
  request, and this project had already been burned once by exactly that
  (blocking I/O in a diagnostic `onData`, caught by the user asking "did you
  write bad netty code?"). Review pushed it to *always* dispatch on a separate
  thread, and `ObservabilityTest.aBlockingSubscriberDoesNotStallTheDataPath`
  now asserts the property instead of the docs asking for it — mutation-checked
  by dispatching inline, which fails it. Worth generalising: when a contract
  exists because violating it breaks something badly, prefer a design where it
  cannot be violated.
- **Loopback hides an entire class of bug, 2026-08-30.** Two defects survived
  the whole project because every test ran over a perfect local link:
  `SrtCaller` sent each handshake step exactly once (a single dropped INDUCTION
  or CONCLUSION killed the connect), and the ACK's available-buffer figure was
  hardcoded to 0 (see "Known gaps"). Neither is subtle once a lossy path or a
  real peer is in play; neither was visible without one. Worth remembering that
  "all tests pass on loopback" says little about a protocol whose entire job is
  coping with loss — and that the loss harness for exactly this had existed,
  unused, since Phase 0.
- **A harness nobody can call is a harness nobody uses, 2026-08-30.**
  `UdpLossProxy` was built in Phase 0 specifically to satisfy Phases 3 and 4's
  "recovered via ARQ under induced loss" definition of done, and then sat
  unused for the entire project — because it was written as a `main` that binds
  a fixed port and loops forever, which no test can drive. Every loss and
  retransmit path was therefore only ever exercised by hand-injected gaps in
  unit tests. Making it embeddable took minutes and immediately paid for
  itself: the very first run failed in `connect`, surfacing that `SrtCaller`
  has no handshake retry. When a test helper exists but nothing references it,
  that's worth treating as a gap rather than as coverage.
- **Writing the rule down is not implementing it, 2026-08-30.** The
  `buildAcceptResponse` javadoc stated Table 2 correctly — 2 = AES-128, 3 =
  AES-192, 4 = AES-256, i.e. the key length in *eight*-byte units — while the
  code beside it divided by four, advertising a 16-byte key as AES-256. The
  wrong value is still a legal one, so no local test could tell; real libsrt
  rejected the handshake, which is the only reason it surfaced. Two takeaways:
  a constant derived by arithmetic from a spec table deserves a test that
  names the expected values, and an interop suite earns its cost precisely on
  changes that look locally correct.
- **The weak-assertion trap caught a second time, 2026-08-30.** The key-rotation
  test for "the retired key is only regenerated well after the switch" passed
  against a deliberately broken version that regenerated it *at* the switch. It
  was asserting on the *peer's* copy of the key, which that timing does not
  affect, instead of the sender's. Rewritten to compare the sender's retired key
  against a peer holding the original, and re-mutated to confirm it now fails.
  Worth internalising: a test for *timing* has to observe the thing whose timing
  changed.
- **Mutation-checking a test that can't fail is worth the two minutes**, added
  2026-08-29 after the flow-window work: the first regression test for the
  advertised receive window asserted `8192`, which was simultaneously the
  negotiated value *and* the hardcoded fallback — it would have passed even
  if the negotiation was ignored entirely. Fixed by parameterizing the test
  handshake and negotiating a deliberately different `4096`, then **verifying
  the test actually fails** when the code is mutated to always use the
  fallback. Any assertion whose expected value coincides with a default is
  suspect until it's been seen to fail.

## Known gaps / deliberately deferred

- **MSS/payload-size negotiation** — the negotiated MTU is now carried on
  `AcceptedConnection` and bounds `write`, but `ListenerHandshake` still echoes
  the peer's value rather than negotiating down to our own limit (no config
  object to hold ours). **Congestion Control extension** parsing/mismatch
  rejection is still skipped entirely; noted inline there.
- **No `SrtConfig`** — `SrtListener` hardcodes 120ms latency (both directions)
  and SRT version `0x010401` (matching gosrt's own baseline).
- **Encryption** (Phase 5 in DESIGN.md) — **feature-complete and proven
  against real libsrt**, which keys with a shared passphrase and decrypts
  payloads we encrypted (`LibsrtInteropTest.realLibsrtCallerDecryptsWhatWeEncrypt`).
  Mid-stream key rotation is implemented and wired. Remaining gaps are narrow
  — see "Next steps": rotation has not been exercised against a real peer
  (the schedule is 16.7M packets), the reverse interop direction (libsrt
  encrypting to us) needs `ffmpeg`, and the handshake's Encryption Field is
  carried but not acted on.
  `KeyMaterialCif`/`KeyEncryption` parse and build the Key Material message
  the KMREQ/KMRSP extensions carry, verified byte-for-byte against gosrt's
  own `TestKM` golden vector. Everything else is still untouched and is the
  bulk of the phase. **Also done**: `StreamKeyWrapper` (new `crypto` package)
  derives the KEK from a passphrase (PBKDF2) and wraps/unwraps the SEKs (AES
  Key Wrap), verified against gosrt's golden vectors — so a KM message's
  `wrap` field can now actually be produced and consumed, though nothing calls
  it yet, and `PayloadCipher` does AES-CTR payload encryption/decryption.
  **Still untouched**: even/odd key rotation with pre-announce, and wiring any
  of it into
  `ListenerHandshake`/`CallerHandshake`/`SrtConnection`. Deliberately stopped
  before the wiring so nothing is half-connected into the data path — see
  "Next steps".
- ~~Caller-side handshake~~ **Closed** — `SrtCaller`/`CallerHandshake`,
  including **encryption** (the caller generates the keys and announces them;
  see "What's built"). No HSv4 fallback, matching gosrt.
- ~~`SrtCaller` does not retry its handshake~~ **Closed, 2026-08-30** — it now
  repeats whichever step is unanswered every 250ms (libsrt's own rule),
  bounded by the 5s connect timeout. One of the few places Roast deliberately
  goes beyond gosrt, which doesn't retry either: loopback-only testing was what
  made this look unnecessary. Verified by connecting through 20% loss.
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
- ~~A second, distinct source of real data corruption under sustained real
  throughput~~ **CLOSED — root-caused and fixed 2026-08-29**: Full ACKs
  advertised an available buffer size of 0, which a peer's sender reads as a
  closed flow-control window; real libsrt responded by dropping ~35-42% of
  the published stream before it ever reached the wire. See "Where we are"
  for the full story, the controlled A/B against libsrt's own
  `pktFlowWindow`/`pktSndDrop` counters, and the lesson about why a long
  chain of internal-consistency checks could not have found it. Regression
  test: `SrtConnectionTest.fullAckAdvertisesRealReceiveWindowNotZero`.
  **Follow-ons, both since done** (this bug being a pointed argument against
  leaving any hardcoded placeholder in a wire field): the *rate* figures in
  the same CIF now come from `ReceiveRateEstimator`, and the window is now
  the **negotiated** one rather than a fixed constant — see "What's built".
  Historical note on what it looked like before root-causing: pushing a real
  ffmpeg stream through `RelayDemo` showed MPEG-TS corruption at a real,
  measurable rate, while control experiments with libsrt's
  `srt-live-transmit` and a locally-built gosrt `contrib/server` relaying the
  *identical* source played back clean — correctly establishing early on that
  it was real and Roast-specific, not the test setup or ffmpeg's strictness.

  **Ruled out along the way, each with a real measurement** — all correct as
  far as they went, and all blind to the actual cause for the same reason:
  every one of them verified Roast's *internal* consistency, and the packets
  at fault were never sent by the peer at all (see "Where we are"):
  - OS-level network loss/corruption — full `netstat -s -p udp` counter
    diff across a repro run: zero drops, zero checksum/length errors.
  - SRT protocol-level loss — zero across both Roast's own counters and an
    independent libsrt receiver's `pktRcvLoss`/`pktRcvDrop`/`pktRcvRetrans`
    stats.
  - Content corruption or reordering anywhere in the pipeline — CRC32
    cross-checked end-to-end: decode → buffer → relay → encode → **actual
    wire bytes**, the last leg confirmed via a real `tcpdump` capture
    decoded with our own codec. Zero mismatches at every stage.
  - Event-loop scheduling lag — `tick()` timing instrumented directly; ticks
    fire on schedule, no lag found.
  - The 16th/17th-packet bandwidth-probe trick's absence — implemented (see
    "What's built"), re-tested empirically: no change.
  - MSS/payload-size negotiation — inspected (`ListenerHandshake` already
    echoes the caller's declared MSS, matching gosrt; every observed payload
    is safely under any reasonable bound), then **directly confirmed** via a
    real handshake-reply diff: Roast and a locally-built gosrt
    `contrib/server` negotiate MSS (1500), flow window (8192), and both
    latency values (120ms) **identically** in response to the same `ffmpeg`
    connection. (libsrt's own reply differs in flags/version, but that's
    expected for a different, newer implementation — not the relevant
    comparison, since gosrt already matches Roast closely here and stays
    clean.)
  - Gradle daemon CPU contention — ran `RelayDemo` as a plain `java` process
    with the daemon stopped; no change.

  **What actually found it**: checking whether the sequence numbers arriving
  at `handleData` were contiguous — 579 of 1671 were missing. Combined with
  the already-established "zero UDP drops", that pinned the loss to packets
  the peer never transmitted, which points at exactly one thing we tell the
  peer: the flow-control window. Cheap check, available from the very first
  hour, run last.
- ~~No RTT measurement~~ **Closed** — `SrtConnection` now tracks real RTT/RTTVar
  from ACK/ACKACK round trips and feeds them to `AckSender.tick` and the
  periodic NAK interval; see "What's built" and "Testing methodology" (the
  latter for the honest caveat that this piece has no gosrt test to ground
  against). The buffer and rate figures fed to `AckSender.tick` are now real
  too — see the flow-window entry below.
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
  - **Message chunking** — `write(ByteBuf)` still sends exactly one DATA packet
    per call and does not split messages, but it no longer fails silently:
    anything over the connection's `maxPayloadSize()` is rejected with a clear
    error, matching libsrt's live-mode behaviour. Actually splitting a message
    needs receive-side reassembly (the packet-position flags), which is Phase
    7's "message mode".
  - **Full send-side stats** (gosrt's `Stats()`: `estimatedInputBW`/
    `estimatedSentBW`/`pktLossRate`) and the 16th/17th-packet bandwidth-probe
    trick — both deliberately not ported, see `SendBuffer`'s javadoc.
  - ~~Live pollable stats and event hooks~~ **Closed** — DESIGN §4's list is
    now complete: `SrtConnectionListener`/`ConnectionStats`, the ACK and
    pre-TSBPD events, and `SrtListener.pipeline()` for embedders. That last one
    is exposed **per port, not per connection**: §4 describes a connection's
    pipeline, but Roast multiplexes every connection onto one
    `NioDatagramChannel` — which is what makes many-sockets-on-one-port work —
    so there is exactly one pipeline, and a handler added to it sees all
    connections. Documented rather than papered over.
  ~~Real interop for the send path~~ **Closed** — see `LibsrtInteropTest`'s
  `realListenerSendsDataToRealLibsrtCaller` above; didn't need the
  caller-side handshake first, since Roast only needed to be the *listener*
  here, with libsrt calling in and reading.

## Phase 6 (multiplexing & polish) — underway

DESIGN.md scopes this as making Roast usable by someone who isn't us: many
sockets on one port, graceful close, Javadoc, README, an
`srt-java-live-transmit` CLI, and polish on the *stats surface* (an external
metrics-sink adapter) — explicitly **not** where hooks first get added, which
was Phase 3-5 work and is done.

- ~~**Graceful close**~~ **Done.** `close()` now drains both directions
  instead of discarding. The receive side was the one costing data: a peer
  finishing a stream and shutting down left up to a full latency window of
  packets already received and merely waiting on TSBPD deadlines, so the tail
  of every stream was silently truncated. Both references discard here
  (gosrt's `close` flushes; libsrt defaults live-mode linger to 0), so this
  deliberately goes further. Nothing waits: outbound is bounded by what's
  queued, inbound by what's arrived. Two ordering bugs surfaced building it —
  see "Testing methodology".
- ~~**Multiplexing asserted**~~ **Done.** `MultiplexingTest` covers
  cross-talk, distinct socket IDs, closing one connection leaving its
  neighbours working, and per-connection stats. Writing it found a real leak
  (below).
- **Configuration** — the remaining blocker for the last phase 0-5 item
  (exercising key rotation against a real peer needs a configurable schedule),
  and what would retire `SrtListener`/`SrtCaller`'s hardcoded latency, version
  and timeout defaults. Note the earlier review point: a passphrase does not
  belong in a general settings bag, so whatever shape this takes should not
  absorb `AcceptDecision`'s.
- ~~**Metrics-sink adapter**~~ **Done.** `ConnectionStatsSink` (a one-method
  interface, so it can be a lambda and Roast still depends on no metrics
  library) plus `StatsSampler`, which owns the scheduling loop everyone would
  otherwise write. Runs on its own thread — a sink blocking on an HTTP push
  can't slow packet processing, same reasoning as the event dispatcher — and
  emits a **final snapshot on disconnect**, since polling alone loses the last
  partial interval, which for a short-lived connection can be most of it. The
  connection is passed alongside the snapshot so a sink can tag by StreamID or
  peer address. A Micrometer binding is a few lines in the embedder's code.
- **README, Javadoc pass, and `srt-java-live-transmit`** — `RelayDemo` is the
  rough prototype, explicitly not held to this codebase's standards.

## Next steps, in order

1. **Finish Phase 5 (encryption) — only the wiring is left.** Every piece of
   the cryptography now exists, and *none of it is connected to anything*:
   `KeyMaterialCif` (the KM wire format), `StreamKeyWrapper` (KEK derivation +
   SEK wrapping), `PayloadCipher` (AES-CTR) — all three verified against gosrt
   golden vectors — and `EncryptionContext`, the per-connection state that
   composes them. See "What's built". The remaining step is
   deliberately the one that was saved for last, because it's the only one
   that changes live behavior:
   - **Phase 5 is complete.** Key material codec, KEK derivation and key
     wrapping, AES-CTR, the per-connection `EncryptionContext`, handshake
     integration, connection wiring, mid-stream key rotation, and the
     Encryption Field are all done, and encryption is proven against a real
     implementation in **both** directions (libsrt decrypts ours;
     `FfmpegInteropTest` shows we decrypt a real sender's).
   - **The one Phase 0-5 item still open: rotation has never run against a
     real peer.** The production schedule is 1<<24 packets, so no test drives
     it end to end. The inbound half (a peer rotating, us adopting and
     acknowledging) is covered over real sockets and the schedule itself is
     unit-tested, but nothing has watched libsrt accept a rotation *we*
     initiated. Doing so needs the schedule to be configurable per connection,
     which is a configuration story rather than a test fixture — deliberately
     carried into Phase 6's config work rather than bolting another parameter
     onto `AcceptDecision` purely to make a test possible.
2. Phase 6 (multiplexing & polish) — the alternative major milestone,
   independent of Phase 5 and not blocked by it. Many connections per port,
   live pollable stats, and the `srt-java-live-transmit` CLI that `RelayDemo`
   is a rough prototype of.
3. *(Optional, low-priority)* Try `ffmpeg --enable-libsrt`'s own `srt://` muxer
   against `SrtListener`, for full belt-and-suspenders confidence beyond
   `srt-live-transmit` — not expected to surface anything new, since ffmpeg
   wraps the same libsrt handshake code already exercised.
4. ~~Report real rate figures in Full ACKs~~ and ~~thread the negotiated flow
   window through `AcceptedConnection`~~ — **both done**; see
   `ReceiveRateEstimator` in "What's built" and the flow-window entry in
   "Known gaps". Nothing in the ACK CIF is a hardcoded placeholder any more.

## How to pick this back up

Read this file, then `CLAUDE.md`, then skim `git log --oneline` (commit messages
carry real rationale, not just "what changed"). `DESIGN.md` at the workspace root
still governs overall phase order and protocol references, including the
extensibility/hooks design constraint added 2026-08-29 — it predates this
implementation work in its original form, but has been actively edited alongside
it since, so treat it as current, not frozen.
