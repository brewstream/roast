# Roast

Pure-Java SRT (Secure Reliable Transport) protocol library. `README.md` covers
the public API and what is deliberately not implemented. `STATUS.md` is a local,
untracked engineering log — present in a working copy but deliberately not in
the repository, so do not cite it from code or documentation that is.
`references/draft-sharabayko-srt.md` is the protocol spec; cite it by section
when a decision follows from the wire format.

## Build

- Gradle, Groovy DSL, JDK 21 toolchain. Single module: `core`.
- `./gradlew compileJava compileTestJava` — compile.
- `./gradlew test` — run tests.

## Conventions

- Package root: `org.brewstream.roast` (Gradle group `org.brewstream`).
- Transport is built on **Netty**, not raw NIO — protocol types (packet header,
  handshake, ARQ) are implemented as Netty codecs/handlers over `DatagramChannel`,
  per engineering direction to lean on Netty's pipeline model rather than
  reimplementing framing/multiplexing/threading by hand. Netty is not optional
  and not an implementation detail to be abstracted away.
- Tests: JUnit 5 (`@Test`/`@ParameterizedTest`), **AssertJ** (`assertThat(...)`) for
  assertions — not JUnit's `Assertions.assertEquals`/etc. **Mockito** for mocking
  collaborators (`@ExtendWith(MockitoExtension.class)`, `mock(...)`), used where a
  real collaborator is a callback/interface rather than a value object worth
  constructing for real (e.g. `SrtPacketSink` in demux tests).
- Netty `ByteBuf` ownership: `SrtPacket.encodeTo(ByteBuf)` consumes and releases
  `body()`. A packet that needs to be sent again (ARQ retransmission) must
  `body().retainedDuplicate()` into a fresh `SrtPacket` first — don't reuse an
  already-encoded packet instance.
