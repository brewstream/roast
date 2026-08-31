package org.brewstream.roast.interop;

import io.netty.buffer.Unpooled;
import org.brewstream.roast.socket.AcceptDecision;
import org.brewstream.roast.socket.AcceptedConnection;
import org.brewstream.roast.socket.ConnectionRequest;
import org.brewstream.roast.socket.SrtCaller;
import org.brewstream.roast.socket.SrtConfig;
import org.brewstream.roast.socket.SrtConnection;
import org.brewstream.roast.socket.SrtListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Interop test against the real SRT reference implementation (libsrt's
 * {@code srt-live-transmit}), not just our own codec — the actual definition of
 * done for the handshake work — ffmpeg/libsrt reaches connected. Everything
 * else in this test suite only verifies against golden vectors and our own code.
 *
 * <p>Requires a local libsrt build (MPL-2.0, not shipped or committed — see
 * {@code roast/references/srt}: {@code cmake -DENABLE_APPS=ON .. && make}).
 * {@code @Tag("interop")} keeps this out of the default {@code test} task (see
 * {@code build.gradle}'s {@code interopTest} task) since it depends on that
 * external binary and spawns a real subprocess; skips itself via
 * {@link Assumptions} if the binary isn't found rather than failing the build.
 */
@Tag("interop")
class LibsrtInteropTest {

    private static final int TIMEOUT_SECONDS = 10;
    private static final String STREAM_ID = "live/test";
    /** libsrt requires at least 10 characters. */
    private static final String PASSPHRASE = "roast-interop-secret";

    private SrtListener listener;
    private Process srtLiveTransmit;
    private Thread outputReaderThread;

    @BeforeEach
    void checkPrerequisite() {
        Assumptions.assumeTrue(Files.isExecutable(srtLiveTransmitPath()),
                "srt-live-transmit not found at " + srtLiveTransmitPath()
                        + " - build it first (see roast/references/srt) or set SRT_LIVE_TRANSMIT");
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (srtLiveTransmit != null) {
            srtLiveTransmit.destroyForcibly();
        }
        if (listener != null) {
            listener.close();
        }
    }

    @Test
    void realLibsrtCallerReachesConnected() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        CompletableFuture<ConnectionRequest> seenRequest = new CompletableFuture<>();
        CompletableFuture<SrtConnection> connected = new CompletableFuture<>();
        listener.setAcceptHandler(request -> {
            seenRequest.complete(request);
            return AcceptDecision.accept();
        });
        listener.onConnection(connected::complete);

        StringBuilder peerOutput = new StringBuilder();
        srtLiveTransmit = launchSrtLiveTransmit(listener.localAddress().getPort(), peerOutput);

        ConnectionRequest request = seenRequest.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        AcceptedConnection connection = connected.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).metadata();

        assertThat(request.streamId()).isEqualTo(STREAM_ID);
        assertThat(request.srtVersion()).isPositive(); // a real version was reported, not asserting which
        assertThat(request.peerSocketId().isZero()).isFalse();

        assertThat(connection.streamId()).isEqualTo(STREAM_ID);
        assertThat(connection.peerSocketId()).isEqualTo(request.peerSocketId());
        assertThat(connection.receiveLatencyMillis()).isEqualTo(120);
        assertThat(connection.sendLatencyMillis()).isEqualTo(120);

        // The peer's own side must agree it connected - independent confirmation,
        // not just our side's bookkeeping.
        boolean exited = srtLiveTransmit.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(exited).as("srt-live-transmit should exit on its own -t timeout").isTrue();
        outputReaderThread.join(TimeUnit.SECONDS.toMillis(2)); // drain fully before reading peerOutput
        synchronized (peerOutput) {
            // Printed unconditionally by srt-live-transmit's own app code, not gated behind a log level.
            assertThat(peerOutput.toString()).contains("SRT target connected");
        }
    }

    /**
     * The reverse direction of {@link #realLibsrtCallerReachesConnected}: once a
     * real libsrt caller connects, <em>we</em> write data and libsrt reads it —
     * the actual definition of done for the sender path's interop story
     * (everything else in this class only confirms the handshake, not that
     * Roast's send path produces correct bytes on the wire against a real peer).
     *
     * <p>Unlike {@link #realLibsrtCallerReachesConnected}, this asserts
     * byte-for-byte correctness, not just a connectivity string match — safe to
     * do because srt-live-transmit's log/verbose output goes to stderr by
     * default ({@code apps/verbose.cpp}'s {@code cverb}), and stats reporting
     * to stdout is opt-in (not enabled here), so stdout redirected straight to
     * a file is guaranteed to carry nothing but the raw bytes its
     * {@code ConsoleTarget} writes for a {@code file://con} output.
     */
    @Test
    void realListenerSendsDataToRealLibsrtCaller() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        byte[] chunk = "roast-send-path-interop\n".getBytes(StandardCharsets.US_ASCII);
        int writes = 5;
        listener.onConnection(connection -> {
            for (int i = 0; i < writes; i++) {
                connection.write(Unpooled.wrappedBuffer(chunk));
            }
        });

        Path outputFile = Files.createTempFile("roast-interop-recv-", ".bin");
        outputFile.toFile().deleteOnExit();
        srtLiveTransmit = launchSrtLiveTransmitSending(listener.localAddress().getPort(), outputFile);

        boolean exited = srtLiveTransmit.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(exited).as("srt-live-transmit should exit on its own -t timeout").isTrue();

        byte[] expected = new byte[chunk.length * writes];
        for (int i = 0; i < writes; i++) {
            System.arraycopy(chunk, 0, expected, i * chunk.length, chunk.length);
        }
        assertThat(Files.readAllBytes(outputFile)).isEqualTo(expected);
    }

    /**
     * The third direction, and the one the other two can't cover: <em>we</em>
     * call out ({@link SrtCaller}) to a real libsrt acting as <em>listener</em>.
     * Both tests above run {@code srt-live-transmit} as the caller, so until
     * now Roast's caller-side handshake had only ever been verified against
     * Roast's own {@code SrtListener} — two pieces of the same codebase agreeing
     * with each other. This is the independent check.
     *
     * <p>The connect is retried in a loop rather than attempted once: libsrt
     * needs a moment to bind its port after the process starts, and {@link
     * SrtCaller} is deliberately single-shot with no induction retry (matching
     * gosrt's {@code dial.go}; real libsrt does retry with backoff — a known,
     * documented simplification). Retrying here exercises that limitation
     * honestly instead of hiding it behind a fixed sleep.
     */
    @Test
    void realCallerSendsDataToRealLibsrtListener() throws Exception {
        int listenerPort = freeUdpPort();
        Path outputFile = Files.createTempFile("roast-interop-caller-", ".bin");
        outputFile.toFile().deleteOnExit();
        srtLiveTransmit = launchSrtLiveTransmitListening(listenerPort, outputFile);

        SrtConnection connection = connectWithRetries(listenerPort);
        try {
            byte[] chunk = "roast-caller-path-interop\n".getBytes(StandardCharsets.US_ASCII);
            int writes = 5;
            for (int i = 0; i < writes; i++) {
                connection.write(Unpooled.wrappedBuffer(chunk));
            }

            boolean exited = srtLiveTransmit.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            assertThat(exited).as("srt-live-transmit should exit on its own -t timeout").isTrue();

            byte[] expected = new byte[chunk.length * writes];
            for (int i = 0; i < writes; i++) {
                System.arraycopy(chunk, 0, expected, i * chunk.length, chunk.length);
            }
            assertThat(Files.readAllBytes(outputFile)).isEqualTo(expected);
        } finally {
            connection.close();
        }
    }

    private static SrtConnection connectWithRetries(int listenerPort) throws Exception {
        InetSocketAddress remote = new InetSocketAddress("127.0.0.1", listenerPort);
        Exception last = null;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (System.nanoTime() < deadline) {
            try {
                return SrtCaller.connect(remote, STREAM_ID).get(2, TimeUnit.SECONDS);
            } catch (Exception e) {
                last = e;
                Thread.sleep(200);
            }
        }
        throw new AssertionError("never connected to the libsrt listener on port " + listenerPort, last);
    }

    /** An ephemeral port, released immediately so libsrt can bind it itself. */
    private static int freeUdpPort() throws IOException {
        try (DatagramSocket socket = new DatagramSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /** libsrt listens and reads; {@code file://con} writes the raw bytes it receives to stdout. */
    private Process launchSrtLiveTransmitListening(int listenerPort, Path outputFile) throws IOException {
        ProcessBuilder builder = new ProcessBuilder(
                srtLiveTransmitPath().toString(),
                "-t", "5",
                "srt://:" + listenerPort + "?mode=listener",
                "file://con")
                .redirectOutput(ProcessBuilder.Redirect.to(outputFile.toFile()))
                .redirectError(ProcessBuilder.Redirect.DISCARD);
        return builder.start();
    }


    /**
     * The definition of done for Phase 5: a real libsrt peer keys with a shared
     * passphrase and <em>decrypts payloads we encrypted</em>. Everything else
     * about encryption is verified against gosrt's golden vectors, or against
     * two of our own contexts agreeing with each other; this is the only test
     * where an independent implementation consumes our key exchange and our
     * AES-CTR output for real.
     *
     * <p>libsrt requires a passphrase of at least 10 characters, and
     * {@code pbkeylen} is passed explicitly so both sides agree on a 16-byte key
     * rather than relying on either default.
     *
     * <p>The reverse direction (libsrt encrypts, we decrypt) is not covered
     * here: {@code srt-live-transmit} reading a redirected file connects but
     * never transmits, which its own empty {@code pktSent} stats confirm. That
     * direction needs {@code ffmpeg}'s muxer instead, and is covered by
     * {@code FfmpegInteropTest.weDecryptWhatARealFfmpegSenderEncrypts}.
     */
    @Test
    void realLibsrtCallerDecryptsWhatWeEncrypt() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        CompletableFuture<ConnectionRequest> seenRequest = new CompletableFuture<>();
        listener.setAcceptHandler(request -> {
            seenRequest.complete(request);
            return AcceptDecision.accept(PASSPHRASE.toCharArray(), 16);
        });

        byte[] chunk = "roast-encrypted-interop\n".getBytes(StandardCharsets.US_ASCII);
        int writes = 5;
        listener.onConnection(connection -> {
            for (int i = 0; i < writes; i++) {
                connection.write(Unpooled.wrappedBuffer(chunk));
            }
        });

        Path outputFile = Files.createTempFile("roast-interop-enc-", ".bin");
        outputFile.toFile().deleteOnExit();
        srtLiveTransmit = launchSrtLiveTransmitSending(listener.localAddress().getPort(), outputFile,
                "&passphrase=" + PASSPHRASE + "&pbkeylen=16");

        boolean exited = srtLiveTransmit.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(exited).as("srt-live-transmit should exit on its own -t timeout").isTrue();

        assertThat(seenRequest.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).encryptionRequested()).isTrue();

        byte[] expected = new byte[chunk.length * writes];
        for (int i = 0; i < writes; i++) {
            System.arraycopy(chunk, 0, expected, i * chunk.length, chunk.length);
        }
        // libsrt writes plaintext out only if it agreed keys with us AND our
        // AES-CTR output matches what its own implementation expects. It drops
        // anything it considers unencrypted, so cleartext would fail here too.
        assertThat(Files.readAllBytes(outputFile)).isEqualTo(expected);
    }


    /**
     * The last Phase 0-5 straggler: a key rotation <em>we</em> initiate, accepted
     * by real libsrt. Everything else about rotation was verified against gosrt
     * vectors or between two of our own contexts; nothing had watched an
     * independent implementation keep decrypting across a key change.
     *
     * <p>This was blocked until {@link SrtConfig} existed. The production
     * schedule is 2^24 packets — roughly 22 GB at full payload — so it cannot be
     * driven by a test; the schedule had to become configurable for its own
     * sake before this was possible, rather than a parameter added to make one
     * test work.
     *
     * <p>libsrt writing plaintext out after the rotation point is the assertion:
     * it only does that if it accepted our mid-stream KMREQ and switched to the
     * announced key. Encrypting with a key it had not adopted would produce
     * garbage, not a short file.
     */
    @Test
    void realLibsrtFollowsAKeyRotationWeInitiate() throws Exception {
        // Rotate every 40 packets, announcing 10 ahead - several rotations
        // within a few hundred packets.
        SrtConfig rotating = SrtConfig.defaults().withKeyRotation(40, 10);
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0), rotating);
        listener.setAcceptHandler(request -> AcceptDecision.accept(PASSPHRASE.toCharArray(), 16));

        // Count rotations we actually performed. Without this the test passes
        // whether or not anything rotated - libsrt decrypts happily with the
        // original key - which would assert nothing about rotation at all.
        java.util.concurrent.atomic.AtomicInteger rotations = new java.util.concurrent.atomic.AtomicInteger();
        listener.addEventListener(new org.brewstream.roast.socket.SrtConnectionListener() {
            @Override
            public void onKeyRotated(org.brewstream.roast.socket.SrtConnection connection) {
                rotations.incrementAndGet();
            }
        });

        byte[] chunk = "roast-rotation-interop\n".getBytes(StandardCharsets.US_ASCII);
        int writes = 200; // comfortably past several rotation boundaries
        listener.onConnection(connection -> {
            for (int i = 0; i < writes; i++) {
                connection.write(Unpooled.wrappedBuffer(chunk));
            }
        });

        Path outputFile = Files.createTempFile("roast-interop-rotation-", ".bin");
        outputFile.toFile().deleteOnExit();
        srtLiveTransmit = launchSrtLiveTransmitSending(listener.localAddress().getPort(), outputFile,
                "&passphrase=" + PASSPHRASE + "&pbkeylen=16");

        boolean exited = srtLiveTransmit.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(exited).as("srt-live-transmit should exit on its own -t timeout").isTrue();

        byte[] expected = new byte[chunk.length * writes];
        for (int i = 0; i < writes; i++) {
            System.arraycopy(chunk, 0, expected, i * chunk.length, chunk.length);
        }
        assertThat(Files.readAllBytes(outputFile))
                .as("libsrt must keep decrypting across every rotation")
                .isEqualTo(expected);
        assertThat(rotations.get())
                .as("rotations must actually have happened, or this asserts nothing")
                .isPositive();
    }

    /** A mismatched passphrase must be refused outright, not silently produce garbage. */
    @Test
    void realLibsrtCallerWithAMismatchedPassphraseNeverConnects() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept("a-different-secret".toCharArray(), 16));

        CompletableFuture<SrtConnection> connected = new CompletableFuture<>();
        listener.onConnection(connected::complete);

        Path outputFile = Files.createTempFile("roast-interop-badsecret-", ".bin");
        outputFile.toFile().deleteOnExit();
        srtLiveTransmit = launchSrtLiveTransmitSending(listener.localAddress().getPort(), outputFile,
                "&passphrase=" + PASSPHRASE + "&pbkeylen=16");

        assertThatThrownBy(() -> connected.get(3, TimeUnit.SECONDS))
                .isInstanceOf(java.util.concurrent.TimeoutException.class);
    }

    /** {@code srt://} as input (libsrt calls and reads), {@code file://con} as output (raw bytes to stdout). */
    private Process launchSrtLiveTransmitSending(int listenerPort, Path outputFile) throws IOException {
        return launchSrtLiveTransmitSending(listenerPort, outputFile, "");
    }

    private Process launchSrtLiveTransmitSending(int listenerPort, Path outputFile, String extraUriParams)
            throws IOException {
        ProcessBuilder builder = new ProcessBuilder(
                srtLiveTransmitPath().toString(),
                "-t", "5",
                "srt://127.0.0.1:" + listenerPort + "?streamid=" + STREAM_ID + extraUriParams,
                "file://con")
                .redirectOutput(ProcessBuilder.Redirect.to(outputFile.toFile()))
                .redirectError(ProcessBuilder.Redirect.DISCARD);
        return builder.start();
    }

    private Process launchSrtLiveTransmit(int listenerPort, StringBuilder outputCollector) throws IOException {
        Path dummySource = Files.createTempFile("roast-interop-", ".ts");
        dummySource.toFile().deleteOnExit();
        byte[] payload = new byte[188 * 100]; // arbitrary - SRT doesn't care about payload content
        new SecureRandom().nextBytes(payload);
        Files.write(dummySource, payload);

        ProcessBuilder builder = new ProcessBuilder(
                srtLiveTransmitPath().toString(),
                "-t", "5",
                "file://con",
                "srt://127.0.0.1:" + listenerPort + "?streamid=" + STREAM_ID)
                .redirectInput(dummySource.toFile())
                .redirectErrorStream(true);
        Process process = builder.start();

        outputReaderThread = new Thread(() -> drain(process.getInputStream(), outputCollector), "srt-live-transmit-output");
        outputReaderThread.setDaemon(true);
        outputReaderThread.start();

        return process;
    }

    private static void drain(InputStream in, StringBuilder into) {
        try (in) {
            byte[] buffer = new byte[4096];
            int read;
            while ((read = in.read(buffer)) != -1) {
                synchronized (into) {
                    into.append(new String(buffer, 0, read));
                }
            }
        } catch (IOException e) {
            // Process ended / stream closed - nothing to do.
        }
    }

    private static Path srtLiveTransmitPath() {
        String override = System.getenv("SRT_LIVE_TRANSMIT");
        if (override != null) {
            return Path.of(override);
        }
        // Gradle's test working directory is the core/ project directory.
        return Path.of("..", "references", "srt", "build", "srt-live-transmit");
    }
}
