package org.brewstream.roast.interop;

import org.brewstream.roast.socket.AcceptDecision;
import org.brewstream.roast.socket.AcceptedConnection;
import org.brewstream.roast.socket.ConnectionRequest;
import org.brewstream.roast.socket.SrtConnection;
import org.brewstream.roast.socket.SrtListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Interop test against the real SRT reference implementation (libsrt's
 * {@code srt-live-transmit}), not just our own codec — the actual definition of
 * done for DESIGN.md's Phase 2 ("ffmpeg/libsrt reaches connected"). Everything
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
