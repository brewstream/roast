package org.brewstream.roast.interop;

import org.brewstream.roast.socket.AcceptDecision;
import org.brewstream.roast.socket.SrtListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Interop against {@code ffmpeg}'s own {@code srt://} muxer, which links libsrt.
 *
 * <p>This exists because {@code srt-live-transmit} cannot drive the direction
 * that matters here. Reading from a redirected file it connects but never
 * transmits — confirmed from its own {@code pktSent} stats staying empty — so
 * every {@link LibsrtInteropTest} case has libsrt <em>receiving</em>. ffmpeg
 * genuinely sends, which makes it the only way to verify that a real
 * implementation's <em>encrypted output</em> is something we can decrypt.
 *
 * <p>ffmpeg is a heavier dependency than the locally-built libsrt the sibling
 * tests use, so like them this skips itself via {@link Assumptions} rather than
 * failing when it isn't installed.
 */
@Tag("interop")
class FfmpegInteropTest {

    private static final int TIMEOUT_SECONDS = 20;
    private static final String STREAM_ID = "live/ffmpeg";
    /** libsrt requires at least 10 characters. */
    private static final String PASSPHRASE = "roast-ffmpeg-secret";

    private SrtListener listener;
    private Process ffmpeg;

    @BeforeEach
    void checkPrerequisite() {
        Assumptions.assumeTrue(ffmpegPath() != null, "ffmpeg not found on PATH");
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (ffmpeg != null) {
            ffmpeg.destroyForcibly();
        }
        if (listener != null) {
            listener.close();
        }
    }

    /**
     * The direction {@code srt-live-transmit} can't drive: a real libsrt-backed
     * sender encrypts, and we decrypt. Asserts on MPEG-TS structure rather than
     * exact bytes, since ffmpeg's output isn't reproducible — but the assertion
     * is still strong: every delivered payload must be a whole number of 188-byte
     * TS packets, each starting with the 0x47 sync byte. Failed decryption
     * produces bytes that are essentially random, so passing this by accident is
     * not a realistic outcome.
     */
    @Test
    void weDecryptWhatARealFfmpegSenderEncrypts() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept(PASSPHRASE.toCharArray(), 16));

        AtomicInteger payloads = new AtomicInteger();
        AtomicInteger bytes = new AtomicInteger();
        CompletableFuture<Void> enoughArrived = new CompletableFuture<>();
        CompletableFuture<String> firstProblem = new CompletableFuture<>();

        listener.onConnection(connection -> connection.onData(payload -> {
            int length = payload.readableBytes();
            if (length == 0 || length % 188 != 0) {
                firstProblem.complete("payload of " + length + " bytes is not a whole number of TS packets");
            } else {
                for (int offset = 0; offset < length; offset += 188) {
                    if (payload.getByte(payload.readerIndex() + offset) != 0x47) {
                        firstProblem.complete("missing TS sync byte at offset " + offset);
                        break;
                    }
                }
            }
            payload.release();

            bytes.addAndGet(length);
            if (payloads.incrementAndGet() >= 50) {
                enoughArrived.complete(null);
            }
        }));

        ffmpeg = launchEncryptedPublisher(listener.localAddress().getPort());

        enoughArrived.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(firstProblem).as("decrypted payloads should be valid MPEG-TS").isNotDone();
        assertThat(payloads.get()).isGreaterThanOrEqualTo(50);
        assertThat(bytes.get()).isGreaterThan(50 * 188);
    }

    private Process launchEncryptedPublisher(int listenerPort) throws IOException {
        return new ProcessBuilder(
                ffmpegPath(),
                "-loglevel", "error",
                "-re",
                "-f", "lavfi", "-i", "testsrc=size=320x240:rate=30",
                "-t", "10",
                "-f", "mpegts",
                "srt://127.0.0.1:" + listenerPort + "?streamid=" + STREAM_ID
                        + "&passphrase=" + PASSPHRASE + "&pbkeylen=16")
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .start();
    }

    private static String ffmpegPath() {
        String override = System.getenv("FFMPEG");
        if (override != null) {
            return override;
        }
        for (String candidate : new String[]{"/opt/homebrew/bin/ffmpeg", "/usr/local/bin/ffmpeg", "/usr/bin/ffmpeg"}) {
            if (new java.io.File(candidate).canExecute()) {
                return candidate;
            }
        }
        return null;
    }
}
