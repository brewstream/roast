/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brewstream.roast.interop;

import org.brewstream.roast.socket.AcceptDecision;
import org.brewstream.roast.socket.SrtListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
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


    /**
     * The receiver path's definition of done: publish a 5 Mbps MPEG-TS from
     * ffmpeg to a Java listener and get byte-exact TS output with 0% loss.
     *
     * <p>Byte-exactness against ffmpeg looks impossible at first, since its
     * output isn't reproducible run to run. The {@code tee} muxer resolves it:
     * one encode, written simultaneously to a file and to us, so the file is
     * ground truth for that exact run. Without it the best available assertion
     * is structural (see {@link #weDecryptWhatARealFfmpegSenderEncrypts}), which
     * would not have caught a payload subtly reordered or truncated.
     *
     * <p>The received stream is compared as a prefix of the file rather than in
     * full: ffmpeg keeps writing until its own timer stops it, and we stop
     * reading when the connection goes away, so the tail is legitimately ragged.
     * Everything we did receive must match byte for byte from the first byte on.
     */
    @Test
    void aFiveMegabitStreamArrivesByteExact() throws Exception {
        listener = SrtListener.bind(new InetSocketAddress("127.0.0.1", 0));
        listener.setAcceptHandler(request -> AcceptDecision.accept());

        ByteArrayOutputStream received = new ByteArrayOutputStream();
        CountDownLatch enough = new CountDownLatch(1);
        listener.onConnection(connection -> connection.onData(payload -> {
            byte[] bytes = new byte[payload.readableBytes()];
            payload.getBytes(payload.readerIndex(), bytes);
            synchronized (received) {
                received.writeBytes(bytes);
                if (received.size() > 1_000_000) { // ~1.5s of 5 Mbps
                    enough.countDown();
                }
            }
            payload.release();
        }));

        Path groundTruth = Files.createTempFile("roast-5mbps-", ".ts");
        groundTruth.toFile().deleteOnExit();
        ffmpeg = launchFiveMegabitTee(listener.localAddress().getPort(), groundTruth);

        assertThat(enough.await(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .as("should have received a couple of seconds of 5 Mbps video").isTrue();

        // Let ffmpeg finish on its own -t timer rather than killing it: it buffers
        // its file writes, so a forced kill loses the tail of the ground truth and
        // we would appear to have received more than was ever produced.
        assertThat(ffmpeg.waitFor(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .as("ffmpeg should exit on its own so the ground-truth file is flushed").isTrue();

        byte[] got;
        synchronized (received) {
            got = received.toByteArray();
        }
        byte[] expected = Files.readAllBytes(groundTruth);
        assertThat(expected.length).isGreaterThanOrEqualTo(got.length);
        assertThat(Arrays.copyOf(expected, got.length))
                .as("every byte received must match what ffmpeg actually produced")
                .isEqualTo(got);
    }

    /** One encode, written to both a file (ground truth) and to us over SRT. */
    private Process launchFiveMegabitTee(int listenerPort, Path groundTruth) throws IOException {
        return new ProcessBuilder(
                ffmpegPath(),
                "-loglevel", "error",
                "-re",
                "-f", "lavfi", "-i", "testsrc=size=1280x720:rate=30",
                "-c:v", "mpeg2video", "-b:v", "5M", "-minrate", "5M", "-maxrate", "5M", "-bufsize", "1M",
                "-t", "6",
                "-f", "tee", "-map", "0:v",
                "[f=mpegts]" + groundTruth.toAbsolutePath()
                        + "|[f=mpegts]srt://127.0.0.1:" + listenerPort + "?streamid=" + STREAM_ID)
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD)
                .start();
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