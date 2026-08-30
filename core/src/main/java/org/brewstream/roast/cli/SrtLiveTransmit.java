package org.brewstream.roast.cli;

import io.netty.buffer.Unpooled;
import org.brewstream.roast.socket.AcceptDecision;
import org.brewstream.roast.socket.ConnectionStats;
import org.brewstream.roast.socket.SrtCaller;
import org.brewstream.roast.socket.SrtConnection;
import org.brewstream.roast.socket.SrtListener;
import org.brewstream.roast.socket.StatsSampler;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Relays a live stream between an {@code srt://} endpoint and standard
 * input/output — Roast's counterpart to libsrt's {@code srt-live-transmit}, and
 * the reference command-line tool.
 *
 * <pre>
 * srt-java-live-transmit [-stats &lt;seconds&gt;] [-t &lt;seconds&gt;] &lt;input&gt; &lt;output&gt;
 *
 *   ffmpeg ... -f mpegts - | srt-java-live-transmit file://con "srt://host:9000?streamid=live/x"
 *   srt-java-live-transmit "srt://:9000?mode=listener" file://con | ffplay -
 * </pre>
 *
 * <p>The argument shape and parameter names deliberately mirror libsrt's tool
 * rather than inventing a dialect, so a command line usually moves between the
 * two unchanged — which also makes the two directly comparable when something
 * looks wrong, exactly as they were during this project's own debugging.
 *
 * <p>Unlike {@code RelayDemo}, which stays a deliberately rough
 * publish/subscribe toy, this is held to the codebase's normal standards: it is
 * the worked example of the public API, so how it reads matters.
 */
public final class SrtLiveTransmit {

    private static final String CONSOLE = "file://con";

    public static void main(String[] args) throws Exception {
        Arguments arguments;
        try {
            arguments = Arguments.parse(args);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.err.println();
            printUsage();
            System.exit(1);
            return;
        }

        try {
            new SrtLiveTransmit().run(arguments);
        } catch (IllegalArgumentException e) {
            System.err.println("error: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void printUsage() {
        System.err.println("""
                Usage: srt-java-live-transmit [options] <input-uri> <output-uri>

                  URIs:
                    srt://host:port[?params]   connect to a listener (caller)
                    srt://[host]:port?mode=listener
                    file://con                 standard input / standard output

                  srt:// parameters (as in libsrt's srt-live-transmit):
                    mode=caller|listener   streamid=<id>
                    passphrase=<secret>    pbkeylen=16|24|32
                    latency=<ms>           fc=<packets>

                  Options:
                    -stats <seconds>   print per-connection statistics
                    -t <seconds>       exit after this long

                  Examples:
                    ffmpeg -re -i in.ts -c copy -f mpegts - \\
                      | srt-java-live-transmit file://con "srt://127.0.0.1:9000?streamid=live/x"

                    srt-java-live-transmit "srt://:9000?mode=listener" file://con | ffplay -
                """);
    }

    private void run(Arguments arguments) throws Exception {
        boolean sending = CONSOLE.equals(arguments.input);
        String endpointUri = sending ? arguments.output : arguments.input;
        if (!SrtUri.isSrt(endpointUri)) {
            throw new IllegalArgumentException(
                    "exactly one side must be an srt:// URI and the other " + CONSOLE);
        }
        SrtUri endpoint = SrtUri.parse(endpointUri);
        System.err.println("Roast: " + (sending ? "stdin -> " : "stdout <- ") + endpoint);

        CountDownLatch finished = new CountDownLatch(1);
        if (endpoint.listener()) {
            runListener(endpoint, sending, arguments, finished);
        } else {
            runCaller(endpoint, sending, arguments, finished);
        }

        if (arguments.timeoutSeconds > 0) {
            finished.await(arguments.timeoutSeconds, TimeUnit.SECONDS);
        } else {
            finished.await();
        }
    }

    private void runListener(SrtUri endpoint, boolean sending, Arguments arguments, CountDownLatch finished)
            throws Exception {
        SrtListener listener = SrtListener.bind(endpoint.address(), endpoint.config());
        listener.setAcceptHandler(request -> endpoint.isEncrypted()
                ? AcceptDecision.accept(endpoint.passphrase(), endpoint.keyLength())
                : AcceptDecision.accept());
        listener.onConnection(connection -> {
            System.err.println("Roast: connected from " + connection.metadata().peerAddress()
                    + (connection.metadata().streamId().isEmpty()
                            ? "" : " streamid=" + connection.metadata().streamId()));
            wire(connection, sending, finished);
        });

        StatsSampler sampler = startStatsIfRequested(arguments, listener);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            closeQuietly(sampler);
            try {
                listener.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    private void runCaller(SrtUri endpoint, boolean sending, Arguments arguments, CountDownLatch finished)
            throws Exception {
        CompletableFuture<SrtConnection> connecting = endpoint.isEncrypted()
                ? SrtCaller.connect(endpoint.address(), endpoint.streamId(),
                        endpoint.passphrase(), endpoint.keyLength(), endpoint.config())
                : SrtCaller.connect(endpoint.address(), endpoint.streamId(), endpoint.config());

        SrtConnection connection = connecting.get(30, TimeUnit.SECONDS);
        System.err.println("Roast: connected to " + endpoint.address());
        wire(connection, sending, finished);

        StatsSampler sampler = arguments.statsSeconds > 0
                ? StatsSampler.start(Duration.ofSeconds(arguments.statsSeconds),
                        () -> java.util.List.of(connection), SrtLiveTransmit::printStats)
                : null;
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            closeQuietly(sampler);
            connection.close();
        }));
    }

    /** Pumps stdin into the connection, or the connection into stdout. */
    private void wire(SrtConnection connection, boolean sending, CountDownLatch finished) {
        connection.onClose(finished::countDown);

        if (!sending) {
            OutputStream out = System.out;
            connection.onData(payload -> {
                try {
                    byte[] bytes = new byte[payload.readableBytes()];
                    payload.getBytes(payload.readerIndex(), bytes);
                    out.write(bytes);
                    out.flush();
                } catch (IOException e) {
                    finished.countDown();
                } finally {
                    payload.release();
                }
            });
            return;
        }

        // One packet per read, sized to what the connection will actually accept:
        // live mode does not split messages, so an oversized write is rejected
        // rather than fragmented (see SrtConnection.write).
        int chunkSize = connection.metadata().maxPayloadSize();
        Thread pump = new Thread(() -> {
            try (InputStream in = System.in) {
                byte[] buffer = new byte[chunkSize];
                int read;
                while ((read = in.read(buffer)) != -1) {
                    if (read > 0) {
                        connection.write(Unpooled.copiedBuffer(buffer, 0, read));
                    }
                }
            } catch (IOException e) {
                System.err.println("Roast: input ended: " + e.getMessage());
            } finally {
                // Closing drains what is still queued rather than discarding it,
                // so the tail of the stream reaches the peer.
                connection.close();
                finished.countDown();
            }
        }, "srt-stdin-pump");
        pump.setDaemon(true);
        pump.start();
    }

    private static StatsSampler startStatsIfRequested(Arguments arguments, SrtListener listener) {
        return arguments.statsSeconds > 0
                ? StatsSampler.start(Duration.ofSeconds(arguments.statsSeconds), listener,
                        SrtLiveTransmit::printStats)
                : null;
    }

    private static void printStats(SrtConnection connection, ConnectionStats stats) {
        System.err.printf(
                "Roast stats %s: sent=%d recv=%d retrans=%d (%.2f%%) lost=%d dropped=%d "
                        + "rtt=%.1fms rate=%dkbps buf(rcv/snd)=%d/%d%n",
                connection.metadata().streamId().isEmpty() ? "-" : connection.metadata().streamId(),
                stats.packetsSent(), stats.packetsReceived(), stats.packetsRetransmitted(),
                stats.retransmitRate() * 100, stats.packetsLost(), stats.packetsDropped(),
                stats.rttMicros() / 1000.0, stats.receiveRateBytesPerSecond() * 8 / 1000,
                stats.receiveBufferedPackets(), stats.sendBufferedPackets());
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception e) {
            // Shutting down anyway.
        }
    }

    private record Arguments(String input, String output, int statsSeconds, int timeoutSeconds) {

        static Arguments parse(String[] args) {
            String input = null;
            String output = null;
            int stats = 0;
            int timeout = 0;

            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "-stats" -> stats = requireNumber(args, ++i, "-stats");
                    case "-t" -> timeout = requireNumber(args, ++i, "-t");
                    default -> {
                        if (input == null) {
                            input = args[i];
                        } else if (output == null) {
                            output = args[i];
                        } else {
                            throw new IllegalArgumentException("unexpected argument: " + args[i]);
                        }
                    }
                }
            }
            if (input == null || output == null) {
                throw new IllegalArgumentException("both an input and an output URI are required");
            }
            return new Arguments(input, output, stats, timeout);
        }

        private static int requireNumber(String[] args, int index, String option) {
            if (index >= args.length) {
                throw new IllegalArgumentException(option + " needs a value");
            }
            try {
                return Integer.parseInt(args[index]);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(option + " must be a number, got '" + args[index] + "'");
            }
        }
    }
}
