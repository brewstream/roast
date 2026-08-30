package org.brewstream.roast.socket;

import java.time.Duration;

/**
 * Tunable settings for a listener or caller. Immutable; {@link #defaults()} is
 * usable as-is, and the {@code with*} methods return modified copies.
 *
 * <pre>{@code
 * SrtListener.bind(address, SrtConfig.defaults()
 *         .withLatency(Duration.ofMillis(200)));
 * }</pre>
 *
 * <p><b>What is deliberately not here.</b>
 *
 * <p>The <em>passphrase</em>. It travels on {@link AcceptDecision} for the
 * listener and as a {@code connect} argument for the caller, so it can be
 * decided per stream and so a secret never sits in an object an application
 * might log, share between listeners, or hold for the process's lifetime. A
 * general settings bag is the wrong home for one.
 *
 * <p>Protocol <em>timing</em> — the ~10ms tick, the NAK interval floor, the
 * 250ms handshake retry, the close-drain timeout, the SYN-cookie window. These
 * are not preferences: they are either fixed by the protocol, derived from
 * measured RTT, or copied from libsrt's own rules, and an embedder changing
 * them would quietly break interoperability or security rather than tune
 * anything. Every public knob is permanent API and a new way to be
 * misconfigured; libsrt has some forty socket options and most people get them
 * wrong. Six is a deliberate ceiling, not an oversight — if something here
 * genuinely needs tuning later, adding it then is easy, whereas removing a knob
 * never is.
 *
 * @param latency               TSBPD delivery delay, the central SRT trade-off: longer gives
 *                              retransmission more room to recover loss, at the cost of
 *                              end-to-end delay. Negotiated with the peer, which may raise it.
 * @param connectTimeout        how long {@code SrtCaller.connect} keeps retrying before failing
 * @param flowWindowPackets     the receive window advertised to a peer, bounding how much it
 *                              may have in flight — memory against throughput
 * @param keyRefreshPackets     packets sent before the encryption key rotates
 * @param keyPreAnnouncePackets how far ahead of a rotation the next key is announced
 * @param srtVersion            the SRT version advertised in the handshake, for compatibility
 *                              testing against a specific peer
 * @param maxMss                largest acceptable MTU, in bytes — lower it for tunnels or VPNs
 *                              whose path MTU is below the usual 1500
 */
public record SrtConfig(
        Duration latency,
        Duration connectTimeout,
        int flowWindowPackets,
        long keyRefreshPackets,
        long keyPreAnnouncePackets,
        int srtVersion,
        int maxMss) {

    /** gosrt's own baseline, and what this codebase advertised before it was configurable. */
    private static final int DEFAULT_SRT_VERSION = 0x010401;

    public SrtConfig {
        // Validate on construction so a bad value fails at the call site rather
        // than halfway through a handshake, where the cause is far less obvious.
        requirePositive(latency, "latency");
        requirePositive(connectTimeout, "connectTimeout");
        if (flowWindowPackets <= 0) {
            throw new IllegalArgumentException("flowWindowPackets must be positive, got " + flowWindowPackets);
        }
        if (keyPreAnnouncePackets >= keyRefreshPackets) {
            throw new IllegalArgumentException(
                    "keyPreAnnouncePackets must be smaller than keyRefreshPackets - the next key has to be "
                            + "announced before the rotation it announces");
        }
        if (keyPreAnnouncePackets <= 0) {
            throw new IllegalArgumentException("keyPreAnnouncePackets must be positive");
        }
        if (maxMss < 576 || maxMss > 1500) {
            // 576 is IPv4's guaranteed-reassembly minimum; above 1500 a datagram
            // would fragment on any ordinary path.
            throw new IllegalArgumentException("maxMss must be between 576 and 1500, got " + maxMss);
        }
    }

    /** SRT's usual defaults: 120ms latency, 8192-packet window, key rotation every 2^24 packets. */
    public static SrtConfig defaults() {
        return new SrtConfig(
                Duration.ofMillis(120),
                Duration.ofSeconds(5),
                8192,
                1L << 24,
                1L << 12,
                DEFAULT_SRT_VERSION,
                1500);
    }

    public SrtConfig withLatency(Duration latency) {
        return new SrtConfig(latency, connectTimeout, flowWindowPackets, keyRefreshPackets,
                keyPreAnnouncePackets, srtVersion, maxMss);
    }

    public SrtConfig withConnectTimeout(Duration connectTimeout) {
        return new SrtConfig(latency, connectTimeout, flowWindowPackets, keyRefreshPackets,
                keyPreAnnouncePackets, srtVersion, maxMss);
    }

    public SrtConfig withFlowWindowPackets(int flowWindowPackets) {
        return new SrtConfig(latency, connectTimeout, flowWindowPackets, keyRefreshPackets,
                keyPreAnnouncePackets, srtVersion, maxMss);
    }

    /**
     * @param refreshPackets     packets sent before the key rotates
     * @param preAnnouncePackets how far ahead the replacement is announced; must be smaller
     */
    public SrtConfig withKeyRotation(long refreshPackets, long preAnnouncePackets) {
        return new SrtConfig(latency, connectTimeout, flowWindowPackets, refreshPackets,
                preAnnouncePackets, srtVersion, maxMss);
    }

    public SrtConfig withSrtVersion(int srtVersion) {
        return new SrtConfig(latency, connectTimeout, flowWindowPackets, keyRefreshPackets,
                keyPreAnnouncePackets, srtVersion, maxMss);
    }

    public SrtConfig withMaxMss(int maxMss) {
        return new SrtConfig(latency, connectTimeout, flowWindowPackets, keyRefreshPackets,
                keyPreAnnouncePackets, srtVersion, maxMss);
    }

    /** Latency as the milliseconds the handshake actually carries. */
    public int latencyMillis() {
        return (int) latency.toMillis();
    }

    private static void requirePositive(Duration value, String name) {
        if (value == null || value.isNegative() || value.isZero()) {
            throw new IllegalArgumentException(name + " must be a positive duration, got " + value);
        }
    }
}
