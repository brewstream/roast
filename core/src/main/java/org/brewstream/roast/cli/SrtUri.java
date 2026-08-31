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

package org.brewstream.roast.cli;

import org.brewstream.roast.socket.SrtConfig;

import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * An {@code srt://} URI as {@code srt-java-live-transmit} accepts it, parsed
 * into the address, mode and {@link SrtConfig} the library actually wants.
 *
 * <pre>
 * srt://host:port?streamid=live/x&amp;latency=200
 * srt://:9000?mode=listener&amp;passphrase=secret&amp;pbkeylen=16
 * </pre>
 *
 * <p>Parameter names follow libsrt's own tool rather than inventing a dialect,
 * so a command line can usually be moved between the two unchanged:
 * {@code mode}, {@code streamid}, {@code passphrase}, {@code pbkeylen},
 * {@code latency}, {@code fc}.
 *
 * <p>Separate from the {@code main} class so it can be tested without spawning
 * a process — the parsing is where the fiddly cases live (a missing host
 * meaning "listen on any", an unknown parameter that should be reported rather
 * than ignored), and those deserve assertions rather than a manual run.
 */
public record SrtUri(
        InetSocketAddress address,
        boolean listener,
        String streamId,
        char[] passphrase,
        int keyLength,
        SrtConfig config) {

    private static final int DEFAULT_KEY_LENGTH = 16;

    public static boolean isSrt(String uri) {
        return uri.startsWith("srt://");
    }

    /**
     * @throws IllegalArgumentException with a message naming the offending part —
     *         a CLI's error text is its main documentation for anyone who got it wrong
     */
    public static SrtUri parse(String uri) {
        URI parsed = URI.create(uri);
        if (!"srt".equals(parsed.getScheme())) {
            throw new IllegalArgumentException("not an srt:// URI: " + uri);
        }
        Map<String, String> params = parseQuery(parsed.getRawQuery());
        boolean listener = "listener".equals(params.remove("mode"));

        String host = parsed.getHost();
        int port = parsed.getPort();
        if (host == null || host.isEmpty()) {
            // "srt://:9000" - the form libsrt's tool accepts for "listen on every
            // interface". java.net.URI gives up on such an authority entirely:
            // host is null AND port is -1, with the whole ":9000" left in
            // getAuthority(), so the port has to be recovered by hand.
            host = "0.0.0.0";
            listener = true;
            port = portFromAuthority(parsed.getAuthority(), uri);
        }
        if (port < 0) {
            throw new IllegalArgumentException("srt:// URI needs a port: " + uri);
        }

        String streamId = params.remove("streamid");
        String passphrase = params.remove("passphrase");
        int keyLength = intParam(params, "pbkeylen", DEFAULT_KEY_LENGTH);

        SrtConfig config = SrtConfig.defaults();
        Integer latency = optionalInt(params, "latency");
        if (latency != null) {
            config = config.withLatency(Duration.ofMillis(latency));
        }
        Integer flowWindow = optionalInt(params, "fc");
        if (flowWindow != null) {
            config = config.withFlowWindowPackets(flowWindow);
        }

        if (!params.isEmpty()) {
            // Silently ignoring a misspelled parameter is how someone ends up
            // convinced encryption is on when it isn't.
            throw new IllegalArgumentException("unknown srt:// parameter(s): " + params.keySet());
        }

        return new SrtUri(new InetSocketAddress(host, port), listener,
                streamId == null ? "" : streamId,
                passphrase == null ? null : passphrase.toCharArray(),
                keyLength, config);
    }

    public boolean isEncrypted() {
        return passphrase != null;
    }

    /** Never renders the passphrase — a CLI banner is exactly where one would leak. */
    @Override
    public String toString() {
        return "srt://" + address.getHostString() + ":" + address.getPort()
                + " [" + (listener ? "listener" : "caller")
                + (streamId.isEmpty() ? "" : ", streamid=" + streamId)
                + (isEncrypted() ? ", encrypted" : "")
                + ", latency=" + config.latencyMillis() + "ms]";
    }

    private static int portFromAuthority(String authority, String uri) {
        if (authority == null || !authority.startsWith(":")) {
            throw new IllegalArgumentException("srt:// URI needs a port: " + uri);
        }
        try {
            return Integer.parseInt(authority.substring(1));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("srt:// URI needs a port: " + uri);
        }
    }

    private static Map<String, String> parseQuery(String rawQuery) {
        Map<String, String> params = new LinkedHashMap<>();
        if (rawQuery == null || rawQuery.isEmpty()) {
            return params;
        }
        for (String pair : rawQuery.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException("malformed srt:// parameter: " + pair);
            }
            params.put(pair.substring(0, eq), java.net.URLDecoder.decode(
                    pair.substring(eq + 1), java.nio.charset.StandardCharsets.UTF_8));
        }
        return params;
    }

    private static int intParam(Map<String, String> params, String name, int fallback) {
        Integer value = optionalInt(params, name);
        return value == null ? fallback : value;
    }

    private static Integer optionalInt(Map<String, String> params, String name) {
        String raw = params.remove(name);
        if (raw == null) {
            return null;
        }
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a number, got '" + raw + "'");
        }
    }

    /** Defensive copy so the caller can zero its own array. */
    @Override
    public char[] passphrase() {
        return passphrase == null ? null : passphrase.clone();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof SrtUri other
                && address.equals(other.address)
                && listener == other.listener
                && streamId.equals(other.streamId)
                && keyLength == other.keyLength;
    }

    @Override
    public int hashCode() {
        return new HashMap<>(Map.of("a", address, "s", streamId)).hashCode() + (listener ? 1 : 0) + keyLength;
    }
}