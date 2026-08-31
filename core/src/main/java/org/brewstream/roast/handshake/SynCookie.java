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

package org.brewstream.roast.handshake;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.function.LongSupplier;

/**
 * SYN cookie for a listener's handshake induction step: lets the listener issue a
 * cookie for an INDUCTION request and later verify it was echoed back correctly in
 * the CONCLUSION, without keeping per-attempt state — the standard SYN-cookie
 * defense against handshake-flood DoS. A cookie is valid for the current ~64-second
 * time window plus the one before it (~128s total), rather than a fixed expiry the
 * listener would otherwise have to track per attempt.
 *
 * <p>Ported from gosrt's {@code net.SYNCookie} (github.com/datarhei/gosrt, MIT
 * licensed; see {@code roast/references/gosrt/net/syncookie.go}), verified against
 * its own golden vector.
 */
public final class SynCookie {

    private static final long WINDOW_SECONDS = 64;
    private static final String ALPHANUMERIC =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    private static final int SECRET_LENGTH = 32;

    private final String secret1;
    private final String secret2;
    private final String listenerAddress;
    private final LongSupplier windowCounter;

    SynCookie(String secret1, String secret2, String listenerAddress, LongSupplier windowCounter) {
        this.secret1 = secret1;
        this.secret2 = secret2;
        this.listenerAddress = listenerAddress;
        this.windowCounter = windowCounter;
    }

    /**
     * Creates a cookie generator for a listener bound at {@code listenerAddress}
     * (its own address — a stable string identifying it, e.g. {@code "host:port"}),
     * with fresh random secrets.
     */
    public static SynCookie forListener(String listenerAddress) {
        SecureRandom random = new SecureRandom();
        return new SynCookie(randomSecret(random), randomSecret(random), listenerAddress,
                () -> Instant.now().getEpochSecond() / WINDOW_SECONDS);
    }

    private static String randomSecret(SecureRandom random) {
        StringBuilder secret = new StringBuilder(SECRET_LENGTH);
        for (int i = 0; i < SECRET_LENGTH; i++) {
            secret.append(ALPHANUMERIC.charAt(random.nextInt(ALPHANUMERIC.length())));
        }
        return secret.toString();
    }

    /** The cookie to send in response to an INDUCTION request from {@code senderAddress}. */
    public int get(String senderAddress) {
        return calculate(windowCounter.getAsLong(), senderAddress);
    }

    /**
     * Whether {@code cookie} could have come from {@link #get} for
     * {@code senderAddress} within the current or immediately preceding time window.
     */
    public boolean verify(int cookie, String senderAddress) {
        long window = windowCounter.getAsLong();
        return calculate(window, senderAddress) == cookie || calculate(window - 1, senderAddress) == cookie;
    }

    private int calculate(long window, String senderAddress) {
        String data = secret1 + listenerAddress + senderAddress + secret2 + window;
        byte[] digest = md5(data.getBytes(StandardCharsets.UTF_8));
        return ((digest[0] & 0xFF) << 24) | ((digest[1] & 0xFF) << 16)
                | ((digest[2] & 0xFF) << 8) | (digest[3] & 0xFF);
    }

    private static byte[] md5(byte[] data) {
        try {
            return MessageDigest.getInstance("MD5").digest(data);
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("MD5 is a required JDK algorithm", e);
        }
    }
}