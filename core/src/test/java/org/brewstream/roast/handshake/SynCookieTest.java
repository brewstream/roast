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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link #matchesGosrtGoldenVector()} mirrors github.com/datarhei/gosrt
 * net.SYNCookie's own TestSYNCookie exactly (see
 * references/gosrt/net/syncookie_test.go), including its window-rollover sequence.
 */
class SynCookieTest {

    @Test
    void matchesGosrtGoldenVector() {
        long[] window = {0};
        SynCookie cookie = new SynCookie(
                "dl2INvNSQTZ5zQu9MxNmGyAVmNkB33io",
                "nwj2qrsh3xyC8OmCp1gObD0iOtQNQsLi",
                "192.168.0.1",
                () -> window[0]);

        int value = cookie.get("192.168.0.2");

        assertThat(value).isEqualTo(0xe6303651);
        assertThat(cookie.verify(value, "192.168.0.2")).isTrue();
        assertThat(cookie.verify(value, "192.168.0.3")).isFalse();
        assertThat(cookie.verify(value - 95854, "192.168.0.2")).isFalse();

        window[0] = 1; // one window later - still within the current-or-previous grace period.
        assertThat(cookie.verify(value, "192.168.0.2")).isTrue();

        window[0] = 2; // two windows later - outside the grace period.
        assertThat(cookie.verify(value, "192.168.0.2")).isFalse();
    }

    @Test
    void forListenerProducesAVerifiableCookie() {
        SynCookie cookie = SynCookie.forListener("10.0.0.1:9710");

        int value = cookie.get("203.0.113.5:4000");

        assertThat(cookie.verify(value, "203.0.113.5:4000")).isTrue();
        assertThat(cookie.verify(value, "203.0.113.6:4000")).isFalse();
    }

    @Test
    void forListenerUsesFreshRandomSecretsEachTime() {
        SynCookie a = SynCookie.forListener("10.0.0.1:9710");
        SynCookie b = SynCookie.forListener("10.0.0.1:9710");

        assertThat(a.get("203.0.113.5:4000")).isNotEqualTo(b.get("203.0.113.5:4000"));
    }
}