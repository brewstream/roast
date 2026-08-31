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

package org.brewstream.roast.packet;

import java.util.Objects;

/**
 * The 32-bit destination socket ID that SRT uses to multiplex many logical
 * connections over a single UDP socket. Socket ID {@code 0} is reserved for
 * induction handshake packets sent before a connection exists.
 */
public final class SrtSocketId {

    public static final SrtSocketId ZERO = new SrtSocketId(0);

    private final int value;

    private SrtSocketId(int value) {
        this.value = value;
    }

    public static SrtSocketId of(int value) {
        return value == 0 ? ZERO : new SrtSocketId(value);
    }

    public int value() {
        return value;
    }

    public boolean isZero() {
        return value == 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SrtSocketId other && other.value == value;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(value);
    }

    @Override
    public String toString() {
        return "SrtSocketId(" + Integer.toUnsignedString(value) + ")";
    }
}