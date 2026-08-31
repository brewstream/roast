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

package org.brewstream.roast.socket;

import org.brewstream.roast.packet.SrtSocketId;

import java.security.SecureRandom;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;

/**
 * Generates SRT socket IDs for newly accepted connections, guaranteeing no
 * collision with an ID this generator has already handed out and not yet
 * {@link #release released}. Socket ID {@code 0} is never generated — it's
 * reserved for handshake induction (see {@link SrtSocketIdDemultiplexer}).
 *
 * <p>This generator is the single source of truth for the IDs it hands out: a
 * caller (a future {@code SrtListener}) must consistently allocate through one
 * instance and release on teardown, or collisions become possible again.
 *
 * <p>Mirrors gosrt's {@code connRequest.generateSocketId} (conn_request.go): a
 * random 32-bit value, retried a bounded number of times against the in-use set.
 */
public final class SrtSocketIdGenerator {

    private static final int MAX_ATTEMPTS = 10;

    private final IntSupplier randomSource;
    private final Set<SrtSocketId> inUse = ConcurrentHashMap.newKeySet();

    public SrtSocketIdGenerator() {
        this(new SecureRandom()::nextInt);
    }

    SrtSocketIdGenerator(IntSupplier randomSource) {
        this.randomSource = randomSource;
    }

    /**
     * Reserves and returns a socket ID not currently in use by this generator.
     *
     * @throws IllegalStateException if no unused ID was found within a bounded
     *         number of attempts — astronomically unlikely against real randomness
     *         short of the ID space being nearly exhausted.
     */
    public SrtSocketId generate() {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            int candidate = randomSource.getAsInt();
            if (candidate == 0) {
                continue;
            }
            SrtSocketId id = SrtSocketId.of(candidate);
            if (inUse.add(id)) {
                return id;
            }
        }
        throw new IllegalStateException(
                "could not generate an unused SRT socket id after " + MAX_ATTEMPTS + " attempts");
    }

    /** Releases {@code id} for reuse, e.g. once its connection has torn down. */
    public void release(SrtSocketId id) {
        inUse.remove(id);
    }

    public boolean isInUse(SrtSocketId id) {
        return inUse.contains(id);
    }
}