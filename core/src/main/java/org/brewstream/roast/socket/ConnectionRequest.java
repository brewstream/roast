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

import java.net.InetSocketAddress;

/**
 * What an {@link AcceptHandler} sees about an incoming connection attempt that has
 * already passed protocol-level validation (SYN cookie, SRT version, required
 * capability flags) — the app-level decision is whatever's left, typically based on
 * {@code streamId} and/or {@code peerAddress}.
 *
 * <p>{@code encryptionRequested} is true when the peer offered key material, or
 * declared a cipher in the handshake's Encryption Field. Both are checked rather
 * than just the field, because gosrt sends zero there even when encrypting —
 * trusting the field alone reports an encrypting peer as plaintext.
 */
public record ConnectionRequest(
        InetSocketAddress peerAddress,
        SrtSocketId peerSocketId,
        String streamId,
        int srtVersion,
        boolean encryptionRequested,
        int peerReceiveTsbpdDelayMillis,
        int peerSendTsbpdDelayMillis) {
}