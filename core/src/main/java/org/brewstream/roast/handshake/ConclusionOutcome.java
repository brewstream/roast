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

import org.brewstream.roast.packet.cif.HandshakeCif;

/**
 * Result of {@link ListenerHandshake#validateConclusion}. {@link Valid} means the
 * CONCLUSION passed protocol-level checks (cookie, version, required capability
 * flags) — the caller must still run its own app-level accept/reject decision (e.g.
 * on StreamID) before calling {@link ListenerHandshake#buildAcceptResponse} or
 * {@link ListenerHandshake#buildRejectResponse}. {@link Rejected} means a
 * protocol-level check already failed, with the response CIF ready to send.
 */
public sealed interface ConclusionOutcome {

    record Valid(HandshakeCif request) implements ConclusionOutcome {
    }

    record Rejected(HandshakeCif response) implements ConclusionOutcome {
    }
}