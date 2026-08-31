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

import org.brewstream.roast.packet.cif.RejectionReason;

/**
 * Result of {@link CallerHandshake#validateConclusionReply} — the caller's
 * counterpart to {@link ConclusionOutcome}, with a third case: a listener always
 * has a rejection response ready to send back for anything it doesn't like, but
 * a caller receiving a broken/incompatible reply has no such universal move.
 */
public sealed interface ConclusionReplyOutcome {

    /** The negotiated TSBPD latencies, per {@code max(ours, peer's opposite-direction value)}. */
    record Connected(int srtVersion, int receiveLatencyMillis, int sendLatencyMillis) implements ConclusionReplyOutcome {
    }

    /** The reply's handshake-type field was a rejection code. May be null if the code has no name in {@link RejectionReason}. */
    record Rejected(RejectionReason reason) implements ConclusionReplyOutcome {
    }

    /** A progression reply that fails protocol-level validation (version, missing extension, required flags, stream mode). */
    record ProtocolViolation(String reason) implements ConclusionReplyOutcome {
    }
}