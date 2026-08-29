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
