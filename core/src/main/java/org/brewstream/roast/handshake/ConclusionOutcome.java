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
