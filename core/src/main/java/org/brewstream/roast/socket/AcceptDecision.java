package org.brewstream.roast.socket;

import org.brewstream.roast.packet.cif.RejectionReason;

/** An application's response to a {@link ConnectionRequest} from {@link AcceptHandler}. */
public sealed interface AcceptDecision {

    record Accept() implements AcceptDecision {
    }

    record Reject(RejectionReason reason) implements AcceptDecision {
    }

    static AcceptDecision accept() {
        return new Accept();
    }

    static AcceptDecision reject(RejectionReason reason) {
        return new Reject(reason);
    }
}
