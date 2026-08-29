package org.brewstream.roast.socket;

/** Application hook for deciding whether to accept an incoming connection. */
@FunctionalInterface
public interface AcceptHandler {
    AcceptDecision handle(ConnectionRequest request);
}
