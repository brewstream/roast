package org.brewstream.roast.socket;

import org.brewstream.roast.packet.SrtSocketId;

import java.net.InetSocketAddress;

/**
 * What an {@link AcceptHandler} sees about an incoming connection attempt that has
 * already passed protocol-level validation (SYN cookie, SRT version, required
 * capability flags) — the app-level decision is whatever's left, typically based on
 * {@code streamId} and/or {@code peerAddress}.
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
