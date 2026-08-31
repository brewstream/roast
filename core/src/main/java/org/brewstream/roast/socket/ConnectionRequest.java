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
