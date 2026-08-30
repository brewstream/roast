package org.brewstream.roast.socket;

import org.brewstream.roast.packet.cif.RejectionReason;

/** An application's response to a {@link ConnectionRequest} from {@link AcceptHandler}. */
public sealed interface AcceptDecision {

    /**
     * {@code passphrase} is {@code null} for an unencrypted connection. When
     * present, the connection decrypts inbound payloads and encrypts outbound
     * ones; a peer whose passphrase doesn't match is rejected with
     * {@link RejectionReason#BADSECRET}, and one that offered no key material at
     * all with {@link RejectionReason#UNSECURE}.
     *
     * <p>The passphrase lives here, on the per-request decision, rather than on
     * {@code SrtListener} as a socket-wide setting — which is what both
     * references do (libsrt's {@code SRTO_PASSPHRASE}, gosrt's
     * {@code Config.Passphrase}). The handler already receives the
     * {@link ConnectionRequest}, StreamID included, and the peer's key material
     * arrives in the very same CONCLUSION packet, so deciding per stream costs
     * nothing and a relay can hold a different secret per stream. Deliberately
     * <em>not</em> folded into a general settings object: a secret has no
     * business in something an application might log or share.
     */
    record Accept(char[] passphrase, int keyLength) implements AcceptDecision {

        public boolean isEncrypted() {
            return passphrase != null;
        }

        /** Deliberately reveals no passphrase — this is exactly the kind of value that lands in a log line. */
        @Override
        public String toString() {
            return "Accept[encrypted=" + isEncrypted() + (isEncrypted() ? ", keyLength=" + keyLength : "") + "]";
        }
    }

    record Reject(RejectionReason reason) implements AcceptDecision {
    }

    /** Accept without encryption. */
    static AcceptDecision accept() {
        return new Accept(null, 0);
    }

    /**
     * Accept, requiring the peer to have keyed with {@code passphrase}. The array
     * is copied, so the caller may zero its own; the connection zeroes the copy
     * when it closes.
     *
     * @param keyLength the Stream Encrypting Key length in bytes — 16, 24, or 32
     */
    static AcceptDecision accept(char[] passphrase, int keyLength) {
        return new Accept(passphrase.clone(), keyLength);
    }

    static AcceptDecision reject(RejectionReason reason) {
        return new Reject(reason);
    }
}
