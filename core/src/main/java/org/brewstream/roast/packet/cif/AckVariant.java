package org.brewstream.roast.packet.cif;

/**
 * Which fields an ACK CIF carries — determined purely by its encoded byte length
 * (4/16/28), not a marker field. Per draft-sharabayko-srt.md's ACK section: a Full
 * ACK is sent periodically (every ~10ms) with every field; Light and Small ACKs
 * are cheaper acknowledgments sent more often at high data rates, and the sender
 * only replies with ACKACK to a Full ACK.
 */
public enum AckVariant {
    /** Just the last-acknowledged sequence number. */
    LITE,
    /** Adds RTT, RTT variance, and available buffer size. */
    SMALL,
    /** All fields, including receiving rate and link capacity estimates. */
    FULL
}
