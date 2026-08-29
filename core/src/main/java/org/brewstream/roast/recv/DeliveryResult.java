package org.brewstream.roast.recv;

import org.brewstream.roast.packet.DataPacket;

import java.util.List;

/**
 * Result of a {@link ReceiveBuffer#deliver} call, in sequence order — the
 * caller now owns each packet's payload and must release it. Abandoned-gap
 * reporting lives on {@link ReceiveBuffer#computeAckBoundary} instead (see its
 * javadoc): gosrt's own TLPKTDROP "give up" decision happens during its ACK
 * boundary walk, not its delivery loop, and {@code deliver} here is now a
 * purely mechanical hand-out gated by that already-computed boundary.
 */
public record DeliveryResult(List<DataPacket> delivered) {
}
