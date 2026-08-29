package org.brewstream.roast.recv;

import org.brewstream.roast.packet.DataPacket;
import org.brewstream.roast.packet.cif.LossRange;

import java.util.List;

/**
 * Result of a {@link ReceiveBuffer#deliver} call. {@code delivered} is in
 * sequence order — the caller now owns each packet's payload and must release
 * it. {@code abandoned} lists any gaps TLPKTDROP gave up on this call (usually
 * at most one, but a single tick can in principle open more than one) — a caller
 * wiring this into a live connection should clear each from its {@link LossList}
 * via {@link LossList#abandon}.
 */
public record DeliveryResult(List<DataPacket> delivered, List<LossRange> abandoned) {
}
