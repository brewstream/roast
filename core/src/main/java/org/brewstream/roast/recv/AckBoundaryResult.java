/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.brewstream.roast.recv;

import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.util.CircularNumber;

import java.util.List;

/**
 * Result of a {@link ReceiveBuffer#computeAckBoundary} call. {@code
 * lastAckSequenceNumber} is the highest sequence number now confirmed (either
 * genuinely received in order, or skipped past because its own TSBPD deadline
 * already passed) — a caller building the ACK CIF's "last ACK" field should
 * report {@code lastAckSequenceNumber.inc()}, matching the field's "everything
 * before this is confirmed" convention. {@code abandoned} lists any gaps given
 * up on to reach that boundary (usually at most one call's worth, but a
 * single tick can in principle skip more than one) — a caller wiring this into
 * a live connection should clear each from its {@link LossList} via {@link
 * LossList#abandon}.
 */
public record AckBoundaryResult(CircularNumber lastAckSequenceNumber, List<LossRange> abandoned) {
}