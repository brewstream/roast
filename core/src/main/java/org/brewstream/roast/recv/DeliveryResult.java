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