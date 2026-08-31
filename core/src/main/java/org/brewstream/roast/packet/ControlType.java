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

package org.brewstream.roast.packet;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** SRT control packet types, encoded in bits 30-16 of the common header. */
public enum ControlType {
    HANDSHAKE(0x0),
    KEEPALIVE(0x1),
    ACK(0x2),
    NAK(0x3),
    CONGESTION_WARNING(0x4),
    SHUTDOWN(0x5),
    ACKACK(0x6),
    DROPREQ(0x7),
    PEERERROR(0x8),
    USER_DEFINED(0x7FFF);

    private static final Map<Integer, ControlType> BY_CODE = Stream.of(values())
            .collect(Collectors.toMap(t -> t.code, t -> t));

    private final int code;

    ControlType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Returns null for a code with no known mapping, rather than throwing. */
    public static ControlType fromCode(int code) {
        return BY_CODE.get(code);
    }
}