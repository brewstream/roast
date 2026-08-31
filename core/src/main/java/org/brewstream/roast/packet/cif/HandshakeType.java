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

package org.brewstream.roast.packet.cif;

import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The handshake CIF's Handshake Type field (draft-sharabayko-srt.md handshake
 * section). {@link #DONE}/{@link #AGREEMENT}/{@link #CONCLUSION} are encoded as the
 * top three values below {@code 0}, i.e. negative when read as a signed 32-bit int
 * — that's not a bug, it's the wire encoding.
 */
public enum HandshakeType {
    DONE(0xFFFFFFFD),
    AGREEMENT(0xFFFFFFFE),
    CONCLUSION(0xFFFFFFFF),
    WAVEHAND(0x0000_0000),
    INDUCTION(0x0000_0001);

    private static final Map<Integer, HandshakeType> BY_CODE =
            Stream.of(values()).collect(Collectors.toMap(t -> t.code, t -> t));

    private final int code;

    HandshakeType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Returns null for an unrecognized code, rather than throwing. */
    public static HandshakeType fromCode(int code) {
        return BY_CODE.get(code);
    }
}