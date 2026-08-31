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
 * HSv5 handshake extension type tags (draft-sharabayko-srt.md handshake extension
 * TLV, carried in a CONCLUSION handshake's CIF). {@link HandshakeCif} only parses
 * {@link #HSREQ}/{@link #HSRSP} and {@link #SID} — KMREQ/KMRSP (encryption, Phase 5)
 * and CONGESTION/FILTER/GROUP are recognized here but their blocks are skipped by
 * declared length rather than decoded.
 */
public enum ExtensionType {
    HSREQ(1),
    HSRSP(2),
    KMREQ(3),
    KMRSP(4),
    SID(5),
    CONGESTION(6),
    FILTER(7),
    GROUP(8);

    private static final Map<Integer, ExtensionType> BY_CODE =
            Stream.of(values()).collect(Collectors.toMap(t -> t.code, t -> t));

    private final int code;

    ExtensionType(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /** Returns null for an unrecognized code, rather than throwing. */
    public static ExtensionType fromCode(int code) {
        return BY_CODE.get(code);
    }
}