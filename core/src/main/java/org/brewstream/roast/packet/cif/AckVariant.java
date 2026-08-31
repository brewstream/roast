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