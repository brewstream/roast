package org.brewstream.roast.send;

import io.netty.buffer.ByteBuf;
import org.brewstream.roast.packet.DataPacket;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.packet.cif.LossRange;
import org.brewstream.roast.util.CircularNumber;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * Queues outgoing DATA packets and hands them to a {@code deliver} callback
 * once due, keeping delivered-but-unacknowledged ones around for possible
 * retransmission — the sending side's counterpart to {@link
 * org.brewstream.roast.recv.LossList}/{@link org.brewstream.roast.recv.ReceiveBuffer}
 * combined. Ported directly from gosrt's {@code congestion/live.sender}
 * ({@code send.go}), which keeps both roles in one struct too; no need to
 * split it further here.
 *
 * <p>Two FIFO queues, unlike {@code ReceiveBuffer}'s sequence-sorted list:
 * packets here only ever arrive in strictly increasing sequence order (this
 * class assigns the sequence number itself in {@link #push}), so no
 * out-of-order insertion is possible and a plain queue suffices.
 * <ul>
 *   <li>{@code packetList} — pushed, not yet due to be handed to {@code deliver}.</li>
 *   <li>{@code lossList} — delivered at least once, kept for possible
 *       retransmission until {@link #ack} confirms it or {@link #tick}'s
 *       TLPKTDROP gives up on it.</li>
 * </ul>
 *
 * <p><b>Pacing</b>: traced directly from gosrt's source, not assumed —
 * {@link #tick} gates delivery purely on each packet's own {@code
 * scheduledSendMicros}; the {@link #avgPayloadSize} EWMA this class also
 * tracks (draft-sharabayko-srt.md §5.1.2, "SRT's Default LiveCC Algorithm")
 * is carried forward the same way gosrt does — computed, not enforced.
 * Nothing here delays or spaces out delivery beyond what the caller already
 * scheduled. Real output-rate shaping, if ever needed, isn't implemented by
 * gosrt's own "live" congestion control either.
 *
 * <p><b>Sender-side TLPKTDROP</b>: {@link #tick}'s second pass drops
 * {@code lossList} entries whose scheduled time plus the configured drop
 * threshold has passed — the other half of TLPKTDROP; {@code ReceiveBuffer}
 * already implements the receiver's half.
 *
 * <p>Grounded against gosrt's own {@code congestion/live/send_test.go} — real
 * reference tests exist for this piece, unlike several other pieces of this
 * codebase (RTT/drift/wraparound) where none did.
 *
 * <p>Deliberately deferred, not part of this class: full bandwidth-rate-window
 * statistics ({@code estimatedInputBW}/{@code estimatedSentBW}/{@code
 * pktLossRate} — gosrt's {@code Stats()}). See STATUS.md's known gaps. The
 * 16th/17th-packet bandwidth-probe trick <em>is</em> ported — see {@link
 * #push}'s javadoc.
 *
 * <p><b>ByteBuf ownership</b>: {@code deliver} always receives a {@link
 * #duplicate}, never the entry actually held in {@code packetList}/{@code
 * lossList} — every packet this class hands out, first send or retransmit
 * alike, is a fresh {@code retainedDuplicate()} so the original stays valid
 * in {@code lossList} for a possible future {@link #nak}, per this
 * codebase's ByteBuf convention ({@code encodeTo} consumes/releases a
 * packet's body once actually sent). {@code deliver}'s implementation owns
 * releasing what it's handed (directly, or by handing it to something that
 * will, like {@code encodeTo}).
 *
 * <p>Not thread-safe, same as this codebase's other buffer/list state.
 */
public final class SendBuffer {

    private record Entry(CircularNumber seq, long scheduledSendMicros, DataPacket packet) {
    }

    private final SrtSocketId destination;
    private final long dropThresholdMicros;
    private final Deque<Entry> packetList = new ArrayDeque<>();
    private final Deque<Entry> lossList = new ArrayDeque<>();
    private final SendRateEstimator rateEstimator = new SendRateEstimator();
    private final Consumer<DataPacket> deliver;

    private CircularNumber nextSequenceNumber;
    private double avgPayloadSize = 1_456; // gosrt's packet.MAX_PAYLOAD_SIZE, its own seed value
    private long probeTimeMicros;

    public SendBuffer(CircularNumber initialSequenceNumber, SrtSocketId destination,
            long dropThresholdMicros, Consumer<DataPacket> deliver) {
        this.nextSequenceNumber = initialSequenceNumber;
        this.destination = destination;
        this.dropThresholdMicros = dropThresholdMicros;
        this.deliver = deliver;
    }

    /** Current EWMA average payload size in bytes (draft-sharabayko-srt.md §5.1.2). */
    public double avgPayloadSize() {
        return avgPayloadSize;
    }

    /**
     * Queues a payload for delivery once {@code scheduledSendMicros} is due,
     * assigning it the next sequence number. Ownership of {@code payload}
     * transfers to this buffer.
     *
     * <p><b>16th/17th-packet bandwidth probe</b>: every packet whose sequence
     * number is {@code ≡ 1 (mod 16)} has its <em>delivery scheduling</em> time
     * overridden to match the packet immediately before it ({@code ≡ 0 (mod
     * 16)}), so the two get handed to {@code deliver} back-to-back on the same
     * {@link #tick} instead of waiting for its own real schedule — this is what
     * lets a peer's receive-side probe window (libsrt: {@code
     * CRcvBufferNew::probe1Arrival}/{@code probe2Arrival} in {@code window.h},
     * matched against real wall-clock arrival gaps, not the wire timestamp
     * field) estimate link capacity independent of application send rate.
     * Ported from gosrt's {@code Push} (traced directly from its source, not
     * assumed): only the <em>scheduling</em> time is touched, matching gosrt's
     * own comment that this is safe specifically because the packet's own
     * <em>wire</em> timestamp has already been set from its real {@code
     * scheduledSendMicros} by this point, one line above. No gosrt test
     * exercises this (checked directly, not assumed — {@code send_test.go} has
     * no probe-related cases), so the test for this is self-designed against
     * gosrt's source, same rigor tier as this codebase's RTT/drift/wraparound
     * pieces.
     */
    public void push(ByteBuf payload, long scheduledSendMicros) {
        rateEstimator.onPushed(payload.readableBytes());

        CircularNumber seq = nextSequenceNumber;
        nextSequenceNumber = nextSequenceNumber.inc();

        DataPacket packet = new DataPacket(
                (int) seq.value(), 3, false, 0, false, 1,
                (int) (scheduledSendMicros & SrtPacket.MAX_TIMESTAMP), destination, payload);

        long effectiveScheduledSendMicros = scheduledSendMicros;
        long probe = seq.value() & 0xF;
        if (probe == 0) {
            probeTimeMicros = scheduledSendMicros;
        } else if (probe == 1) {
            effectiveScheduledSendMicros = probeTimeMicros;
        }

        packetList.addLast(new Entry(seq, effectiveScheduledSendMicros, packet));
    }

    /**
     * Delivers whatever's due as of {@code nowMicros}, then drops whatever in
     * {@link #lossList} has aged past the drop threshold (sender-side
     * TLPKTDROP). Ported from gosrt's {@code Tick} — with one necessary
     * departure: gosrt hands {@code deliver} the same in-memory packet object
     * that also stays in its loss list, harmless in Go since nothing there
     * consumes/releases it. Here, the packet actually going out over the wire
     * has its {@code ByteBuf} consumed/released by {@code encodeTo} once
     * sent — so this hands {@code deliver} a {@link #duplicate} instead,
     * keeping the original safely in {@link #lossList} for a possible later
     * {@link #nak}. (An earlier version of this method delivered the
     * original directly; a NAK-triggered retransmit after a real send would
     * have called {@code retainedDuplicate()} on an already-released buffer.)
     */
    public void tick(long nowMicros) {
        rateEstimator.tick(nowMicros);

        while (!packetList.isEmpty() && packetList.peekFirst().scheduledSendMicros() <= nowMicros) {
            Entry entry = packetList.pollFirst();
            avgPayloadSize = avgPayloadSize * 0.875 + entry.packet().payload().readableBytes() * 0.125;
            rateEstimator.onSent(entry.packet().payload().readableBytes(), false);
            deliver.accept(duplicate(entry.packet(), false));
            lossList.addLast(entry);
        }

        while (!lossList.isEmpty() && lossList.peekFirst().scheduledSendMicros() + dropThresholdMicros <= nowMicros) {
            lossList.pollFirst().packet().payload().release();
        }
    }

    /**
     * Prunes every {@link #lossList} entry older than {@code
     * lastAckPacketSequenceNumber} — confirmed delivered, no longer needed
     * for retransmission. Ported from gosrt's {@code ACK}.
     */
    public void ack(CircularNumber lastAckPacketSequenceNumber) {
        while (!lossList.isEmpty() && lossList.peekFirst().seq().lessThan(lastAckPacketSequenceNumber)) {
            lossList.pollFirst().packet().payload().release();
        }
    }

    /**
     * Retransmits every {@link #lossList} entry whose sequence number falls
     * within any of the given ranges — a fresh {@link DataPacket} with {@code
     * retransmitted=true} and a {@code retainedDuplicate()} of the payload
     * (the original stays in {@code lossList}, per this codebase's ByteBuf
     * ownership convention: {@code encodeTo} consumes/releases a packet's
     * body, so a packet that might be resent again can't reuse the same
     * buffer instance). Ported from gosrt's {@code NAK}, including its
     * back-to-front scan direction (no test observes the order, only the
     * count, but faithfulness is cheap).
     */
    public void nak(List<LossRange> ranges) {
        if (ranges.isEmpty()) {
            return;
        }

        Iterator<Entry> it = lossList.descendingIterator();
        while (it.hasNext()) {
            Entry entry = it.next();
            for (LossRange range : ranges) {
                if (entry.seq().greaterThanOrEqual(range.start()) && entry.seq().lessThanOrEqual(range.end())) {
                    rateEstimator.onSent(entry.packet().payload().readableBytes(), true);
                    deliver.accept(duplicate(entry.packet(), true));
                    break;
                }
            }
        }
    }

    private static DataPacket duplicate(DataPacket original, boolean retransmitted) {
        return new DataPacket(
                original.sequenceNumber(), original.pp(), original.inOrder(), original.kk(), retransmitted,
                original.messageNumber(), original.timestamp(), original.destination(),
                original.payload().retainedDuplicate());
    }

    /** Test-support only, mirrors gosrt's own same-package whitebox test access to its lists' lengths. */
    int packetListSize() {
        return packetList.size();
    }

    /** Test-support only, mirrors gosrt's own same-package whitebox test access to its lists' lengths. */
    int lossListSize() {
        return lossList.size();
    }

    /** Packets queued but not yet due to send — surfaced through {@code ConnectionStats}. */
    /** The send-side rate figures; see {@link SendRateEstimator}. */
    public SendRateEstimator rates() {
        return rateEstimator;
    }

    public int queuedCount() {
        return packetList.size();
    }

    /** Packets sent and retained for possible retransmission — surfaced through {@code ConnectionStats}. */
    public int inFlightCount() {
        return lossList.size();
    }

    /** Releases every queued/unacknowledged payload. */
    public void flush() {
        packetList.forEach(entry -> entry.packet().payload().release());
        packetList.clear();
        lossList.forEach(entry -> entry.packet().payload().release());
        lossList.clear();
    }
}
