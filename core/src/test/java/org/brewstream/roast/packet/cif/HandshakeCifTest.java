package org.brewstream.roast.packet.cif;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.brewstream.roast.packet.SrtPacket;
import org.brewstream.roast.packet.SrtSocketId;
import org.brewstream.roast.util.CircularNumber;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Golden hex fragments here are lifted byte-for-byte from
 * github.com/datarhei/gosrt packet.CIFHandshake's own tests (TestHandshakeV4,
 * TestHandshakeV5) — see references/gosrt/packet/handshake_test.go.
 *
 * <p>{@link #decodesGosrtsCompleteV5GoldenVector()} uses gosrt's TestHandshakeV5
 * vector <em>in full</em>, KMREQ/KMRSP and Congestion Control included. Earlier
 * versions of this class had to strip those two out because neither was parsed;
 * key material now is, so only Congestion Control is still skipped-by-length —
 * and the vector exercises that skip against real bytes rather than a
 * hand-built case.
 */
class HandshakeCifTest {

    // TestHandshakeV4: version 4, CONCLUSION, no extensions.
    private static final String V4_GOLDEN_HEX =
            "00000004000000020000002a000005dc00000064ffffffff00274921001234560100007f000000000000000000000000";

    private static InetAddress localhost() throws UnknownHostException {
        return InetAddress.getByName("127.0.0.1");
    }

    private static CircularNumber seq(long value) {
        return CircularNumber.of(value, SrtPacket.MAX_SEQUENCE_NUMBER);
    }

    @Test
    void decodesV4BaseFieldsFromGosrtGoldenVector() throws UnknownHostException {
        var buf = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(V4_GOLDEN_HEX));

        HandshakeCif cif = HandshakeCif.decode(buf, false);

        assertThat(cif).isNotNull();
        assertThat(cif.version()).isEqualTo(4);
        assertThat(cif.encryptionField()).isZero();
        assertThat(cif.extensionField()).isEqualTo(2);
        assertThat(cif.initialPacketSequenceNumber()).isEqualTo(seq(42));
        assertThat(cif.maxTransmissionUnitSize()).isEqualTo(1500);
        assertThat(cif.maxFlowWindowSize()).isEqualTo(100);
        assertThat(cif.handshakeType()).isEqualTo(HandshakeType.CONCLUSION);
        assertThat(cif.srtSocketId()).isEqualTo(SrtSocketId.of(0x274921));
        assertThat(cif.synCookie()).isEqualTo(0x123456);
        assertThat(cif.peerAddress()).isEqualTo(localhost());
        assertThat(cif.handshakeExtension()).isNull();
        assertThat(cif.streamId()).isNull();
        buf.release();
    }

    @Test
    void v4RoundTripIsByteExact() {
        var buf = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(V4_GOLDEN_HEX));
        HandshakeCif cif = HandshakeCif.decode(buf, false);
        buf.release();

        var out = ByteBufAllocator.DEFAULT.buffer();
        cif.encodeTo(out);

        assertThat(ByteBufUtil.hexDump(out)).isEqualTo(V4_GOLDEN_HEX);
        out.release();
    }

    /**
     * gosrt's TestHandshakeV5 vector in full: base fields, HSRSP, KMRSP, SID and
     * a Congestion Control block. Proves the KM extension is read out of a real
     * peer's handshake rather than only out of one we built ourselves.
     */
    @Test
    void decodesGosrtsCompleteV5GoldenVector() {
        String fullV5 = "00000005000200070000002a000005dc00000064ffffffff00274921001234560100007f"
                + "00000000000000000000000000020003000104020000003f0064006400040"
                + "00e122029010000000002000200000004040102030405060708090a0b0c0d0e0f10"
                + "f0f1f2f3f4f5f6f71112131415161718191a1b1c1d1e1f20"
                + "0005000576696c2f74732f656d6165726f6f662e0072616200060001626f6f66";

        var buf = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(fullV5));
        HandshakeCif cif = HandshakeCif.decode(buf, false);
        buf.release();

        assertThat(cif).isNotNull();
        assertThat(cif.streamId()).isEqualTo("/live/stream.foobar");
        assertThat(cif.handshakeExtension().srtVersion()).isEqualTo(0x010402);

        KeyMaterialCif km = cif.keyMaterial();
        assertThat(km).isNotNull();
        assertThat(km.isError()).isFalse();
        assertThat(km.keyEncryption()).isEqualTo(KeyEncryption.EVEN);
        assertThat(km.cipher()).isEqualTo(KeyMaterialCif.CIPHER_AES_CTR);
        assertThat(km.keyLength()).isEqualTo(16);
        assertThat(km.salt()).isEqualTo(ByteBufUtil.decodeHexDump("0102030405060708090a0b0c0d0e0f10"));
        assertThat(km.wrap()).isEqualTo(ByteBufUtil.decodeHexDump(
                "f0f1f2f3f4f5f6f71112131415161718191a1b1c1d1e1f20"));
    }

    /**
     * The same vector re-encoded. Congestion Control is not modelled, so it is
     * dropped on the way back out - everything up to it must be byte-identical,
     * which is what this asserts.
     */
    @Test
    void reEncodesGosrtsV5VectorUpToTheUnmodelledCongestionBlock() {
        String fullV5 = "00000005000200070000002a000005dc00000064ffffffff00274921001234560100007f"
                + "00000000000000000000000000020003000104020000003f0064006400040"
                + "00e122029010000000002000200000004040102030405060708090a0b0c0d0e0f10"
                + "f0f1f2f3f4f5f6f71112131415161718191a1b1c1d1e1f20"
                + "0005000576696c2f74732f656d6165726f6f662e00726162";
        String withCongestion = fullV5 + "00060001626f6f66";

        var buf = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(withCongestion));
        HandshakeCif cif = HandshakeCif.decode(buf, false);
        buf.release();

        var out = ByteBufAllocator.DEFAULT.buffer();
        cif.encodeTo(out);

        assertThat(ByteBufUtil.hexDump(out)).isEqualTo(fullV5);
        out.release();
    }

    @Test
    void aKeyMaterialRejectionSurvivesARoundTrip() throws UnknownHostException {
        HandshakeCif cif = new HandshakeCif(
                false, 5, 0, 3, seq(42), 1500, 100, HandshakeType.CONCLUSION.code(),
                SrtSocketId.of(0x274921), 0x123456, localhost(), null, null,
                KeyMaterialCif.error(KeyMaterialCif.ERROR_BAD_SECRET));

        var out = ByteBufAllocator.DEFAULT.buffer();
        cif.encodeTo(out);
        HandshakeCif decoded = HandshakeCif.decode(out, false);
        out.release();

        assertThat(decoded).isNotNull();
        assertThat(decoded.keyMaterial()).isNotNull();
        assertThat(decoded.keyMaterial().isError()).isTrue();
        assertThat(decoded.keyMaterial().errorCode()).isEqualTo(KeyMaterialCif.ERROR_BAD_SECRET);
    }

    @Test
    void encodesHsAndSidExtensionsMatchingGosrtGoldenFragments() throws UnknownHostException {
        // Base fields + HSRSP extension + SID extension, in the same byte layout
        // gosrt produces for these three pieces. KM and Congestion are simply not
        // present on this narrower case - see decodesGosrtsCompleteV5GoldenVector
        // for the full-vector version.
        String expected = "00000005000000050000002a000005dc00000064ffffffff00274921001234560100007f0000000000000000"
                + "0000000000020003000104020000003f006400640005000576696c2f74732f656d6165726f6f662e00726162";

        HandshakeExtension extension = new HandshakeExtension(
                0x010402,
                new HandshakeExtensionFlags(true, true, true, true, true, true, false, false),
                100, 100);
        HandshakeCif cif = new HandshakeCif(
                false, 5, 0, 5, seq(42), 1500, 100, HandshakeType.CONCLUSION.code(),
                SrtSocketId.of(0x274921), 0x123456, localhost(), extension, "/live/stream.foobar");

        var out = ByteBufAllocator.DEFAULT.buffer();
        cif.encodeTo(out);

        assertThat(ByteBufUtil.hexDump(out)).isEqualTo(expected);
        out.release();
    }

    @Test
    void roundTripsHsAndSidExtensions() throws UnknownHostException {
        HandshakeExtension extension = new HandshakeExtension(
                0x010402,
                new HandshakeExtensionFlags(true, true, true, true, true, true, false, false),
                100, 100);
        HandshakeCif original = new HandshakeCif(
                false, 5, 0, 5, seq(42), 1500, 100, HandshakeType.CONCLUSION.code(),
                SrtSocketId.of(0x274921), 0x123456, localhost(), extension, "/live/stream.foobar");

        var buf = ByteBufAllocator.DEFAULT.buffer();
        original.encodeTo(buf);
        HandshakeCif decoded = HandshakeCif.decode(buf, false);

        assertThat(decoded).isEqualTo(original);
        buf.release();
    }

    @Test
    void decodeSkipsUnknownExtensionAndStillFindsStreamId() {
        String hex = "00000005000000050000002a000005dc00000064ffffffff00274921001234560100007f00000000000000000000000"
                + "0bd0100010000000000050005"
                + "76696c2f74732f656d6165726f6f662e00726162";
        // (Mirrors gosrt's TestHandshakeV5UnsupportedExtension: an unknown extension
        // type 0xbd01 must be skipped by its declared length, not fail decoding.)
        var buf = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(hex));

        HandshakeCif cif = HandshakeCif.decode(buf, false);

        assertThat(cif).isNotNull();
        assertThat(cif.handshakeExtension()).isNull();
        assertThat(cif.streamId()).isEqualTo("/live/stream.foobar");
        buf.release();
    }

    @Test
    void decodeReturnsNullForTooShortBuffer() {
        var buf = Unpooled.wrappedBuffer(new byte[47]);
        assertThat(HandshakeCif.decode(buf, false)).isNull();
        buf.release();
    }

    @Test
    void decodeTreatsUnrecognizedHandshakeTypeAsRejectionNotMalformed() {
        var buf = ByteBufAllocator.DEFAULT.buffer();
        buf.writeInt(5); // version
        buf.writeShort(0); // encryptionField
        buf.writeShort(0); // extensionField
        buf.writeInt(0); // initialPacketSequenceNumber
        buf.writeInt(1500); // mtu
        buf.writeInt(100); // flowWindow
        buf.writeInt(RejectionReason.ROGUE.code()); // not one of the 5 progression values
        buf.writeInt(0); // srtSocketId
        buf.writeInt(0); // synCookie
        buf.writeZero(16); // peerAddress

        HandshakeCif cif = HandshakeCif.decode(buf, false);

        assertThat(cif).isNotNull();
        assertThat(cif.handshakeType()).isNull();
        assertThat(cif.isRejection()).isTrue();
        assertThat(cif.rejectionReason()).isEqualTo(RejectionReason.ROGUE);
        buf.release();
    }

    @Test
    void decodeTreatsUnnamedRejectionCodeAsRejectionWithNullReason() {
        var buf = ByteBufAllocator.DEFAULT.buffer();
        buf.writeInt(5);
        buf.writeShort(0);
        buf.writeShort(0);
        buf.writeInt(0);
        buf.writeInt(1500);
        buf.writeInt(100);
        buf.writeInt(0x1234); // not a progression value, and not a named RejectionReason
        buf.writeInt(0);
        buf.writeInt(0);
        buf.writeZero(16);

        HandshakeCif cif = HandshakeCif.decode(buf, false);

        assertThat(cif).isNotNull();
        assertThat(cif.isRejection()).isTrue();
        assertThat(cif.rejectionReason()).isNull();
        buf.release();
    }

    @Test
    void decodeReturnsNullForWrongSizedHandshakeExtension() {
        var buf = ByteBufAllocator.DEFAULT.buffer();
        buf.writeInt(5); // version
        buf.writeShort(0); // encryptionField
        buf.writeShort(1); // extensionField (nonzero, so extensions are scanned)
        buf.writeInt(0); // initialPacketSequenceNumber
        buf.writeInt(1500); // mtu
        buf.writeInt(100); // flowWindow
        buf.writeInt(HandshakeType.CONCLUSION.code());
        buf.writeInt(0); // srtSocketId
        buf.writeInt(0); // synCookie
        buf.writeZero(16); // peerAddress
        buf.writeShort(ExtensionType.HSREQ.code());
        buf.writeShort(2); // 8 bytes, not the required 12
        buf.writeZero(8);

        assertThat(HandshakeCif.decode(buf, false)).isNull();
        buf.release();
    }
}
