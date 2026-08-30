package org.brewstream.roast.cli;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * URI parsing is where a CLI's fiddly cases live, and it's the one part
 * worth testing without spawning a process.
 */
class SrtUriTest {

    @Test
    void parsesACallerUri() {
        SrtUri uri = SrtUri.parse("srt://192.0.2.10:9000?streamid=live/x");

        assertThat(uri.listener()).isFalse();
        assertThat(uri.address().getPort()).isEqualTo(9000);
        assertThat(uri.address().getHostString()).isEqualTo("192.0.2.10");
        assertThat(uri.streamId()).isEqualTo("live/x");
        assertThat(uri.isEncrypted()).isFalse();
        assertThat(uri.config().latencyMillis()).isEqualTo(120); // untouched default
    }

    @Test
    void modeListenerIsHonoured() {
        SrtUri uri = SrtUri.parse("srt://127.0.0.1:9000?mode=listener");

        assertThat(uri.listener()).isTrue();
        assertThat(uri.address().getHostString()).isEqualTo("127.0.0.1");
    }

    /** "srt://:9000" has no host, which can only sensibly mean listen on any interface. */
    @Test
    void anOmittedHostImpliesListeningOnEveryInterface() {
        SrtUri uri = SrtUri.parse("srt://:9000");

        assertThat(uri.listener()).isTrue();
        assertThat(uri.address().getAddress().isAnyLocalAddress()).isTrue();
    }

    @Test
    void encryptionParametersAreParsed() {
        SrtUri uri = SrtUri.parse("srt://127.0.0.1:9000?passphrase=a-long-secret&pbkeylen=32");

        assertThat(uri.isEncrypted()).isTrue();
        assertThat(uri.passphrase()).isEqualTo("a-long-secret".toCharArray());
        assertThat(uri.keyLength()).isEqualTo(32);
    }

    @Test
    void configParametersReachTheConfig() {
        SrtUri uri = SrtUri.parse("srt://127.0.0.1:9000?latency=250&fc=4096");

        assertThat(uri.config().latencyMillis()).isEqualTo(250);
        assertThat(uri.config().flowWindowPackets()).isEqualTo(4096);
    }

    /** The passphrase must not appear in a banner, a log line, or a crash dump. */
    @Test
    void toStringNeverRevealsThePassphrase() {
        SrtUri uri = SrtUri.parse("srt://127.0.0.1:9000?passphrase=super-secret-value");

        assertThat(uri.toString()).doesNotContain("super-secret-value").contains("encrypted");
    }

    @Test
    void theReturnedPassphraseIsACopyTheCallerCanZero() {
        SrtUri uri = SrtUri.parse("srt://127.0.0.1:9000?passphrase=a-long-secret");

        char[] first = uri.passphrase();
        java.util.Arrays.fill(first, '\0');

        assertThat(uri.passphrase()).isEqualTo("a-long-secret".toCharArray());
    }

    /**
     * Silently ignoring a misspelled parameter is how someone ends up convinced
     * encryption is on when it isn't.
     */
    @Test
    void anUnknownParameterIsReportedRatherThanIgnored() {
        assertThatThrownBy(() -> SrtUri.parse("srt://127.0.0.1:9000?passphrse=typo"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("passphrse");
    }

    @Test
    void malformedUrisAreRejectedWithAUsefulMessage() {
        assertThatThrownBy(() -> SrtUri.parse("srt://127.0.0.1"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("port");
        assertThatThrownBy(() -> SrtUri.parse("udp://127.0.0.1:9000"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("srt://");
        assertThatThrownBy(() -> SrtUri.parse("srt://127.0.0.1:9000?latency=soon"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("latency");
        assertThatThrownBy(() -> SrtUri.parse("srt://127.0.0.1:9000?bare"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    /** An invalid setting should fail here, not once a handshake is already under way. */
    @Test
    void invalidConfigValuesFailAtParseTime() {
        assertThatThrownBy(() -> SrtUri.parse("srt://127.0.0.1:9000?latency=0"))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void isSrtRecognisesTheScheme() {
        assertThat(SrtUri.isSrt("srt://127.0.0.1:9000")).isTrue();
        assertThat(SrtUri.isSrt("file://con")).isFalse();
    }
}
