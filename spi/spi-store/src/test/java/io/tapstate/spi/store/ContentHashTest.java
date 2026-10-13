package io.tapstate.spi.store;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class ContentHashTest {

    @Test
    void streamsTheSameLowerHexSha256AsRegisteredBytes() throws IOException {
        byte[] bytes = "abc".getBytes(StandardCharsets.US_ASCII);
        String hash = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

        assertThat(ContentHash.of(bytes)).isEqualTo(hash);
        assertThat(ContentHash.of(new ByteArrayInputStream(bytes))).isEqualTo(hash);
        assertThat(ContentHash.of(new ByteArrayInputStream(new byte[0])))
                .isEqualTo("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    @Test
    void hashesEveryByteAcrossShortReadsAndMultipleBuffers() throws IOException {
        byte[] bytes = new byte[20_017];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) index;
        }
        InputStream stream = new ByteArrayInputStream(bytes) {
            @Override
            public synchronized int read(byte[] buffer, int offset, int length) {
                return super.read(buffer, offset, Math.min(length, 7));
            }
        };

        assertThat(ContentHash.of(new ByteArrayInputStream(bytes))).isEqualTo(ContentHash.of(bytes));
        assertThat(ContentHash.of(stream)).isEqualTo(ContentHash.of(bytes));
    }

    @Test
    void leavesReadFailuresForTheCallerToTranslate() {
        IOException failure = new IOException("unreadable stream");
        InputStream stream = new InputStream() {
            @Override
            public int read() throws IOException {
                throw failure;
            }
        };

        assertThatThrownBy(() -> ContentHash.of(stream)).isSameAs(failure);
    }
}
