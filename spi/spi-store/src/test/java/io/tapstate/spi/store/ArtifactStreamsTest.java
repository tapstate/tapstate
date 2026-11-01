package io.tapstate.spi.store;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InterruptedIOException;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ArtifactStreamsTest {
    @Test
    void interruptionStopsBeforeReadingOrPublishingArtifactBytes() {
        byte[] bytes = new byte[1024];
        var output = new ByteArrayOutputStream();
        Thread.currentThread().interrupt();
        try {
            assertThatThrownBy(() -> ArtifactStreams.copyVerified(new ByteArrayInputStream(bytes), output,
                    ContentHash.of(bytes), bytes.length)).isInstanceOf(InterruptedIOException.class);
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
            assertThat(output.size()).isZero();
        } finally { Thread.interrupted(); }
    }
}
