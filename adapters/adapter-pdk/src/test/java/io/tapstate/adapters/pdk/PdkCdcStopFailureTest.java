package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.Subscription;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkCdcStopFailureTest {

    @Test
    void aNormalCloseRetainsTheExactNativeStopFailureEvenAfterTheReadEnded(@TempDir Path directory) throws Exception {
        String key = "throwing-cdc-stop-" + UUID.randomUUID();
        IOException original = new IOException("native stop could not release its source");
        AtomicReference<Throwable> refusal = new AtomicReference<>(original);
        AtomicReference<Thread> reader = new AtomicReference<>();
        AtomicInteger stops = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        Map<String, Object> state = Map.of("refusal", refusal, "reader", reader, "stops", stops, "readEntered", entered);
        assertThat(System.getProperties().put(key, state)).isNull();
        ConnectorRef reference = new ConnectorRef(List.of(ThrowingCdcStopJars.source(directory)),
                "synthetic.ThrowingCdcStopSource", "2.0.8", null);
        PdkCapturePort port = new PdkCapturePort(ignored -> reference);
        Subscription capture = null;
        try {
            capture = port.cdc(new CaptureConfig("throwing_stop", Map.of("fixtureKey", key), List.of("t1")),
                    CaptureStart.present(), (rows, position) -> { });
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(reader.get()).isNotNull(); reader.get().join(5_000);
            assertThat(reader.get().isAlive()).isFalse();
            assertStopRefused(capture, original);
            assertStopRefused(capture, original);
            assertThat(stops).hasValue(2);
            refusal.set(null);
            assertThatCode(capture::close).doesNotThrowAnyException();
            assertThat(stops).hasValue(3);
            assertThatCode(capture::close).doesNotThrowAnyException();
            assertThat(stops).hasValue(3);
        } finally {
            refusal.set(null);
            try {
                if (reader.get() != null) { reader.get().join(5_000); assertThat(reader.get().isAlive()).isFalse(); }
                if (capture != null) { capture.close(); }
            } finally { System.getProperties().remove(key); }
        }
    }

    private static void assertStopRefused(Subscription capture, Throwable original) {
        assertThatThrownBy(capture::close).isInstanceOf(TapstateException.class).satisfies(thrown -> {
            TapstateException failure = (TapstateException) thrown;
            assertThat(failure.code()).isEqualTo(ConnectorError.CAPTURE_FAILED);
            assertThat(failure.args()).containsEntry("connector", "throwing_stop")
                    .containsEntry("detail", original.getMessage());
            assertThat(failure.getCause()).isSameAs(original);
        });
    }
}
