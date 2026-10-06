package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.SnapshotSession;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkConfirmedSnapshotEndTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void neitherStandaloneNorSessionCloseConfirmsAnActualReadThatIgnoresInterrupts(
            boolean sessionOwned, @TempDir Path directory) throws Exception {
        try (Fixture fixture = new Fixture(directory, "stubborn")) {
            SnapshotSession session = sessionOwned ? fixture.port().snapshotSession(fixture.config()) : null;
            CaptureBatch batch = sessionOwned ? session.read("t1") : fixture.port().snapshot(fixture.config());
            AutoCloseable capture = sessionOwned ? session : batch;
            try {
                assertThat(fixture.latch("rowEmitted").await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(batch.hasNext()).isTrue(); batch.next();
                assertThat(fixture.reader().isAlive()).isTrue();
                assertUnfinished(capture, null);
                assertThat(fixture.reader().isAlive()).isTrue();
                assertUnfinished(capture, null);
                assertThat(fixture.reader().isAlive()).isTrue();
                fixture.latch("releaseRead").countDown(); fixture.join();
                assertThatCode(capture::close).doesNotThrowAnyException();
                assertThatCode(capture::close).doesNotThrowAnyException();
            } finally {
                fixture.latch("releaseRead").countDown(); fixture.refusal().set(null); fixture.join();
                capture.close();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void neitherStandaloneNorSessionCloseLosesTheExactNativeStopFailureAfterReadEnd(
            boolean sessionOwned, @TempDir Path directory) throws Exception {
        try (Fixture fixture = new Fixture(directory, "ended")) {
            IOException original = new IOException("native snapshot stop could not release its source");
            fixture.refusal().set(original);
            SnapshotSession session = sessionOwned ? fixture.port().snapshotSession(fixture.config()) : null;
            CaptureBatch batch = sessionOwned ? session.read("t1") : fixture.port().snapshot(fixture.config());
            AutoCloseable capture = sessionOwned ? session : batch;
            try {
                assertThat(fixture.latch("rowEmitted").await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(batch.hasNext()).isTrue(); batch.next(); assertThat(batch.hasNext()).isFalse();
                fixture.join(); assertThat(fixture.reader().isAlive()).isFalse();
                assertUnfinished(capture, original);
                assertUnfinished(capture, original);
                assertThat(fixture.counter("stops")).hasValue(2);
                fixture.refusal().set(null);
                assertThatCode(capture::close).doesNotThrowAnyException();
                assertThat(fixture.counter("stops")).hasValue(3);
                assertThatCode(capture::close).doesNotThrowAnyException();
                assertThat(fixture.counter("stops")).hasValue(3);
            } finally {
                fixture.refusal().set(null); fixture.latch("releaseRead").countDown(); fixture.join(); capture.close();
            }
        }
    }

    private static void assertUnfinished(AutoCloseable capture, Throwable original) {
        assertThatThrownBy(capture::close).isInstanceOf(TapstateException.class).satisfies(thrown -> {
            TapstateException failure = (TapstateException) thrown;
            assertThat(failure.code()).isEqualTo(ConnectorError.CAPTURE_FAILED);
            assertThat(failure.args()).containsEntry("connector", "unfinished_snapshot").containsKey("detail");
            if (original != null) { assertThat(failure.getCause()).isSameAs(original); }
        });
    }

    private static final class Fixture implements AutoCloseable {
        private final String key = "unfinished-snapshot-" + UUID.randomUUID();
        private final Map<String, Object> state = new ConcurrentHashMap<>();
        private final ConnectorRef reference;
        Fixture(Path directory, String mode) {
            reference = new ConnectorRef(List.of(UnfinishedSnapshotJars.source(directory)),
                    "synthetic.UnfinishedSnapshotSource", "2.0.8", null);
            state.put("mode", mode); state.put("reader", new AtomicReference<Thread>());
            state.put("refusal", new AtomicReference<Throwable>()); state.put("stops", new AtomicInteger());
            state.put("rowEmitted", new CountDownLatch(1)); state.put("releaseRead", new CountDownLatch(1));
            assertThat(System.getProperties().put(key, state)).isNull();
        }
        PdkCapturePort port() { return new PdkCapturePort(ignored -> reference); }
        CaptureConfig config() { return new CaptureConfig("unfinished_snapshot", Map.of("fixtureKey", key), List.of("t1")); }
        CountDownLatch latch(String name) { return (CountDownLatch) state.get(name); }
        AtomicInteger counter(String name) { return (AtomicInteger) state.get(name); }
        @SuppressWarnings("unchecked")
        Thread reader() { return ((AtomicReference<Thread>) state.get("reader")).get(); }
        @SuppressWarnings("unchecked")
        AtomicReference<Throwable> refusal() { return (AtomicReference<Throwable>) state.get("refusal"); }
        void join() throws InterruptedException {
            assertThat(reader()).as("the actual snapshot worker was captured").isNotNull();
            reader().join(5_000); assertThat(reader().isAlive()).as("the actual snapshot worker must end").isFalse();
        }
        @Override public void close() throws InterruptedException {
            refusal().set(null); latch("releaseRead").countDown();
            try { if (reader() != null) { join(); } }
            finally { System.getProperties().remove(key); }
        }
    }
}
