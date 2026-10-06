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
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

class PdkSnapshotOpeningAndPrimaryTest {

    @Test
    void sessionCloseCannotMissAnActualWorkerStillInitializingBeforeItsSeam(@TempDir Path directory) throws Exception {
        try (Fixture fixture = new Fixture(directory, "blockedInit")) {
            SnapshotSession session = fixture.port().snapshotSession(fixture.config());
            var caller = Executors.newSingleThreadExecutor();
            var opened = caller.submit(() -> session.read("t1"));
            try {
                assertThat(fixture.latch("entered").await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(fixture.reader().isAlive()).isTrue(); assertThat(opened.isDone()).isFalse();
                assertThatThrownBy(session::close).isInstanceOf(TapstateException.class).satisfies(thrown ->
                        assertThat(((TapstateException) thrown).code()).isEqualTo(ConnectorError.CAPTURE_FAILED));
                assertThat(fixture.reader().isAlive()).isTrue();
                assertThatThrownBy(session::close).isInstanceOf(TapstateException.class);
                assertThat(fixture.reader().isAlive()).isTrue();
            } finally {
                fixture.latch("release").countDown(); fixture.join();
                caller.shutdownNow(); assertThat(caller.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                assertThatCode(session::close).doesNotThrowAnyException();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void anInitializationFailureKeepsItsExactPrimaryWhenNativeStopAlsoFails(
            boolean sessionOwned, @TempDir Path directory) throws Exception {
        verifyPrimary(sessionOwned, directory, "initError");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aReadFailureKeepsItsExactPrimaryWhenNativeStopAlsoFails(
            boolean sessionOwned, @TempDir Path directory) throws Exception {
        verifyPrimary(sessionOwned, directory, "readError");
    }

    private static void verifyPrimary(boolean sessionOwned, Path directory, String mode) throws Exception {
        try (Fixture fixture = new Fixture(directory, mode)) {
            IOException nativeRead = new IOException("native snapshot " + mode);
            TapstateException primary = new TapstateException(ConnectorError.CAPTURE_FAILED,
                    Map.of("connector", "opening_snapshot", "detail", "original snapshot refusal"), nativeRead);
            IOException stop = new IOException("native stop cleanup refusal");
            fixture.state.put("primary", primary); fixture.stopRefusal().set(stop);
            Throwable observed = catchThrowable(() -> {
                if (sessionOwned) {
                    try (SnapshotSession session = fixture.port().snapshotSession(fixture.config());
                            CaptureBatch batch = session.read("t1")) {
                        while (batch.hasNext()) { batch.next(); }
                    }
                } else {
                    try (CaptureBatch batch = fixture.port().snapshot(fixture.config())) {
                        while (batch.hasNext()) { batch.next(); }
                    }
                }
            });
            assertThat(observed).isSameAs(primary);
            assertThat(observed.getCause()).isSameAs(nativeRead);
            assertThat(List.of(observed.getSuppressed())).anySatisfy(cleanup -> {
                assertThat(cleanup).isInstanceOf(TapstateException.class);
                assertThat(((TapstateException) cleanup).code()).isEqualTo(ConnectorError.CAPTURE_FAILED);
                assertThat(cleanup.getCause()).isSameAs(stop);
            });
            fixture.stopRefusal().set(null); fixture.join();
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final String key = "opening-snapshot-" + UUID.randomUUID();
        private final Map<String, Object> state = new ConcurrentHashMap<>();
        private final ConnectorRef reference;
        Fixture(Path directory, String mode) {
            reference = new ConnectorRef(List.of(OpeningSnapshotJars.source(directory)),
                    "synthetic.OpeningSnapshotSource", "2.0.8", null);
            state.put("mode", mode); state.put("reader", new AtomicReference<Thread>());
            state.put("stopRefusal", new AtomicReference<Throwable>());
            state.put("entered", new CountDownLatch(1)); state.put("release", new CountDownLatch(1));
            assertThat(System.getProperties().put(key, state)).isNull();
        }
        PdkCapturePort port() { return new PdkCapturePort(ignored -> reference); }
        CaptureConfig config() { return new CaptureConfig("opening_snapshot", Map.of("fixtureKey", key), List.of("t1")); }
        CountDownLatch latch(String name) { return (CountDownLatch) state.get(name); }
        @SuppressWarnings("unchecked")
        Thread reader() { return ((AtomicReference<Thread>) state.get("reader")).get(); }
        @SuppressWarnings("unchecked")
        AtomicReference<Throwable> stopRefusal() { return (AtomicReference<Throwable>) state.get("stopRefusal"); }
        void join() throws InterruptedException {
            assertThat(reader()).isNotNull(); reader().join(5_000); assertThat(reader().isAlive()).isFalse();
        }
        @Override public void close() throws InterruptedException {
            stopRefusal().set(null); latch("release").countDown();
            try { if (reader() != null) { join(); } }
            finally { System.getProperties().remove(key); }
        }
    }
}
