package io.tapstate.adapters.pdk;

import io.tapstate.adapters.pdk.fixture.PreparedSnapshotSource;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.SnapshotPreparation;
import io.tapstate.spi.capture.SnapshotSession;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SourceTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PdkSnapshotPreparationTest {
    private final Map<String, Object> state = new HashMap<>();
    @BeforeEach void install() {
        for (String key : List.of("inits", "stops", "reads", "earlyReads", "discoveries")) state.put(key, new AtomicInteger());
        state.put("pinned", new AtomicBoolean()); state.put("reportSeam", new AtomicBoolean(true));
        System.getProperties().put(PreparedSnapshotSource.STATE, state);
    }
    @AfterEach void clear() { System.getProperties().remove(PreparedSnapshotSource.STATE); }
    private AtomicInteger counter(String key) { return (AtomicInteger) state.get(key); }
    private AtomicBoolean flag(String key) { return (AtomicBoolean) state.get(key); }
    private static CaptureConfig config(String... tables) { return new CaptureConfig("prepared", Map.of(), List.of(tables)); }
    private static PdkCapturePort port() throws Exception {
        Path classes = Path.of(PreparedSnapshotSource.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        return new PdkCapturePort(id -> new ConnectorRef(List.of(classes), PreparedSnapshotSource.class.getName(), "2.0.8", null));
    }
    private static void drain(CaptureBatch batch) { try (batch) { assertThat(batch.hasNext()).isFalse(); } }

    @Test void noPreparationOwnerRefusesBeforeProvisioningAnyConnector() {
        var provisions = new AtomicInteger();
        var port = new PdkCapturePort(id -> { provisions.incrementAndGet(); throw new AssertionError("must not provision"); });
        assertThatThrownBy(() -> SnapshotSession.openPrepared(port, config("t1"), new SnapshotPreparation(1, null)))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ConnectorError.SNAPSHOT_PREPARATION_UNAVAILABLE));
        assertThat(provisions).hasValue(0);
    }

    @Test void actualSessionMetadataAndFixedSnapshotContextAreAcceptedOnceBeforeEitherTableReads() throws Exception {
        var calls = new AtomicInteger(); var sampled = new AtomicReference<SourcePosition>();
        var original = new SnapshotPreparation(83, new SourcePosition("saved-replay-anchor"));
        var hostLoader = Thread.currentThread().getContextClassLoader();
        var port = port().withSnapshotPreparation((request, seam, tables) -> {
            calls.incrementAndGet(); sampled.set(seam);
            assertThat(request).isEqualTo(original); assertThat(counter("reads")).hasValue(0);
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(hostLoader);
            assertThat(tables.keySet()).containsExactlyInAnyOrder("t1", "t2");
            SourceTable actual = tables.get("t1");
            assertThat(actual.fields()).extracting(field -> field.name()).containsExactly("session_id");
            assertThat(actual.fields().getFirst().dataType()).isEqualTo("int");
            assertThat(actual.primaryKey()).containsExactly("session_id");
            assertThat(actual.indexes()).extracting(index -> index.name()).containsExactly("idx_session");
            assertThat(actual.approximateRowCount()).isNull();
            assertThatThrownBy(tables::clear).isInstanceOf(UnsupportedOperationException.class);
            flag("pinned").set(true);
        });
        try (var session = SnapshotSession.openPrepared(port, config("t1", "t2"), original).orElseThrow()) {
            try (var first = session.read("t1")) { assertThat(first.seam()).contains(sampled.get()); assertThat(first.hasNext()).isFalse(); }
            drain(session.read("t2"));
        }
        assertThat(calls).hasValue(1); assertThat(counter("reads")).hasValue(2); assertThat(counter("earlyReads")).hasValue(0);
        assertThat(counter("inits")).hasValue(1); assertThat(counter("discoveries")).hasValue(1); assertThat(counter("stops")).hasValue(1);
    }

    @Test void pinRefusalPreservesItsCodeAndStopsTheSessionWithoutReadingRows() throws Exception {
        var refused = new TapstateException(IoError.SRS_MODEL_UNAVAILABLE, Map.of("chain", "chain", "table", "t1", "version", "0"), null);
        var port = port().withSnapshotPreparation((request, seam, tables) -> { throw refused; });
        try (var session = SnapshotSession.openPrepared(port, config("t1"), new SnapshotPreparation(1, null)).orElseThrow()) {
            assertThatThrownBy(() -> session.read("t1")).isSameAs(refused);
            assertThat(counter("reads")).hasValue(0); assertThat(counter("stops")).hasValue(1);
            assertThatThrownBy(() -> session.read("t1")).isInstanceOf(CancellationException.class);
        }
        assertThat(counter("stops")).hasValue(1);
    }

    @Test void aHostPreparationBugIsNotWrappedAsAConnectorReadFailure() throws Exception {
        var defect = new AssertionError("host invariant");
        var port = port().withSnapshotPreparation((request, seam, tables) -> { throw defect; });
        try (var session = SnapshotSession.openPrepared(port, config("t1"), new SnapshotPreparation(1, null)).orElseThrow()) {
            assertThatThrownBy(() -> session.read("t1")).isSameAs(defect);
        }
        assertThat(counter("reads")).hasValue(0); assertThat(counter("stops")).hasValue(1);
    }

    @Test void aMissingSeamCannotCallThePinOwnerOrReadRows() throws Exception {
        flag("reportSeam").set(false); var calls = new AtomicInteger();
        var port = port().withSnapshotPreparation((request, seam, tables) -> calls.incrementAndGet());
        try (var session = SnapshotSession.openPrepared(port, config("t1"), new SnapshotPreparation(1, null)).orElseThrow()) {
            assertThatThrownBy(() -> session.read("t1")).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(ConnectorError.SNAPSHOT_PREPARATION_UNAVAILABLE));
        }
        assertThat(calls).hasValue(0); assertThat(counter("reads")).hasValue(0); assertThat(counter("stops")).hasValue(1);
    }

    @Test void thePinOwnerCannotReceiveAPartialSelectedTableSet() throws Exception {
        var calls = new AtomicInteger(); var port = port().withSnapshotPreparation((request, seam, tables) -> calls.incrementAndGet());
        try (var session = SnapshotSession.openPrepared(port, config("t1", "absent"), new SnapshotPreparation(1, null)).orElseThrow()) {
            assertThatThrownBy(() -> session.read("t1")).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(ConnectorError.SNAPSHOT_PREPARATION_UNAVAILABLE));
        }
        assertThat(calls).hasValue(0); assertThat(counter("reads")).hasValue(0); assertThat(counter("stops")).hasValue(1);
    }

    @Test void sourceReadWaitsForTheSynchronousPinAndCannotStartAnotherTableDuringIt() throws Exception {
        var entered = new CountDownLatch(1); var accepted = new CountDownLatch(1);
        var port = port().withSnapshotPreparation((request, seam, tables) -> {
            entered.countDown();
            try { assertThat(accepted.await(5, TimeUnit.SECONDS)).isTrue(); }
            catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new CancellationException(); }
            flag("pinned").set(true);
        });
        try (var executor = Executors.newSingleThreadExecutor(); var session = SnapshotSession.openPrepared(port, config("t1"), new SnapshotPreparation(1, null)).orElseThrow()) {
            var opened = executor.submit(() -> session.read("t1"));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(counter("reads")).hasValue(0); assertThat(opened.isDone()).isFalse();
                assertThatThrownBy(() -> session.read("t1")).isInstanceOf(IllegalStateException.class);
                accepted.countDown(); drain(opened.get(5, TimeUnit.SECONDS));
            } finally { accepted.countDown(); }
        }
        assertThat(counter("reads")).hasValue(1); assertThat(counter("earlyReads")).hasValue(0);
    }

    @Test void anUnselectedDiscoveredTableCannotBypassAcceptedMetadata() throws Exception {
        var calls = new AtomicInteger(); var port = port().withSnapshotPreparation((request, seam, tables) -> {
            calls.incrementAndGet(); assertThat(tables.keySet()).containsExactly("t1"); flag("pinned").set(true);
        });
        try (var session = SnapshotSession.openPrepared(port, config("t1"), new SnapshotPreparation(1, null)).orElseThrow()) {
            assertThatThrownBy(() -> { try (var unexpected = session.read("t2")) { unexpected.hasNext(); } })
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(ConnectorError.SNAPSHOT_PREPARATION_UNAVAILABLE));
        }
        assertThat(calls).hasValue(0); assertThat(counter("reads")).hasValue(0);
    }

    @Test void closingReleasesTheCallerEvenWhenThePinCallbackIgnoresInterruption() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var exited = new CountDownLatch(1);
        var worker = new AtomicReference<Thread>();
        var port = port().withSnapshotPreparation((request, seam, tables) -> {
            worker.set(Thread.currentThread()); entered.countDown();
            try {
                boolean allowed = false;
                while (!allowed) {
                    try { allowed = release.await(10, TimeUnit.SECONDS); assertThat(allowed).isTrue(); }
                    catch (InterruptedException ignored) { /* The database driver may not stop on interruption. */ }
                }
                flag("pinned").set(true);
            } finally { exited.countDown(); }
        });
        try (var executor = Executors.newSingleThreadExecutor(); var session = SnapshotSession.openPrepared(port, config("t1"), new SnapshotPreparation(1, null)).orElseThrow()) {
            var opened = executor.submit(() -> session.read("t1"));
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue(); session.close();
                assertThat(exited.getCount()).as("the callback is deliberately still pending").isOne();
                assertThatThrownBy(() -> opened.get(1, TimeUnit.SECONDS)).hasCauseInstanceOf(CancellationException.class);
            } finally {
                release.countDown(); assertThat(exited.await(5, TimeUnit.SECONDS)).isTrue();
                worker.get().join(5000); assertThat(worker.get().isAlive()).isFalse();
            }
        }
        assertThat(counter("reads")).hasValue(0); assertThat(counter("stops")).hasValue(1);
    }

    @Test void closingWhilePinningInterruptsTheRegisteredWorkerBeforeAnyRead() throws Exception {
        var entered = new CountDownLatch(1); var interrupted = new CountDownLatch(1);
        var port = port().withSnapshotPreparation((request, seam, tables) -> {
            entered.countDown();
            try { new CountDownLatch(1).await(); throw new AssertionError("unexpected wake"); }
            catch (InterruptedException stopped) { interrupted.countDown(); Thread.currentThread().interrupt(); throw new CancellationException(); }
        });
        try (var executor = Executors.newSingleThreadExecutor(); var session = SnapshotSession.openPrepared(port, config("t1"), new SnapshotPreparation(1, null)).orElseThrow()) {
            var opened = executor.submit(() -> session.read("t1")); assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            session.close(); assertThat(interrupted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> opened.get(5, TimeUnit.SECONDS)).hasCauseInstanceOf(CancellationException.class);
        }
        assertThat(counter("reads")).hasValue(0); assertThat(counter("stops")).hasValue(1);
    }
}
