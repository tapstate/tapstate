package io.tapstate.adapters.pdk;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureBatch;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.SnapshotSession;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PostgresPublicationStartupTest {

    @Test
    void twoIndependentLocalChannelsCannotRaceTheirInitialPublicationCreation(@TempDir Path directory)
            throws Exception {
        try (Fixture fixture = new Fixture(directory, "race"); var callers = Executors.newFixedThreadPool(2)) {
            Future<List<Envelope>> first = callers.submit(() -> fixture.read("first", "postgres"));
            assertThat(fixture.latch("sampleEntered").await(5, TimeUnit.SECONDS)).isTrue();
            Future<List<Envelope>> second = callers.submit(() -> fixture.read("second", "postgres"));
            assertThatCode(() -> assertThat(first.get(5, TimeUnit.SECONDS)).hasSize(1))
                    .doesNotThrowAnyException();
            assertThatCode(() -> assertThat(second.get(5, TimeUnit.SECONDS)).hasSize(1))
                    .doesNotThrowAnyException();
            assertThat(fixture.counter("samples")).hasValue(2);
            assertThat(fixture.counter("batches")).hasValue(2);
            assertThat((AtomicBoolean) fixture.state.get("published")).isTrue();
        }
    }

    @Test
    void aHeldSnapshotBatchCannotHoldTheSiblingChannelsInitialPosition(@TempDir Path directory)
            throws Exception {
        try (Fixture fixture = new Fixture(directory, "holdFirstBatch"); var callers = Executors.newFixedThreadPool(2)) {
            Future<List<Envelope>> first = callers.submit(() -> fixture.read("first", "postgres"));
            try {
                assertThat(fixture.latch("rowEmitted").await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(first.isDone()).isFalse();
                Future<List<Envelope>> second = callers.submit(() -> fixture.read("second", "postgres"));
                assertThat(second.get(5, TimeUnit.SECONDS)).hasSize(1);
                assertThat(first.isDone()).isFalse();
                assertThat(fixture.counter("samples")).hasValue(2);
                assertThat(fixture.counter("batches")).hasValue(2);
            } finally { fixture.latch("releaseBatch").countDown(); }
            assertThat(first.get(5, TimeUnit.SECONDS)).hasSize(1);
        }
    }

    @Test
    void differentResolvedSettingsAndOtherConnectorsPrepareIndependently(@TempDir Path directory)
            throws Exception {
        try (Fixture held = new Fixture(directory, "blockedOffset");
                Fixture other = new Fixture(directory, "ordinary"); var callers = Executors.newFixedThreadPool(3)) {
            Future<List<Envelope>> first = callers.submit(() -> held.read("first", "postgres"));
            try {
                assertThat(held.latch("sampleEntered").await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(callers.submit(() -> other.read("other", "postgres")).get(5, TimeUnit.SECONDS)).hasSize(1);
                // Same connection settings, another connector: it must not join the PostgreSQL turn.
                held.state.put("mode", "ordinary");
                assertThat(callers.submit(() -> held.read("non_postgres", "demo")).get(5, TimeUnit.SECONDS)).hasSize(1);
                assertThat(first.isDone()).isFalse();
            } finally { held.latch("releaseOffset").countDown(); }
            assertThat(first.get(5, TimeUnit.SECONDS)).hasSize(1);
        }
    }

    @Test
    void otherSqlErrorsBeforeTheSeamRemainCodedAndAreNeverRetried(@TempDir Path directory) {
        for (String sqlState : List.of("42501", "23505")) {
            try (Fixture fixture = new Fixture(directory, "offsetError")) {
                String detail = "permission or another unique constraint failed; relation already exists";
                fixture.state.put("errorMessage", detail); fixture.state.put("sqlState", sqlState);
                assertThatThrownBy(() -> fixture.read("failed", "postgres"))
                        .isInstanceOf(TapstateException.class).satisfies(thrown -> {
                            TapstateException failure = (TapstateException) thrown;
                            assertThat(failure.code()).isEqualTo(ConnectorError.CAPTURE_FAILED);
                            assertThat(failure.args()).containsEntry("connector", "postgres").containsEntry("detail", detail);
                            assertThat(failure.getCause()).isInstanceOf(SQLException.class);
                            assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo(sqlState);
                        });
                assertThat(fixture.counter("samples")).hasValue(1);
                assertThat(fixture.counter("batches")).hasValue(0);
            }
        }
    }

    @Test
    void aPublicationShapedFailureAfterAnEmittedRowCannotRestartTheRead(@TempDir Path directory) {
        try (Fixture fixture = new Fixture(directory, "afterRowError")) {
            fixture.state.put("errorMessage", "duplicate key value violates unique constraint pg_publication_pubname_index");
            fixture.state.put("sqlState", "23505");
            try (SnapshotSession session = fixture.port().snapshotSession(fixture.config("failed", "postgres"));
                    CaptureBatch batch = session.read("t1")) {
                assertThat(batch.seam()).isPresent();
                assertThat(batch.hasNext()).isTrue();
                assertThat(batch.next().op()).isEqualTo(io.tapstate.core.event.Op.READ);
                assertThatThrownBy(batch::hasNext).isInstanceOf(TapstateException.class).satisfies(thrown -> {
                    TapstateException failure = (TapstateException) thrown;
                    assertThat(failure.code()).isEqualTo(ConnectorError.CAPTURE_FAILED);
                    assertThat(failure.getCause()).isInstanceOf(SQLException.class);
                    assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo("23505");
                });
            }
            assertThat(fixture.counter("samples")).hasValue(1);
            assertThat(fixture.counter("batches")).hasValue(1);
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final String key = "publication-start-" + UUID.randomUUID();
        private final Map<String, Object> state = new java.util.concurrent.ConcurrentHashMap<>();
        private final ConnectorRef reference;
        Fixture(Path directory, String mode) {
            reference = new ConnectorRef(List.of(PublicationStartupJars.source(directory)),
                    "synthetic.PublicationStartupSource", "2.0.8", null);
            state.put("mode", mode); state.put("samples", new AtomicInteger()); state.put("batches", new AtomicInteger());
            state.put("published", new AtomicBoolean()); state.put("absentChecks", new CountDownLatch(2));
            for (String name : List.of("sampleEntered", "rowEmitted", "releaseOffset", "releaseBatch")) {
                state.put(name, new CountDownLatch(1));
            }
            assertThat(System.getProperties().put(key, state)).isNull();
        }
        PdkCapturePort port() { return new PdkCapturePort(ignored -> reference); }
        CaptureConfig config(String owner, String connector) {
            Map<String, Object> settings = new HashMap<>();
            settings.put("host", "local"); settings.put("database", key); settings.put("fixtureKey", key);
            settings.put("password", "unpublished-test-value");
            return new CaptureConfig(connector, settings, List.of("t1"), new PipelineNode(owner, "source"));
        }
        List<Envelope> read(String owner, String connector) {
            try (SnapshotSession session = port().snapshotSession(config(owner, connector));
                    CaptureBatch batch = session.read("t1")) {
                List<Envelope> rows = new ArrayList<>();
                while (batch.hasNext()) { rows.add(batch.next()); }
                return rows;
            }
        }
        CountDownLatch latch(String name) { return (CountDownLatch) state.get(name); }
        AtomicInteger counter(String name) { return (AtomicInteger) state.get(name); }
        @Override public void close() {
            latch("releaseOffset").countDown(); latch("releaseBatch").countDown();
            System.getProperties().remove(key);
        }
    }
}
