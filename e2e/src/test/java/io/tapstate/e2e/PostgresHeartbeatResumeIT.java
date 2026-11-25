package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.adapters.pdk.ConnectorClassLoader;
import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkCapturePort;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.capture.CaptureListener;
import io.tapstate.spi.capture.CaptureStart;
import io.tapstate.spi.capture.SourcePosition;
import io.tapstate.spi.capture.Subscription;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.testsupport.DockerGate;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** A real connector must deliver the INSERT beginning at its recorded post-commit heartbeat LSN. */
class PostgresHeartbeatResumeIT {

    private static final Duration BOUND = Duration.ofSeconds(60);

    @BeforeAll
    static void requireDockerAndConnector() {
        DockerGate.require();
        RealConnectorGate.require("postgres");
    }

    @Test
    void aKeepStateRestartDeliversTheInsertBeginningAtTheRecordedHeartbeatLsn() throws Exception {
        Map<String, Object> source = SharedPostgres.settings("heartbeat_resume_source");
        try (Connection writer = SharedPostgres.connect(source);
                Statement sql = writer.createStatement()) {
            sql.execute("CREATE TABLE customers (id INT PRIMARY KEY, name TEXT)");
            sql.execute("ALTER TABLE customers REPLICA IDENTITY FULL");
        }
        Path jar = connectorJar();
        var inspected = new ConnectorIntrospector().introspect(List.of(jar));
        ConnectorRef ref = new ConnectorRef(List.of(jar), inspected.className(), inspected.pdkApiVersion(),
                null, inspected.spec());
        Map<String, Object> settings = new LinkedHashMap<>(source);
        settings.put("user", settings.remove("username"));
        settings.put("schema", "public");
        CaptureConfig config = new CaptureConfig("postgres", settings, List.of("customers"),
                new PipelineNode("heartbeat_resume", "src_pg"));
        PdkCapturePort capture = new PdkCapturePort(id -> ref, new Notes());
        Recording delivered = new Recording();
        try (ConnectorClassLoader offsets = ConnectorClassLoader.open(List.of(jar));
                Connection writer = SharedPostgres.connect(source)) {
            try (Subscription initial = capture.cdc(config, CaptureStart.present(), delivered)) {
                Await.until("the initial PostgreSQL slot to become active", BOUND,
                        () -> PostgresSlots.active(source), () -> String.valueOf(PostgresSlots.of(source)));
                // The stream's own startup transaction must precede the measured transaction.
                for (int id : List.of(100, 101)) {
                    insert(writer, id);
                    Await.until("warm-up row " + id + " to be captured", BOUND,
                            () -> delivered.ids().contains(String.valueOf(id)), () -> delivered.ids().toString());
                }
                long afterWarmup = walInsertLsn(writer);
                Await.until("a post-commit heartbeat at the WAL head after row 101", Duration.ofSeconds(30),
                        () -> {
                            Map<?, ?> offset = sourceOffset(delivered.position.get(), offsets);
                            return offset.get("txId") == null && offset.get("lsn_proc") instanceof Number lsn
                                    && lsn.longValue() == afterWarmup
                                    && offset.get("lsn_proc").equals(offset.get("lsn_commit"));
                        }, () -> "WAL head: " + afterWarmup
                                + "; captured offset: " + sourceOffset(delivered.position.get(), offsets));
            }
            Await.until("the stopped capture to release its slot", BOUND,
                    () -> !PostgresSlots.active(source), () -> String.valueOf(PostgresSlots.of(source)));
            String slot = PostgresSlots.only(source).name();
            SourcePosition recorded = delivered.position.get();
            Map<String, Object> heartbeat = sourceOffset(recorded, offsets);
            assertThat(heartbeat.get("txId")).isNull();
            assertThat(heartbeat).containsEntry("lsn_proc", heartbeat.get("lsn_commit"));
            long resumeLsn = ((Number) heartbeat.get("lsn_proc")).longValue();

            // Closing keeps the connector's notes and the exact opaque position; resume uses both.
            try (Subscription resumed = capture.cdc(config, CaptureStart.resume(recorded), delivered)) {
                Await.until("the resumed PostgreSQL slot to become active", BOUND,
                        () -> PostgresSlots.active(source), () -> String.valueOf(PostgresSlots.of(source)));
                assertThat(PostgresSlots.names(source)).containsExactly(slot);
                assertThat(walInsertLsn(writer))
                        .as("no intervening WAL: row 102 must begin exactly at the recorded heartbeat LSN")
                        .isEqualTo(resumeLsn);
                insert(writer, 102);

                Await.until("the resumed capture to advance beyond row 102", BOUND,
                        () -> sourceOffset(delivered.position.get(), offsets).get("lsn_proc") instanceof Number lsn
                                && lsn.longValue() > resumeLsn,
                        () -> String.valueOf(sourceOffset(delivered.position.get(), offsets)));
                Await.until("row 102, whose INSERT began at the recorded heartbeat LSN, to be delivered",
                        Duration.ofSeconds(10), () -> delivered.ids().equals(List.of("100", "101", "102")),
                        () -> "delivered IDs: " + delivered.ids() + "; resume offset: " + heartbeat
                                + "; advanced offset: " + sourceOffset(delivered.position.get(), offsets));
            }
        }
    }

    private static void insert(Connection writer, int id) throws Exception {
        try (Statement sql = writer.createStatement()) {
            sql.execute("INSERT INTO customers (id, name) VALUES (" + id + ", 'row" + id + "')");
        }
    }

    private static long walInsertLsn(Connection writer) throws Exception {
        try (Statement sql = writer.createStatement();
                ResultSet row = sql.executeQuery("SELECT pg_current_wal_insert_lsn() - '0/0'::pg_lsn")) {
            row.next();
            return row.getLong(1);
        }
    }

    private static Path connectorJar() throws Exception {
        try (var files = Files.list(Path.of(System.getProperty("tapstate.e2e.connectors-dir")))) {
            List<Path> jars = files.filter(path -> path.getFileName().toString().startsWith("postgres"))
                    .filter(path -> path.getFileName().toString().endsWith(".jar")).toList();
            assertThat(jars).hasSize(1);
            return jars.getFirst();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> sourceOffset(SourcePosition position, ConnectorClassLoader loader) {
        if (position == null) {
            return Map.of();
        }
        try (var input = new ObjectInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(position.token()))) {
            @Override
            protected Class<?> resolveClass(ObjectStreamClass descriptor) throws ClassNotFoundException {
                return loader.load(descriptor.getName());
            }
        }) {
            Object offset = input.readObject();
            String source = (String) offset.getClass().getMethod("getSourceOffset").invoke(offset);
            return source == null ? Map.of() : (Map<String, Object>) JsonReader.parse(source);
        } catch (Exception failure) {
            throw new AssertionError("the real connector's recorded offset must be readable", failure);
        }
    }

    private static final class Recording implements CaptureListener {
        private final List<Envelope> events = new CopyOnWriteArrayList<>();
        private final AtomicReference<SourcePosition> position = new AtomicReference<>();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();

        @Override
        public void onBatch(List<Envelope> batch, Optional<SourcePosition> at) {
            events.addAll(batch);
            at.ifPresent(position::set);
        }

        @Override
        public void onError(Throwable error) {
            failure.set(error);
        }

        private List<String> ids() {
            assertThat(failure.get()).as("the capture must remain healthy").isNull();
            return events.stream().map(event -> String.valueOf(event.after().get("id"))).sorted().toList();
        }
    }

    private static final class Notes implements KeyedStateStore {
        private final Map<String, Map<String, byte[]>> namespaces = new ConcurrentHashMap<>();

        private Map<String, byte[]> entries(String namespace) {
            return namespaces.computeIfAbsent(namespace, ignored -> new ConcurrentHashMap<>());
        }

        public Optional<byte[]> load(String namespace, String key) { return Optional.ofNullable(entries(namespace).get(key)); }
        public void save(String namespace, String key, byte[] value) { entries(namespace).put(key, value); }
        public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] value) {
            return Optional.ofNullable(entries(namespace).putIfAbsent(key, value));
        }
        public void delete(String namespace, String key) { entries(namespace).remove(key); }
        public void dropNamespace(String namespace) { namespaces.remove(namespace); }
        public long count(String namespace) { return entries(namespace).size(); }
    }
}
