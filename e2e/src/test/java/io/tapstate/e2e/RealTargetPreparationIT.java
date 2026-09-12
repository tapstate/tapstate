package io.tapstate.e2e;

import io.tapstate.adapters.pdk.ConnectorIntrospector;
import io.tapstate.adapters.pdk.ConnectorRef;
import io.tapstate.adapters.pdk.PdkSinkPort;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.model.PipelineNode;
import io.tapstate.spi.sink.*;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.testsupport.DockerGate;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Real target readback for preparation policies, recovery, and the key required by upsert.
 * The declarative vocabulary cannot assert a rejected write, inspect physical indexes, or reopen
 * the sink with a receipt while deliberately withholding a source checkpoint. Those controls make
 * this a Java integration test; the ordinary cross-database pipeline remains a published example.
 */
@RequiresDocker
class RealTargetPreparationIT {
    private static final PipelineNode NODE = new PipelineNode("preparation", "sink");
    private static final Map<String, ConnectorRef> connectors = new HashMap<>();

    @BeforeAll
    static void requireRealTarget() throws Exception {
        DockerGate.require();
        RealConnectorGate.require("postgres", "mysql");
        for (String id : List.of("postgres", "mysql")) {
            Path jar;
            try (var paths = Files.list(Path.of(System.getProperty("tapstate.e2e.connectors-dir")))) {
                jar = paths.filter(path -> path.getFileName().toString().startsWith(id)
                        && path.toString().endsWith(".jar")).findFirst().orElseThrow();
            }
            var inspected = new ConnectorIntrospector().introspect(List.of(jar));
            connectors.put(id, new ConnectorRef(List.of(jar), inspected.className(), inspected.pdkApiVersion(),
                    null, inspected.spec()));
        }
    }

    @ParameterizedTest
    @EnumSource(OnFullLoad.class)
    void anExistingTargetHonorsThePolicyBeforeAnyRowLands(OnFullLoad policy) throws Exception {
        Map<String, Object> settings = settings("policy_" + policy.name().toLowerCase());
        try (Connection connection = SharedPostgres.connect(settings)) {
            seed(connection, true);
            State state = new State();
            if (policy == OnFullLoad.FAIL) {
                assertThatThrownBy(() -> write(settings, state, policy, true, target(true), 1, 10))
                        .hasRootCauseInstanceOf(IllegalStateException.class)
                        .hasStackTraceContaining("orders").hasStackTraceContaining("not empty")
                        .hasStackTraceContaining(TapstateException.class.getName());
                assertThat(rows(connection)).containsExactlyEntriesOf(Map.of(99L, 99L));
            } else {
                write(settings, state, policy, true, target(true), 1, 10);
                assertThat(rows(connection)).containsEntry(1L, 10L);
                assertThat(rows(connection).containsKey(99L)).isEqualTo(policy == OnFullLoad.APPEND);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(OnFullLoad.class)
    void aMissingTableIsCreatedAndAnEmptyTableNeverFails(OnFullLoad policy) throws Exception {
        Map<String, Object> settings = settings("new_" + policy.name().toLowerCase());
        try (Connection connection = SharedPostgres.connect(settings)) {
            try (var statement = connection.createStatement()) { statement.execute("DROP TABLE IF EXISTS orders"); }
            State state = new State();
            write(settings, state, policy, true, target(true), 1, 10);
            // A fresh writer must remember that create-table already supplied the primary key.
            write(settings, state, policy, true, target(true), 2, 20);
            assertThat(rows(connection)).containsExactlyInAnyOrderEntriesOf(Map.of(1L, 10L, 2L, 20L));
            try (var statement = connection.createStatement(); var result = statement.executeQuery(
                    "SELECT count(*) FROM pg_indexes WHERE tablename = 'orders'")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getLong(1)).as("creation must not duplicate the primary key index").isEqualTo(1);
            }
        }
    }

    @Test
    void resumeRecoveryCdcOnlyAndAnAppendRerunPreserveRowsAlreadyDelivered() throws Exception {
        Map<String, Object> settings = settings("resume");
        try (Connection connection = SharedPostgres.connect(settings)) {
            seed(connection, true);
            State state = new State();
            write(settings, state, OnFullLoad.CLEAR, true, target(true), 1, 10);
            assertThat(rows(connection)).containsExactlyEntriesOf(Map.of(1L, 10L));
            write(settings, state, OnFullLoad.CLEAR, false, target(true), 2, 20);
            // Recovery before a source checkpoint persisted still has the durable sink receipt.
            write(settings, state, OnFullLoad.CLEAR, true, target(true), 3, 30);
            state.dropNamespace(SinkPreparationNamespace.of(NODE));
            write(settings, state, OnFullLoad.APPEND, true, target(true), 4, 40);
            write(settings, new State(), OnFullLoad.CLEAR, false, target(true), 5, 50);
            assertThat(rows(connection)).containsExactlyInAnyOrderEntriesOf(
                    Map.of(1L, 10L, 2L, 20L, 3L, 30L, 4L, 40L, 5L, 50L));
        }
    }

    @Test
    void aUniqueIndexMakesRepeatedUpsertsConvergeAndItsAbsenceIsObservable() throws Exception {
        Map<String, Object> settings = SharedMySql.settings("target_preparation_index");
        try (Connection connection = SharedMySql.connect(settings)) {
            seed(connection, false);
            write("mysql", settings, new State(), OnFullLoad.APPEND, true, target(false), 1, 10);
            write("mysql", settings, new State(), OnFullLoad.APPEND, true, target(false), 1, 20);
            try (var statement = connection.createStatement(); var result = statement.executeQuery(
                    "SELECT count(*) FROM orders WHERE id = 1")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getLong(1)).as("without a physical unique key the repeated upsert inserts twice")
                        .isEqualTo(2);
            }
            seed(connection, false);
            State state = new State();
            write("mysql", settings, state, OnFullLoad.APPEND, true, target(true), 1, 10);
            write("mysql", settings, state, OnFullLoad.APPEND, true, target(true), 1, 20);
            assertThat(rows(connection)).containsExactlyInAnyOrderEntriesOf(Map.of(99L, 99L, 1L, 20L));
            try (var statement = connection.createStatement(); var result = statement.executeQuery(
                    "SHOW INDEX FROM orders WHERE Non_unique = 0")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getString("Column_name")).isEqualTo("id");
            }
        }
    }

    private static TargetTable target(boolean index) {
        return new TargetTable("orders", List.of(
                new TargetField("id", "source_integer", true, TapstateType.INT64),
                new TargetField("seq", "source_integer", false, TapstateType.INT64)),
                index ? List.of(new TargetIndex(List.of("id"), true)) : List.of());
    }

    private static void write(Map<String, Object> settings, State state, OnFullLoad policy, boolean fullLoad,
            TargetTable target, long id, long seq) throws Exception {
        write("postgres", settings, state, policy, fullLoad, target, id, seq);
    }

    private static void write(String connectorId, Map<String, Object> settings, State state, OnFullLoad policy,
            boolean fullLoad, TargetTable target, long id, long seq) throws Exception {
        Map<String, Object> config = new LinkedHashMap<>(settings);
        if (connectorId.equals("postgres")) {
            config.put("user", config.remove("username"));
            config.put("schema", "public");
        }
        try (SinkWriter writer = new PdkSinkPort(connectors::get, state).open(new SinkConfig(
                connectorId, config, WriteMode.UPSERT, DdlPolicy.FAIL, target, NODE, policy, fullLoad))) {
            assertThat(writer.write(List.of(Envelope.insert(1L, "orders", Map.of("id", id, "seq", seq), null)))
                    .toCompletableFuture().get(30, TimeUnit.SECONDS).written()).isEqualTo(1);
        }
    }

    private static Map<String, Object> settings(String suffix) {
        return SharedPostgres.settings("target_preparation_" + suffix);
    }

    private static void seed(Connection connection, boolean primaryKey) throws Exception {
        try (var statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS orders");
            statement.execute("CREATE TABLE orders (id BIGINT " + (primaryKey ? "PRIMARY KEY" : "") + ", seq BIGINT)");
            statement.execute("INSERT INTO orders VALUES (99, 99)");
        }
    }

    private static Map<Long, Long> rows(Connection connection) throws Exception {
        Map<Long, Long> result = new LinkedHashMap<>();
        try (var statement = connection.createStatement(); var rows = statement.executeQuery("SELECT id, seq FROM orders")) {
            while (rows.next()) {
                assertThat(result.put(rows.getLong(1), rows.getLong(2))).as("upsert must not duplicate a key").isNull();
            }
        }
        return result;
    }

    private static final class State implements KeyedStateStore {
        private final Map<String, byte[]> entries = new HashMap<>();
        public Optional<byte[]> load(String namespace, String key) { return Optional.ofNullable(entries.get(namespace + "/" + key)); }
        public void save(String namespace, String key, byte[] value) { entries.put(namespace + "/" + key, value); }
        public Optional<byte[]> saveIfAbsent(String namespace, String key, byte[] value) {
            return Optional.ofNullable(entries.putIfAbsent(namespace + "/" + key, value));
        }
        public void delete(String namespace, String key) { entries.remove(namespace + "/" + key); }
        public void dropNamespace(String namespace) { entries.keySet().removeIf(key -> key.startsWith(namespace + "/")); }
        public long count(String namespace) { return entries.keySet().stream().filter(key -> key.startsWith(namespace + "/")).count(); }
    }
}
