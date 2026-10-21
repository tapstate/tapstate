package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.Decimal128;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Real connector rows keep their declared types through snapshot, CDC and durable resume.
 * SQL temporal/decimal/binary fixtures and BSON runtime type assertions are outside the
 * declarative seed and matcher vocabulary, so the same Java witness runs on both fidelity tiers.
 */
class RealPdkSchemaConversionIT {
    private static final Instant STAMP = Instant.parse("2021-11-22T00:08:03.849711Z");
    private static final Instant DAY = Instant.parse("2017-07-23T00:00:00Z");
    private static final BigDecimal AMOUNT = new BigDecimal("12345678901234.5678901234");
    private static final long ORDINARY_NUMBER = 1637539683849711L;

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void mysqlFieldsKeepTheirTypesAfterChangesAndResume(Tiers tier) throws Exception {
        exercise("mysql", tier);
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void postgresFieldsKeepTheirTypesAfterChangesAndResume(Tiers tier) throws Exception {
        exercise("postgres", tier);
    }

    private static void exercise(String kind, Tiers tier) throws Exception {
        DockerGate.require();
        RealConnectorGate.require(kind, "mongodb");
        String suffix = kind + "_" + tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> database = kind.equals("mysql")
                ? SharedMySql.settings("typed_" + suffix) : SharedPostgres.settings("typed_" + suffix);
        try (Connection connection = connect(kind, database); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE orders (id BIGINT PRIMARY KEY, label VARCHAR(32), c_date DATE, c_ts "
                    + (kind.equals("mysql") ? "DATETIME(6)" : "TIMESTAMP(6)")
                    + ", amount DECIMAL(24,10), payload " + (kind.equals("mysql") ? "VARBINARY(8)" : "BYTEA")
                    + ", ordinary_number BIGINT)");
            if (kind.equals("postgres")) {
                statement.execute("ALTER TABLE orders REPLICA IDENTITY FULL");
            }
            insert(connection, 1, "original");
            insert(connection, 3, "to-delete");
        }
        String targetUri = SharedMongo.replicaSetUrl("typed_target_" + suffix);
        EndpointAddress target = EndpointAddress.uri(targetUri);
        String pipeline = "typed_" + suffix;
        try (ServerHandle server = tier.launch(SharedMongo.replicaSetUrl("typed_store_" + suffix));
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(kind, ConnectorJars.bytesFor(kind));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            Map<String, Object> config = new LinkedHashMap<>(database);
            config.put("timezone", "+00:00");
            if (kind.equals("postgres")) {
                config.put("user", config.remove("username"));
                config.put("schema", "public");
            }
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("source.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: typed_source
                    connector: %s
                    config: %s
                    mode: cdc
                    tables: [orders]
                    """.formatted(kind, JsonWriter.write(config)));
            resources.put("target.tap.yml", """
                    version: tapstate/v1
                    kind: source
                    id: typed_target
                    connector: mongodb
                    config: { uri: "%s" }
                    """.formatted(targetUri));
            resources.put("pipeline.tap.yml", """
                    version: tapstate/v1
                    kind: pipeline
                    id: %s
                    source: typed_source
                    settings: { read_mode: snapshot_and_cdc }
                    serve:
                      from: [orders]
                      sync:
                        - source: typed_target
                          execution: { parallelism: 1 }
                    """.formatted(pipeline));
            control.apply(resources);
            control.discoverSchema("typed_source", kind, config);
            control.lifecycle(pipeline, LifecycleVerb.START);
            awaitRows(mongo, target, control, pipeline, Map.of(1L, "original", 3L, "to-delete"));
            assertTypes(mongo.documents(target, "orders"));

            try (Connection connection = connect(kind, database); Statement statement = connection.createStatement()) {
                statement.execute("UPDATE orders SET label='updated' WHERE id=1");
                insert(connection, 2, "inserted");
                statement.execute("DELETE FROM orders WHERE id=3");
            }
            awaitRows(mongo, target, control, pipeline, Map.of(1L, "updated", 2L, "inserted"));
            assertTypes(mongo.documents(target, "orders"));

            control.lifecycle(pipeline, LifecycleVerb.PAUSE);
            Await.until("capture is paused", () -> control.state(pipeline).filter(PipelineState.PAUSED::equals).isPresent(),
                    () -> control.logs(pipeline));
            try (Connection connection = connect(kind, database)) {
                insert(connection, 4, "resumed");
            }
            control.lifecycle(pipeline, LifecycleVerb.RESUME);
            awaitRows(mongo, target, control, pipeline, Map.of(1L, "updated", 2L, "inserted", 4L, "resumed"));
            assertTypes(mongo.documents(target, "orders"));
            assertThat(control.errorCount(pipeline)).contains(0L);
        }
    }

    private static void assertTypes(List<Document> rows) {
        assertThat(rows).allSatisfy(row -> {
            assertThat(row.get("c_date")).isInstanceOf(Date.class).isEqualTo(Date.from(DAY));
            assertThat(row.get("c_ts")).isInstanceOf(Date.class).isEqualTo(Date.from(STAMP));
            assertThat(row.get("ordinary_number")).isInstanceOf(Long.class).isEqualTo(ORDINARY_NUMBER);
            assertThat(row.get("amount")).isInstanceOf(Decimal128.class);
            assertThat(((Decimal128) row.get("amount")).bigDecimalValue()).isEqualByComparingTo(AMOUNT);
            Object payload = row.get("payload");
            byte[] bytes = payload instanceof Binary binary ? binary.getData() : (byte[]) payload;
            assertThat(bytes).containsExactly((byte) 0, (byte) 1, (byte) 127, (byte) 255);
        });
    }

    private static void awaitRows(MongoEndpoints mongo, EndpointAddress target, ControlPlane control,
            String pipeline, Map<Long, String> expected) {
        Await.until("the expected keys and changed labels reach Mongo", () -> {
            List<Document> rows = mongo.documents(target, "orders");
            if (rows.size() != expected.size()) {
                return false;
            }
            Set<String> actual = rows.stream().map(row -> ((Number) row.get("id")).longValue() + ":" + row.get("label"))
                    .collect(Collectors.toSet());
            return actual.equals(expected.entrySet().stream().map(entry -> entry.getKey() + ":" + entry.getValue())
                    .collect(Collectors.toSet()));
        }, () -> control.logs(pipeline));
    }

    private static Connection connect(String kind, Map<String, Object> settings) throws Exception {
        return kind.equals("mysql") ? SharedMySql.connect(settings) : SharedPostgres.connect(settings);
    }

    private static void insert(Connection connection, long id, String label) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement("INSERT INTO orders VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            statement.setLong(1, id);
            statement.setString(2, label);
            Calendar utc = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            statement.setDate(3, new java.sql.Date(DAY.toEpochMilli()), utc);
            statement.setTimestamp(4, Timestamp.from(STAMP), utc);
            statement.setBigDecimal(5, AMOUNT);
            statement.setBytes(6, new byte[] {0, 1, 127, (byte) 255});
            statement.setLong(7, ORDINARY_NUMBER);
            statement.executeUpdate();
        }
    }
}
