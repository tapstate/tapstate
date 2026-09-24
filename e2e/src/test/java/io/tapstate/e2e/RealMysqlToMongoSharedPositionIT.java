package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.sql.Connection;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Two pipeline nodes share a source position without sharing their connector identities. The second
 * starts from a tail position written by the first; after a process replacement both must resume.
 * Deletes made while the server is down distinguish resumed CDC from another snapshot.
 *
 * <p>The declarative vocabulary cannot compare the connector-owned identity bytes across nodes and
 * process replacements or require a tail position to replace the snapshot seam before another start.
 * The real MySQL connector, its position object and its separate class loader are essential here.
 */
class RealMysqlToMongoSharedPositionIT {
    @BeforeAll
    static void requireConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void anotherReaderAndARestartKeepThePositionButNotTheOtherReadersIdentity(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        String first = "reader_first_" + suffix;
        String second = "reader_second_" + suffix;
        Map<String, Object> mysql = SharedMySql.settings("reader_position_" + suffix);
        sql(mysql, "CREATE TABLE orders (id INT PRIMARY KEY, name VARCHAR(64))",
                "CREATE TABLE later_orders (id INT PRIMARY KEY, name VARCHAR(64))",
                "INSERT INTO orders VALUES (1, 'seeded')",
                "INSERT INTO later_orders VALUES (1, 'before-cdc')");
        String store = SharedMongo.replicaSetUrl("reader_position_store_" + suffix);
        String targetUri = SharedMongo.replicaSetUrl("reader_position_target_" + suffix);
        EndpointAddress target = EndpointAddress.uri(targetUri);
        byte[] firstIdentity;
        byte[] secondIdentity;

        try (MongoEndpoints mongo = new MongoEndpoints()) {
            try (ServerHandle server = tier.launch(store)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                apply(control, mysql, targetUri, first, "first_source", "orders", "snapshot_and_cdc");
                awaitIds(control, first, mongo, target, "orders", List.of(1L));
                sql(mysql, "UPDATE orders SET name='first-tail' WHERE id=1");
                // The position face reports the durable source-read offset, not the snapshot seam.
                // It is absent until CDC has delivered and acknowledged a real change.
                Await.until("the first reader's tail position", () -> !control.resumePoint(first).isEmpty(),
                        () -> control.logs(first));
                firstIdentity = identity(store, first, "first_source");

                // This is a new node with no state, taking a position issued by the first node. A new
                // snapshot would hide that path by replacing the shared position with its own seam.
                apply(control, mysql, targetUri, second, "second_source", "later_orders", "cdc_only");
                sql(mysql, "INSERT INTO later_orders VALUES (2, 'second-tail')");
                awaitIds(control, second, mongo, target, "later_orders", List.of(2L));
                secondIdentity = identity(store, second, "second_source");
                assertThat(secondIdentity).as("each live reader keeps a separate logical identity")
                        .isNotEqualTo(firstIdentity);
                assertThat(identity(store, first, "first_source")).isEqualTo(firstIdentity);
                assertThat(control.state(first)).contains(PipelineState.RUNNING);
            }

            sql(mysql, "DELETE FROM orders WHERE id=1", "DELETE FROM later_orders WHERE id=2",
                    "INSERT INTO later_orders VALUES (3, 'during-downtime')");
            try (ServerHandle server = tier.launch(store)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.login("e2e", "e2e-password");
                sql(mysql, "INSERT INTO orders VALUES (99, 'after-restart')",
                        "INSERT INTO later_orders VALUES (99, 'after-restart')");
                // The old target rows must disappear, and genuinely new rows must arrive on both
                // paths. RUNNING alone could have been an observation left by the previous process.
                awaitIds(control, first, mongo, target, "orders", List.of(99L));
                awaitIds(control, second, mongo, target, "later_orders", List.of(3L, 99L));
                assertThat(identity(store, first, "first_source")).isEqualTo(firstIdentity);
                assertThat(identity(store, second, "second_source")).isEqualTo(secondIdentity);
                assertThat(control.errorCount(first)).contains(0L);
                assertThat(control.errorCount(second)).contains(0L);
            }
        }
    }

    private static void apply(ControlPlane control, Map<String, Object> mysql, String targetUri, String pipeline,
            String source, String table, String mode) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: reader_target
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetUri));
        resources.put(source + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """.formatted(source, mysql.get("host"), mysql.get("port"), mysql.get("database"),
                        mysql.get("username"), mysql.get("password"), table));
        resources.put(pipeline + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: %s }
                serve:
                  from: %s
                  sync:
                    - source: reader_target
                """.formatted(pipeline, source, mode, table));
        control.apply(resources);
        control.discoverSchema(source, "mysql", mysql);
        control.lifecycle(pipeline, LifecycleVerb.START);
    }

    private static void awaitIds(ControlPlane control, String pipeline, MongoEndpoints mongo,
            EndpointAddress target, String table, List<Long> expected) {
        Await.until("rows in " + table + " from " + pipeline,
                () -> ids(mongo, target, table).equals(expected),
                () -> "rows=" + ids(mongo, target, table) + ", state=" + control.state(pipeline)
                        + ", logs=" + control.logs(pipeline));
        assertThat(control.state(pipeline)).contains(PipelineState.RUNNING);
    }

    private static List<Long> ids(MongoEndpoints mongo, EndpointAddress target, String table) {
        return mongo.documents(target, table).stream()
                .map(row -> ((Number) row.get("id")).longValue()).sorted().toList();
    }

    private static byte[] identity(String store, String pipeline, String source) {
        try (MongoClient client = MongoClients.create(store)) {
            Document id = new Document("ns", "pdk.state." + pipeline + "." + source).append("k", "SERVER_NAME");
            Document record = client.getDatabase("tapstate_nest").getCollection("operator_state")
                    .find(new Document("_id", id)).first();
            assertThat(record).as("the reader identity is durable for %s/%s", pipeline, source).isNotNull();
            return record.get("state", Binary.class).getData();
        }
    }

    private static void sql(Map<String, Object> mysql, String... statements) throws Exception {
        try (Connection connection = SharedMySql.connect(mysql); Statement statement = connection.createStatement()) {
            for (String sql : statements) {
                statement.execute(sql);
            }
        }
    }
}
