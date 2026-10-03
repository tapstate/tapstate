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
 * Two pipelines reading one source share its chain: one reader, one position, and one connector identity,
 * kept in the chain's own notes rather than in either pipeline's. The second pipeline starts from a tail
 * position the first one's run wrote; after a process replacement both must resume. Deletes made while the
 * server is down distinguish resumed CDC from another snapshot.
 *
 * <p>The identity is the connector's own name for its reader of the source. Kept per pipeline, it would be
 * a different reader's for whichever pipeline opened the stream next; kept on the chain, it is the one
 * reader's whoever opens it, so it has to stay the same when another pipeline joins and across the
 * process being replaced, and no pipeline's own notes may hold one.
 *
 * <p>The declarative vocabulary cannot compare the connector-owned identity bytes across pipelines and
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
    void anotherPipelineAndARestartKeepThePositionAndTheChainsOneReaderIdentity(Tiers tier) throws Exception {
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
        byte[] identity;
        String chain;

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
                chain = ChainNotes.chainOf(store, first);
                identity = chainIdentity(store, chain);
                assertThat(nodeIdentity(store, first, "first_source"))
                        .as("the reader's identity is the chain's, not the pipeline that opened it")
                        .isEmpty();

                // This is a new node with no state, taking a position issued by the first node. A new
                // snapshot would hide that path by replacing the shared position with its own seam.
                apply(control, mysql, targetUri, second, "second_source", "later_orders", "cdc_only");
                sql(mysql, "INSERT INTO later_orders VALUES (2, 'second-tail')");
                awaitIds(control, second, mongo, target, "later_orders", List.of(2L));
                assertThat(ChainNotes.chainOf(store, second)).as("both pipelines read one chain").isEqualTo(chain);
                assertThat(chainIdentity(store, chain))
                        .as("the chain's one reader kept its identity when it took the second table on")
                        .isEqualTo(identity);
                assertThat(nodeIdentity(store, second, "second_source")).isEmpty();
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
                assertThat(chainIdentity(store, chain)).as("and across the process being replaced")
                        .isEqualTo(identity);
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

    /** The chain {@code pipeline} reads, as its cursor on the store names it. */
    /** The reader identity the chain's notes hold; it has to be there. */
    private static byte[] chainIdentity(String store, String chain) {
        return identityIn(store, "pdk.chain." + chain)
                .orElseThrow(() -> new AssertionError("the chain " + chain + " keeps no reader identity"));
    }

    /** A reader identity kept in one pipeline node's own notes, if any is. */
    private static java.util.Optional<byte[]> nodeIdentity(String store, String pipeline, String source) {
        return identityIn(store, "pdk.state." + pipeline + "." + source);
    }

    private static java.util.Optional<byte[]> identityIn(String store, String namespace) {
        try (MongoClient client = MongoClients.create(store)) {
            Document id = new Document("ns", namespace).append("k", "SERVER_NAME");
            Document record = client.getDatabase("tapstate_nest").getCollection("operator_state")
                    .find(new Document("_id", id)).first();
            return java.util.Optional.ofNullable(record).map(found -> found.get("state", Binary.class).getData());
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
