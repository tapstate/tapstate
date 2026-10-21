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
 * Two pipelines reading one MySQL database the same way share one capture: its position, and the identity its
 * connector minted. The second starts cdc_only on the chain the first started, after the first has a tail
 * position; after a process replacement both must resume. Deletes made while the server is down distinguish
 * resumed CDC from another snapshot.
 *
 * <p>The second pipeline does not start a reader of its own: the capture widens to its table and restarts from
 * the chain's position, and the connector it restarts reads back the identity the first run filed rather than
 * minting another. A fresh identity is what leaves a recorded position filed under a name nothing looks up.
 *
 * <p>The declarative vocabulary cannot compare the connector-owned identity bytes across a widening and a
 * process replacement, or require a tail position to replace the snapshot seam before another start. The real
 * MySQL connector, its position object and its separate class loader are essential here.
 */
class RealMysqlToMongoSharedPositionIT {

    /** The note the MySQL connector mints on a first run and looks for on every later one. */
    private static final String SERVER_NAME = "SERVER_NAME";

    /** Where a shared capture's connector notes are kept, before the chain the capture reads. */
    private static final String CHAIN_NAMESPACE_PREFIX = "pdk.chain.";

    @BeforeAll
    static void requireConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aSecondPipelineAndARestartKeepTheChainsPositionAndItsIdentity(Tiers tier) throws Exception {
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
        byte[] minted;

        try (MongoEndpoints mongo = new MongoEndpoints()) {
            try (ServerHandle server = tier.launch(store); StoreDocuments documents = StoreDocuments.at(store)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                apply(control, mysql, targetUri, first, "first_source", "orders", "snapshot_and_cdc");
                awaitIds(control, first, mongo, target, "orders", List.of(1L));
                sql(mysql, "UPDATE orders SET name='first-tail' WHERE id=1");
                // The position face reports the durable source-read offset, not the snapshot seam.
                // It is absent until CDC has delivered and acknowledged a real change.
                Await.until("the first pipeline's tail position", () -> !control.resumePoint(first).isEmpty(),
                        () -> control.logs(first));
                Await.until("the connector to have filed the identity it minted",
                        () -> identity(store, documents) != null, () -> "chains=" + documents.miningChainIds());
                minted = identity(store, documents);

                // The second pipeline reads the same database the same way, so it joins the chain the first
                // started, and the capture widens to its table and restarts from that chain's position. A new
                // snapshot would hide that path by replacing the chain's position with its own seam.
                apply(control, mysql, targetUri, second, "second_source", "later_orders", "cdc_only");
                sql(mysql, "INSERT INTO later_orders VALUES (2, 'second-tail')");
                awaitIds(control, second, mongo, target, "later_orders", List.of(2L));
                assertThat(documents.miningChainIds()).as("both pipelines read through one chain").hasSize(1);
                assertThat(identity(store, documents))
                        .as("the capture that widened to the second pipeline's table reads under the identity the "
                                + "first run minted, rather than one minted for a reader it never starts")
                        .isEqualTo(minted);
                assertThat(control.state(first)).contains(PipelineState.RUNNING);
            }

            sql(mysql, "DELETE FROM orders WHERE id=1", "DELETE FROM later_orders WHERE id=2",
                    "INSERT INTO later_orders VALUES (3, 'during-downtime')");
            try (ServerHandle server = tier.launch(store); StoreDocuments documents = StoreDocuments.at(store)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.login("e2e", "e2e-password");
                sql(mysql, "INSERT INTO orders VALUES (99, 'after-restart')",
                        "INSERT INTO later_orders VALUES (99, 'after-restart')");
                // The old target rows must disappear, and genuinely new rows must arrive on both
                // paths. RUNNING alone could have been an observation left by the previous process.
                awaitIds(control, first, mongo, target, "orders", List.of(99L));
                awaitIds(control, second, mongo, target, "later_orders", List.of(3L, 99L));
                assertThat(identity(store, documents))
                        .as("the run that came back reads under the identity the first run minted")
                        .isEqualTo(minted);
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

    /**
     * The identity the chain's connector filed under the one chain this tier's store holds, or null while it has
     * filed none.
     */
    private static byte[] identity(String store, StoreDocuments documents) {
        assertThat(documents.miningChainIds()).as("one chain in this tier's store").hasSize(1);
        String namespace = CHAIN_NAMESPACE_PREFIX + documents.miningChainIds().iterator().next();
        try (MongoClient client = MongoClients.create(store)) {
            Document id = new Document("ns", namespace).append("k", SERVER_NAME);
            Document record = client.getDatabase("tapstate_nest").getCollection("operator_state")
                    .find(new Document("_id", id)).first();
            return record == null ? null : record.get("state", Binary.class).getData();
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
