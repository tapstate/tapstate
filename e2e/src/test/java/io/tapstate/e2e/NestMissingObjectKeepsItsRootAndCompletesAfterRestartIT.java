package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.testsupport.DockerGate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/** The public JSON pipeline keeps all roots, then fills eight absent objects after a keep-state restart. */
class NestMissingObjectKeepsItsRootAndCompletesAfterRestartIT {

    private static final String ORDERS = "ACC_ORDERS";
    private static final String LINES = "ACC_LINES";
    private static final String CUSTOMERS = "ACC_CUSTOMERS";
    private static final String TARGET = "nest_mysql_" + ORDERS;

    @BeforeAll
    static void prerequisites() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void missingCustomersDoNotRemoveOrdersAndLateCustomersCompletePersistedRoots(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> mysql = SharedMySql.settings("nest_missing_src_" + suffix);
        seed(mysql);
        String store = SharedMongo.replicaSetUrl("nest_missing_store_" + suffix);
        String targetUri = SharedMongo.replicaSetUrl("nest_missing_target_" + suffix);
        String stateDatabase = "nest_missing_state_" + suffix;
        String pipeline = "acc_nest_mysql_" + suffix;
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (MongoEndpoints mongo = new MongoEndpoints()) {
            try (ServerHandle server = tier.launch(store, stateDatabase)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("source.tap.yml", sourceYaml(mysql));
                resources.put("target.tap.yml", """
                        version: tapstate/v1
                        kind: source
                        id: atlas-store
                        connector: mongodb
                        config: { uri: "%s" }
                        """.formatted(targetUri));
                control.apply(resources);
                control.discoverSchema("acc-mysql", "mysql", mysql);
                control.sourceRequest("POST", "/api/pipelines", pipeline(pipeline), null, 201);
                control.lifecycle(pipeline, LifecycleVerb.START);

                awaitRows(mongo, target, false);
                assertRows(mongo, target, false);
                Await.until("all three source snapshots durably landed before the keep-state restart", () -> {
                    var snapshots = control.snapshotTables(pipeline);
                    return snapshots.size() == 3 && snapshots.values().stream().allMatch(TableSnapshot::landed);
                }, () -> String.valueOf(control.snapshotTables(pipeline)));
                assertThat(control.state(pipeline)).contains(PipelineState.RUNNING);
            }

            // Restart the running deployment, as the existing restart witnesses do. A pause/resume
            // addresses a suspended Jet job in the same cluster, not a job in a cluster that has ended.
            try (ServerHandle server = tier.launch(store, stateDatabase)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.login("e2e", "e2e-password");
                Await.until("the restarted server to adopt the existing continuous nest", () -> control.state(pipeline).filter(
                        PipelineState.RUNNING::equals).isPresent(), () -> String.valueOf(control.state(pipeline)));
                try (Connection connection = SharedMySql.connect(mysql);
                        PreparedStatement insert = connection.prepareStatement(
                                "INSERT INTO " + CUSTOMERS + " VALUES (?, ?)")) {
                    for (int key = 25; key <= 200; key += 25) {
                        insert.setInt(1, key);
                        insert.setString(2, "customer-" + key);
                        insert.addBatch();
                    }
                    insert.executeBatch();
                }
                awaitRows(mongo, target, true);
                assertRows(mongo, target, true);
                control.lifecycle(pipeline, LifecycleVerb.PAUSE);
            }
        }
    }

    private static Map<String, Object> pipeline(String id) {
        return Map.of("id", id, "sources", List.of("acc-mysql"),
                "transforms", List.of(Map.of("id", "order_doc", "type", "nest",
                        "from", Map.of("o", ORDERS, "l", LINES, "c", CUSTOMERS),
                        "root", Map.of("from", "o", "key", List.of("ORDER_ID"), "embed", List.of(
                                Map.of("from", "l", "on", Map.of("ORDER_ID", "ORDER_ID"),
                                        "as", "array", "path", "lines", "arrayKey", List.of("LINE_NO")),
                                Map.of("from", "c", "on", Map.of("CUSTOMER_ID", "CUSTOMER_ID"),
                                        "as", "object", "path", "customer"))))),
                "serve", Map.of("from", "order_doc", "sync", List.of(Map.of("source", "atlas-store",
                        "rename", Map.of("prefix", "nest_mysql_"), "execution", Map.of("parallelism", 1)))));
    }

    private static String sourceYaml(Map<String, Object> mysql) {
        return """
                version: tapstate/v1
                kind: source
                id: acc-mysql
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ ACC_ORDERS, ACC_LINES, ACC_CUSTOMERS ]
                """.formatted(mysql.get("host"), mysql.get("port"), mysql.get("database"),
                        mysql.get("username"), mysql.get("password"));
    }

    private static void awaitRows(MongoEndpoints mongo, EndpointAddress target, boolean completed) {
        AtomicReference<String> last = new AtomicReference<>("not observed");
        Await.until("all 200 complete root/line values and the expected customer objects", () -> {
            try {
                assertRows(mongo, target, completed);
                return true;
            } catch (AssertionError pending) {
                last.set(pending.getMessage());
                return false;
            }
        }, last::get);
    }

    private static void assertRows(MongoEndpoints mongo, EndpointAddress target, boolean completed) {
        assertThat(mongo.count(target, TARGET)).isEqualTo(200);
        for (int key = 1; key <= 200; key++) {
            Map<String, Object> order = mongo.fetch(target, TARGET, Map.of("ORDER_ID", key)).orElseThrow();
            assertThat(order).containsEntry("ORDER_ID", (long) key).containsEntry("CUSTOMER_ID", (long) key);
            if (!completed && key % 25 == 0) {
                assertThat(order).doesNotContainKey("customer");
            } else {
                assertThat(order.get("customer")).isEqualTo(new Document("CUSTOMER_ID", (long) key)
                        .append("NAME", "customer-" + key));
            }
            assertThat(order.get("lines")).isEqualTo(key % 10 == 0 ? List.of()
                    : List.of(new Document("ORDER_ID", (long) key).append("LINE_NO", 1L).append("SKU", "line-" + key)));
        }
    }

    private static void seed(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedMySql.connect(settings); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE " + ORDERS + " (ORDER_ID INT PRIMARY KEY, CUSTOMER_ID INT NOT NULL)");
            statement.execute("CREATE TABLE " + LINES
                    + " (ORDER_ID INT NOT NULL, LINE_NO INT NOT NULL, SKU VARCHAR(64), PRIMARY KEY (ORDER_ID, LINE_NO))");
            statement.execute("CREATE TABLE " + CUSTOMERS + " (CUSTOMER_ID INT PRIMARY KEY, NAME VARCHAR(64))");
            try (PreparedStatement orders = connection.prepareStatement("INSERT INTO " + ORDERS + " VALUES (?, ?)");
                    PreparedStatement lines = connection.prepareStatement("INSERT INTO " + LINES + " VALUES (?, ?, ?)");
                    PreparedStatement customers = connection.prepareStatement("INSERT INTO " + CUSTOMERS + " VALUES (?, ?)")) {
                for (int key = 1; key <= 200; key++) {
                    orders.setInt(1, key);orders.setInt(2, key);orders.addBatch();
                    if (key % 10 != 0) {
                        lines.setInt(1, key);lines.setInt(2, 1);lines.setString(3, "line-" + key);lines.addBatch();
                    }
                    if (key % 25 != 0) {
                        customers.setInt(1, key);customers.setString(2, "customer-" + key);customers.addBatch();
                    }
                }
                orders.executeBatch();lines.executeBatch();customers.executeBatch();
            }
        }
    }
}
