package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A pointed-at row's first filing publishes each completed document once.
 *
 * <p>The two source tables travel on independent chains. An order registers the customer it names at
 * the lookup while its other copy travels toward the assembler; the customer is then filed before its
 * wake travels toward that same assembler. When the root crosses between the filing and the wake, it
 * reads the filed customer and publishes the complete document. The later wake must settle the
 * customer's position without publishing the identical document again.
 *
 * <p><b>Why the target contents do not prove this.</b> Mongo upserts both copies under the order's key,
 * so one write and two identical writes leave exactly the same collection. The target is still read to
 * prove every document completed. The discriminating reading is taken at the unchanged real connector's
 * write callback: the observer records every physical record before delegating, and the run is stopped
 * before its ledger is read.
 *
 * <p>Enough independent pairs are loaded to put both paths in flight together. One pair would leave the
 * relative scheduling to chance; a load wider than the connector's batch keeps registrations, filings,
 * roots and wakes crossing while every key remains independent and has exactly one legitimate output.
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=ANestFirstFilingPublishesEachDocumentOnceIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class ANestFirstFilingPublishesEachDocumentOnceIT {

    private static final int ROWS = 2_000;
    private static final String ORDERS = "orders";
    private static final String CUSTOMERS = "customers";
    private static final String PIPELINE_ID = "first_filing_once";

    @TempDir
    Path directory;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aFiledCustomerAlreadyReadByItsOrderDoesNotPublishThatOrderAgain(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> mysql = SharedMySql.settings("nest_first_filing_" + suffix);
        seed(mysql);

        String pipelineId = PIPELINE_ID + "_" + suffix;
        String storeUri = SharedMongo.replicaSetUrl("nest_first_filing_store_" + suffix);
        String targetUri = SharedMongo.replicaSetUrl("nest_first_filing_target_" + suffix);
        Path witness = directory.resolve("writes-" + suffix + ".tsv");

        try (ServerHandle server = tier.launch(storeUri);
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb",
                    ObservedMongoConnectorJar.build(ConnectorJars.bytesFor("mongodb"), witness));

            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("src_orders.tap.yml", sourceYaml("src_orders", ORDERS, mysql));
            resources.put("src_customers.tap.yml", sourceYaml("src_customers", CUSTOMERS, mysql));
            resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
            resources.put("pipeline.tap.yml", pipelineYaml(pipelineId));
            control.apply(resources);
            control.discoverSchema("src_orders", "mysql", mysql);
            control.discoverSchema("src_customers", "mysql", mysql);
            control.lifecycle(pipelineId, LifecycleVerb.START);

            EndpointAddress target = EndpointAddress.uri(targetUri);
            Await.until("every completed order to reach the target",
                    () -> mongo.count(target, ORDERS) == ROWS,
                    () -> mongo.count(target, ORDERS) + " orders");
            assertCompleted(mongo, target, 1);
            assertCompleted(mongo, target, ROWS);

            control.stop(pipelineId, true);
            Await.until("the nest run to stop before its write ledger is read",
                    () -> control.state(pipelineId).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> String.valueOf(control.state(pipelineId)));

            assertThat(recordsWritten(witness))
                    .as("physical Mongo records written for %d independently completed orders", ROWS)
                    .isEqualTo(ROWS);
        }
    }

    private static void assertCompleted(MongoEndpoints mongo, EndpointAddress target, int id) {
        Map<String, Object> order = mongo.fetch(target, ORDERS, Map.of("id", id)).orElseThrow();
        assertThat(order.get("customer"))
                .as("the customer embedded in order %d", id)
                .isInstanceOfSatisfying(Document.class, customer -> assertThat(customer)
                        .containsEntry("id", (long) id)
                        .containsEntry("name", "customer-" + id));
    }

    private static long recordsWritten(Path witness) throws Exception {
        assertThat(witness).as("the real Mongo connector's write ledger").isRegularFile();
        long records = 0;
        for (String call : Files.readAllLines(witness)) {
            String[] fields = call.split("\\t", -1);
            assertThat(fields).as("one observed Mongo write callback").hasSize(3);
            assertThat(fields[0]).as("the table handed to the Mongo connector").isEqualTo(ORDERS);
            records += Long.parseLong(fields[1]);
        }
        return records;
    }

    private static void seed(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedMySql.connect(settings)) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS " + ORDERS);
                statement.execute("DROP TABLE IF EXISTS " + CUSTOMERS);
                statement.execute("CREATE TABLE " + ORDERS
                        + " (id INT PRIMARY KEY, customer_id INT NOT NULL)");
                statement.execute("CREATE TABLE " + CUSTOMERS
                        + " (id INT PRIMARY KEY, name VARCHAR(64) NOT NULL)");
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + ORDERS + " (id, customer_id) VALUES (?, ?)")) {
                for (int id = 1; id <= ROWS; id++) {
                    insert.setInt(1, id);
                    insert.setInt(2, id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO " + CUSTOMERS + " (id, name) VALUES (?, ?)")) {
                for (int id = 1; id <= ROWS; id++) {
                    insert.setInt(1, id);
                    insert.setString(2, "customer-" + id);
                    insert.addBatch();
                }
                insert.executeBatch();
            }
        }
    }

    private static String sourceYaml(
            String sourceId, String table, Map<String, Object> settings) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(sourceId, settings.get("host"), settings.get("port"),
                        settings.get("database"), settings.get("username"), settings.get("password"),
                        table);
    }

    private static String targetYaml(String targetUri) {
        return """
                version: tapstate/v1
                kind: source
                id: tgt_mongo
                connector: mongodb
                config: { uri: "%s" }
                """
                .formatted(targetUri);
    }

    private static String pipelineYaml(String pipelineId) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: [ src_orders, src_customers ]
                settings: { read_mode: snapshot_only }
                transforms:
                  - id: order_doc
                    type: nest
                    from: { o: orders, c: customers }
                    root:
                      from: o
                      key: [ id ]
                      embed:
                        - { from: c, on: { id: customer_id }, as: object, path: customer }
                serve:
                  from: order_doc
                  sync:
                    - source: tgt_mongo
                """
                .formatted(pipelineId);
    }
}
