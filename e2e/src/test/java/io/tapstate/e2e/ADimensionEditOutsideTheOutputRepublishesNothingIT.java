package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A dimension row is edited in a column the join never publishes, and nothing is rebuilt.
 *
 * <p><b>Why this cannot be a published example.</b> Rebuilding a bucket writes the same values back, so
 * the target table is byte for byte identical whether the filter engaged or not - the count does not
 * move, no document changes, and every reading the declarative vocabulary can take is satisfied by both
 * answers. The two are separated here by writing a value into the target that the source does not hold
 * and reading it again afterwards: a rebuild overwrites it, and nothing else in the run touches it. That
 * is a write into the product's output, which the specification language deliberately has no word for.
 *
 * <p><b>Why a unit case is not enough either.</b> The filter compares the before image against the after
 * image, and an absent before image reads as a difference - so a change-capture connector that reports
 * only the columns it must would leave the filter permanently disengaged while every test that hands the
 * driver a hand-built envelope still passes. What is under test here is that the before image survives a
 * real MySQL binlog, the codec and the source stage, which is only observable end to end.
 *
 * <p>What each assertion has to discriminate:
 *
 * <ul>
 *   <li><b>The sentinels are read back before anything is changed.</b> A plant that silently did not
 *       land would leave every reading below satisfied by the values the pipeline wrote itself.</li>
 *   <li><b>Bo's order is republished by a change of its own, wiping its sentinel.</b> Without this the
 *       run cannot tell "the filter held" from "sentinels are never overwritten by anything", and it is
 *       also the fence: it is a change made after the one under test, so a pipeline that had simply
 *       stopped consuming cannot reach it.</li>
 *   <li><b>Ada's orders are republished at the end by an edit inside the projection.</b> Without this
 *       the silence in the middle is equally well explained by ada's bucket being dead - never indexed,
 *       never watched - and that would pass with no filter in the product at all.</li>
 * </ul>
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=ADimensionEditOutsideTheOutputRepublishesNothingIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class ADimensionEditOutsideTheOutputRepublishesNothingIT {

    private static final String DATABASE = "admission_db";
    private static final String PIPELINE_ID = "order_pipeline";
    private static final String TARGET = "order_state";

    /** A value the source never holds, so reading it back can only mean nothing was written over it. */
    private static final String SENTINEL = "planted-not-republished";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void anEditToAColumnTheJoinDoesNotPublishLeavesTheTargetUntouched() throws Exception {
        Map<String, Object> mysql = SharedMySql.settings(DATABASE);
        seed(mysql);

        String storeUri = SharedMongo.replicaSetUrl("admission_store");
        String targetUri = SharedMongo.replicaSetUrl("admission_target");
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = Tiers.IN_PROCESS.launch(storeUri)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

            Map<String, String> resources = new LinkedHashMap<>();
            resources.put("shop_db.tap.yml", sourceYaml(mysql));
            resources.put("views.tap.yml", targetYaml(targetUri));
            resources.put("join_pipeline.tap.yml", pipelineYaml());
            control.apply(resources);
            control.discoverSchema("shop_db", "mysql", mysql);
            control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

            awaitName(mongo, target, 10, "ada", "the snapshot to reach the target");
            awaitName(mongo, target, 11, "ada", "ada's second order to reach the target");
            awaitName(mongo, target, 12, "bo", "bo's order to reach the target");

            // Written into the product's output, over the column the join publishes. A rebuild of the
            // bucket restores the name from the source; nothing else in this run writes these rows.
            plant(mongo, target, 10);
            plant(mongo, target, 11);
            plant(mongo, target, 12);
            assertThat(nameOf(mongo, target, 10)).as("the plant on ada's first order").isEqualTo(SENTINEL);
            assertThat(nameOf(mongo, target, 11)).as("the plant on ada's second order").isEqualTo(SENTINEL);
            assertThat(nameOf(mongo, target, 12)).as("the plant on bo's order").isEqualTo(SENTINEL);

            // The change under test. address is not in the select list, so no byte of any published row
            // can differ because of it.
            execute(mysql, "UPDATE customers SET address = 'a-new-address' WHERE id = 1");

            // Made after it, on a different customer, and inside the projection. Reaching this reading
            // proves the pipeline consumed past the edit above, and proves a sentinel is overwritable.
            execute(mysql, "UPDATE customers SET name = 'bosley' WHERE id = 2");
            awaitName(mongo, target, 12, "bosley", "bo's order to be republished under its new name");

            assertThat(nameOf(mongo, target, 10))
                    .as("ada's first order after an edit the join never publishes")
                    .isEqualTo(SENTINEL);
            assertThat(nameOf(mongo, target, 11))
                    .as("ada's second order after an edit the join never publishes")
                    .isEqualTo(SENTINEL);

            // And ada's bucket is alive: an edit inside the projection rebuilds both of her rows. Without
            // this the two readings above would also hold for a bucket that was never indexed at all.
            execute(mysql, "UPDATE customers SET name = 'adelaide' WHERE id = 1");
            awaitName(mongo, target, 10, "adelaide", "ada's first order under her new name");
            awaitName(mongo, target, 11, "adelaide", "ada's second order under her new name");
        }
    }

    private static void seed(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS orders");
            statement.execute("DROP TABLE IF EXISTS customers");
            statement.execute(
                    "CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(64), address VARCHAR(64))");
            statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, qty INT)");
            statement.execute(
                    "INSERT INTO customers (id, name, address) VALUES (1, 'ada', 'an-old-address'),"
                            + " (2, 'bo', 'another-address')");
            statement.execute(
                    "INSERT INTO orders (id, customer_id, qty) VALUES (10, 1, 2), (11, 1, 7), (12, 2, 5)");
        }
    }

    private static void execute(Map<String, Object> settings, String sql) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    /** Throws unless it located exactly one document, so a plant that missed cannot pass unnoticed. */
    private static void plant(MongoEndpoints mongo, EndpointAddress target, int orderId) {
        mongo.update(target, TARGET, Map.of("order_id", orderId), Map.of("customer_name", SENTINEL));
    }

    private static void awaitName(
            MongoEndpoints mongo, EndpointAddress target, int orderId, String expected, String what) {
        Await.until(what, () -> expected.equals(nameOf(mongo, target, orderId)),
                () -> String.valueOf(nameOf(mongo, target, orderId)));
    }

    /** One target row's published customer name, or null before the row is there. Read from Mongo. */
    private static String nameOf(MongoEndpoints mongo, EndpointAddress target, int orderId) {
        return mongo.fetch(target, TARGET, Map.of("order_id", orderId))
                .map(document -> document.get("customer_name"))
                .map(String::valueOf)
                .orElse(null);
    }

    private static String sourceYaml(Map<String, Object> config) {
        return """
                version: tapstate/v1
                kind: source
                id: shop_db
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ orders, customers ]
                """
                .formatted(config.get("host"), config.get("port"), config.get("database"),
                        config.get("username"), config.get("password"));
    }

    private static String targetYaml(String targetUri) {
        return """
                version: tapstate/v1
                kind: source
                id: views
                connector: mongodb
                config: { uri: "%s" }
                """
                .formatted(targetUri);
    }

    /**
     * address is read by neither the select list nor the join condition, which is the whole point: it is
     * a column of the dimension row that no published row can depend on.
     */
    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: order_pipeline
                source: shop_db
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - id: widen
                    type: join
                    from: { o: orders, c: customers }
                    engine: builtin
                    sql: |
                      SELECT o.id AS order_id, o.qty AS qty, c.name AS customer_name
                      FROM o JOIN c ON o.customer_id = c.id
                view:
                  id: order_state
                  from: widen
                  primary_key: order_id
                """;
    }
}
