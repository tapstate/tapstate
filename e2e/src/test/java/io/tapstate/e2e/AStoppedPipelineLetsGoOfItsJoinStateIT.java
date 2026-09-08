package io.tapstate.e2e;

import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.testsupport.DockerGate;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A stopped pipeline lets go of the state its join kept, and comes back working afterwards.
 *
 * <p>The state a join keeps is not a cache: the mirrors hold each dimension row as it last was, and the
 * reverse index holds which driving rows referenced which key. Left behind by a takedown they are
 * inherited by whatever is applied under the same id next, and that run then widens fresh driving rows
 * with dimension values the source no longer holds. Nothing reports it - the pipeline runs, the row count
 * is right, every row is present, and each column reads as plausible - so the only thing that finds it is
 * comparing the target against the source by hand.
 *
 * <p><b>Why the store is read directly rather than the target.</b> The symptom needs a rebuilt run that
 * starts from a fresh snapshot, which means the pipeline being removed and a new one applied under the
 * same id; the specification vocabulary has {@code stop} and {@code start} but no word for removing a
 * pipeline and applying another over it, so the sequence is out of reach declaratively. What is under
 * test is anyway one step earlier and is exact: whether the takedown named those namespaces at all. The
 * store has no way to list what it holds, deliberately, so a namespace a stop does not name is state
 * nothing will ever name again.
 *
 * <p>What each assertion has to discriminate:
 *
 * <ul>
 *   <li><b>The namespaces are read while the pipeline is still up.</b> Without this, every reading after
 *       the stop is one that also holds against a run that never wrote any state - and the fact mirror
 *       alone would pass while the dimension side, which is what carries the stale values, was missed.
 *   <li><b>A neighbouring pipeline's namespace is seeded and read again afterwards.</b> A takedown that
 *       swept everything under the {@code join.} prefix would satisfy every other reading here and would
 *       take a working pipeline's state with it.
 *   <li><b>The pipeline is started again and made to rebuild.</b> Letting go of the mirrors is only right
 *       because a run re-reads its sources as it starts; if it did not, this change would trade a stale
 *       target for a silently empty index, where a dimension change finds none of the rows that reference
 *       it. The dimension edit at the end is what tells those two apart, and it is made on a driving row
 *       written before the stop, so nothing but a rebuilt index can carry it.
 * </ul>
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=AStoppedPipelineLetsGoOfItsJoinStateIT \
 *     -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class AStoppedPipelineLetsGoOfItsJoinStateIT {

    private static final String DATABASE = "join_teardown_db";
    private static final String PIPELINE_ID = "order_pipeline";
    private static final String STEP_ID = "widen";
    private static final String TARGET = "order_state";

    /** The three namespaces one join step keeps, under the alias its {@code from:} declares. */
    private static final String FACT_NAMESPACE = "join." + PIPELINE_ID + "." + STEP_ID + ".fact";
    private static final String DIMENSION_NAMESPACE = "join." + PIPELINE_ID + "." + STEP_ID + ".dim.c";
    private static final String INDEX_NAMESPACE = "join." + PIPELINE_ID + "." + STEP_ID + ".index.c";

    /** A namespace shaped like one of ours but belonging to a pipeline nobody stopped. */
    private static final String OTHER_PIPELINE_NAMESPACE = "join.other_pipeline." + STEP_ID + ".dim.c";

    /** Bytes nobody reads: what is asked of the neighbour's entry is presence, not content. */
    private static final byte[] HELD = "a-neighbour-mid-run".getBytes(StandardCharsets.UTF_8);
    private static final String KEY = "held-1";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @Test
    void stoppingLetsGoOfEveryNamespaceTheJoinKeptAndTheNextRunStillRebuilds() throws Exception {
        Map<String, Object> mysql = SharedMySql.settings(DATABASE);
        seed(mysql);

        String storeUri = SharedMongo.replicaSetUrl("join_teardown_store");
        String targetUri = SharedMongo.replicaSetUrl("join_teardown_target");
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = Tiers.IN_PROCESS.launch(storeUri);
                MongoConnection connection = new MongoConnection(
                        new MongoConnectionSettings(storeUri, null, Duration.ofSeconds(5)))) {
            connection.verify();
            KeyedStateStore state = new MongoStorePort(connection).keyedState();

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
            awaitName(mongo, target, 11, "bo", "the second order to reach the target");

            // Belongs to nobody this stop is about. Seeded while the run is up, so what is read back
            // afterwards is a difference the stop did not make rather than an entry written after it.
            state.save(OTHER_PIPELINE_NAMESPACE, KEY, HELD);

            // Read before the stop: "gone afterwards" means nothing at all unless something was there.
            // Each namespace separately, because the dimension side is what carries a stale value and a
            // reading over the fact mirror alone would pass with the other two untouched.
            awaitHeld(state, FACT_NAMESPACE, "the fact mirror to hold the driving rows");
            awaitHeld(state, DIMENSION_NAMESPACE, "the dimension mirror to hold the customers");
            awaitHeld(state, INDEX_NAMESPACE, "the reverse index to hold a bucket per customer");

            control.lifecycle(PIPELINE_ID, LifecycleVerb.STOP);
            Await.until(
                    PIPELINE_ID + " to reach " + PipelineState.STOPPED,
                    () -> control.state(PIPELINE_ID).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> String.valueOf(control.state(PIPELINE_ID)));

            assertThat(state.count(FACT_NAMESPACE))
                    .as("the fact mirror after the stop")
                    .isZero();
            assertThat(state.count(DIMENSION_NAMESPACE))
                    .as("the dimension mirror after the stop - the one that serves a rebuilt run values "
                            + "its source no longer holds")
                    .isZero();
            assertThat(state.count(INDEX_NAMESPACE))
                    .as("the reverse index after the stop")
                    .isZero();
            assertThat(state.load(OTHER_PIPELINE_NAMESPACE, KEY))
                    .as("a stop lets go of what this pipeline named, never of what merely looks like it")
                    .isPresent();

            // Everything above is still satisfied by a takedown that broke the join outright. What
            // separates that from letting go of state a run rebuilds for itself is the run after it.
            control.lifecycle(PIPELINE_ID, LifecycleVerb.START);
            awaitName(mongo, target, 10, "ada", "the restarted run to re-read the driving rows");

            // On a row written before the stop, so only a reverse index built again from the source can
            // find it. A run whose index stayed empty leaves this row on its old name, silently.
            execute(mysql, "UPDATE customers SET name = 'adelaide' WHERE id = 1");
            awaitName(mongo, target, 10, "adelaide",
                    "a dimension change to reach a row published before the stop");
        }
    }

    private static void seed(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS orders");
            statement.execute("DROP TABLE IF EXISTS customers");
            statement.execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(64))");
            statement.execute("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, qty INT)");
            statement.execute("INSERT INTO customers (id, name) VALUES (1, 'ada'), (2, 'bo')");
            statement.execute("INSERT INTO orders (id, customer_id, qty) VALUES (10, 1, 2), (11, 2, 5)");
        }
    }

    private static void execute(Map<String, Object> settings, String sql) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    /**
     * Waits for a namespace to hold something. Polled rather than read once: the target row reaching Mongo
     * says the join emitted, and the write through to the layer behind the map is a separate journey.
     */
    private static void awaitHeld(KeyedStateStore state, String namespace, String what) {
        Await.until(what, () -> state.count(namespace) > 0, () -> namespace + " holds 0 entries");
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

    /** One join step, one dimension, aliased c - which is the alias the namespaces above are named for. */
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
