package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A PostgreSQL source captured through the shared change log is told it may release a change once the change
 * is written into that log, not once every pipeline has landed it: a paused pipeline does not hold the
 * replication slot, and catches up from the log when it resumes.
 *
 * <p>Two pipelines read one source -- one of them orders, the other customers -- and the one reading orders is
 * paused. An orders change and then a customers change are made, and the customers change lands. The slot has
 * to move past the orders change all the same, while that change is still missing from the paused pipeline's
 * target: the change is in the log, which is where the paused pipeline will read it from. Resumed, the
 * pipeline lands it, once.
 *
 * <p>"Past a change" is read off the server's own log positions: at or beyond where the server was about to
 * write inside the change's transaction, before its commit, which holds whichever end of a commit record the
 * connector confirms. The case first shows the slot moving past a change both pipelines landed, so the reading
 * that matters cannot be a slot that moves on its own clock.
 */
class APausedPipelineDoesNotHoldTheSlotIT {

    private static final Duration BOUND = Duration.ofSeconds(180);
    private static final Duration SLOT_FOLLOWS = Duration.ofSeconds(30);

    private static final String SOURCE_ID = "src_pg";
    private static final String TARGET_ID = "tgt_mongo";
    private static final String ORDERS = "orders";
    private static final String CUSTOMERS = "customers";
    private static final String ORDERS_PIPELINE = "paused_orders";
    private static final String CUSTOMERS_PIPELINE = "running_customers";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("postgres", "mongodb");
    }

    @Test
    void aChangeAPausedPipelineHasNotLandedDoesNotHoldTheSlotAndArrivesOnResume() throws Exception {
        Map<String, Object> source = SharedPostgres.settings("paused_pipeline_slot");
        createTables(source);
        String storeUri = SharedMongo.replicaSetUrl("paused_pipeline_store");
        String targetUri = SharedMongo.replicaSetUrl("paused_pipeline_target");

        try (ServerHandle server = Tiers.IN_PROCESS.launch(storeUri); MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put(SOURCE_ID + ".tap.yml", sourceYaml(source));
            resources.put(TARGET_ID + ".tap.yml", targetYaml(targetUri));
            resources.put(ORDERS_PIPELINE + ".tap.yml",
                    Workspaces.pipelineYaml(ORDERS_PIPELINE, SOURCE_ID, TARGET_ID, ORDERS));
            resources.put(CUSTOMERS_PIPELINE + ".tap.yml",
                    Workspaces.pipelineYaml(CUSTOMERS_PIPELINE, SOURCE_ID, TARGET_ID, CUSTOMERS));
            control.apply(resources);
            control.discoverSchema(SOURCE_ID, "postgres", discoveryConfig(source));
            Target target = new Target(mongo, targetUri);

            start(control, ORDERS_PIPELINE);
            start(control, CUSTOMERS_PIPELINE);
            Await.until("a slot held by the capture's reader", BOUND,
                    () -> PostgresSlots.active(source), () -> String.valueOf(PostgresSlots.of(source)));

            // Both land a change, and the slot follows.
            write(source, ORDERS, 2);
            String bothLanded = write(source, CUSTOMERS, 2);
            target.await(ORDERS, 2);
            target.await(CUSTOMERS, 2);
            Await.until("the slot to move past a change both pipelines landed", SLOT_FOLLOWS,
                    () -> PostgresSlots.confirmedAtOrPast(source, bothLanded),
                    () -> String.valueOf(PostgresSlots.of(source)));

            control.lifecycle(ORDERS_PIPELINE, LifecycleVerb.PAUSE);
            awaitState(control, ORDERS_PIPELINE, PipelineState.PAUSED);

            String order = write(source, ORDERS, 3);
            write(source, CUSTOMERS, 3);
            target.await(CUSTOMERS, 3);
            Await.until("the slot to move past the orders change the paused pipeline has not landed", SLOT_FOLLOWS,
                    () -> PostgresSlots.confirmedAtOrPast(source, order),
                    () -> String.valueOf(PostgresSlots.of(source)));
            assertThat(target.ids(ORDERS))
                    .as("the orders change is still not at the paused pipeline's target")
                    .doesNotContain("3");

            control.lifecycle(ORDERS_PIPELINE, LifecycleVerb.RESUME);
            target.await(ORDERS, 3);
            assertThat(target.ids(ORDERS)).as("replayed from the log, once").containsExactlyInAnyOrder("1", "2", "3");
            assertThat(PostgresSlots.names(source)).as("one slot for the capture throughout").hasSize(1);
        }
    }

    /**
     * Inserts row {@code id} into {@code table} in a transaction of its own, and answers where the server was
     * about to write inside that transaction, before its commit.
     */
    private static String write(Map<String, Object> source, String table, int id) throws Exception {
        try (Connection connection = SharedPostgres.connect(source)) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("INSERT INTO " + table + " (id, name) VALUES (" + id + ", 'r" + id + "')");
            }
            String inside = PostgresSlots.insertPosition(connection);
            connection.commit();
            return inside;
        }
    }

    private static void start(ControlPlane control, String pipeline) {
        control.lifecycle(pipeline, LifecycleVerb.START);
        awaitState(control, pipeline, PipelineState.RUNNING);
    }

    private static void awaitState(ControlPlane control, String pipeline, PipelineState state) {
        Await.until(pipeline + " to reach " + state, BOUND,
                () -> control.state(pipeline).filter(state::equals).isPresent(),
                () -> control.state(pipeline) + ", logs: " + control.logs(pipeline));
    }

    private static void createTables(Map<String, Object> source) throws Exception {
        try (Connection connection = SharedPostgres.connect(source);
                Statement statement = connection.createStatement()) {
            for (String table : List.of(ORDERS, CUSTOMERS)) {
                statement.execute("DROP TABLE IF EXISTS " + table);
                statement.execute("CREATE TABLE " + table + " (id INT PRIMARY KEY, name VARCHAR(64))");
                statement.execute("ALTER TABLE " + table + " REPLICA IDENTITY FULL");
                statement.execute("INSERT INTO " + table + " (id, name) VALUES (1, 'seed')");
            }
        }
    }

    /** The target collections, read by primary key. */
    private record Target(MongoEndpoints mongo, String uri) {

        void await(String table, int id) {
            Await.until("row " + id + " of " + table + " at the target", BOUND,
                    () -> ids(table).contains(String.valueOf(id)), () -> String.valueOf(ids(table)));
        }

        List<String> ids(String table) {
            return mongo.documents(EndpointAddress.uri(uri), table).stream()
                    .map(document -> String.valueOf(document.get("id")))
                    .toList();
        }
    }

    private static Map<String, Object> discoveryConfig(Map<String, Object> settings) {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("host", settings.get("host"));
        config.put("port", settings.get("port"));
        config.put("database", settings.get("database"));
        config.put("schema", "public");
        config.put("user", settings.get("username"));
        config.put("password", settings.get("password"));
        return config;
    }

    private static String sourceYaml(Map<String, Object> settings) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: postgres
                config: { host: %s, port: %s, database: %s, schema: public, user: %s, password: %s }
                mode: cdc
                tables: [ %s, %s ]
                """
                .formatted(SOURCE_ID, settings.get("host"), settings.get("port"), settings.get("database"),
                        settings.get("username"), settings.get("password"), ORDERS, CUSTOMERS);
    }

    private static String targetYaml(String targetUri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """
                .formatted(TARGET_ID, targetUri);
    }
}
