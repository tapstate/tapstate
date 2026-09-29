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
 * A PostgreSQL source is told it may release its log only past what every pipeline reading it has landed:
 * a change one pipeline has not landed holds the replication slot before it, however far another pipeline
 * reading another table of the same source has got.
 *
 * <p>Two pipelines read one source through one chain -- one of them orders, the other customers -- and the
 * one reading orders is paused. An orders change and then a customers change are made; the customers change
 * lands. Telling the source it may release past the customers change would release the orders change as
 * well, which nobody has landed: a restart would never be sent it again. Held before it, the slot moves on
 * once the paused pipeline resumes and lands its change.
 *
 * <p>"Past a change" and "before a change" are read off the server's own log positions: past is at or beyond
 * where the server was about to write inside the change's transaction, before its commit; before is at or
 * short of where it was about to write before the transaction began. Both hold whichever end of a commit
 * record the connector confirms.
 *
 * <p>Holding still is read only once enough of the server's own time has passed since the customers change
 * landed for any confirmation made by then to be showing on the slot -- a slot that simply had not been told
 * anything yet would pass the same reading. And the case first shows the slot moving past a change both
 * pipelines landed, so "held" cannot be a slot that never moves.
 */
class PendingTableHoldsTheSlotIT {

    private static final Duration BOUND = Duration.ofSeconds(180);

    /**
     * Seconds of the server's time after which a confirmation would be showing on the slot: the owner reads
     * the durable position every 5 seconds, the connector is handed it at most every 5, and the reader tells
     * the server what it flushed every 10 -- 40 is past all three together, with room.
     */
    private static final int CONFIRMATION_REACHES_THE_SERVER = 40;
    private static final Duration SLOT_FOLLOWS = Duration.ofSeconds(30);

    private static final String SOURCE_ID = "src_pg";
    private static final String TARGET_ID = "tgt_mongo";
    private static final String ORDERS = "orders";
    private static final String CUSTOMERS = "customers";
    private static final String ORDERS_PIPELINE = "pending_orders";
    private static final String CUSTOMERS_PIPELINE = "pending_customers";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("postgres", "mongodb");
    }

    @Test
    void aChangeAPausedPipelineHasNotLandedHoldsTheSlotBeforeIt() throws Exception {
        Map<String, Object> source = SharedPostgres.settings("pending_table_slot");
        createTables(source);
        String storeUri = SharedMongo.replicaSetUrl("pending_table_store");
        String targetUri = SharedMongo.replicaSetUrl("pending_table_target");

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
            Await.until("a slot held by the chain's reader", BOUND,
                    () -> PostgresSlots.active(source), () -> String.valueOf(PostgresSlots.of(source)));

            // Both land a change, and the slot follows: without this, a slot that never moves would pass
            // everything below.
            write(source, ORDERS, 2);
            String bothLanded = write(source, CUSTOMERS, 2).inside();
            target.await(ORDERS, 2);
            target.await(CUSTOMERS, 2);
            Await.until("the slot to move past a change both pipelines landed", SLOT_FOLLOWS,
                    () -> PostgresSlots.confirmedAtOrPast(source, bothLanded),
                    () -> String.valueOf(PostgresSlots.of(source)));

            control.lifecycle(ORDERS_PIPELINE, LifecycleVerb.PAUSE);
            awaitState(control, ORDERS_PIPELINE, PipelineState.PAUSED);

            Position order = write(source, ORDERS, 3);
            Position customer = write(source, CUSTOMERS, 3);
            target.await(CUSTOMERS, 3);
            String landed = PostgresSlots.now(source);
            Await.until("a confirmation made since the customers change landed to have reached the server", BOUND,
                    () -> PostgresSlots.serverTimePassed(source, landed, CONFIRMATION_REACHES_THE_SERVER),
                    () -> String.valueOf(PostgresSlots.of(source)));
            assertThat(PostgresSlots.confirmedAtOrBefore(source, order.before()))
                    .as("the orders change nobody has landed holds the slot before it; slots: %s",
                            PostgresSlots.of(source))
                    .isTrue();

            control.lifecycle(ORDERS_PIPELINE, LifecycleVerb.RESUME);
            target.await(ORDERS, 3);
            Await.until("the slot to move past the customers change once the orders change landed", SLOT_FOLLOWS,
                    () -> PostgresSlots.confirmedAtOrPast(source, customer.inside()),
                    () -> String.valueOf(PostgresSlots.of(source)));
            assertThat(PostgresSlots.names(source)).as("one slot for the chain throughout").hasSize(1);
        }
    }

    /** Where the server was about to write before a change's transaction began, and inside it before commit. */
    private record Position(String before, String inside) {
    }

    /** Inserts row {@code id} into {@code table} in a transaction of its own, reading where it lies. */
    private static Position write(Map<String, Object> source, String table, int id) throws Exception {
        try (Connection connection = SharedPostgres.connect(source)) {
            String before = PostgresSlots.insertPosition(connection);
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("INSERT INTO " + table + " (id, name) VALUES (" + id + ", 'r" + id + "')");
            }
            String inside = PostgresSlots.insertPosition(connection);
            connection.commit();
            return new Position(before, inside);
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
