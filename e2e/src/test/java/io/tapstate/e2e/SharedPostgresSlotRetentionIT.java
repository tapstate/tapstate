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
import java.util.Locale;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * A PostgreSQL slot used by SRS belongs to the shared capture chain. Stopping a pipeline with retained
 * state preserves its read position. Clearing a pipeline detaches it without retiring the shared chain,
 * even when it was the last consumer. A later start on the same chain reads through the retained slot.
 * The witness reads the source database's actual slots and checks that CDC still reaches the target.
 */
class SharedPostgresSlotRetentionIT {

    private static final Duration BOUND = Duration.ofSeconds(180);

    private static final String SOURCE_ID = "src_pg";
    private static final String TARGET_ID = "tgt_mongo";
    private static final String ORDERS = "orders";
    private static final String CUSTOMERS = "customers";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("postgres", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void clearingPipelinesKeepsTheSharedSlot(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> source = SharedPostgres.settings("purge_slot_" + suffix);
        createTables(source);
        String targetUri = SharedMongo.replicaSetUrl("purge_slot_target_" + suffix);
        String p = "purge_p_" + suffix;
        String q = "purge_q_" + suffix;

        try (PreparedNativeStore storage = PreparedNativeStore.create();
                ServerHandle server = tier.launch(storage); MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
            Map<String, String> resources = new LinkedHashMap<>();
            resources.put(SOURCE_ID + ".tap.yml", sourceYaml(source));
            resources.put(TARGET_ID + ".tap.yml", targetYaml(targetUri));
            resources.put(p + ".tap.yml", Workspaces.pipelineYaml(p, SOURCE_ID, TARGET_ID, ORDERS));
            resources.put(q + ".tap.yml", Workspaces.pipelineYaml(q, SOURCE_ID, TARGET_ID, CUSTOMERS));
            control.apply(resources);
            control.discoverSchema(SOURCE_ID, "postgres", discoveryConfig(source));
            Target target = new Target(mongo, targetUri);

            // ---- a stop that keeps the state keeps the slot ----------------------------------------
            start(control, p);
            carry(source, target, ORDERS, 1);
            String first = PostgresSlots.only(source).name();

            control.stop(p, false);
            awaitState(control, p, PipelineState.STOPPED);
            assertThat(PostgresSlots.names(source)).as("kept by a stop that kept the state").containsExactly(first);
            start(control, p);
            carry(source, target, ORDERS, 2);
            assertThat(PostgresSlots.names(source)).as("and read through again").containsExactly(first);

            // ---- two pipelines on the chain --------------------------------------------------------
            start(control, q);
            carry(source, target, CUSTOMERS, 1);
            assertThat(PostgresSlots.names(source)).as("one slot for the chain, whoever reads it")
                    .containsExactly(first);

            control.stop(p, true);
            awaitState(control, p, PipelineState.STOPPED);
            assertThat(PostgresSlots.names(source)).as("q still reads through it").containsExactly(first);
            control.stop(q, false);
            awaitState(control, q, PipelineState.STOPPED);
            start(control, q);
            carry(source, target, CUSTOMERS, 2);
            assertThat(PostgresSlots.names(source))
                    .as("q's next start still finds the slot: clearing p let go of nothing of the chain's")
                    .containsExactly(first);

            control.stop(q, true);
            awaitState(control, q, PipelineState.STOPPED);
            awaitInactiveSlot(source, first);

            // ---- clearing and starting again, as restart --rerun does -------------------------------
            startKeepingTarget(control, p);
            carry(source, target, ORDERS, 3);
            assertThat(PostgresSlots.names(source)).containsExactly(first);
            control.stop(p, true);
            awaitState(control, p, PipelineState.STOPPED);
            startKeepingTarget(control, p);
            carry(source, target, ORDERS, 4);
            assertThat(PostgresSlots.names(source)).as("the rerun reads through the original shared slot")
                    .containsExactly(first);

            // ---- clearing a pipeline alone on its chain --------------------------------------------
            control.stop(p, true);
            awaitState(control, p, PipelineState.STOPPED);
            awaitInactiveSlot(source, first);
        }
    }

    /** Starts {@code pipeline} and waits until it runs and its source is being read through a slot. */
    private static void start(ControlPlane control, String pipeline) {
        control.lifecycle(pipeline, LifecycleVerb.START);
        awaitState(control, pipeline, PipelineState.RUNNING);
    }

    /** A new full load after purge intentionally keeps the target rows from the preceding run. */
    private static void startKeepingTarget(ControlPlane control, String pipeline) {
        control.startAnswering(pipeline, "target-not-empty", "keep");
        awaitState(control, pipeline, PipelineState.RUNNING);
    }

    /**
     * Inserts row {@code id} into {@code table} and waits for it at the target: the pipeline reading it is
     * then carrying changes through the slot, not only holding one.
     *
     * <p>An empty transaction is committed just before the row, so the row never begins exactly where the
     * position a resumed capture started from ends. A capture resuming from a position recorded after a commit
     * skips a change that begins there (tapstate/tapstate#639). That defect is not what this test measures, and
     * the row it would skip is the one each segment relies on to show that a pipeline still reads through the
     * slot.
     */
    private static void carry(Map<String, Object> source, Target target, String table, int id) throws Exception {
        Await.until("a slot on " + source.get("database") + " held by a reader", BOUND,
                () -> PostgresSlots.active(source), () -> String.valueOf(PostgresSlots.of(source)));
        try (Connection connection = SharedPostgres.connect(source);
                Statement statement = connection.createStatement()) {
            statement.execute("SELECT pg_current_xact_id()");
            statement.execute("INSERT INTO " + table + " (id, name) VALUES (" + (100 + id) + ", 'r" + id + "')");
        }
        Await.until("row " + (100 + id) + " of " + table + " at the target", BOUND,
                () -> target.holds(table, 100 + id), () -> String.valueOf(target.ids(table)));
    }

    private static void awaitInactiveSlot(Map<String, Object> source, String name) {
        Await.until("the original shared slot to remain without an active reader", BOUND,
                () -> PostgresSlots.of(source).stream().anyMatch(slot -> slot.name().equals(name) && !slot.active()),
                () -> String.valueOf(PostgresSlots.of(source)));
        assertThat(PostgresSlots.names(source)).containsExactly(name);
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

        boolean holds(String table, int id) {
            return ids(table).contains(String.valueOf(id));
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
