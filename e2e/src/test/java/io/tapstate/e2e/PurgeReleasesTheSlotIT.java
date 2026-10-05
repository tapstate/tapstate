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
 * Clearing the state of the last pipeline reading a PostgreSQL source through its chain drops the replication
 * slot the connector created there; nothing short of that does.
 *
 * <p>A slot is held on the user's own server and keeps every write-ahead log segment its reader has not
 * confirmed. Once nothing reads through it any more nobody confirms it again, so a slot left behind after a
 * clearing pins the source's log for good. But the slot belongs to the chain, not to any one pipeline on it:
 * letting go of it while another pipeline still reads through it would hand that pipeline a fresh slot at its
 * next start, beginning where the source is then, with every change in between gone.
 *
 * <p>The segments run in order over one database, each starting from what the one before left:
 * <ul>
 *   <li><b>A stop that keeps the state</b> leaves the slot, under the same name, and the next start reads
 *       through it again.</li>
 *   <li><b>Two pipelines on the chain</b>: clearing one leaves the slot -- and the other's next start still
 *       finds it, which is what tells a release that dropped the chain's notes while the slot was busy from
 *       one that left them alone. Clearing the second drops the slot.</li>
 *   <li><b>Clearing and starting again</b> -- what {@code restart --rerun} sends: the old slot is gone and
 *       exactly one new one serves the new run.</li>
 *   <li><b>Clearing a pipeline alone on its chain</b> drops the slot.</li>
 * </ul>
 *
 * <p>A slot is dropped only while no reader holds it, so every segment that expects one gone waits for it
 * rather than reading once: the stream stops on its own schedule. Slots are read for this database alone.
 */
class PurgeReleasesTheSlotIT {

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
    void onlyClearingTheLastPipelineOnTheChainDropsItsSlot(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> source = SharedPostgres.settings("purge_slot_" + suffix);
        createTables(source);
        String storeUri = SharedMongo.replicaSetUrl("purge_slot_store_" + suffix);
        String targetUri = SharedMongo.replicaSetUrl("purge_slot_target_" + suffix);
        String p = "purge_p_" + suffix;
        String q = "purge_q_" + suffix;

        try (ServerHandle server = tier.launch(storeUri); MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
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
            awaitNoSlot(source, "clearing the last pipeline on the chain to drop its slot");

            // ---- clearing and starting again, as restart --rerun does -------------------------------
            start(control, p);
            carry(source, target, ORDERS, 3);
            String before = PostgresSlots.only(source).name();
            control.stop(p, true);
            awaitState(control, p, PipelineState.STOPPED);
            start(control, p);
            carry(source, target, ORDERS, 4);
            List<String> after = PostgresSlots.names(source);
            assertThat(after).as("the rerun reads through exactly one slot").hasSize(1);
            assertThat(after).as("and not the one the cleared run held").doesNotContain(before);

            // ---- clearing a pipeline alone on its chain --------------------------------------------
            control.stop(p, true);
            awaitState(control, p, PipelineState.STOPPED);
            awaitNoSlot(source, "clearing the only pipeline on the chain to drop its slot");
        }
    }

    /** Starts {@code pipeline} and waits until it runs and its source is being read through a slot. */
    private static void start(ControlPlane control, String pipeline) {
        control.lifecycle(pipeline, LifecycleVerb.START);
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

    private static void awaitNoSlot(Map<String, Object> source, String what) {
        Await.until(what, BOUND, () -> PostgresSlots.of(source).isEmpty(),
                () -> String.valueOf(PostgresSlots.of(source)));
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
