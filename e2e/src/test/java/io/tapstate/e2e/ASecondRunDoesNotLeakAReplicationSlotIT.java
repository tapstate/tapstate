package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A pipeline started a second time reuses the replication slot its first run made, rather than leaving
 * that one behind on the user's database and making another.
 *
 * <p>A PostgreSQL change-capture read needs a logical replication slot, and the connector creates one the
 * first time it runs. The slot is server-side state that outlives the reader: PostgreSQL keeps it, and
 * keeps every write-ahead log segment the slot has not consumed, until somebody drops it. So the connector
 * records the slot's name for itself, through the plugin contract's state map, and looks it up on its next
 * run. That lookup is only as good as the map: handed a fresh, empty one on every open, the connector finds
 * nothing, concludes it has never run here, and creates a second slot - the first one staying behind,
 * inactive, holding log files on a database that is not ours.
 *
 * <p>This is the one consequence of an unpersisted state map that a user sees on their own server rather
 * than in our behaviour, and it is the reason the case is worth a real database. It was read out of the
 * connector's source before it was ever run; this is what settles it.
 *
 * <p>What the assertions have to discriminate:
 * <ul>
 *   <li><b>The first run is held to having made exactly one slot.</b> A run that never reached its change
 *       stream makes none, and a case that only counted at the end would read "one slot" off a pipeline
 *       that never streamed at all - the same number the fix produces, for the opposite reason.</li>
 *   <li><b>The second run is waited for by its slot going active, not by rows arriving.</b> A row can reach
 *       the target from the second run's snapshot, which needs no slot; only an active slot for this
 *       database says the change stream itself is up. Waiting on the wrong signal would let a second run
 *       that never streamed satisfy the count.</li>
 *   <li><b>The slot names are compared, not merely counted.</b> A run that dropped its predecessor's slot
 *       and made a differently-named one leaves the count at one while still not reusing anything, and the
 *       write-ahead log the old slot pinned would have been released on a different schedule than reuse
 *       implies.</li>
 *   <li><b>Slots are read for this database alone.</b> The server is shared by every PostgreSQL case in the
 *       JVM and its slots are not dropped when a stream stops, so a count over the whole server would be
 *       measuring the other cases.</li>
 * </ul>
 *
 * <p>One tier, not both: what is asked here is whether the source database is left with a second slot, and
 * the pipeline is stopped and started rather than the process. The connector is opened afresh either way,
 * so a real-process run would exercise the same path at several times the cost.
 *
 * <p>Gated on Docker and on a directory of real connector jars, like its siblings. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors \
 *     -Dit.test=ASecondRunDoesNotLeakAReplicationSlotIT -Dfailsafe.failIfNoSpecifiedTests=false
 * </pre>
 */
class ASecondRunDoesNotLeakAReplicationSlotIT {

    /** Wide, for the reason its siblings give: a real connector drives a snapshot and a stream first. */
    private static final Duration BOUND = Duration.ofSeconds(180);

    private static final String TABLE = "orders";
    private static final String DATABASE = "slot_reuse_src";
    private static final String PIPELINE_ID = "slot_reuse";
    private static final String SOURCE_ID = "src_pg";
    private static final String TARGET_ID = "tgt_mongo";

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("postgres", "mongodb");
    }

    @Test
    void aPipelineStartedAgainReusesTheSlotItsFirstRunRecorded() throws Exception {
        Map<String, Object> source = SharedPostgres.settings(DATABASE);
        createTable(source);
        insert(source, 1);

        String storeUri = SharedMongo.replicaSetUrl("slot_reuse_store");
        String targetUri = SharedMongo.replicaSetUrl("slot_reuse_target");

        try (ServerHandle server = Tiers.IN_PROCESS.launch(storeUri);
                MongoEndpoints mongo = new MongoEndpoints()) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
            control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

            Map<String, String> resources = new LinkedHashMap<>();
            resources.put(SOURCE_ID + ".tap.yml", sourceYaml(source));
            resources.put(TARGET_ID + ".tap.yml", targetYaml(targetUri));
            resources.put(PIPELINE_ID + ".tap.yml", Workspaces.pipelineYaml(
                    PIPELINE_ID, SOURCE_ID, TARGET_ID, TABLE));
            control.apply(resources);
            control.discoverSchema(SOURCE_ID, "postgres", discoveryConfig(source));

            // ---- the first run -----------------------------------------------------------------
            control.lifecycle(PIPELINE_ID, LifecycleVerb.START);
            awaitState(control, PipelineState.RUNNING);
            awaitActiveSlot(source, "the first run's change stream to take a replication slot");
            // Proven to carry, not merely to have opened a slot: a change laid down while the run is live
            // reaching the target is what says this pipeline works at all.
            insert(source, 2);
            Await.until("the first run to carry a change to the target", BOUND,
                    () -> holdsRow(mongo, targetUri, 2),
                    () -> String.valueOf(documentsIn(mongo, targetUri)));

            List<String> afterTheFirstRun = slotNamesFor(source);
            assertThat(afterTheFirstRun)
                    .as("the first run made exactly one slot - without it there is nothing to reuse and "
                            + "the count below would be met by a second run that never streamed")
                    .hasSize(1);

            control.lifecycle(PIPELINE_ID, LifecycleVerb.STOP);
            awaitState(control, PipelineState.STOPPED);

            // ---- the second run ----------------------------------------------------------------
            control.lifecycle(PIPELINE_ID, LifecycleVerb.START);
            awaitState(control, PipelineState.RUNNING);
            // An active slot, not a row: a row can arrive from this run's snapshot, which needs no slot.
            awaitActiveSlot(source, "the second run's change stream to take a replication slot");

            assertThat(slotNamesFor(source))
                    .as("the second run finds the slot its first run recorded and reuses it, leaving the "
                            + "source database with one slot rather than one more")
                    .isEqualTo(afterTheFirstRun);
        }
    }

    // ---- the source database ------------------------------------------------------------------

    private static void createTable(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedPostgres.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + TABLE);
            statement.execute("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, customer VARCHAR(64))");
            statement.execute("ALTER TABLE " + TABLE + " REPLICA IDENTITY FULL");
        }
    }

    private static void insert(Map<String, Object> settings, int id) throws Exception {
        try (Connection connection = SharedPostgres.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO " + TABLE + " (id, customer) VALUES (" + id + ", 'c" + id + "')");
        }
    }

    /**
     * The slots this database holds, by name. Read for this database alone: the server is shared by every
     * PostgreSQL case in the JVM, and its slots outlive the streams that made them.
     */
    private static List<String> slotNamesFor(Map<String, Object> settings) throws Exception {
        return slots(settings, false);
    }

    private static void awaitActiveSlot(Map<String, Object> settings, String what) {
        Await.until(what, BOUND,
                () -> !slotsQuietly(settings, true).isEmpty(),
                () -> "slots: " + slotsQuietly(settings, false));
    }

    private static List<String> slots(Map<String, Object> settings, boolean activeOnly) throws Exception {
        String sql = "SELECT slot_name FROM pg_replication_slots WHERE database = '"
                + settings.get("database") + "'" + (activeOnly ? " AND active" : "") + " ORDER BY slot_name";
        List<String> names = new ArrayList<>();
        try (Connection connection = SharedPostgres.connect(settings);
                Statement statement = connection.createStatement();
                ResultSet rows = statement.executeQuery(sql)) {
            while (rows.next()) {
                names.add(rows.getString(1));
            }
        }
        return names;
    }

    private static List<String> slotsQuietly(Map<String, Object> settings, boolean activeOnly) {
        try {
            return slots(settings, activeOnly);
        } catch (Exception e) {
            throw new IllegalStateException("cannot read the replication slots of " + settings.get("database"), e);
        }
    }

    // ---- the target ---------------------------------------------------------------------------

    private static boolean holdsRow(MongoEndpoints mongo, String targetUri, int id) {
        return documentsIn(mongo, targetUri).stream()
                .anyMatch(document -> String.valueOf(document.get("id")).equals(String.valueOf(id)));
    }

    private static List<Document> documentsIn(MongoEndpoints mongo, String targetUri) {
        return mongo.documents(EndpointAddress.uri(targetUri), TABLE);
    }

    // ---- the specification --------------------------------------------------------------------

    private static void awaitState(ControlPlane control, PipelineState state) {
        Await.until(PIPELINE_ID + " to reach " + state, BOUND,
                () -> control.state(PIPELINE_ID).filter(state::equals).isPresent(),
                () -> String.valueOf(control.state(PIPELINE_ID)));
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
                tables: [ %s ]
                """
                .formatted(SOURCE_ID, settings.get("host"), settings.get("port"), settings.get("database"),
                        settings.get("username"), settings.get("password"), TABLE);
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
