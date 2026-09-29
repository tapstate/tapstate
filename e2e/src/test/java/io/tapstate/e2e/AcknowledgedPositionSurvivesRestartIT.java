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
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A PostgreSQL source is told it may release a change only once the change has landed, so a process killed
 * while its target is unreachable loses nothing: the slot still holds every change the target never took,
 * and the restarted process is sent them again.
 *
 * <p>The target is a MongoDB of its own that the case freezes -- its processes stop while its connections
 * stay open, which is what a target going away looks like from the pipeline. Fifty rows are written while it
 * is frozen: they are read, and none of them lands. A source told how far it had been read rather than how
 * far that had landed would be let go of past all fifty; killed then, the restarted process would ask for
 * what comes after them, the source would never send them again, and the target would stay short with
 * nothing reported.
 *
 * <p>The process is the shipped jar in a process of its own and it is killed, not stopped: nothing it would
 * do on the way down is allowed to happen. The slot is shown holding before the kill only once enough of the
 * server's own time has passed since the rows were written for any confirmation made by then to be showing --
 * a slot not yet told anything would read the same. After the restart the target holds every row exactly once, and only then does the slot move past
 * them.
 */
class AcknowledgedPositionSurvivesRestartIT {

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
    private static final String PIPELINE = "ack_restart";
    private static final String TABLE = "orders";
    private static final int WHILE_FROZEN = 50;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("postgres", "mongodb");
    }

    @Test
    void aKillWhileTheTargetIsUnreachableLosesNothingTheSlotHeld() throws Exception {
        Map<String, Object> source = SharedPostgres.settings("ack_restart_src");
        createTable(source);
        String storeUri = SharedMongo.replicaSetUrl("ack_restart_store");

        try (NetworkedMongo frozenTarget = NetworkedMongo.start(); MongoEndpoints mongo = new MongoEndpoints()) {
            String targetUri = frozenTarget.uriForThisHost("ack_restart_target");
            String lastInside;
            String firstBefore;

            RealProcessServer server = RealProcessServer.start(storeUri);
            try {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put(SOURCE_ID + ".tap.yml", sourceYaml(source));
                resources.put(TARGET_ID + ".tap.yml", targetYaml(targetUri));
                resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE_ID, TARGET_ID, TABLE));
                control.apply(resources);
                control.discoverSchema(SOURCE_ID, "postgres", discoveryConfig(source));
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until(PIPELINE + " to reach RUNNING", BOUND,
                        () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> control.state(PIPELINE) + ", logs: " + control.logs(PIPELINE));
                Await.until("the reader to hold a slot", BOUND,
                        () -> PostgresSlots.active(source), () -> String.valueOf(PostgresSlots.of(source)));
                String warm = write(source, 2)[1];
                awaitIds(mongo, targetUri, List.of(1, 2));
                Await.until("the slot to move past a change that landed", SLOT_FOLLOWS,
                        () -> PostgresSlots.confirmedAtOrPast(source, warm),
                        () -> String.valueOf(PostgresSlots.of(source)));

                frozenTarget.freeze();
                String[] first = write(source, 1000);
                firstBefore = first[0];
                String inside = first[1];
                for (int id = 1001; id < 1000 + WHILE_FROZEN; id++) {
                    inside = write(source, id)[1];
                }
                lastInside = inside;
                String written = PostgresSlots.now(source);
                Await.until("a confirmation made since the rows were written to have reached the server", BOUND,
                        () -> PostgresSlots.serverTimePassed(source, written, CONFIRMATION_REACHES_THE_SERVER),
                        () -> String.valueOf(PostgresSlots.of(source)));
                assertThat(PostgresSlots.confirmedAtOrBefore(source, firstBefore))
                        .as("none of the rows has landed, so the slot is held before the first; slots: %s",
                                PostgresSlots.of(source))
                        .isTrue();

                server.kill();
            } finally {
                server.close();
            }
            frozenTarget.thaw();

            try (RealProcessServer restarted = RealProcessServer.start(storeUri)) {
                ControlPlane control = new ControlPlane(restarted.baseUrl());
                control.login("e2e", "e2e-password");
                control.startUnlessRunning(PIPELINE);
                List<Integer> expected = Stream.concat(Stream.of(1, 2),
                        IntStream.range(1000, 1000 + WHILE_FROZEN).boxed()).toList();
                awaitIds(mongo, targetUri, expected);
                String last = lastInside;
                Await.until("the slot to move past the rows once they landed", SLOT_FOLLOWS,
                        () -> PostgresSlots.confirmedAtOrPast(source, last),
                        () -> String.valueOf(PostgresSlots.of(source)));
                assertThat(PostgresSlots.names(source)).as("one slot throughout").hasSize(1);
            }
        }
    }

    /**
     * Inserts row {@code id} in a transaction of its own, answering where the server was about to write
     * before the transaction began and inside it, before its commit.
     */
    private static String[] write(Map<String, Object> source, int id) throws Exception {
        try (Connection connection = SharedPostgres.connect(source)) {
            String before = PostgresSlots.insertPosition(connection);
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("INSERT INTO " + TABLE + " (id, name) VALUES (" + id + ", 'r" + id + "')");
            }
            String inside = PostgresSlots.insertPosition(connection);
            connection.commit();
            return new String[] {before, inside};
        }
    }

    /** Waits for the target to hold exactly {@code expected}, each row once. */
    private static void awaitIds(MongoEndpoints mongo, String targetUri, List<Integer> expected) {
        List<String> want = expected.stream().map(String::valueOf).sorted().toList();
        Await.until(expected.size() + " rows at the target, each once", BOUND,
                () -> ids(mongo, targetUri).equals(want), () -> String.valueOf(ids(mongo, targetUri)));
    }

    private static List<String> ids(MongoEndpoints mongo, String targetUri) {
        return mongo.documents(EndpointAddress.uri(targetUri), TABLE).stream()
                .map(document -> String.valueOf(document.get("id")))
                .sorted()
                .toList();
    }

    private static void createTable(Map<String, Object> source) throws Exception {
        try (Connection connection = SharedPostgres.connect(source);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + TABLE);
            statement.execute("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, name VARCHAR(64))");
            statement.execute("ALTER TABLE " + TABLE + " REPLICA IDENTITY FULL");
            statement.execute("INSERT INTO " + TABLE + " (id, name) VALUES (1, 'seed')");
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
