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
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Two pipelines reading one PostgreSQL source through one chain read it through one replication slot, and
 * keep reading through that slot whichever of them opens the stream after a restart.
 *
 * <p>The connector records the slot it created in notes of its own and looks it up the next time it opens.
 * Kept per pipeline, those notes are empty for any pipeline but the one that opened the stream first: when
 * the other one happens to open it after a restart, the connector reads that as a first run and creates a
 * new slot where the source is then. The changes made while the server was down are between the old slot
 * and the new one, and neither pipeline ever receives them -- quietly, with both pipelines running.
 *
 * <p>So the case restarts the server with changes made while it was down, and starts the pipeline that did
 * not open the stream first. The rows reaching both targets is what says nothing in between was skipped;
 * the slot's name staying the same is what says the stream was not simply started over.
 */
class SharedChainKeepsOneSlotIT {

    private static final Duration BOUND = Duration.ofSeconds(180);

    private static final String SOURCE_ID = "src_pg";
    private static final String TABLE = "orders";
    private static final int WHILE_DOWN = 20;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("postgres", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void whicheverPipelineOpensTheStreamAfterARestartReadsThroughTheSameSlot(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> source = SharedPostgres.settings("shared_slot_" + suffix);
        createTable(source);
        String storeUri = SharedMongo.replicaSetUrl("shared_slot_store_" + suffix);
        String firstTarget = SharedMongo.replicaSetUrl("shared_slot_first_" + suffix);
        String secondTarget = SharedMongo.replicaSetUrl("shared_slot_second_" + suffix);
        String first = "shared_first_" + suffix;
        String second = "shared_second_" + suffix;
        String slot;

        try (MongoEndpoints mongo = new MongoEndpoints()) {
            try (ServerHandle server = tier.launch(storeUri)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                control.registerConnector("postgres", ConnectorJars.bytesFor("postgres"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put(SOURCE_ID + ".tap.yml", sourceYaml(source));
                resources.put("tgt_first.tap.yml", targetYaml("tgt_first", firstTarget));
                resources.put("tgt_second.tap.yml", targetYaml("tgt_second", secondTarget));
                resources.put(first + ".tap.yml", Workspaces.pipelineYaml(first, SOURCE_ID, "tgt_first", TABLE));
                resources.put(second + ".tap.yml", Workspaces.pipelineYaml(second, SOURCE_ID, "tgt_second", TABLE));
                control.apply(resources);
                control.discoverSchema(SOURCE_ID, "postgres", discoveryConfig(source));

                start(control, first);
                start(control, second);
                insert(source, 2);
                awaitRows(mongo, firstTarget, List.of(1, 2));
                awaitRows(mongo, secondTarget, List.of(1, 2));
                slot = PostgresSlots.only(source).name();

                control.stop(first, false);
                control.stop(second, false);
                awaitState(control, first, PipelineState.STOPPED);
                awaitState(control, second, PipelineState.STOPPED);
            }

            for (int id : IntStream.range(1000, 1000 + WHILE_DOWN).toArray()) {
                insert(source, id);
            }

            try (ServerHandle server = tier.launch(storeUri)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.login("e2e", "e2e-password");

                start(control, second);
                Await.until("the second pipeline's run to read the source through a slot", BOUND,
                        () -> PostgresSlots.active(source), () -> String.valueOf(PostgresSlots.of(source)));
                assertThat(PostgresSlots.names(source))
                        .as("the pipeline that did not open the stream first opens it through the same slot")
                        .containsExactly(slot);
                start(control, first);

                List<Integer> expected = IntStream.concat(IntStream.of(1, 2),
                        IntStream.range(1000, 1000 + WHILE_DOWN)).boxed().toList();
                awaitRows(mongo, secondTarget, expected);
                awaitRows(mongo, firstTarget, expected);
                assertThat(PostgresSlots.names(source)).as("still one slot, and the same one").containsExactly(slot);
            }
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

    private static void awaitRows(MongoEndpoints mongo, String target, List<Integer> expected) {
        List<String> ids = expected.stream().map(String::valueOf).sorted().toList();
        Await.until(expected.size() + " rows at " + target, BOUND,
                () -> idsAt(mongo, target).equals(ids), () -> String.valueOf(idsAt(mongo, target)));
    }

    private static List<String> idsAt(MongoEndpoints mongo, String target) {
        return mongo.documents(EndpointAddress.uri(target), TABLE).stream()
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

    private static void insert(Map<String, Object> source, int id) throws Exception {
        try (Connection connection = SharedPostgres.connect(source);
                Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO " + TABLE + " (id, name) VALUES (" + id + ", 'r" + id + "')");
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

    private static String targetYaml(String id, String targetUri) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s" }
                """
                .formatted(id, targetUri);
    }
}
