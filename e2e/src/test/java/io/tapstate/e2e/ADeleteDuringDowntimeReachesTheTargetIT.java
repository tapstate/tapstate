package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;

import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A row deleted while the server was down is gone from the target once it comes back.
 *
 * <p>A delete is the one change a restart cannot paper over. Every other kind survives a full re-read:
 * an insert or an update made during downtime is read again from the table and lands, so a run that
 * resumes and a run that starts over are indistinguishable at the target. A delete is not in the table
 * to be re-read, and an idempotent upsert never removes anything -- so if the restarted run does not
 * pick up the change stream where the last one left it, the deleted row stays in the target forever
 * and nothing anywhere reports it.
 *
 * <p>That makes the assertion here the absence of one row, and absence is exactly what an unwatched
 * witness gets for free. So the restarted run is first made to carry one unrelated new row: only once a
 * row written after the restart has crossed is the target's answer about the deleted row worth reading.
 *
 * <p>Gated on Docker and on a directory of real connector jars
 * ({@code -Dtapstate.e2e.connectors-dir}); the real-process tier additionally needs the app module
 * packaged. Run it with:
 *
 * <pre>
 *   mvn -pl e2e -am verify -Dapi.version=1.44 \
 *     -Dtapstate.e2e.connectors-dir=/path/to/connectors
 * </pre>
 */
class ADeleteDuringDowntimeReachesTheTargetIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final Duration POLL = Duration.ofMillis(250);

    private static final long SEEDED_ROWS = 5;
    private static final String TABLE = "orders";
    private static final String PIPELINE_ID = "downtime_delete";
    private static final String BEFORE_STOP = "changed-before-stop";
    private static final String LIVENESS_ROW = "after-restart-liveness";
    private static final int DELETED_ID = 3;
    private static final int LIVENESS_ID = 99;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void theRowDeletedWhileTheServerWasDownIsGoneFromTheTarget(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> source = SharedMySql.settings("downtime_delete_src_" + suffix);
        seed(source);

        String storeUri = SharedMongo.replicaSetUrl("downtime_delete_state_" + suffix);
        String targetUri = SharedMongo.replicaSetUrl("downtime_delete_target_" + suffix);
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (MongoEndpoints mongo = new MongoEndpoints()) {
            try (ServerHandle server = tier.launch(storeUri)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("e2e", "e2e-password");
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

                Map<String, String> resources = new LinkedHashMap<>();
                resources.put("src_mysql.tap.yml", sourceYaml(source));
                resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
                resources.put("pipeline.tap.yml", pipelineYaml());
                control.apply(resources);
                control.discoverSchema("src_mysql", "mysql", source);
                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                awaitCount(mongo, target, SEEDED_ROWS, "the snapshot of the seeded rows");

                // A change after the snapshot, so the first run stops holding a cdc position rather than
                // only a completed snapshot.
                update(source, 1, BEFORE_STOP);
                awaitName(mongo, target, 1, BEFORE_STOP, "the change the first run captured");
            }

            // The downtime window. A delete leaves nothing behind in the table to be found later.
            delete(source, DELETED_ID);

            try (ServerHandle server = tier.launch(storeUri)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.login("e2e", "e2e-password");
                // No start verb here: the pipeline's desired state is in the store the restart reopened,
                // so the converge loop brings it back on its own -- issuing start is refused outright as
                // an illegal transition out of RUNNING.

                // Liveness first -- absence proves nothing until something present has proved capture came back.
                insert(source, LIVENESS_ID, LIVENESS_ROW);
                awaitName(mongo, target, LIVENESS_ID, LIVENESS_ROW, "the liveness row written after the restart");

                assertThat(nameOf(mongo, target, DELETED_ID))
                        .as("row %d in the target: it was deleted from the source while the server was "
                                + "down, and a restart that resumed the change stream removes it", DELETED_ID)
                        .isEmpty();
            }
        }
    }

    private static void awaitCount(
            MongoEndpoints mongo, EndpointAddress target, long expected, String what) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        long last = -1;
        while (System.nanoTime() - deadline < 0) {
            last = mongo.count(target, TABLE);
            if (last == expected) {
                return;
            }
            sleep();
        }
        assertThat(last).as("rows in the Mongo target after %s", what).isEqualTo(expected);
    }

    private static void awaitName(
            MongoEndpoints mongo, EndpointAddress target, int id, String expected, String what) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        Optional<String> last = Optional.empty();
        while (System.nanoTime() - deadline < 0) {
            last = nameOf(mongo, target, id);
            if (last.filter(expected::equals).isPresent()) {
                return;
            }
            sleep();
        }
        assertThat(last.orElse("<no such row>")).as("%s, read back from the target", what).isEqualTo(expected);
    }

    private static Optional<String> nameOf(MongoEndpoints mongo, EndpointAddress target, int id) {
        for (Document document : mongo.documents(target, TABLE)) {
            if (document.get("id") instanceof Number found && found.intValue() == id) {
                return Optional.ofNullable(document.getString("name"));
            }
        }
        return Optional.empty();
    }

    private static void seed(Map<String, Object> settings) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + TABLE);
            statement.execute("CREATE TABLE " + TABLE + " (id INT PRIMARY KEY, name VARCHAR(64))");
            for (long id = 1; id <= SEEDED_ROWS; id++) {
                statement.execute("INSERT INTO " + TABLE + " (id, name) VALUES (" + id + ", 'order-" + id + "')");
            }
        }
    }

    private static void update(Map<String, Object> settings, int id, String name) throws Exception {
        execute(settings, "UPDATE " + TABLE + " SET name = '" + name + "' WHERE id = " + id);
    }

    private static void insert(Map<String, Object> settings, int id, String name) throws Exception {
        execute(settings, "INSERT INTO " + TABLE + " (id, name) VALUES (" + id + ", '" + name + "')");
    }

    private static void delete(Map<String, Object> settings, int id) throws Exception {
        execute(settings, "DELETE FROM " + TABLE + " WHERE id = " + id);
    }

    private static void execute(Map<String, Object> settings, String sql) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static String sourceYaml(Map<String, Object> config) {
        return """
                version: tapstate/v1
                kind: source
                id: src_mysql
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ orders ]
                """
                .formatted(
                        config.get("host"),
                        config.get("port"),
                        config.get("database"),
                        config.get("username"),
                        config.get("password"));
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

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: downtime_delete
                source: src_mysql
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [orders], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: tgt_mongo
                """;
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting on the restarted pipeline", e);
        }
    }
}
