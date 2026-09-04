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
 * A restart carries on from the position it recorded, instead of reading the source table again.
 *
 * <p>The claim under test is not that the target ends up correct -- re-reading the whole table also
 * gets there, which is exactly why a value assertion alone witnesses nothing here. What separates the
 * two is how much the restarted run had to read: resuming costs the changes made while it was down,
 * re-reading costs the table. So the discriminating reading is the restarted run's own record count,
 * and it is asserted to stay below the seeded row count.
 *
 * <p>Both counts are read per run: they come from the live job, so a restarted pipeline starts a fresh
 * one and the second reading is not a continuation of the first. The write count is the cheaper signal
 * and the read count is the claim: "it did not read the table again" is a statement about reads, and a
 * run that re-read four rows of five and wrote them satisfies "wrote fewer than were seeded" while
 * being exactly what this case says did not happen. So the rows-read reading is asserted at the value
 * the claim names -- zero -- and the write count stays beside it.
 *
 * <p>A liveness gate runs before the assertion. An observation outliving the server it observes is the
 * standing failure of a restart witness: the pipeline reads RUNNING, every await passes against values
 * the previous run already landed, and the assertion is vacuous. So the restarted run is made to carry
 * one unrelated new row first, and only a row that actually crosses proves capture came back at all.
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
class RestartResumesTheTailIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(90);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final Duration SETTLE = Duration.ofSeconds(8);

    private static final long SEEDED_ROWS = 5;
    private static final String TABLE = "orders";
    private static final String PIPELINE_ID = "restart_tail";
    private static final String BEFORE_STOP = "changed-before-stop";
    private static final String DURING_DOWNTIME = "changed-during-downtime";
    private static final String LIVENESS_ROW = "after-restart-liveness";
    private static final int LIVENESS_ID = 99;

    @BeforeAll
    static void requireDockerAndRealConnectors() {
        DockerGate.require();
        RealConnectorGate.require("mysql", "mongodb");
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aRestartCarriesOnFromTheRecordedPositionInsteadOfReadingTheTableAgain(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        Map<String, Object> source = SharedMySql.settings("restart_tail_src_" + suffix);
        seed(source);

        // One store per tier, and the restart reuses it -- that reuse is the whole subject: the position
        // the first run recorded is only there to be resumed from if the second run reads the same store.
        String storeUri = SharedMongo.replicaSetUrl("restart_tail_state_" + suffix);
        String targetUri = SharedMongo.replicaSetUrl("restart_tail_target_" + suffix);
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
                resources.put("pipeline.tap.yml", pipelineYaml(PIPELINE_ID));
                control.apply(resources);
                control.discoverSchema("src_mysql", "mysql", source);
                control.lifecycle(PIPELINE_ID, LifecycleVerb.START);

                awaitCount(mongo, target, SEEDED_ROWS, "the snapshot of the seeded rows");

                // A change after the snapshot, so the first run ends with a recorded cdc position rather
                // than only a completed snapshot -- resuming has to have something to resume from.
                update(source, 1, BEFORE_STOP);
                awaitName(mongo, target, 1, BEFORE_STOP, "the change the first run captured");
            }

            // The downtime window. Nothing is watching the source now, so this change is only ever seen
            // by a run that comes back and reads on from where the last one stopped.
            update(source, 2, DURING_DOWNTIME);

            try (ServerHandle server = tier.launch(storeUri)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                // The admin already exists in the store this restart reuses, so this logs in rather than
                // bootstrapping -- bootstrapping again would be a different server, not a restart.
                control.login("e2e", "e2e-password");
                // No start verb here: the pipeline's desired state is in the store the restart reopened,
                // so the converge loop brings it back on its own -- issuing start is refused outright as
                // an illegal transition out of RUNNING. Whether it really came back is what the liveness
                // gate below settles; the state face alone would say RUNNING either way.

                // Liveness first: an unrelated row written now, crossing now, is what rules out an
                // assertion that would pass against what the previous run had already landed.
                insert(source, LIVENESS_ID, LIVENESS_ROW);
                awaitName(mongo, target, LIVENESS_ID, LIVENESS_ROW, "the liveness row written after the restart");

                awaitName(mongo, target, 2, DURING_DOWNTIME, "the change made while the server was down");

                long written = settledRecordCount(control);
                assertThat(written)
                        .as("records the restarted run wrote: resuming costs the downtime change and the "
                                + "liveness row, re-reading the table costs %d more", SEEDED_ROWS)
                        .isLessThan(SEEDED_ROWS);

                // The claim itself. A resumed run reads no snapshot rows at all, so the entry is either
                // absent or zero -- both say the table was not read again, and which of the two it is
                // depends on whether the run published a snapshot face for a phase it never entered.
                assertThat(control.snapshotRowsRead(PIPELINE_ID).getOrDefault(TABLE, 0L))
                        .as("rows the restarted run read from %s: it resumes from a recorded position, "
                                + "so the table is not read again at all", TABLE)
                        .isZero();
            }
        }
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void thePlainRestartCarriesOnWithoutReadingTheTableAgain(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        String pipelineId = "restart_verb_keep";
        Map<String, Object> source = SharedMySql.settings("restart_keep_src_" + suffix);
        seed(source);
        String storeUri = SharedMongo.replicaSetUrl("restart_keep_state_" + suffix);
        String targetUri = SharedMongo.replicaSetUrl("restart_keep_target_" + suffix);
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = tier.launch(storeUri)) {
            ControlPlane control = startedPipeline(server, source, targetUri, pipelineId);

            awaitCount(mongo, target, SEEDED_ROWS, "the snapshot of the seeded rows");
            // A change after the snapshot, so the run ends with a recorded position rather than only a
            // finished snapshot: carrying on has to have something to carry on from.
            update(source, 1, BEFORE_STOP);
            awaitName(mongo, target, 1, BEFORE_STOP, "the change the first run captured");

            // The pair the terminal composes for `restart`: a stop that keeps, then a start.
            control.stop(pipelineId, false);
            control.lifecycle(pipelineId, LifecycleVerb.START);

            // Liveness before the count. A run that has not begun its snapshot has read nought too, so
            // without a row that actually crosses after the restart the assertion below is vacuous.
            insert(source, LIVENESS_ID, LIVENESS_ROW);
            awaitName(mongo, target, LIVENESS_ID, LIVENESS_ROW, "a row written after the restart");

            assertThat(settledRowsRead(control, pipelineId))
                    .as("rows the restarted run read from %s: the plain word carries on, so the table is "
                            + "not read again", TABLE)
                    .isZero();
        }
    }

    @ParameterizedTest
    @EnumSource(Tiers.class)
    void aRerunReadsTheWholeTableAgain(Tiers tier) throws Exception {
        String suffix = tier.name().toLowerCase(Locale.ROOT);
        String pipelineId = "restart_verb_rerun";
        Map<String, Object> source = SharedMySql.settings("restart_rerun_src_" + suffix);
        seed(source);
        String storeUri = SharedMongo.replicaSetUrl("restart_rerun_state_" + suffix);
        String targetUri = SharedMongo.replicaSetUrl("restart_rerun_target_" + suffix);
        EndpointAddress target = EndpointAddress.uri(targetUri);

        try (MongoEndpoints mongo = new MongoEndpoints();
                ServerHandle server = tier.launch(storeUri)) {
            ControlPlane control = startedPipeline(server, source, targetUri, pipelineId);

            awaitCount(mongo, target, SEEDED_ROWS, "the snapshot of the seeded rows");
            update(source, 1, BEFORE_STOP);
            awaitName(mongo, target, 1, BEFORE_STOP, "the change the first run captured");

            // The pair `restart --rerun` composes: the stop clears, so there is nothing to carry on from.
            control.stop(pipelineId, true);
            control.lifecycle(pipelineId, LifecycleVerb.START);

            // The count itself is the liveness here: nought would mean the run never started, and the
            // whole table is the only reading that says it really read the table again.
            assertThat(settledRowsRead(control, pipelineId))
                    .as("rows the rerun read from %s: it was told to read everything, so it reads every "
                            + "seeded row", TABLE)
                    .isEqualTo(SEEDED_ROWS);
        }
    }

    /** Everything up to and including the start, which the three cases here do identically. */
    private static ControlPlane startedPipeline(
            ServerHandle server, Map<String, Object> source, String targetUri, String pipelineId) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        control.bootstrapAndLogin("e2e", "e2e-password");
        control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
        control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));

        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src_mysql.tap.yml", sourceYaml(source));
        resources.put("tgt_mongo.tap.yml", targetYaml(targetUri));
        resources.put("pipeline.tap.yml", pipelineYaml(pipelineId));
        control.apply(resources);
        control.discoverSchema("src_mysql", "mysql", source);
        control.lifecycle(pipelineId, LifecycleVerb.START);
        return control;
    }

    /**
     * The restarted run's record count once it stops moving. A single reading is the last one collected
     * rather than the current total, so a count taken the moment an await returns can be short by
     * whatever the last collection missed -- and short is the direction that would make this pass.
     */
    private static long settledRecordCount(ControlPlane control) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        long last = -1;
        long unchangedSince = System.nanoTime();
        while (System.nanoTime() - deadline < 0) {
            long now = control.recordCount(PIPELINE_ID).orElse(-1L);
            if (now != last) {
                last = now;
                unchangedSince = System.nanoTime();
            } else if (System.nanoTime() - unchangedSince > SETTLE.toNanos()) {
                return last;
            }
            sleep();
        }
        return last;
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
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute("UPDATE " + TABLE + " SET name = '" + name + "' WHERE id = " + id);
        }
    }

    private static void insert(Map<String, Object> settings, int id, String name) throws Exception {
        try (Connection connection = SharedMySql.connect(settings);
                Statement statement = connection.createStatement()) {
            statement.execute("INSERT INTO " + TABLE + " (id, name) VALUES (" + id + ", '" + name + "')");
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

    private static String pipelineYaml(String pipelineId) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: src_mysql
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [orders], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: tgt_mongo
                """
                .formatted(pipelineId);
    }

    /**
     * What the run has read from the table once that reading stops moving. A single sample is the last
     * collection rather than the current total, and low is the direction that would make the plain-restart
     * case pass for the wrong reason.
     */
    private static long settledRowsRead(ControlPlane control, String pipelineId) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        long last = -1;
        long unchangedSince = System.nanoTime();
        while (System.nanoTime() - deadline < 0) {
            long now = control.snapshotRowsRead(pipelineId).getOrDefault(TABLE, 0L);
            if (now != last) {
                last = now;
                unchangedSince = System.nanoTime();
            } else if (System.nanoTime() - unchangedSince > SETTLE.toNanos()) {
                return last;
            }
            sleep();
        }
        return last;
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
