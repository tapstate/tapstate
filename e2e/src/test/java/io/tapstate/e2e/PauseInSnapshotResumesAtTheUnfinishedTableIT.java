package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A full load that comes back to a source whose tables are not all confirmed reads only the ones that are
 * not. Two of three tables are confirmed at the sink before the server goes down; the run that follows
 * reads the third and leaves the other two alone.
 *
 * <h2>What discriminates, and what does not</h2>
 *
 * <p>The reading is the later run's own per-table read count, never the target. Every row is in the target
 * either way: a run that re-read all three would land exactly the same data, because the write is
 * idempotent and the re-read rows are the rows already there. So a value assertion witnesses nothing here,
 * and the only thing separating "resumed" from "started over" is how much the second run had to read. Two
 * of the three must read zero rows and the third must read the whole table. The target is asserted too,
 * but only as the other half: it says the skipping was safe, not that any skipping happened.
 *
 * <p>The three tables carry different row counts on purpose. Equal counts would let a count attributed to
 * the wrong table pass unnoticed, and the tables are read in a fixed order, so an off-by-one in the
 * attribution has somewhere to hide unless the numbers themselves disagree.
 *
 * <h2>Why the unconfirmed table is arranged by selection rather than by timing</h2>
 *
 * <p>The state this case needs is a boundary inside one chain's record: some tables confirmed, one not.
 * Reaching it by interrupting a running full load does not work, and both reasons are properties of the
 * shipped path rather than of this harness.
 *
 * <p>A run cut short while <em>reading</em> has confirmed nothing at all. The bounded round drains into a
 * member-local buffer and the source vertex drains that once the job runs, so a round that stopped part way
 * wrote no row, confirmed no table, and is owed every one of them on the way back. That is a real state,
 * but it is not one with a boundary in it.
 *
 * <p>A run stalled while <em>writing</em> confirms nothing either, and this was measured rather than
 * assumed: holding one table's writes while the other two completed left the record naming no table at all
 * after two minutes, not the two that had gone through. A table's confirmation is published when the sink
 * has no batch in flight, so one held table withholds every table's confirmation, not its own.
 *
 * <p>What does produce the boundary is the selection. A chain is keyed by the physical source coordinate
 * and deliberately excludes the table subset, so two sources over one directory share one chain and one
 * record: a source reading two of the tables confirms exactly those two, and a source over the same
 * directory reading all three then finds the third owed and the first two not. The record is the subject
 * either way, and it is the only thing carried across the restart.
 *
 * <p>Gated on Docker for the state store. Source and target are the harness's own file connector, so this
 * needs no directory of real connector jars and runs in the ordinary pull-request lane.
 */
class PauseInSnapshotResumesAtTheUnfinishedTableIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final Duration SETTLE = Duration.ofSeconds(5);

    private static final String NARROW_SOURCE = "src_confirmed";
    private static final String WIDE_SOURCE = "src_all";
    private static final String TARGET_ID = "tgt_files";
    private static final String NARROW_PIPELINE = "confirms_two";
    private static final String WIDE_PIPELINE = "reads_what_is_owed";

    /** Different row counts, so a count attributed to the wrong table cannot pass. */
    private static final String FIRST = "alpha";
    private static final String SECOND = "beta";
    private static final String OWED = "gamma";
    private static final Map<String, Integer> ROWS = new LinkedHashMap<>();

    static {
        ROWS.put(FIRST, 2);
        ROWS.put(SECOND, 3);
        ROWS.put(OWED, 5);
    }

    private static final String SRS_META = "srs_meta";
    private static final String CONSUMERS = "consumerOffsets";
    private static final String COMPLETED = "snapshotCompletedTables";

    @TempDir
    private Path connectorJars;

    @TempDir
    private Path sourceDirectory;

    @TempDir
    private Path targetDirectory;

    private String previousConnectorsDir;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @BeforeEach
    void publishTheConnectorJar() {
        E2eConnectorJar.buildInto(connectorJars);
        previousConnectorsDir = System.setProperty("tapstate.e2e.connectors-dir", connectorJars.toString());
    }

    @AfterEach
    void restoreTheConnectorsDirectory() {
        if (previousConnectorsDir == null) {
            System.clearProperty("tapstate.e2e.connectors-dir");
        } else {
            System.setProperty("tapstate.e2e.connectors-dir", previousConnectorsDir);
        }
    }

    @Test
    void theRunThatComesBackReadsOnlyTheTableNoSinkHadConfirmed() throws Exception {
        ROWS.forEach(this::seed);

        String storeUri = SharedMongo.replicaSetUrl("pause_in_snapshot_state");
        byte[] jar = Files.readAllBytes(E2eConnectorJar.buildInto(connectorJars));

        Map<String, Long> confirmingRunReads;
        try (ServerHandle server = InProcessServer.start(storeUri)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, jar);
            control.apply(resources());
            control.discoverSchema(NARROW_SOURCE, E2eConnectorJar.CONNECTOR_ID, sourceSettings());
            control.discoverSchema(WIDE_SOURCE, E2eConnectorJar.CONNECTOR_ID, sourceSettings());

            control.lifecycle(NARROW_PIPELINE, LifecycleVerb.START);

            // A durable synchronisation point rather than a sleep: the record itself is what says the two
            // tables are confirmed, and it is the only thing this test carries across the restart.
            awaitCompleted(storeUri, Set.of(FIRST, SECOND));
            assertThat(completedTables(storeUri))
                    .as("the third table is in no selection that has run, so nothing has confirmed it")
                    .doesNotContain(OWED);

            confirmingRunReads = control.snapshotRowsRead(NARROW_PIPELINE);

            // Paused rather than left running, so the run that comes back after the restart is the only one
            // reading, and the counts it publishes are unambiguously its own.
            control.lifecycle(NARROW_PIPELINE, LifecycleVerb.PAUSE);
        }

        assertThat(confirmingRunReads)
                .as("the first run read the two tables it selected, in full")
                .containsEntry(FIRST, 2L)
                .containsEntry(SECOND, 3L);

        try (ServerHandle server = InProcessServer.start(storeUri)) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            // The admin is already in the store this reopens, so this logs in rather than bootstrapping --
            // bootstrapping again would be a different server, not the same one coming back.
            control.login("e2e", "e2e-password");

            control.lifecycle(WIDE_PIPELINE, LifecycleVerb.START);
            Map<String, Long> owedRunReads = awaitSettledReads(control);

            assertThat(owedRunReads)
                    .as("the two tables the sink confirmed are not read again; the one it never confirmed "
                            + "is read in full, which is the accepted cost of recording that a table was "
                            + "written without recording how far its read got")
                    .containsEntry(FIRST, 0L)
                    .containsEntry(SECOND, 0L)
                    .containsEntry(OWED, 5L);

            // The other half: not what discriminates -- re-reading everything would land the same rows --
            // but a run that skipped a table nothing had confirmed would be short here.
            awaitTargetRows();
        }
    }

    /** Writes one table's file: an ordering id column the tail reads, and a name to make rows distinct. */
    private void seed(String table, int rows) {
        List<String> lines = new ArrayList<>();
        lines.add("id,name");
        for (int id = 1; id <= rows; id++) {
            lines.add(id + "," + table + "-" + id);
        }
        try {
            Files.write(sourceDirectory.resolve(table + ".csv"), lines);
        } catch (IOException e) {
            throw new UncheckedIOException("cannot seed " + table, e);
        }
    }

    /** Waits until the chain record names every table a sink is expected to have confirmed. */
    private static void awaitCompleted(String storeUri, Set<String> expected) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        List<String> last = List.of();
        while (System.nanoTime() - deadline < 0) {
            last = completedTables(storeUri);
            if (last.containsAll(expected)) {
                return;
            }
            sleep();
        }
        assertThat(last).as("tables the sink confirmed before the restart").containsAll(expected);
    }

    /**
     * The later run's per-table read counts, once they stop moving. The map is published whole once the
     * bounded round finishes, never part way through, so the wait is for the run to exist rather than for
     * the reading to complete.
     */
    private static Map<String, Long> awaitSettledReads(ControlPlane control) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        Map<String, Long> last = Map.of();
        long unchangedSince = System.nanoTime();
        while (System.nanoTime() - deadline < 0) {
            Map<String, Long> now = control.snapshotRowsRead(WIDE_PIPELINE);
            if (!now.equals(last)) {
                last = now;
                unchangedSince = System.nanoTime();
            } else if (!now.isEmpty() && System.nanoTime() - unchangedSince > SETTLE.toNanos()) {
                return now;
            }
            sleep();
        }
        assertThat(last).as("the run that came back published no per-table read counts").isNotEmpty();
        return last;
    }

    /** Waits for every seeded row to be in the target, table by table. */
    private void awaitTargetRows() {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        Map<String, Integer> last = Map.of();
        while (System.nanoTime() - deadline < 0) {
            last = targetRowCounts();
            if (last.equals(ROWS)) {
                return;
            }
            sleep();
        }
        assertThat(last).as("rows in the target once the run that came back settled").isEqualTo(ROWS);
    }

    private Map<String, Integer> targetRowCounts() {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (String table : ROWS.keySet()) {
            Path file = targetDirectory.resolve(table + ".csv");
            if (!Files.isRegularFile(file)) {
                continue;
            }
            try {
                // Every line but the header is a row.
                counts.put(table, Math.max(0, Files.readAllLines(file).size() - 1));
            } catch (IOException e) {
                throw new UncheckedIOException("cannot read the target file " + file, e);
            }
        }
        return counts;
    }

    /**
     * Every table any chain in this store has marked snapshot-complete, read straight from the collection
     * rather than through the store port -- the port is part of what is under test, and asking it whether
     * it wrote would take its own word for it.
     */
    private static List<String> completedTables(String storeUri) {
        try (MongoClient client = MongoClients.create(storeUri)) {
            String database = new ConnectionString(storeUri).getDatabase();
            List<String> tables = new ArrayList<>();
            for (Document chain : client.getDatabase(database).getCollection(SRS_META).find()) {
                // Completion is recorded against the pipeline that confirmed it, under its own consumer
                // entry: a chain-level list would hand every pipeline on the chain the first one's answer.
                // One pipeline runs here, so gathering every consumer's marks is the same set.
                if (!(chain.get(CONSUMERS) instanceof Document byPipeline)) {
                    continue;
                }
                for (String pipelineId : byPipeline.keySet()) {
                    if (byPipeline.get(pipelineId) instanceof Document record
                            && record.get(COMPLETED) instanceof List<?> entries) {
                        entries.forEach(entry -> tables.add(String.valueOf(entry)));
                    }
                }
            }
            return tables;
        }
    }

    private Map<String, Object> sourceSettings() {
        return Map.of("uri", sourceDirectory.toString());
    }

    private Map<String, String> resources() {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src_confirmed.tap.yml", sourceYaml(NARROW_SOURCE, FIRST + ", " + SECOND));
        resources.put("src_all.tap.yml", sourceYaml(WIDE_SOURCE, FIRST + ", " + SECOND + ", " + OWED));
        resources.put("tgt_files.tap.yml", targetYaml());
        resources.put("confirms_two.tap.yml",
                pipelineYaml(NARROW_PIPELINE, NARROW_SOURCE, FIRST + ", " + SECOND));
        resources.put("reads_what_is_owed.tap.yml",
                pipelineYaml(WIDE_PIPELINE, WIDE_SOURCE, FIRST + ", " + SECOND + ", " + OWED));
        return resources;
    }

    /**
     * Two sources over one directory. Their settings are identical and only their table lists differ, which
     * is what puts them on one chain: the chain is keyed by the physical coordinate and the table subset is
     * deliberately not part of it.
     */
    private String sourceYaml(String id, String tables) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(id, E2eConnectorJar.CONNECTOR_ID, sourceDirectory, tables);
    }

    private String targetYaml() {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                """
                .formatted(TARGET_ID, E2eConnectorJar.CONNECTOR_ID, targetDirectory);
    }

    private static String pipelineYaml(String id, String sourceId, String tables) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [%s], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: %s
                """
                .formatted(id, sourceId, tables, TARGET_ID);
    }

    private static void sleep() {
        try {
            Thread.sleep(POLL.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting on the run that came back", e);
        }
    }
}
