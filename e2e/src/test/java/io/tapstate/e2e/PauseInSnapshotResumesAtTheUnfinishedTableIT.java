package io.tapstate.e2e;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.SrsConsumerId;
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
 * <p>A source-side hold pauses before the third table is read. The first two tables must be confirmed
 * through their real writers before the pipeline is stopped with state kept. Its definition and target
 * roster stay fixed across replacement; releasing the third read then proves partial snapshot recovery
 * without treating a definition change as a continuation of the same target preparation.
 *
 * <p>Gated on Docker for the state store. Source and target are the harness's own file connector, so this
 * needs no directory of real connector jars and runs in the ordinary pull-request lane.
 */
class PauseInSnapshotResumesAtTheUnfinishedTableIT {

    private static final Duration TIMEOUT = Duration.ofSeconds(120);
    private static final Duration POLL = Duration.ofMillis(250);
    private static final Duration SETTLE = Duration.ofSeconds(5);

    private static final String SOURCE = "src_files";
    private static final String TARGET_ID = "tgt_files";
    private static final String PIPELINE = "reads_what_is_owed";

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

    private static final String COMPLETED = "snapshotCompletedTables";

    @TempDir
    private Path connectorJars;

    @TempDir
    private Path sourceDirectory;

    @TempDir
    private Path targetDirectory;

    @TempDir
    private Path holds;

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
        Files.writeString(holds.resolve("read-" + OWED), "");

        byte[] jar = Files.readAllBytes(E2eConnectorJar.buildInto(connectorJars));

        try (PreparedNativeStore storage = PreparedNativeStore.create();
                NativeSrsDocuments documents = new NativeSrsDocuments(storage)) {
            Map<String, Long> confirmingRunReads;
            long originalGeneration;
            try (ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, jar);
                control.apply(resources(FIRST + ", " + SECOND + ", " + OWED));
                control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, sourceSettings());

                control.lifecycle(PIPELINE, LifecycleVerb.START);

                Await.until("the third snapshot read to be held before delivering rows", TIMEOUT,
                        this::thirdReadHeld, () -> "the source has not reached its third read");
                // Completion comes from the actual target writers while the third source read is held.
                awaitCompleted(documents, Set.of(FIRST, SECOND));
                assertThat(completedTables(documents))
                        .as("the third table is selected but its held read has delivered no rows")
                        .doesNotContain(OWED);

            // The durable proof keeps its original bound. Metrics then get a separate, fresh publication
            // allowance, so completion near that bound cannot reduce this phase to a single sample.
            confirmingRunReads = control.awaitSnapshotRowsRead(PIPELINE, Set.of(FIRST, SECOND));

                originalGeneration = documents.pipelineMembership(PIPELINE, SOURCE).getLong("stateGeneration");
                control.stop(PIPELINE, false);
                Await.until("the partial snapshot to stop with its confirmed tables retained", TIMEOUT,
                        () -> control.state(PIPELINE).filter(PipelineState.STOPPED::equals).isPresent(),
                        () -> "state=" + control.state(PIPELINE) + ", failure=" + control.failureCode(PIPELINE));
                assertThat(completedTables(documents)).containsExactlyInAnyOrder(FIRST, SECOND);
            }

            assertThat(confirmingRunReads)
                    .as("the first run read the two unheld tables in full")
                    .containsEntry(FIRST, 2L)
                    .containsEntry(SECOND, 3L);
            assertThat(confirmingRunReads.getOrDefault(OWED, 0L)).isZero();
            Files.delete(holds.resolve("read-" + OWED));

            try (ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                // The admin is already in the store this reopens, so this logs in rather than bootstrapping --
                // bootstrapping again would be a different server, not the same one coming back.
                storage.loginAndAwaitBusiness(control);

                // The persisted definition is unchanged; only the held source read is now released.
                control.lifecycle(PIPELINE, LifecycleVerb.START);
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
                assertThat(documents.pipelineMembership(PIPELINE, SOURCE).getLong("stateGeneration"))
                        .isEqualTo(originalGeneration);
                assertThat(completedTables(documents)).containsExactlyInAnyOrder(FIRST, SECOND, OWED);
            }
        }
    }

    private boolean thirdReadHeld() {
        try (var files = Files.list(holds)) {
            return files.anyMatch(file -> file.getFileName().toString().startsWith("held-read-" + OWED + "-"));
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
    }

    /** Writes one table's file: an ordering id column the tail reads, and a name to make rows distinct. */
    private void seed(String table, int rows) {
        List<String> lines = new ArrayList<>();
        lines.add("id,name");
        for (int id = 1; id <= rows; id++) {
            lines.add(id + "," + table + "-" + id);
        }
        FileEndpoints.replaceTable(sourceDirectory.resolve(table + ".csv"),
                String.join(System.lineSeparator(), lines) + System.lineSeparator());
    }

    /** Waits until the chain record names every table a sink is expected to have confirmed. */
    private static void awaitCompleted(NativeSrsDocuments documents, Set<String> expected) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        List<String> last = List.of();
        while (System.nanoTime() - deadline < 0) {
            last = completedTables(documents);
            if (last.containsAll(expected)) {
                return;
            }
            sleep();
        }
        assertThat(last).as("tables the sink confirmed before the restart").containsAll(expected);
    }

    /**
     * The later run's per-table read counts, once they stop moving. Tables reach the sampled observation
     * individually after their bounded reads finish; keep the original quiet window before taking the
     * sample whose exact selected-table counts are asserted by the caller.
     */
    private static Map<String, Long> awaitSettledReads(ControlPlane control) {
        long deadline = System.nanoTime() + TIMEOUT.toNanos();
        Map<String, Long> last = Map.of();
        long unchangedSince = System.nanoTime();
        while (System.nanoTime() - deadline < 0) {
            Map<String, Long> now = control.snapshotRowsRead(PIPELINE);
            if (!now.equals(last)) {
                last = now;
                unchangedSince = System.nanoTime();
            } else if (now.getOrDefault(OWED, 0L) > 0 && System.nanoTime() - unchangedSince > SETTLE.toNanos()) {
                return now;
            }
            sleep();
        }
        assertThat(last).as("the replacement run did not publish its selected tables; state=%s failure=%s logs=%s",
                control.state(PIPELINE), control.failureCode(PIPELINE), control.logs(PIPELINE)).containsKey(OWED);
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

    /** Reads only the current native membership's exact generation and revision, without aggregating cursors. */
    private static List<String> completedTables(NativeSrsDocuments documents) {
        Document member = documents.pipelineMembership(PIPELINE, SOURCE);
        if (member == null) return List.of();
        String chain = member.get("_id", Document.class).getString("chain");
        Document cursor = documents.consumerOffset(chain, SrsConsumerId.of(PIPELINE, SOURCE).value());
        if (cursor == null || !member.get("stateGeneration").equals(cursor.get("stateGeneration"))
                || !member.get("revision").equals(cursor.get("membershipRevision"))) return List.of();
        return cursor.get("progress", Document.class).getList(COMPLETED, String.class, List.of());
    }

    private Map<String, Object> sourceSettings() {
        return Map.of("uri", sourceDirectory.toString(), "hold", holds.toString());
    }

    /** The same fixed workspace is reused across the interrupted snapshot and replacement. */
    private Map<String, String> resources(String tables) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("src_files.tap.yml", sourceYaml(SOURCE, tables));
        resources.put("tgt_files.tap.yml", targetYaml());
        resources.put("reads_what_is_owed.tap.yml", pipelineYaml(PIPELINE, SOURCE, tables));
        return resources;
    }

    /** The source hold remains part of the same connection identity across replacement. */
    private String sourceYaml(String id, String tables) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s", hold: "%s" }
                mode: cdc
                tables: [ %s ]
                """
                .formatted(id, E2eConnectorJar.CONNECTOR_ID, sourceDirectory, holds, tables);
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
