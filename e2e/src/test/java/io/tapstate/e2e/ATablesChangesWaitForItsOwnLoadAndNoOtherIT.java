package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A table's changes wait for its own load to land, on real members, and for nothing else: while one table's load is
 * still being written, that table's changes are held back, and another table whose load has landed goes on taking its
 * changes.
 *
 * <p>A sink's writers take a load's rows in any order, to use them all, while a table's changes go by key; so a
 * change could reach the target before the load row it follows and be overwritten by it. The load of each target
 * table is therefore waited for before that table's changes are let through - and only that table's: a pipeline
 * waiting for every table's load would stall a table whose load landed long ago behind one still going.
 *
 * <p>The late table's load is held back from being read until the early table's has landed, so the early table
 * cannot be caught behind it; then one row of the late table's load is held before it is written. The late table's
 * changes are made first and the early table's after them, so the early table's arriving is proof enough that the
 * late table's would have arrived by then had nothing held them.
 *
 * <p>Each table is read by a source of its own. A source hands its tables' loads over once it has read all of them,
 * so two tables of one source are handed over together and a read held on one holds the other's back as well:
 * they could not land apart whatever the pipeline waited for.
 */
class ATablesChangesWaitForItsOwnLoadAndNoOtherIT {

    private static final String EARLY = "early";
    private static final String LATE = "late";
    private static final String PIPELINE = "own_load_pipe";
    private static final String EARLY_SOURCE = "own_load_early_src";
    private static final String LATE_SOURCE = "own_load_late_src";
    private static final String TARGET = "own_load_tgt";
    private static final long SEEDED_ROWS = 5;
    private static final long CHANGES = 6;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theLateTablesChangesWaitForItsLoadWhileTheEarlyTablesChangesArrive(@TempDir Path directory)
            throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path earlySource = Files.createDirectories(directory.resolve("src-early"));
        Path lateSource = Files.createDirectories(directory.resolve("src-late"));
        Path target = Files.createDirectories(directory.resolve("tgt"));
        EndpointAddress earlyAddress = EndpointAddress.uri(earlySource.toString());
        EndpointAddress lateAddress = EndpointAddress.uri(lateSource.toString());
        EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
        Holds holds = new Holds(directory.resolve("holds"));
        WriteWitness written = new WriteWitness(directory.resolve("written"));

        try (FileEndpoints files = new FileEndpoints()) {
            String store = SharedMongo.replicaSetUrl("e2e_own_load_cluster");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-own-load")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                files.seed(earlyAddress, EARLY, SeedRows.generated(SEEDED_ROWS));
                files.seed(lateAddress, LATE, SeedRows.generated(SEEDED_ROWS));
                control.discoverSchema(EARLY_SOURCE, E2eConnectorJar.CONNECTOR_ID,
                        Map.of("uri", earlySource.toString()));
                control.discoverSchema(LATE_SOURCE, E2eConnectorJar.CONNECTOR_ID,
                        Map.of("uri", lateSource.toString()));
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put(EARLY_SOURCE + ".tap.yml", Workspaces.cdcSourceYaml(EARLY_SOURCE, earlySource,
                        List.of(EARLY), Map.of("hold", holds.directory().toString())));
                resources.put(LATE_SOURCE + ".tap.yml", Workspaces.cdcSourceYaml(LATE_SOURCE, lateSource,
                        List.of(LATE), Map.of("hold", holds.directory().toString())));
                resources.put(TARGET + ".tap.yml", Workspaces.targetYaml(TARGET, target, Map.of(
                        "hold", holds.directory().toString(), "write_witness", written.directory().toString())));
                resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE,
                        List.of(EARLY_SOURCE, LATE_SOURCE), TARGET, List.of(EARLY, LATE)));
                holds.read(LATE);
                control.apply(resources);
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until(PIPELINE + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(1),
                        () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> control.state(PIPELINE) + ", failure " + control.failure(PIPELINE));
                Await.until("the early table's load to land while the late table's is held back from being read",
                        Duration.ofMinutes(1),
                        () -> landed(control, EARLY) && holds.holderOfRead(LATE).isPresent(),
                        () -> "the snapshot read says " + control.snapshotTables(PIPELINE) + "; the late table's "
                                + "read is held by " + holds.holderOfRead(LATE));

                holds.before(LATE, 1);
                holds.releaseRead(LATE);
                Await.answered("a writer to hold the late table's first load row before writing it",
                        Duration.ofMinutes(1), () -> holds.holderBefore(LATE, 1));

                files.cdc(lateAddress, LATE, CdcOp.INSERT, CHANGES);
                files.cdc(earlyAddress, EARLY, CdcOp.INSERT, CHANGES);
                Await.until("a change of the early table to arrive while the late table's load is held",
                        Duration.ofMinutes(1),
                        () -> files.count(targetAddress, EARLY) > SEEDED_ROWS,
                        () -> "rows of the early table at the target = " + files.count(targetAddress, EARLY));
                assertThat(landed(control, LATE)).as("the late table's load has not landed").isFalse();
                assertThat(written.rowsOf(LATE))
                        .extracting(WriteWitness.Written::id)
                        .as("none of the late table's changes is written while its load is held - they were made "
                                + "before the early table's, which have arrived")
                        .allSatisfy(id -> assertThat(Long.parseLong(id)).isLessThanOrEqualTo(SEEDED_ROWS));

                holds.releaseBefore(LATE, 1);
                Await.until("every row of both tables to arrive once the late table's load is let go",
                        Duration.ofMinutes(1),
                        () -> files.count(targetAddress, LATE) == SEEDED_ROWS + CHANGES
                                && files.count(targetAddress, EARLY) == SEEDED_ROWS + CHANGES,
                        () -> "rows at the target: early " + files.count(targetAddress, EARLY) + ", late "
                                + files.count(targetAddress, LATE));
                Await.until("the late table's load to land", Duration.ofMinutes(1),
                        () -> landed(control, LATE),
                        () -> "the snapshot read says " + control.snapshotTables(PIPELINE));
                assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
            }
        }
    }

    private static boolean landed(ControlPlane control, String table) {
        return control.snapshotTables(PIPELINE).containsKey(table)
                && control.snapshotTables(PIPELINE).get(table).landed();
    }
}
