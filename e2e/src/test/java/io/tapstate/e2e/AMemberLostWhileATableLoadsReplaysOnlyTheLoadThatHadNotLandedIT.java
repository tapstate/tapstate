package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A member lost while a table's load is still being written costs that load a second reading and nothing more: a
 * table whose load had landed is not read again, the load that had not is read and written again in full, and the
 * changes made meanwhile arrive.
 *
 * <p>A load is spread over every writer of the sink in any order, so when a member dies part of a table's load is
 * written, part is in a writer's hands and part has not been handed to anyone. What was written is not recorded
 * as done row by row - only the load as a whole, once every writer has written its share - so the replacing run
 * cannot resume a load part way, and reads it again; the target, keyed, takes the rows it already has as the same
 * rows. What it must not do is read again a table whose load the target had already confirmed whole, nor start the
 * load again from nothing on a member that did not lose it.
 *
 * <p>The loading table's read is held until the landed table's load has landed, so the landed table cannot be
 * caught behind it; then a writer is held after writing one of the loading table's rows and before answering, and
 * the member that writer runs on is killed. How often each table was read is taken from the connector, which notes
 * every load it reads in a file of its process's own, so a read on the member that died is counted too.
 */
class AMemberLostWhileATableLoadsReplaysOnlyTheLoadThatHadNotLandedIT {

    private static final String LANDED = "landed_first";
    private static final String LOADING = "loading";
    private static final String PIPELINE = "lost_mid_load_pipe";
    private static final String SOURCE = "lost_mid_load_src";
    private static final String TARGET = "lost_mid_load_tgt";
    private static final long LANDED_ROWS = 5;
    private static final long LOADING_ROWS = 30;
    private static final long HELD = 20;

    /** Bound on the replacement: the cluster has to notice the member is gone, then the run is rebuilt. */
    private static final Duration TAKEOVER = Duration.ofMinutes(3);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theLoadThatHadNotLandedIsReadAgainAndTheOneThatHadIsNot(@TempDir Path directory) throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));
        Path reads = Files.createDirectories(directory.resolve("reads"));
        EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
        EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
        Holds holds = new Holds(directory.resolve("holds"));

        try (FileEndpoints files = new FileEndpoints()) {
            String store = SharedMongo.replicaSetUrl("e2e_lost_mid_load_cluster");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-lost-mid-load")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                files.seed(sourceAddress, LANDED, SeedRows.generated(LANDED_ROWS));
                files.seed(sourceAddress, LOADING, SeedRows.generated(LOADING_ROWS));
                control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put(SOURCE + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE, source, List.of(LANDED, LOADING),
                        Map.of("hold", holds.directory().toString(), "read_witness", reads.toString())));
                resources.put(TARGET + ".tap.yml",
                        Workspaces.targetYaml(TARGET, target, Map.of("hold", holds.directory().toString())));
                resources.put(PIPELINE + ".tap.yml",
                        Workspaces.pipelineYaml(PIPELINE, SOURCE, TARGET, List.of(LANDED, LOADING)));
                holds.read(LOADING);
                control.apply(resources);
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until(PIPELINE + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(1),
                        () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> control.state(PIPELINE) + ", failure " + control.failure(PIPELINE));
                Await.until("one table's load to land while the other's is held back from being read",
                        Duration.ofMinutes(1),
                        () -> landed(control, LANDED) && holds.holderOfRead(LOADING).isPresent(),
                        () -> "the snapshot read says " + control.snapshotTables(PIPELINE));

                holds.after(LOADING, HELD);
                holds.releaseRead(LOADING);
                long holder = Await.answered("a writer to write load row " + HELD + " and hold before answering",
                        Duration.ofMinutes(1), () -> holds.holderAfter(LOADING, HELD));
                assertThat(landed(control, LOADING)).as("the load being written has not landed").isFalse();

                String victim = memberRunning(cluster, holder);
                ControlPlane survivor = cluster.memberOtherThan(victim);
                long generation = survivor.executionGenerationOf(PIPELINE).orElseThrow();
                cluster.processCarrying(victim).kill();
                holds.releaseAfter(LOADING, HELD);

                // Nothing is asked of the product from here until the assertions below, bar the changes made after.
                files.cdc(sourceAddress, LANDED, CdcOp.INSERT, 1);
                files.cdc(sourceAddress, LOADING, CdcOp.INSERT, 1);
                Await.until("every row of both tables, and a change of each made after the kill, to arrive",
                        TAKEOVER,
                        () -> files.count(targetAddress, LANDED) == LANDED_ROWS + 1
                                && files.count(targetAddress, LOADING) == LOADING_ROWS + 1,
                        () -> "rows at the target: " + LANDED + " " + files.count(targetAddress, LANDED) + ", "
                                + LOADING + " " + files.count(targetAddress, LOADING) + "; the pipeline is "
                                + survivor.state(PIPELINE));
                assertThat(survivor.state(PIPELINE)).contains(PipelineState.RUNNING);
                assertThat(survivor.executionGenerationOf(PIPELINE).orElseThrow())
                        .as("the run that died was replaced").isGreaterThan(generation);
                assertThat(loadsRead(reads, LANDED))
                        .as("the table whose load had landed is not read again").isEqualTo(1);
                assertThat(loadsRead(reads, LOADING))
                        .as("the load that had not landed is read again, once").isEqualTo(2);
                Await.until("both loads to read as landed", Duration.ofMinutes(1),
                        () -> landed(survivor, LANDED) && landed(survivor, LOADING),
                        () -> "the snapshot read says " + survivor.snapshotTables(PIPELINE));
            }
        }
    }

    private static boolean landed(ControlPlane control, String table) {
        return control.snapshotTables(PIPELINE).containsKey(table)
                && control.snapshotTables(PIPELINE).get(table).landed();
    }

    /** How many times any process read {@code table}'s load, as the connector noted each read. */
    private static long loadsRead(Path reads, String table) {
        try (Stream<Path> ledgers = Files.list(reads)) {
            long count = 0;
            for (Path ledger : ledgers.toList()) {
                count += Files.readAllLines(ledger).stream()
                        .map(line -> line.split("\t", -1))
                        .filter(cells -> cells.length >= 3 && "snapshot".equals(cells[1]) && table.equals(cells[2]))
                        .count();
            }
            return count;
        } catch (IOException e) {
            throw new UncheckedIOException("reading the read witness at " + reads, e);
        }
    }

    /** The member whose process is {@code pid}. */
    private static String memberRunning(TwoMemberCluster cluster, long pid) {
        for (String node : List.of(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B)) {
            if (cluster.processCarrying(node).pid() == pid) {
                return node;
            }
        }
        throw new AssertionError("no member of the cluster runs in process " + pid);
    }
}
