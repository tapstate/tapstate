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
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A change the target already holds, whose write was never confirmed back because its member died first, is written
 * again by the run that replaces it - and nothing the pipeline recorded durably ever claimed it was done.
 *
 * <p>This is the window a sink must never close early. The connector has taken the batch and put it in the table; the
 * write has not come back; the member dies. The row is in the target, so a target that is read looks fine, but a
 * pipeline that counted the write as done when the connector took it would record a position past it, and the run
 * that replaced it would start after it - with nothing lost this time only because the row happened to get in.
 * Holding the durable position behind every write that has not come back is what makes the replay owed.
 *
 * <p>So the case holds one writer after its write and before it answers, reads the durable position while it is held
 * - it must stand before the held change even though other writers have gone past it - and then kills the member
 * that writer runs on. The replacement writes the change again, and a change made after the kill arrives too.
 *
 * <p>One change moves the durable position by one, which the case measures before it relies on it: two changes are
 * confirmed one at a time first, and the case reads positions from where they left it.
 */
class AWriteTheTargetHoldsButNeverConfirmedIsWrittenAgainIT {

    private static final String TABLE = "orders";
    private static final String PIPELINE = "unconfirmed_pipe";
    private static final String SOURCE = "unconfirmed_src";
    private static final String TARGET = "unconfirmed_tgt";
    private static final long SEEDED_ROWS = 6;

    /** Bound on the replacement: the cluster has to notice the member is gone, then the run is rebuilt. */
    private static final Duration TAKEOVER = Duration.ofMinutes(3);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theHeldChangeKeepsTheDurablePositionBehindItAndIsWrittenAgainAfterItsMemberDies(@TempDir Path directory)
            throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));
        EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
        EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
        Holds holds = new Holds(directory.resolve("holds"));
        WriteWitness written = new WriteWitness(directory.resolve("written"));

        try (FileEndpoints files = new FileEndpoints()) {
            String store = SharedMongo.replicaSetUrl("e2e_unconfirmed_cluster");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-unconfirmed")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                files.seed(sourceAddress, TABLE, SeedRows.generated(SEEDED_ROWS));
                control.discoverSchema(SOURCE, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put(SOURCE + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE, source));
                resources.put(TARGET + ".tap.yml", Workspaces.targetYaml(TARGET, target, Map.of(
                        "hold", holds.directory().toString(), "write_witness", written.directory().toString())));
                resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE, TARGET, TABLE));
                control.apply(resources);
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until(PIPELINE + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(1),
                        () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> control.state(PIPELINE) + ", failure " + control.failure(PIPELINE));
                Await.until("the seeded rows to cross", Duration.ofMinutes(1),
                        () -> files.count(targetAddress, TABLE) == SEEDED_ROWS,
                        () -> "rows at the target = " + files.count(targetAddress, TABLE));

                long first = confirmOneChange(control, files, sourceAddress, targetAddress, SEEDED_ROWS + 1);
                long second = confirmOneChange(control, files, sourceAddress, targetAddress, SEEDED_ROWS + 2);
                assertThat(second)
                        .as("one change moves the durable position by one, which the positions below rely on")
                        .isEqualTo(first + 1);
                long confirmedBefore = Await.answered("the metrics to count the rows confirmed so far",
                        Duration.ofMinutes(1),
                        () -> control.recordsOut(PIPELINE).filter(count -> count >= SEEDED_ROWS + 2));

                // Changes 9 to 14; the writer that takes 10 writes it and holds before answering.
                long held = SEEDED_ROWS + 4;
                holds.after(TABLE, held);
                files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 6);
                long holder = Await.answered("a writer to write change " + held + " and hold before answering",
                        Duration.ofMinutes(1), () -> holds.holderAfter(TABLE, held));
                Await.until("a writer other than the held one to write a change after it and have it confirmed",
                        Duration.ofMinutes(1),
                        () -> !writtenPast(written, held).isEmpty() && control.recordsOut(PIPELINE).orElse(0L)
                                >= confirmedBefore + writtenPast(written, held).size(),
                        () -> "written so far: " + written.rowsOf(TABLE) + "; rows confirmed "
                                + control.recordsOut(PIPELINE) + ", " + confirmedBefore + " before");
                assertThat(files.count(targetAddress, TABLE))
                        .as("the held change is in the target: the connector took it")
                        .isGreaterThanOrEqualTo(SEEDED_ROWS + 4);
                long durable = control.ackedChangeSeq(PIPELINE).orElseThrow();
                assertThat(durable)
                        .as("the durable position stands before the held change - change %d sits at %d - even "
                                + "though another writer has written past it and had that confirmed", held,
                                second + 2)
                        .isLessThan(second + 2);

                String victim = memberRunning(cluster, holder);
                ControlPlane survivor = cluster.memberOtherThan(victim);
                long generation = survivor.executionGenerationOf(PIPELINE).orElseThrow();
                cluster.processCarrying(victim).kill();
                holds.releaseAfter(TABLE, held);

                // Nothing is asked of the product from here until the assertions below, bar the change made after.
                files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);
                long every = SEEDED_ROWS + 2 + 6 + 1;
                Await.until("every change, the one made after the kill included, to be in the target", TAKEOVER,
                        () -> files.count(targetAddress, TABLE) == every,
                        () -> "rows at the target = " + files.count(targetAddress, TABLE) + " of " + every
                                + "; the pipeline is " + survivor.state(PIPELINE));
                Await.until("the replacement to write the held change again", Duration.ofMinutes(1),
                        () -> writersOf(written, held).stream().anyMatch(writer -> !writer.startsWith(holder + ":")),
                        () -> "change " + held + " was written by " + writersOf(written, held));
                assertThat(survivor.state(PIPELINE)).contains(PipelineState.RUNNING);
                assertThat(survivor.executionGenerationOf(PIPELINE).orElseThrow())
                        .as("the run that died was replaced")
                        .isGreaterThan(generation);
                Await.until("the durable position to reach every change once they are confirmed", Duration.ofMinutes(1),
                        () -> survivor.ackedChangeSeq(PIPELINE).filter(seq -> seq >= second + 7).isPresent(),
                        () -> "the durable position is " + survivor.ackedChangeSeq(PIPELINE));
            }
        }
    }

    /** Makes change {@code id}, waits for it to be confirmed, and answers the durable position it left. */
    private static long confirmOneChange(ControlPlane control, FileEndpoints files, EndpointAddress source,
            EndpointAddress target, long id) {
        long before = control.ackedChangeSeq(PIPELINE).orElse(-1L);
        files.cdc(source, TABLE, CdcOp.INSERT, 1);
        Await.until("change " + id + " to reach the target", Duration.ofMinutes(1),
                () -> files.count(target, TABLE) == id,
                () -> "rows at the target = " + files.count(target, TABLE));
        return Await.answered("change " + id + " to be confirmed", Duration.ofMinutes(1),
                () -> control.ackedChangeSeq(PIPELINE).filter(seq -> seq > before));
    }

    /** The changes after {@code held} written so far by writers other than the one holding it. */
    private static List<WriteWitness.Written> writtenPast(WriteWitness written, long held) {
        Set<String> holding = Set.copyOf(writersOf(written, held));
        return written.rowsOf(TABLE).stream()
                .filter(row -> Long.parseLong(row.id()) > held && !holding.contains(row.writerId()))
                .toList();
    }

    /** Every writer that has written change {@code id}, in the order they wrote it. */
    private static List<String> writersOf(WriteWitness written, long id) {
        return written.rowsOf(TABLE).stream()
                .filter(row -> row.id().equals(String.valueOf(id)))
                .map(WriteWitness.Written::writerId)
                .toList();
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
