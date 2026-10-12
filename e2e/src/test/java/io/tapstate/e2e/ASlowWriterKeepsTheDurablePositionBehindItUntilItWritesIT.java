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
 * A writer that is slow to write a change keeps the pipeline's durable position behind that change, however far the
 * other writers get, and the position moves on once it has written.
 *
 * <p>The durable position is where a run replacing this one would start reading, so it can only stand where every
 * writer has written everything before it: the slowest writer decides it. A position taken from the fastest writer
 * would move past the slow one's change, and a run started from there would never write it.
 *
 * <p>So the case holds one writer before it writes one change, waits until other writers have written and had
 * confirmed changes after it, and reads the durable position: it must still stand before the held change. Then the
 * writer is let go, and the position moves past everything.
 *
 * <p>One change moves the durable position by one, which the case measures before it relies on it: two changes are
 * confirmed one at a time first, and the case reads positions from where they left it.
 *
 * <p>The source is a real one because the durable position is kept only for a source that says where each change
 * sits in its stream; the harness's own source says nothing of the kind, so no position would be kept to read.
 */
class ASlowWriterKeepsTheDurablePositionBehindItUntilItWritesIT {

    private static final String CONNECTOR = "mongodb";
    private static final String TABLE = "orders";
    private static final String PIPELINE = "slow_writer_pipe";
    private static final String SOURCE = "slow_writer_src";
    private static final String TARGET = "slow_writer_tgt";
    private static final long SEEDED_ROWS = 6;

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require(CONNECTOR);
    }

    @Test
    void theDurablePositionWaitsForTheSlowWriterAndMovesOnOnceItWrites(@TempDir Path directory) throws IOException {
        byte[] files = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        String sourceUri = SharedMongo.replicaSetUrl("e2e_slow_writer_src");
        Path target = Files.createDirectories(directory.resolve("tgt"));
        EndpointAddress sourceAddress = EndpointAddress.uri(sourceUri);
        EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
        Holds holds = new Holds(directory.resolve("holds"));
        WriteWitness written = new WriteWitness(directory.resolve("written"));

        try (MongoEndpoints mongo = new MongoEndpoints(); FileEndpoints file = new FileEndpoints()) {
            mongo.seed(sourceAddress, TABLE, SeedRows.generated(SEEDED_ROWS));
            String store = SharedMongo.replicaSetUrl("e2e_slow_writer_cluster");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-slow-writer")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(CONNECTOR, ConnectorJars.bytesFor(CONNECTOR));
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, files);
                control.discoverSchema(SOURCE, CONNECTOR, Map.of("uri", sourceUri));
                Map<String, String> resources = new LinkedHashMap<>();
                resources.put(SOURCE + ".tap.yml", """
                        version: tapstate/v1
                        kind: source
                        id: %s
                        connector: %s
                        config: { uri: "%s" }
                        mode: cdc
                        tables: [ %s ]
                        """.formatted(SOURCE, CONNECTOR, sourceUri, TABLE));
                resources.put(TARGET + ".tap.yml", Workspaces.targetYaml(TARGET, target, Map.of(
                        "hold", holds.directory().toString(), "write_witness", written.directory().toString())));
                resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE, TARGET, TABLE));
                control.apply(resources);
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until(PIPELINE + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(1),
                        () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> control.state(PIPELINE) + ", failure " + control.failure(PIPELINE));
                Await.until("the seeded rows to cross", Duration.ofMinutes(1),
                        () -> file.countRows(targetAddress, TABLE) == SEEDED_ROWS,
                        () -> "rows at the target = " + file.countRows(targetAddress, TABLE));

                long first = confirmOneChange(control, mongo, file, sourceAddress, targetAddress, SEEDED_ROWS + 1);
                long second = confirmOneChange(control, mongo, file, sourceAddress, targetAddress, SEEDED_ROWS + 2);
                assertThat(second)
                        .as("one change moves the durable position by one, which the positions below rely on")
                        .isEqualTo(first + 1);
                long confirmedBefore = Await.answered("the metrics to count the rows confirmed so far",
                        Duration.ofMinutes(1),
                        () -> control.recordsOut(PIPELINE).filter(count -> count >= SEEDED_ROWS + 2));

                // Changes 9 to 14; the writer that takes 10 holds before writing it.
                long held = SEEDED_ROWS + 4;
                holds.before(TABLE, held);
                mongo.cdc(sourceAddress, TABLE, CdcOp.INSERT, 6);
                Await.answered("a writer to hold change " + held + " before writing it", Duration.ofMinutes(1),
                        () -> holds.holderBefore(TABLE, held));
                Await.until("other writers to write changes after the held one and have them confirmed",
                        Duration.ofMinutes(1),
                        () -> !writtenPast(written, held).isEmpty() && control.recordsOut(PIPELINE).orElse(0L)
                                >= confirmedBefore + writtenPast(written, held).size(),
                        () -> "written past it: " + writtenPast(written, held) + "; rows confirmed "
                                + control.recordsOut(PIPELINE) + ", " + confirmedBefore + " before");
                assertThat(written.rowsOf(TABLE)).extracting(WriteWitness.Written::id)
                        .as("the held change is not written").doesNotContain(String.valueOf(held));
                assertThat(control.ackedChangeSeq(PIPELINE).orElseThrow())
                        .as("the durable position stands before the held change - change %d sits at %d - though "
                                + "other writers have written and had confirmed changes after it", held, second + 2)
                        .isLessThan(second + 2);

                holds.releaseBefore(TABLE, held);
                Await.until("every change to arrive once the writer is let go", Duration.ofMinutes(1),
                        () -> file.countRows(targetAddress, TABLE) == SEEDED_ROWS + 2 + 6,
                        () -> "rows at the target = " + file.countRows(targetAddress, TABLE));
                Await.until("the durable position to move past every change", Duration.ofMinutes(1),
                        () -> control.ackedChangeSeq(PIPELINE).filter(seq -> seq >= second + 6).isPresent(),
                        () -> "the durable position is " + control.ackedChangeSeq(PIPELINE));
            }
        }
    }

    /** Makes change {@code id}, waits for it to be confirmed, and answers the durable position it left. */
    private static long confirmOneChange(ControlPlane control, MongoEndpoints mongo, FileEndpoints file,
            EndpointAddress source, EndpointAddress target, long id) {
        long before = control.ackedChangeSeq(PIPELINE).orElse(-1L);
        mongo.cdc(source, TABLE, CdcOp.INSERT, 1);
        Await.until("change " + id + " to reach the target", Duration.ofMinutes(1),
                () -> file.countRows(target, TABLE) == id,
                () -> "rows at the target = " + file.countRows(target, TABLE));
        long[] confirmed = {-1L};
        Await.until("change " + id + " to be confirmed", Duration.ofMinutes(1),
                () -> {
                    control.ackedChangeSeq(PIPELINE).filter(seq -> seq > before).ifPresent(seq -> confirmed[0] = seq);
                    return confirmed[0] > before;
                },
                () -> "the position face reads " + control.positionReading(PIPELINE) + "; the cluster reads "
                        + control.clusterReading());
        return confirmed[0];
    }

    /** The changes after {@code held} written so far - by other writers, since the held one writes nothing. */
    private static List<WriteWitness.Written> writtenPast(WriteWitness written, long held) {
        return written.rowsOf(TABLE).stream().filter(row -> Long.parseLong(row.id()) > held).toList();
    }
}
