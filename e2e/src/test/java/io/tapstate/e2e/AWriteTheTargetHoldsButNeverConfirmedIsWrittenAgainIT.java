package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A change the target already holds, whose write was never confirmed back because its member died first, is written
 * again by the run that replaces it - and the changes confirmed before it are not.
 *
 * <p>This is the window a sink must never close early. The connector has taken the batch and put it in the table; the
 * write has not come back; the member dies. The row is in the target, so a target that is read looks fine, but a
 * pipeline that counted the write as done when the connector took it would record a position past it, and the run
 * that replaced it would start after it - with nothing lost this time only because the row happened to get in.
 * Holding the durable position behind every write that has not come back is what makes the replay owed.
 *
 * <p>So the case holds one writer after its write and before it answers, reads the durable position while it is held
 * - it must stand before the held change even though other writers have gone past it and had that confirmed - and
 * then kills the member that writer runs on. The replacement writes the held change again, and none of the changes
 * confirmed before it: a replacement starts where the durable position stands, not where its source's buffer does.
 *
 * <p>The source is a real one because only a source that says where each change sits in its stream lets a run be
 * started part way through it; the harness's own source reads its tables again from the start on every run. One
 * change moves the durable position by one, which the case measures before relying on it: two changes are confirmed
 * one at a time first, and positions are read from where they left it.
 */
class AWriteTheTargetHoldsButNeverConfirmedIsWrittenAgainIT {

    private static final String CONNECTOR = "mongodb";
    private static final String TABLE = "orders";
    private static final String PIPELINE = "unconfirmed_pipe";
    private static final String SOURCE = "unconfirmed_src";
    private static final String TARGET = "unconfirmed_tgt";
    private static final long SEEDED_ROWS = 6;

    /** Bound on the replacement: the cluster has to notice the member is gone, then the run is rebuilt. */
    private static final Duration TAKEOVER = Duration.ofMinutes(3);

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require(CONNECTOR);
    }

    @Test
    void theHeldChangeKeepsTheDurablePositionBehindItAndIsWrittenAgainAfterItsMemberDies(@TempDir Path directory)
            throws IOException {
        byte[] files = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        String sourceUri = SharedMongo.replicaSetUrl("e2e_unconfirmed_src");
        Path target = Files.createDirectories(directory.resolve("tgt"));
        EndpointAddress sourceAddress = EndpointAddress.uri(sourceUri);
        EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
        Holds holds = new Holds(directory.resolve("holds"));
        WriteWitness written = new WriteWitness(directory.resolve("written"));

        try (MongoEndpoints mongo = new MongoEndpoints(); FileEndpoints file = new FileEndpoints()) {
            mongo.seed(sourceAddress, TABLE, SeedRows.generated(SEEDED_ROWS));
            String store = SharedMongo.replicaSetUrl("e2e_unconfirmed_cluster");
            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-unconfirmed")) {
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

                long first = confirmOneChange(
                        control, mongo, file, sourceAddress, targetAddress, SEEDED_ROWS + 1, store);
                long second = confirmOneChange(
                        control, mongo, file, sourceAddress, targetAddress, SEEDED_ROWS + 2, store);
                assertThat(second)
                        .as("one change moves the durable position by one, which the positions below rely on")
                        .isEqualTo(first + 1);
                long confirmedBefore = Await.answered("the metrics to count the rows confirmed so far",
                        Duration.ofMinutes(1),
                        () -> control.recordsOut(PIPELINE).filter(count -> count >= SEEDED_ROWS + 2));

                // Changes 9 to 14; the writer that takes 10 writes it and holds before answering.
                long held = SEEDED_ROWS + 4;
                holds.after(TABLE, held);
                mongo.cdc(sourceAddress, TABLE, CdcOp.INSERT, 6);
                long holder = Await.answered("a writer to write change " + held + " and hold before answering",
                        Duration.ofMinutes(1), () -> holds.holderAfter(TABLE, held));
                Await.until("a writer other than the held one to write a change after it and have it confirmed",
                        Duration.ofMinutes(1),
                        () -> !writtenPast(written, held).isEmpty() && control.recordsOut(PIPELINE).orElse(0L)
                                >= confirmedBefore + writtenPast(written, held).size(),
                        () -> "written so far: " + written.rowsOf(TABLE) + "; rows confirmed "
                                + control.recordsOut(PIPELINE) + ", " + confirmedBefore + " before");
                assertThat(control.ackedChangeSeq(PIPELINE).orElseThrow())
                        .as("the durable position stands before the held change - change %d sits at %d - even "
                                + "though another writer has written past it and had that confirmed", held,
                                second + 2)
                        .isLessThan(second + 2);

                Set<String> writersBeforeTheKill = written.rows().stream()
                        .map(WriteWitness.Written::writerId).collect(Collectors.toSet());
                String victim = memberRunning(cluster, holder);
                ControlPlane survivor = cluster.memberOtherThan(victim);
                long generation = survivor.executionGenerationOf(PIPELINE).orElseThrow();
                cluster.processCarrying(victim).kill();
                holds.releaseAfter(TABLE, held);

                // Nothing is asked of the product from here until the assertions below, bar the change made after.
                mongo.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);
                long every = SEEDED_ROWS + 2 + 6 + 1;
                Await.until("every change, the one made after the kill included, to be in the target", TAKEOVER,
                        () -> file.countRows(targetAddress, TABLE) == every,
                        () -> "rows at the target = " + file.countRows(targetAddress, TABLE) + " of " + every
                                + "; the pipeline is " + survivor.state(PIPELINE));
                assertThat(survivor.state(PIPELINE)).contains(PipelineState.RUNNING);
                assertThat(survivor.executionGenerationOf(PIPELINE).orElseThrow())
                        .as("the run that died was replaced").isGreaterThan(generation);

                Set<Long> rewritten = written.rowsOf(TABLE).stream()
                        .filter(row -> !writersBeforeTheKill.contains(row.writerId()))
                        .map(row -> Long.parseLong(row.id()))
                        .collect(Collectors.toSet());
                assertThat(rewritten)
                        .as("the replacement wrote the change the dead member's writer held again")
                        .contains(held);
                assertThat(rewritten)
                        .as("and none of the changes confirmed before it: it started where the durable position "
                                + "stood")
                        .allSatisfy(id -> assertThat(id).isGreaterThan(SEEDED_ROWS + 2));
            }
        }
    }

    /** Makes change {@code id}, waits for it to be confirmed, and answers the durable position it left. */
    private static long confirmOneChange(ControlPlane control, MongoEndpoints mongo, FileEndpoints file,
            EndpointAddress source, EndpointAddress target, long id, String store) {
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
                        + control.clusterReading() + "; the store's records of the pipeline read "
                        + consumerRecords(store));
        return confirmed[0];
    }

    /** What the coordination store holds for this pipeline on every chain, for a failure to say. */
    private static String consumerRecords(String store) {
        try (MongoClient client = MongoClients.create(store)) {
            String database = new ConnectionString(store).getDatabase();
            List<String> records = new ArrayList<>();
            for (Document record : client.getDatabase(database)
                    .getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS).find()) {
                records.add(record.toJson());
            }
            return records.toString();
        }
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
