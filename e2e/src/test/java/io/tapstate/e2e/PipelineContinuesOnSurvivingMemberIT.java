package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A member is killed outright and the pipeline it was running carries on, with nobody asking it to.
 *
 * <p>This is the promise the whole cluster is for, and every cheaper way of checking it passes against
 * a product that has not kept it. A job back at RUNNING says the engine put something behind the name
 * again - it does not say the capture came back, and a pipeline whose job runs over a source nobody is
 * reading is a pipeline that has stopped without saying so. A final row count says even less, because
 * the rows that crossed before the failure are still at the target and nothing removes them.
 *
 * <p>So what is asserted is what only a working failover produces: a change made <em>after</em> the
 * kill reaching the target. Everything before it is a precondition, and the readings around it say
 * which member is carrying the work, under which execution generation, and that the durable position
 * did not go backwards on the way.
 *
 * <p><b>Nothing is asked of the product between the kill and the assertions.</b> No resume, no start,
 * no touching the pipeline at all - because "it recovers if you tell it to" is a different promise, and
 * the one operators actually rely on is this one.
 *
 * <p><b>What this case cannot see, said rather than left to be assumed.</b> The harness connector
 * hands the product no position at all - every batch it delivers carries a null one, and a position
 * with no token is dropped before anything durable is written - and its tail replays its table from
 * the beginning of every run. So on this lane there is no recorded position to carry on from, and a
 * survivor that re-read everything is indistinguishable from one that resumed exactly: the idempotent
 * sink absorbs either. Where a capture resumes is asserted a level down, against the start the run is
 * handed, by a case that can see it. A position assertion written here anyway would read as coverage
 * and be none, because nothing on this lane ever produces one to compare. What this case adds is the
 * half this lane can show: that the handover happens at all, unprompted, across two real processes.
 *
 * <p>Which member to kill is read off the cluster rather than assumed: the claims say who is driving
 * the pipeline, and that is the process this ends.
 */
class PipelineContinuesOnSurvivingMemberIT {

    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 3;

    private static final String SOURCE_ID = "failover_src";
    private static final String TARGET_ID = "failover_tgt";
    private static final String PIPELINE = "failover_pipe";

    /**
     * Bound on the takeover. A killed holder releases nothing, so the cluster has to notice the member
     * is gone and then wait out the lease it never released; both are product timings, and this only
     * decides how long a broken failover takes to say so.
     */
    private static final Duration TAKEOVER = Duration.ofMinutes(3);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theWorkOfAKilledMemberIsPickedUpByTheOneLeftWithoutAnybodyAskingForIt(@TempDir Path directory)
            throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));

        try (FileEndpoints files = new FileEndpoints()) {
            EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
            EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
            files.seed(sourceAddress, TABLE, SeedRows.generated(SEEDED_ROWS));

            String store = SharedMongo.replicaSetUrl("e2e_failover_cluster");
            try (MongoClient durable = MongoClients.create(store);
                    TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-failover")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                control.discoverSchema(SOURCE_ID, E2eConnectorJar.CONNECTOR_ID,
                        Map.of("uri", source.toString()));
                control.apply(resources(source, target));
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                awaitRunning(control, PIPELINE, Duration.ofMinutes(1));
                Await.until("the seeded rows to cross before anything is killed",
                        () -> files.count(targetAddress, TABLE) >= SEEDED_ROWS,
                        () -> "rows at target = " + files.count(targetAddress, TABLE));
                Await.until("the target to confirm the initial load in the shared record",
                        () -> loadCompleted(durable, store),
                        () -> "completion mark absent for " + TABLE);
                TableSnapshot before = Await.answered("the first run's snapshot reading",
                        () -> control.snapshotTable(PIPELINE, TABLE)
                                .filter(reading -> reading.rowsDone() == SEEDED_ROWS));
                assertThat(before.rowsDone()).isEqualTo(SEEDED_ROWS);

                // Who is carrying it, said by the cluster rather than guessed.
                String driver = Await.answered("the cluster to name the member driving the pipeline",
                        () -> control.pipelineControllerOf(PIPELINE));
                long generationBefore = control.executionGenerationOf(PIPELINE).orElseThrow();
                assertThat(control.captureOwnersOf(PIPELINE).values())
                        .describedAs("the member driving the pipeline is also the one reading its source")
                        .containsOnly(driver);

                ControlPlane survivor = cluster.memberOtherThan(driver);
                cluster.processCarrying(driver).kill();

                // Nothing is asked of the product from here until the assertions below.
                long rowsAtTheKill = files.count(targetAddress, TABLE);
                files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);

                Await.until("a change made after the kill to reach the target, which no reading of the "
                                + "job's state can stand in for", TAKEOVER,
                        () -> files.count(targetAddress, TABLE) > rowsAtTheKill,
                        () -> "rows at target = " + files.count(targetAddress, TABLE)
                                + ", was " + rowsAtTheKill + " when the member was killed; the pipeline is "
                                + survivor.state(PIPELINE) + " and its captures are owned by "
                                + survivor.captureOwnersOf(PIPELINE).values());

                assertThat(survivor.state(PIPELINE))
                        .describedAs("the pipeline is running again, and nobody asked it to be")
                        .contains(PipelineState.RUNNING);
                String stillHere = TwoMemberCluster.NODE_A.equals(driver)
                        ? TwoMemberCluster.NODE_B
                        : TwoMemberCluster.NODE_A;
                assertThat(survivor.captureOwnersOf(PIPELINE).values())
                        .describedAs("the source is read by the member that is still here - a job back at "
                                + "RUNNING over a source nobody reads is the failure this rules out")
                        .containsOnly(stillHere);
                assertThat(survivor.executionGenerationOf(PIPELINE))
                        .describedAs("what replaced the dead run is one new execution, told apart from it "
                                + "by a number that only goes up. Exactly one, rather than merely more: a "
                                + "rebuild that kept going would also leave this higher, and a pipeline "
                                + "rebuilt three times before it caught is not a recovered one")
                        .contains(generationBefore + 1);
                // Landed as well as complete: the load the dead run confirmed is one its target durably holds.
                TableSnapshot completedLoad = new TableSnapshot(SEEDED_ROWS, SEEDED_ROWS, 100, true);
                Await.until("the replacement observation to retain the confirmed load without rereading it",
                        Duration.ofSeconds(30),
                        () -> Long.valueOf(0L).equals(survivor.snapshotRowsRead(PIPELINE).get(TABLE))
                                && survivor.snapshotTable(PIPELINE, TABLE)
                                        .filter(completedLoad::equals).isPresent(),
                        () -> "snapshot = " + survivor.snapshotTable(PIPELINE, TABLE)
                                + ", rows read this run = " + survivor.snapshotRowsRead(PIPELINE));
                assertThat(survivor.snapshotTable(PIPELINE, TABLE))
                        .contains(completedLoad);
                assertThat(survivor.snapshotRowsRead(PIPELINE))
                        .describedAs("the replacement run skipped the already confirmed table")
                        .containsEntry(TABLE, 0L);
            }
        }
    }

    private static boolean loadCompleted(MongoClient client, String storeUri) {
        String database = new ConnectionString(storeUri).getDatabase();
        for (Document consumer : client.getDatabase(database)
                .getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS).find()) {
            if (consumer.get("snapshotCompletedTables") instanceof List<?> tables
                    && tables.contains(TABLE)) {
                return true;
            }
        }
        return false;
    }

    private static void awaitRunning(ControlPlane control, String pipelineId, Duration bound) {
        Await.until(pipelineId + " to reach " + PipelineState.RUNNING, bound,
                () -> control.state(pipelineId).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(pipelineId)));
    }

    private static Map<String, String> resources(Path source, Path target) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE_ID + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE_ID, source));
        resources.put(TARGET_ID + ".tap.yml", Workspaces.targetYaml(TARGET_ID, target));
        resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE_ID, TARGET_ID, TABLE));
        return resources;
    }
}
