package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * What a member killed halfway through a write to the store leaves open there is let go as soon as the cluster
 * sees the member go, so the member left carries the pipeline on at once rather than when the store gives up.
 *
 * <p>A transaction holds everything it wrote until it ends, and a member killed in the middle of one never ends
 * it: the store does, by itself, a minute or more after it began. A pipeline's record on its chain is written by
 * whatever moves the pipeline on - its capture reading, its sinks reporting, its run starting - so while a
 * transaction holds that record open the pipeline stands still on every member left. Left to the store, a change
 * made after the kill reaches the target only once that minute is up; ended by the member left, within the time
 * it takes to replace the run.
 *
 * <p>Where in its writes a member is killed cannot be chosen from outside it. So the write left open is opened by
 * the case, on the pipeline's record, under the name the killed member's store client connects with - which is
 * the one thing that makes a transaction that member's - just before the member is killed. The member killed is
 * the one not driving the pipeline, so what follows the kill is its driver replacing the run, not a claim changing
 * hands and the lease that takes.
 */
class AMemberKilledMidWriteLeavesNothingHeldIT {

    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 3;

    private static final String SOURCE_ID = "held_src";
    private static final String TARGET_ID = "held_tgt";
    private static final String PIPELINE = "held_pipe";

    /**
     * Well inside the minute a store goes on holding a transaction nobody ends, and ample to replace a run: the
     * bound is what tells an ended hold from one the store is still keeping.
     */
    private static final Duration WITHIN_THE_HOLD = Duration.ofSeconds(40);

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void aChangeMadeAfterTheKillLandsWhileTheStoreWouldStillBeHoldingWhatTheMemberLeftOpen(@TempDir Path directory)
            throws IOException {
        byte[] connector = Files.readAllBytes(E2eConnectorJar.buildInto(directory));
        Path source = Files.createDirectories(directory.resolve("src"));
        Path target = Files.createDirectories(directory.resolve("tgt"));

        try (FileEndpoints files = new FileEndpoints()) {
            EndpointAddress sourceAddress = EndpointAddress.uri(source.toString());
            EndpointAddress targetAddress = EndpointAddress.uri(target.toString());
            files.seed(sourceAddress, TABLE, SeedRows.generated(SEEDED_ROWS));

            try (PreparedNativeStore storage = PreparedNativeStore.create();
                    NativeSrsDocuments documents = new NativeSrsDocuments(storage);
                    TwoMemberCluster cluster = TwoMemberCluster.startPrepared(storage)) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(E2eConnectorJar.CONNECTOR_ID, connector);
                control.discoverSchema(SOURCE_ID, E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
                control.apply(resources(source, target));
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until(PIPELINE + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(1),
                        () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> String.valueOf(control.state(PIPELINE)));
                Await.until("the seeded rows to cross before anything is killed",
                        () -> files.count(targetAddress, TABLE) >= SEEDED_ROWS,
                        () -> "rows at target = " + files.count(targetAddress, TABLE));

                String driver = Await.answered("the cluster to name the member driving the pipeline",
                        () -> control.pipelineControllerOf(PIPELINE));
                String killed = TwoMemberCluster.NODE_A.equals(driver) ? TwoMemberCluster.NODE_B : TwoMemberCluster.NODE_A;
                String killedBoot = control.clusterMembers().stream()
                        .filter(member -> killed.equals(member.nodeId()))
                        .map(ClusterMemberFacts::bootId)
                        .findFirst()
                        .orElseThrow(() -> new AssertionError("the cluster lists no boot of " + killed));
                var membership = documents.pipelineMembership(PIPELINE, SOURCE_ID);
                assertThat(membership).isNotNull();
                String chain = membership.get("_id", org.bson.Document.class).getString("chain");
                var original = documents.chain(chain);
                var captureFence = original.get("captureFence", org.bson.Document.class);
                assertThat(captureFence.getString("resourceId")).isEqualTo(original.getString("captureId"));
                assertThat(captureFence.getString("ownerNodeId"))
                        .as("the killed non-driver leaves the actual source owner alive")
                        .isEqualTo(driver);
                ControlPlane survivor = cluster.memberOtherThan(killed);

                long holdStarted = System.nanoTime();
                try (var held = new NativeHeldTransaction(storage, captureFence, MongoConnection.processTag(killedBoot))) {
                    assertThat(held.stillHeld()).as("the exact tagged session holds an actual server-side transaction").isTrue();
                    cluster.processCarrying(killed).kill();

                    long rowsAtTheKill = files.count(targetAddress, TABLE);
                    files.cdc(sourceAddress, TABLE, CdcOp.INSERT, 1);
                    Duration remaining = WITHIN_THE_HOLD.minusNanos(System.nanoTime() - holdStarted);
                    assertThat(remaining).as("the whole proof must precede natural transaction expiry").isPositive();
                    Await.until("a change made after the kill to reach the target while the store would still be "
                                    + "holding the record the killed member left open", remaining,
                            () -> files.count(targetAddress, TABLE) > rowsAtTheKill,
                            () -> "rows at target = " + files.count(targetAddress, TABLE) + ", was " + rowsAtTheKill
                                    + " when " + killed + " was killed; the pipeline is " + survivor.state(PIPELINE)
                                    + " under execution " + survivor.executionGenerationOf(PIPELINE));
                    assertThat(held.stillHeld())
                            .as("because what the killed member left open was ended when it left, well before the "
                                    + "store would have given up on it by itself")
                            .isFalse();
                }
                assertThat(survivor.state(PIPELINE)).contains(PipelineState.RUNNING);
            }
        }
    }

    private static Map<String, String> resources(Path source, Path target) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE_ID + ".tap.yml", Workspaces.cdcSourceYaml(SOURCE_ID, source));
        resources.put(TARGET_ID + ".tap.yml", Workspaces.targetYaml(TARGET_ID, target));
        resources.put(PIPELINE + ".tap.yml", Workspaces.pipelineYaml(PIPELINE, SOURCE_ID, TARGET_ID, TABLE));
        return resources;
    }
}
