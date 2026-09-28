package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A pipeline driven by a real connector crosses on a two-member cluster, and carries on when the member
 * driving it is killed.
 *
 * <p>Every other multi-member case moves rows through the harness's own connector, which the product
 * never has to fetch or unpack anywhere but where the case registered it, and which hands back no
 * position at all. A real connector is the other way round on both counts: it is registered on one
 * member and each member unpacks it only when it first needs it, and the change stream it reads is
 * resumed from a position the product wrote down. So this is the one clustered case that goes through
 * the member that did not see the registration having to get the connector for itself, and through a
 * capture that has somewhere real to carry on from.
 *
 * <p>The source and the target are both Mongo, the shape a two-machine run once carried nothing through
 * while reporting nothing wrong. The same shape crosses on a single member, witnessed by the published
 * example that pairs them, so what this adds is the second member and nothing else -- and a second member
 * is what that run needed to go wrong. An initial load reaches the source through a hand-off local to the
 * member whose capture read it, and a source the engine placed on the other member found that hand-off
 * empty: the job running, the load read in full, nothing crossing. The harness connector hides this,
 * because its tail replays the whole table through the shared ring on every run.
 *
 * <p>Nothing is asked of the product between the kill and the assertions, for the reason the harness
 * connector's version of this case gives: recovering when told to is a different promise.
 *
 * <p><b>And the run that replaces the dead one carries on from where the target got to.</b> The source's
 * change buffer outlives the run that read it, so after the kill it still holds every change made before
 * it -- all of them already written and confirmed. A replacement reading that buffer from its start writes
 * them all again; one carrying on from what its target confirmed writes the change made after the kill and
 * nothing else. The count is taken once the target has confirmed everything written before the kill, so
 * nothing the dead run left unconfirmed is owed again and the only honest answer is one.
 */
class RealMongoPipelineContinuesOnSurvivingMemberIT {

    private static final String CONNECTOR = "mongodb";
    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 5;
    private static final int CHANGES_BEFORE_THE_KILL = 3;

    /** How long a published reading has to hold still to be read: metrics and positions lag the work. */
    private static final Duration SETTLED = Duration.ofSeconds(8);

    private static final String SOURCE_ID = "real_failover_src";
    private static final String TARGET_ID = "real_failover_tgt";
    private static final String PIPELINE = "real_failover_pipe";

    /**
     * Bound on the takeover: the cluster has to notice the member is gone and wait out the lease it never
     * released. Both are product timings; this decides only how long a broken failover takes to say so.
     */
    private static final Duration TAKEOVER = Duration.ofMinutes(3);

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require(CONNECTOR);
    }

    @Test
    void theCapturePickedUpBySurvivingMemberCarriesChangesMadeAfterTheKill() {
        String sourceUri = SharedMongo.replicaSetUrl("e2e_real_failover_src");
        String targetUri = SharedMongo.replicaSetUrl("e2e_real_failover_tgt");
        String store = SharedMongo.replicaSetUrl("e2e_real_failover_cluster");

        try (MongoEndpoints mongo = new MongoEndpoints()) {
            EndpointAddress source = EndpointAddress.uri(sourceUri);
            EndpointAddress target = EndpointAddress.uri(targetUri);
            mongo.seed(source, TABLE, SeedRows.generated(SEEDED_ROWS));

            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-real-failover")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                // Registered on one member only, as an operator would: the other has to fetch it.
                control.registerConnector(CONNECTOR, ConnectorJars.bytesFor(CONNECTOR));
                control.discoverSchema(SOURCE_ID, CONNECTOR, Map.of("uri", sourceUri));
                control.apply(resources(sourceUri, targetUri));
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                awaitRunning(control, PIPELINE, Duration.ofMinutes(1));

                Await.until("the seeded documents to cross on two members",
                        () -> mongo.count(target, TABLE) >= SEEDED_ROWS,
                        () -> reading(mongo, target, control));
                mongo.cdc(source, TABLE, CdcOp.INSERT, CHANGES_BEFORE_THE_KILL);
                Await.until("the changes made while both members are up to cross",
                        () -> mongo.count(target, TABLE) >= SEEDED_ROWS + CHANGES_BEFORE_THE_KILL,
                        () -> reading(mongo, target, control));
                awaitSettled("the target to confirm every change it was written", Duration.ofMinutes(1),
                        () -> control.ackedChangeSeq(PIPELINE).orElse(-1L));

                String driver = Await.answered("the cluster to name the member driving the pipeline",
                        () -> control.pipelineControllerOf(PIPELINE));
                long generationBefore = control.executionGenerationOf(PIPELINE).orElseThrow();
                assertThat(control.captureOwnersOf(PIPELINE).values())
                        .describedAs("the member driving the pipeline is also the one reading its source")
                        .containsOnly(driver);

                ControlPlane survivor = cluster.memberOtherThan(driver);
                cluster.processCarrying(driver).kill();

                // Nothing is asked of the product from here until the assertions below.
                long rowsAtTheKill = mongo.count(target, TABLE);
                mongo.cdc(source, TABLE, CdcOp.INSERT, 1);

                Await.until("a change made after the kill to reach the target", TAKEOVER,
                        () -> mongo.count(target, TABLE) > rowsAtTheKill,
                        () -> "rows at target = " + mongo.count(target, TABLE) + ", was " + rowsAtTheKill
                                + " when the member was killed; " + reading(mongo, target, survivor));

                assertThat(survivor.state(PIPELINE))
                        .describedAs("the pipeline is running again, and nobody asked it to be")
                        .contains(PipelineState.RUNNING);
                String stillHere = TwoMemberCluster.NODE_A.equals(driver)
                        ? TwoMemberCluster.NODE_B
                        : TwoMemberCluster.NODE_A;
                assertThat(survivor.captureOwnersOf(PIPELINE).values())
                        .describedAs("the source is read by the member that is still here")
                        .containsOnly(stillHere);
                assertThat(survivor.executionGenerationOf(PIPELINE))
                        .describedAs("what replaced the dead run is exactly one new execution")
                        .contains(generationBefore + 1);
                long writtenByTheReplacement = awaitSettled("the replacing run's count of confirmed rows to settle",
                        Duration.ofMinutes(1), () -> survivor.recordsOut(PIPELINE).orElse(-1L));
                assertThat(writtenByTheReplacement)
                        .describedAs("the run that replaced the dead one wrote the change made after the kill and "
                                + "none of the %d before it: they were confirmed, and the buffer that still held "
                                + "them is not where a replacement starts", CHANGES_BEFORE_THE_KILL)
                        .isEqualTo(1L);
            }
        }
    }

    /** Waits until {@code reading} holds still for {@link #SETTLED} at zero or above, and answers it. */
    private static long awaitSettled(String what, Duration bound, LongSupplier reading) {
        long[] last = {Long.MIN_VALUE};
        long[] since = {System.nanoTime()};
        Await.until(what, bound, () -> {
            long now = reading.getAsLong();
            if (now != last[0]) {
                last[0] = now;
                since[0] = System.nanoTime();
                return false;
            }
            return now >= 0 && System.nanoTime() - since[0] >= SETTLED.toNanos();
        }, () -> "last reading " + last[0]);
        return last[0];
    }

    private static String reading(MongoEndpoints mongo, EndpointAddress target, ControlPlane control) {
        return "rows at target = " + mongo.count(target, TABLE) + "; the pipeline is " + control.state(PIPELINE)
                + " and its captures are owned by " + control.captureOwnersOf(PIPELINE).values();
    }

    private static void awaitRunning(ControlPlane control, String pipelineId, Duration bound) {
        Await.until(pipelineId + " to reach " + PipelineState.RUNNING, bound,
                () -> control.state(pipelineId).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(pipelineId)));
    }

    private static Map<String, String> resources(String sourceUri, String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put(SOURCE_ID + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE_ID, CONNECTOR, sourceUri, TABLE));
        resources.put(TARGET_ID + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                """.formatted(TARGET_ID, CONNECTOR, targetUri));
        resources.put(PIPELINE + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: rows_through, from: [%s], type: filter, expr: "op == 'r' || op == 'i'" }
                serve:
                  from: rows_through
                  sync:
                    - source: %s
                """.formatted(PIPELINE, SOURCE_ID, TABLE, TARGET_ID));
        return resources;
    }
}
