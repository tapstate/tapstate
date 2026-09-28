package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.DockerGate;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A member joining the cluster leaves the work already running where it is: the member tailing a source
 * goes on tailing it, and a change made after the join reaches the pipeline reading it.
 *
 * <p>A join is the one thing that moves the cluster's committed topology on, and the capture claims were
 * renewed only under the topology they were taken under. Within one renewal of any member joining, every
 * capture tail in the cluster stopped and every pipeline reading one failed -- and stayed failed, since a
 * member joining is not a member leaving and nothing rebuilds a run for it.
 *
 * <p>What is waited for after the join is the capture's claim being renewed twice, read off the store
 * because a renewal has no read face. Twice, because a member installs the new topology on its own
 * cadence, and the first renewal after the join is committed may still be made under the topology before
 * it; the second is not, and it is the one a join used to refuse. The source and the target are real
 * Mongo: the harness's own connector replays its whole table through the ring on every run, which would
 * deliver a row to a pipeline whose tail had stopped.
 */
class AMemberJoiningLeavesTheRunningCaptureAloneIT {

    private static final String CONNECTOR = "mongodb";
    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 5;

    private static final String SOURCE_ID = "join_src";
    private static final String TARGET_ID = "join_tgt";
    private static final String PIPELINE = "join_pipe";
    private static final String JOINING = "node-c";

    /**
     * How long one renewal of the capture's claim is given: several of the product's default renewal
     * intervals. This decides only how long a claim nobody renews takes to say so.
     */
    private static final Duration RENEWAL = Duration.ofSeconds(45);

    /** How long a change is given to arrive; this decides only how long a broken tail takes to say so. */
    private static final Duration DELIVERY = Duration.ofMinutes(1);

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require(CONNECTOR);
    }

    @Test
    void aChangeMadeAfterAThirdMemberJoinsStillReachesThePipeline() {
        String sourceUri = SharedMongo.replicaSetUrl("e2e_join_src");
        String targetUri = SharedMongo.replicaSetUrl("e2e_join_tgt");
        String store = SharedMongo.replicaSetUrl("e2e_join_cluster");

        try (MongoEndpoints mongo = new MongoEndpoints(); StoreDocuments documents = StoreDocuments.at(store)) {
            EndpointAddress source = EndpointAddress.uri(sourceUri);
            EndpointAddress target = EndpointAddress.uri(targetUri);
            mongo.seed(source, TABLE, SeedRows.generated(SEEDED_ROWS));

            try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-join")) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(CONNECTOR, ConnectorJars.bytesFor(CONNECTOR));
                control.discoverSchema(SOURCE_ID, CONNECTOR, Map.of("uri", sourceUri));
                start(control, sourceUri, targetUri);
                Await.until("the load to cross", () -> mongo.count(target, TABLE) >= SEEDED_ROWS,
                        () -> "rows at the target = " + mongo.count(target, TABLE));
                Map<String, String> tailedBy = Await.answered("the cluster to name the member tailing the source",
                        () -> Optional.of(control.captureOwnersOf(PIPELINE)).filter(owners -> !owners.isEmpty()));
                long beforeTheJoin = mongo.count(target, TABLE);
                mongo.cdc(source, TABLE, CdcOp.INSERT, 1);
                Await.until("a change made before the join to arrive", DELIVERY,
                        () -> mongo.count(target, TABLE) > beforeTheJoin,
                        () -> "rows at the target = " + mongo.count(target, TABLE) + ", was " + beforeTheJoin);

                RealProcessServer third = cluster.launching(JOINING);
                try {
                    cluster.awaitMembers(3);
                    Await.until(JOINING + " to be committed to the cluster", Duration.ofMinutes(2),
                            () -> control.clusterMembers().stream().anyMatch(member ->
                                    JOINING.equals(member.nodeId()) && "ACTIVE".equals(member.state())),
                            () -> "the members were " + control.clusterMembers());
                    String clusterId = control.clusterId();
                    String captureId = tailedBy.keySet().iterator().next();
                    for (int renewal = 1; renewal <= 2; renewal++) {
                        Instant leasedUntil = documents.captureLeaseUntil(clusterId, captureId);
                        assertThat(leasedUntil).as("the store holds the claim over capture " + captureId).isNotNull();
                        Await.until("renewal " + renewal + " of the capture's claim after " + JOINING + " joined",
                                RENEWAL,
                                () -> documents.captureLeaseUntil(clusterId, captureId).isAfter(leasedUntil),
                                () -> "the claim is leased until " + documents.captureLeaseUntil(clusterId, captureId)
                                        + ", as it was before; the pipeline is " + control.state(PIPELINE));
                    }

                    assertThat(control.state(PIPELINE))
                            .describedAs("the pipeline was running before the join and nobody asked it to stop")
                            .contains(PipelineState.RUNNING);
                    assertThat(control.captureOwnersOf(PIPELINE))
                            .describedAs("the member that tailed the source before the join tails it still")
                            .isEqualTo(tailedBy);

                    long afterTheJoin = mongo.count(target, TABLE);
                    mongo.cdc(source, TABLE, CdcOp.INSERT, 1);
                    Await.until("a change made after " + JOINING + " joined to arrive", DELIVERY,
                            () -> mongo.count(target, TABLE) > afterTheJoin,
                            () -> "rows at the target = " + mongo.count(target, TABLE) + ", was " + afterTheJoin
                                    + "; the pipeline is " + control.state(PIPELINE)
                                    + " and its capture is owned by " + control.captureOwnersOf(PIPELINE));
                    assertThat(control.state(PIPELINE)).contains(PipelineState.RUNNING);
                } finally {
                    third.close();
                }
            }
        }
    }

    private static void start(ControlPlane control, String sourceUri, String targetUri) {
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
        control.apply(resources);
        control.lifecycle(PIPELINE, LifecycleVerb.START);
        Await.until(PIPELINE + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(1),
                () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(PIPELINE)));
    }
}
