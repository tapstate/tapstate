package io.tapstate.e2e;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.TableSnapshot;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The vocabulary has no live actuation-handoff fault; this case moves only that claim in its own store. */
class AMemberLosingActuationLetsGoOfItsCaptureIT {

    private static final String CONNECTOR = "mongodb";
    private static final String PIPELINE = "handoff_pipe";
    private static final String TABLE = "orders";
    private static final Duration BOUND = Duration.ofSeconds(45);

    @BeforeAll
    static void requireDockerAndConnector() {
        DockerGate.require();
        RealConnectorGate.require(CONNECTOR);
    }

    @Test
    void theSuccessorRestartsAfterPurgeWithoutRestartingTheOldCaptureHolder() {
        String sourceUri = SharedMongo.replicaSetUrl("e2e_actuation_handoff_source");
        String targetUri = SharedMongo.replicaSetUrl("e2e_actuation_handoff_target");
        try (PreparedNativeStore storage = PreparedNativeStore.create();
             MongoConfigurationDomain configuration = MongoConfigurationDomain.openExisting(
                     new MongoConnectionSettings(storage.configurationUri(), null, BOUND));
             MongoCacheDomain cache = MongoCacheDomain.connect(configuration.bindings().load().orElseThrow().cache(), BOUND, "handoff-witness");
             MongoEndpoints mongo = new MongoEndpoints();
             StoreDocuments documents = StoreDocuments.nativeSrsAt(storage.cacheUri())) {
            var claims = cache.workloadClaims(configuration.bindings().cachePreparation().orElseThrow().businessScope());
            EndpointAddress source = EndpointAddress.uri(sourceUri);
            EndpointAddress target = EndpointAddress.uri(targetUri);
            mongo.seed(source, TABLE, List.of(Map.of("id", 1L), Map.of("id", 2L)));
            try (TwoMemberCluster cluster = TwoMemberCluster.startPreparedSequentially(storage)) {
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(CONNECTOR, ConnectorJars.bytesFor(CONNECTOR));
                control.discoverSchema("handoff_src", CONNECTOR, Map.of("uri", sourceUri));
                control.apply(resources(sourceUri, targetUri));
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until("the original pipeline to deliver its load", BOUND,
                        () -> mongo.count(target, TABLE) == 2,
                        () -> "target rows=" + mongo.count(target, TABLE) + ", state=" + control.state(PIPELINE));
                Await.until("the target to confirm the original load", BOUND,
                        () -> {
                            TableSnapshot snapshot = control.snapshotTables(PIPELINE).get(TABLE);
                            return snapshot != null && snapshot.landed();
                        },
                        () -> "snapshot=" + control.snapshotTables(PIPELINE));
                Map<String, String> captures = control.captureOwnersOf(PIPELINE);
                assertThat(captures).hasSize(1);
                String captureId = captures.keySet().iterator().next();
                String oldNode = captures.get(captureId);
                assertThat(control.pipelineControllerOf(PIPELINE)).contains(oldNode);
                String successor = oldNode.equals(TwoMemberCluster.NODE_A)
                        ? TwoMemberCluster.NODE_B : TwoMemberCluster.NODE_A;
                WorkloadClaimKey capture = new WorkloadClaimKey(
                        control.clusterId(), WorkloadClaimType.CAPTURE, captureId);
                assertThat(claims.read(capture)).hasValueSatisfying(reading -> {
                    assertThat(reading.leased()).as("the original capture claim is live").isTrue();
                    assertThat(reading.claim().owner().nodeId()).isEqualTo(oldNode);
                });
                WorkloadClaimKey actuation = new WorkloadClaimKey(
                        control.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE);

                documents.handPipelineTo(control.clusterId(), PIPELINE, oldNode, successor);
                Instant handedOffLease = claims.read(actuation).orElseThrow().claim().leaseUntil();
                Await.until("the other member to hold actuation", BOUND,
                        () -> control.pipelineControllerOf(PIPELINE).filter(successor::equals).isPresent(),
                        () -> "controller=" + control.pipelineControllerOf(PIPELINE));
                // Seeing the changed document alone does not mean the old driver's cached permit is gone.
                Await.until("the successor to adopt its actuation claim", BOUND,
                        () -> claims.read(actuation)
                                .filter(reading -> reading.claim().owner().nodeId().equals(successor)
                                        && !reading.claim().leaseUntil().equals(handedOffLease)).isPresent(),
                        () -> "actuation=" + claims.read(actuation));
                Instant adoptedLease = claims.read(actuation).orElseThrow().claim().leaseUntil();
                Await.until("the successor to renew its adopted claim before the stop", BOUND,
                        () -> claims.read(actuation)
                                .filter(reading -> reading.claim().owner().nodeId().equals(successor)
                                        && reading.claim().leaseUntil().isAfter(adoptedLease)).isPresent(),
                        () -> "actuation=" + claims.read(actuation));
                control.stop(PIPELINE, true);
                Await.until("the successor to drive the purging stop", BOUND,
                        () -> control.state(PIPELINE).filter(PipelineState.STOPPED::equals).isPresent(),
                        () -> "state=" + control.state(PIPELINE));
                Await.until("the old member to stop holding the capture", BOUND,
                        () -> claims.read(capture)
                                .filter(reading -> reading.leased() && reading.claim().owner().nodeId().equals(oldNode))
                                .isEmpty(),
                        () -> "capture=" + claims.read(capture));
                assertThat(cluster.processCarrying(oldNode).isAlive()).isTrue();

                // Purging capture state requires a full load; retain the target rows from the first run.
                control.startAnswering(PIPELINE, "target-not-empty", "keep");
                Await.until("the restarted pipeline to run", BOUND,
                        () -> control.state(PIPELINE).filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> "state=" + control.state(PIPELINE));
                long before = mongo.count(target, TABLE);
                mongo.cdc(source, TABLE, CdcOp.INSERT, 1);
                Await.until("a change to reach the restarted pipeline", BOUND,
                        () -> mongo.count(target, TABLE) > before,
                        () -> "target rows=" + mongo.count(target, TABLE) + ", before=" + before);
                assertThat(cluster.processCarrying(oldNode).isAlive()).isTrue();
                control.stop(PIPELINE, false);
            }
        }
    }

    private static Map<String, String> resources(String sourceUri, String targetUri) {
        Map<String, String> resources = new LinkedHashMap<>();
        resources.put("handoff_src.tap.yml", """
                version: tapstate/v1
                kind: source
                id: handoff_src
                connector: mongodb
                config: { uri: "%s" }
                mode: cdc
                tables: [orders]
                """.formatted(sourceUri));
        resources.put("handoff_tgt.tap.yml", """
                version: tapstate/v1
                kind: source
                id: handoff_tgt
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetUri));
        resources.put("handoff_pipe.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: handoff_pipe
                source: handoff_src
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: rows_through, from: [orders], type: filter, expr: "op == 'r' || op == 'i'" }
                serve:
                  from: rows_through
                  sync:
                    - source: handoff_tgt
                """);
        return resources;
    }
}
