package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.testsupport.DockerGate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A second pipeline over a source that is already being tailed reads it the way its own settings say,
 * whichever member ends up driving it: the rows that were there before it started, then the changes after.
 *
 * <p>The tail over a source is shared -- one capture, claimed once, running on one member -- and the
 * initial load is not: every pipeline is owed the rows its source already held. A pipeline driven by the
 * member holding the capture attaches to that tail and loads for itself. One driven by the other member
 * does the same there: it loads for itself, then reads the changes the holder's tail writes into the shared
 * ring. Left unattached instead, it ran healthy with none of the rows that were there before it started.
 * The harness's own connector cannot show that, because its tail replays the whole table on every run; a
 * real one's tail carries only what changed after it began.
 *
 * <p>START is sent through the member away from the capture. Either member can prepare the intent.
 * The first candidate is driven by the capture holder and skipped. The same loop retries until a
 * candidate's live claim and reported controller agree that it is driven away from the original holder.
 */
class ASecondPipelineOverATailedSourceLoadsWhereverItLandsIT {

    private static final String CONNECTOR = "mongodb";
    private static final String TABLE = "orders";
    private static final long SEEDED_ROWS = 5;
    private static final int MOST_PIPELINES = 6;

    private static final String SOURCE_ID = "shared_src";

    @BeforeAll
    static void requireDockerAndTheRealConnector() {
        DockerGate.require();
        RealConnectorGate.require(CONNECTOR);
    }

    @Test
    void thePipelineDrivenAwayFromTheCaptureGetsTheRowsAlreadyThereAndTheChangesAfter() {
        String run = UUID.randomUUID().toString().replace("-", "");
        String sourceUri = SharedMongo.replicaSetUrl("e2e_shared_src_" + run);
        Duration storeTimeout = Duration.ofSeconds(5);

        try (MongoEndpoints mongo = new MongoEndpoints()) {
            EndpointAddress source = EndpointAddress.uri(sourceUri);
            mongo.seed(source, TABLE, SeedRows.generated(SEEDED_ROWS));

            // Delay the request member's convergence so the capture holder prepares the first candidate.
            // Restoring its default cadence before the retry gives it a head start on later candidates.
            try (PreparedNativeStore storage = PreparedNativeStore.create();
                    TwoMemberCluster cluster = TwoMemberCluster.startPreparedSequentially(storage,
                            List.of("--tapstate.converge.interval-ms=5000"),
                            List.of("--tapstate.converge.interval-ms=300000"));
                    var configuration = MongoConfigurationDomain.openExisting(
                            new MongoConnectionSettings(storage.configurationUri(), null, storeTimeout));
                    var cache = MongoCacheDomain.connect(configuration.bindings().load().orElseThrow().cache(),
                            storeTimeout, "snapshot-placement-witness")) {
                var business = configuration.bindings().cachePreparation().orElseThrow().businessScope();
                var claims = cache.workloadClaims(business);
                cluster.awaitBothMembers();
                ControlPlane control = cluster.first();
                control.registerConnector(CONNECTOR, ConnectorJars.bytesFor(CONNECTOR));
                control.discoverSchema(SOURCE_ID, CONNECTOR, Map.of("uri", sourceUri));

                String first = "shared_pipe_1";
                EndpointAddress firstTarget = startPipelineInto(control, 1, sourceUri, run);
                Await.until("the first pipeline's load to cross",
                        () -> mongo.count(firstTarget, TABLE) >= SEEDED_ROWS,
                        () -> "rows at the first target = " + mongo.count(firstTarget, TABLE));
                Map<String, String> capture = Await.answered("the cluster to name the capture and its holder",
                        () -> Optional.of(control.captureOwnersOf(first)).filter(owners -> !owners.isEmpty()));
                String holder = capture.values().iterator().next();
                assertThat(holder).as("the controlled capture holder").isEqualTo(TwoMemberCluster.NODE_A);
                assertThat(control.pipelineControllerOf(first)).as("the first pipeline established the capture locally")
                        .contains(holder);
                String remoteMember = TwoMemberCluster.NODE_B;
                ControlPlane remote = cluster.second();
                remote.registerConnector(CONNECTOR, ConnectorJars.bytesFor(CONNECTOR));

                List<String> placements = new ArrayList<>();
                for (int n = 2; n <= MOST_PIPELINES; n++) {
                    String pipeline = "shared_pipe_" + n;
                    EndpointAddress target = startPipelineInto(remote, n, sourceUri, run);
                    String request = "pipeline=" + pipeline + ", START member=" + remoteMember + ", capture holder=" + holder;
                    Placement placement = awaitPlacement(configuration, claims, remote, pipeline, request);
                    String trace = request + ", " + placement;
                    placements.add(trace);
                    String driver = placement.controller().orElseThrow();
                    boolean remoteCandidate = isRemoteCandidate(driver, holder, remoteMember, trace);
                    if (n == 2) {
                        var claim = placement.claim().orElseThrow().claim();
                        assertThat(claim.claimGeneration()).as("the initial actuation claim, %s", trace).isEqualTo(1);
                        assertThat(placement.preparation().orElseThrow().issuerNode().owner())
                                .as("the first preparation issued the observed claim, %s", trace).isEqualTo(claim.owner());
                        assertThat(remoteCandidate).as("the local candidate is excluded, %s", trace).isFalse();
                    }
                    if (!remoteCandidate) {
                        System.out.println("Excluded local snapshot candidate: " + trace);
                        if (n == 2) {
                            // Restore normal convergence on the request member while the holder keeps tailing.
                            remote = cluster.restartSecond();
                        }
                        continue;
                    }
                    Await.until(pipeline + ", driven by " + driver + " while " + holder + " holds the capture, "
                                    + "to receive the rows seeded before any pipeline started",
                            () -> mongo.count(target, TABLE) >= SEEDED_ROWS,
                            () -> "rows at its target = " + mongo.count(target, TABLE) + " of " + SEEDED_ROWS
                                    + "; it is " + control.state(pipeline));
                    assertThat(control.captureOwnersOf(pipeline)).as("the remote pipeline uses the original capture")
                            .isEqualTo(capture);

                    long firstTargetBefore = mongo.count(firstTarget, TABLE);
                    mongo.cdc(source, TABLE, CdcOp.INSERT, 1);
                    Await.until("a change made after " + pipeline + " started to reach its target through "
                                    + "the tail on " + holder,
                            () -> mongo.count(target, TABLE) >= SEEDED_ROWS + 1,
                            () -> "rows at its target = " + mongo.count(target, TABLE) + "; it is "
                                    + control.state(pipeline));
                    Await.until("and the same change to reach the first pipeline's target",
                            () -> mongo.count(firstTarget, TABLE) > firstTargetBefore,
                            () -> "rows at the first target = " + mongo.count(firstTarget, TABLE));
                    assertThat(control.captureOwnersOf(pipeline)).as("the original capture delivered the change")
                            .isEqualTo(capture);
                    Placement afterChange = awaitPlacement(configuration, claims, remote, pipeline, request);
                    assertThat(afterChange.claim().orElseThrow().claim().owner())
                            .as("the remote member still drives the pipeline after the change, %s", afterChange)
                            .isEqualTo(placement.claim().orElseThrow().claim().owner());
                    System.out.println("Verified remote snapshot and CDC on both targets: " + trace);
                    return;
                }
                throw new AssertionError("no remote driver among " + MOST_PIPELINES + " pipelines, placements=" + placements);
            }
        }
    }

    private static boolean isRemoteCandidate(String driver, String holder, String remoteMember, String trace) {
        assertThat(driver).as("the controller belongs to one of the two members, %s", trace).isIn(holder, remoteMember);
        return !holder.equals(driver);
    }

    private record Placement(Optional<String> controller, Optional<PipelinePreparation> preparation,
            Optional<WorkloadClaimReading> claim) {
        boolean agrees() {
            return controller.isPresent() && claim.filter(WorkloadClaimReading::leased)
                    .map(reading -> reading.claim().owner().nodeId()).equals(controller);
        }
    }

    private static Placement awaitPlacement(MongoConfigurationDomain configuration, WorkloadClaimStore claims,
            ControlPlane control, String pipeline, String request) {
        var last = new AtomicReference<Placement>();
        Await.until("the API and the live claim to agree on the controller of " + pipeline, () -> {
            var controller = control.pipelineControllerOf(pipeline);
            var preparation = configuration.preparations().read(pipeline);
            var claim = preparation.filter(value -> value.phase() == PipelinePreparation.Phase.COMPLETE)
                    .flatMap(value -> claims.readPipeline(value.scope()));
            var placement = new Placement(controller, preparation, claim);
            last.set(placement);
            return placement.agrees();
        }, () -> request + ", " + last.get());
        return last.get();
    }

    private static EndpointAddress startPipelineInto(ControlPlane control, int n, String sourceUri, String run) {
        String targetUri = SharedMongo.replicaSetUrl("e2e_shared_tgt_" + n + "_" + run);
        String targetId = "shared_tgt_" + n;
        String pipeline = "shared_pipe_" + n;
        Map<String, String> resources = new LinkedHashMap<>();
        // Sent every time, unchanged: an apply resolves references within what it is given.
        resources.put(SOURCE_ID + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE_ID, CONNECTOR, sourceUri, TABLE));
        resources.put(targetId + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: %s
                config: { uri: "%s" }
                """.formatted(targetId, CONNECTOR, targetUri));
        resources.put(pipeline + ".tap.yml", """
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
                """.formatted(pipeline, SOURCE_ID, TABLE, targetId));
        control.apply(resources);
        control.lifecycle(pipeline, LifecycleVerb.START);
        Await.until(pipeline + " to reach " + PipelineState.RUNNING, Duration.ofMinutes(1),
                () -> control.state(pipeline).filter(PipelineState.RUNNING::equals).isPresent(),
                () -> String.valueOf(control.state(pipeline)));
        return EndpointAddress.uri(targetUri);
    }
}
