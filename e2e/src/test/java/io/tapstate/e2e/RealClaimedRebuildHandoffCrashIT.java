package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoDesiredStore;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStateStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.adapters.mongostore.MongoClusterIdentityStore;
import io.tapstate.adapters.mongostore.MongoAuthStores;
import io.tapstate.adapters.mongostore.MongoWorkloadClaimStore;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.HandoffIdentity;
import io.tapstate.spi.store.ObservationContinuation;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Offset.offset;

/** A real controller crash before replacement submission preserves a qualified floor on its survivor. */
@RequiresDocker
@EnabledIfSystemProperty(named = "tapstate.e2e.claimed-rebuild-crash.jar", matches = ".+")
class RealClaimedRebuildHandoffCrashIT {
    private static final String PREFIX = "tapstate.e2e.claimed-rebuild-crash.";
    private static final String PIPELINE = "handoff_bulk";
    private static final String TABLE = "bulk_orders";
    private static final String USER = "rebuild-crash", PASSWORD = "rebuild-crash-password";
    private static final int ROWS = 524_288, UPDATED = 7, DELETED = 11, INSERTED = ROWS + 1;
    private static final String PAYLOAD = "x".repeat(64);
    private static final Duration SETUP_WAIT = Duration.ofMinutes(3), DELIVERY_WAIT = Duration.ofMinutes(12);
    private record Series(String name, Map<String, String> attributes) { }
    private record Matched(ObservationStore.Stored publicValue, ObservationStore.StoredContinuation privateValue,
            RebuildHandoffJdiSession.Raw raw) { }
    private static final class MatchTrace {
        private final Map<String, Long> counts = new TreeMap<>();
        private String lastStage = "NOT_POLLED";
        private Map<String, Object> lastPublic = Map.of(), lastQualified = Map.of();

        void publicValue(Optional<ObservationStore.Stored> reading) {
            lastPublic = reading.map(value -> {
                Map<String, Object> evidence = new LinkedHashMap<>();
                evidence.put("state", value.observation().state().name());
                evidence.put("observedAt", value.observation().observedAt().toString());
                value.scope().ifPresent(scope -> evidence.put("scope", RebuildHandoffJdiSession.scopeEvidence(scope)));
                return evidence;
            }).orElseGet(LinkedHashMap::new);
        }

        void stage(String stage) {
            lastStage = stage;
            counts.merge(stage, 1L, Math::addExact);
        }

        void qualified(Matched value) {
            stage("QUALIFIED");
            lastQualified = matchedEvidence(value);
        }

        Map<String, Object> evidence() {
            return Map.of("action", "unchanged-native-sample-diagnostic", "stageCounts", Map.copyOf(counts),
                    "lastStage", lastStage, "lastPublic", lastPublic, "lastQualified", lastQualified,
                    "capturedAt", Instant.now().toString());
        }
    }

    @BeforeAll
    static void requireInputs() {
        RealConnectorGate.require("mysql", "mongodb");
        assertThat(Path.of(required("jar"))).isRegularFile();
        assertThat(Path.of(required("output")).isAbsolute()).as("diagnostic output is explicitly owned").isTrue();
        assertThat(required("sha256")).matches("[0-9a-f]{64}");
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    void aClaimedPreAdmissionCrashKeepsItsKnownFloorThroughSurvivorTakeover() throws Exception {
        verifyClaimedCrash(RebuildHandoffJdiSession.Cut.PRE_ADMISSION);
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    void aClaimedPostAdmissionCrashKeepsItsKnownFloorThroughSurvivorTakeover() throws Exception {
        verifyClaimedCrash(RebuildHandoffJdiSession.Cut.POST_ADMISSION_PRE_SUBMIT);
    }

    private void verifyClaimedCrash(RebuildHandoffJdiSession.Cut cut) throws Exception {
        assertThat(cut).isIn(RebuildHandoffJdiSession.Cut.PRE_ADMISSION,
                RebuildHandoffJdiSession.Cut.POST_ADMISSION_PRE_SUBMIT);
        Path jar = Path.of(required("jar")).toRealPath();
        String expectedSha = required("sha256");
        assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(expectedSha);
        Path requested = Path.of(required("output")).toAbsolutePath().normalize();
        Path output = cut == RebuildHandoffJdiSession.Cut.PRE_ADMISSION ? requested
                : requested.resolveSibling(requested.getFileName() + "." + cut.name().toLowerCase(Locale.ROOT) + ".json");
        Path harnessRoot = PipelineBenchmarkLiveRunIT.harnessRoot();
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, harnessRoot);
        Path logDirectory = output.resolveSibling(output.getFileName() + ".server-logs");
        Map<String, Object> inputs = inputHashes(harnessRoot);
        Map<String, Object> application = PipelineBenchmarkLiveRunIT.artifact(jar);
        Map<String, Object> connectors = Map.of("mysql", PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("mysql")),
                "mongodb", PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor("mongodb")));
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        report.begin(Map.of("purpose", cut == RebuildHandoffJdiSession.Cut.PRE_ADMISSION
                        ? "REAL_CLAIMED_PRE_ADMISSION_CRASH" : "REAL_CLAIMED_POST_ADMISSION_PRE_SUBMIT_CRASH",
                        "application", application,
                        "expectedJarSha256", expectedSha, "connectors", connectors, "harness", inputs,
                        "rows", ROWS, "cut", cut.name(), "serverLogDirectory", logDirectory.toString()),
                Map.of("kind", "correctness-only", "clusterProfile", "process-failure-only", "clusterMembers", 2), List.of());
        Map<String, RebuildHandoffJdiSession> observers = new LinkedHashMap<>();
        TwoMemberCluster cluster = null;
        Throwable primary = null;
        PendingProbe pending = new PendingProbe();
        try {
            String suffix = UUID.randomUUID().toString().replace("-", "").substring(0, 12);
            Map<String, Object> source = SharedMySql.settings("claimed_crash_" + suffix);
            seed(source);
            String storeUri = SharedMongo.replicaSetUrl("claimed_crash_" + suffix + "_store");
            String targetUri = SharedMongo.replicaSetUrl("claimed_crash_" + suffix + "_target");
            String operatorDatabase = "claimed_crash_" + suffix + "_operator";
            try (MongoClient storeClient = MongoClients.create(storeUri); MongoClient targetClient = MongoClients.create(targetUri)) {
                MongoDatabase database = storeClient.getDatabase(new ConnectionString(storeUri).getDatabase());
                MongoDatabase target = targetClient.getDatabase(new ConnectionString(targetUri).getDatabase());
                var actual = new MongoStateStore(database.getCollection(MongoStorePort.PIPELINE_STATE));
                var desired = new MongoDesiredStore(database.getCollection(MongoStorePort.PIPELINE_DESIRED));
                var latest = new MongoObservationStore(storeClient, database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                        database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
                var claims = new MongoWorkloadClaimStore(database.getCollection(MongoStorePort.WORKLOAD_CLAIMS));
                String clusterId;
                try (var setup = RealProcessServer.start(storeUri, operatorDatabase, jar)) {
                    ControlPlane control = new ControlPlane(setup.baseUrl());
                    control.bootstrapAndLogin(USER, PASSWORD);
                    control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                    control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                    control.apply(resources(source, targetUri));
                    control.discoverSchema("bulk_source", "mysql", source);
                    clusterId = new MongoClusterIdentityStore(database.getCollection(MongoAuthStores.CLUSTER_IDENTITY))
                            .find().orElseThrow().clusterId();
                    assertThat(database.getCollection(MongoStorePort.WORKLOAD_CLAIMS).countDocuments(
                            new Document("clusterId", clusterId).append("resourceType", "PIPELINE_ACTUATION")
                                    .append("resourceId", PIPELINE))).isZero();
                }
                cluster = TwoMemberCluster.start(storeUri, operatorDatabase, jar, clusterId, USER, PASSWORD,
                        List.of(), List.of(), (node, address, arguments, jvm) -> {
                            try {
                                var observer = RebuildHandoffJdiSession.startClaimed(jar, PIPELINE, TABLE, cut,
                                        logDirectory, node.equals(TwoMemberCluster.NODE_A) ? "membera" : "memberb",
                                        (artifact, debug) -> {
                                            List<String> options = new ArrayList<>(jvm); options.addAll(debug);
                                            return RealProcessServer.launchingWithJvmArguments(storeUri, operatorDatabase,
                                                    artifact, address, httpPort -> {
                                                        List<String> applicationArgs = new ArrayList<>(arguments.apply(httpPort));
                                                        applicationArgs.add("--tapstate.metrics.history.sample-interval=PT2S");
                                                        return List.copyOf(applicationArgs);
                                                    }, List.copyOf(options));
                                        });
                                observers.put(node, observer);
                                return observer.server();
                            } catch (RuntimeException | Error failure) { throw failure; }
                            catch (Exception failure) { throw new AssertionError("the owned observed member could not start", failure); }
                        });
                assertThat(cluster.awaitBothMembers()).containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                assertThat(cluster.second().clusterMemberNodeIds()).containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                assertThat(cluster.first().clusterId()).isEqualTo(clusterId);
                assertThat(cluster.second().clusterId()).isEqualTo(clusterId);
                assertThat(observers).containsOnlyKeys(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                WorkloadClaimKey key = new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE);
                TwoMemberCluster owned = cluster;
                owned.first().lifecycle(PIPELINE, LifecycleVerb.START);
                var beforePause = Await.answered("real claimed partial snapshot delivery with known counters and histogram", SETUP_WAIT,
                        () -> latest.readStored(PIPELINE).filter(value -> value.scope().isPresent()
                                && hasKnownDelivery(value.observation().facts())
                                && target.getCollection(TABLE).countDocuments() > 0
                                && target.getCollection(TABLE).countDocuments() < ROWS
                                && target.getCollection(TABLE).find(new Document("id", (long) DELETED)).first() != null));
                ObservationStore.Scope oldScope = beforePause.scope().orElseThrow();
                WorkloadClaim oldClaim = Await.answered("a real live controller owns the admitted source execution", SETUP_WAIT,
                        () -> claims.read(key).filter(reading -> reading.leased()
                                && reading.claim().executionGeneration() == oldScope.executionGeneration()).map(reading -> reading.claim()));
                assertThat(oldClaim.executionNodeIds()).containsExactlyInAnyOrder(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                assertThat(oldClaim.contextExecutionGeneration()).isEqualTo(oldScope.executionGeneration());
                assertThat(oldClaim.executionClaimGeneration()).isEqualTo(oldClaim.claimGeneration());
                assertNodeSession(claims, clusterId, oldClaim);
                assertThat(owned.first().pipelineControllerOf(PIPELINE)).contains(oldClaim.owner().nodeId());
                owned.first().lifecycle(PIPELINE, LifecycleVerb.PAUSE);
                Await.until("the real claimed snapshot is paused before its table is delivered", SETUP_WAIT,
                        () -> actual.read(PIPELINE).filter(value -> StateJson.parse(value.stateJson()) == PipelineState.PAUSED).isPresent()
                                && owned.first().state(PIPELINE).filter(PipelineState.PAUSED::equals).isPresent(),
                        () -> "actual=" + actual.read(PIPELINE));
                long pausedRows = target.getCollection(TABLE).countDocuments();
                assertThat(pausedRows).isBetween(1L, ROWS - 1L);
                var paused = Await.answered("known scoped claimed PAUSED frame", SETUP_WAIT,
                        () -> latest.readStored(PIPELINE).filter(value -> value.scope().equals(Optional.of(oldScope))
                                && value.observation().state() == PipelineState.PAUSED && hasKnownDelivery(value.observation().facts())));
                assertFloorAtLeast(delivery(beforePause.observation().facts()), delivery(paused.observation().facts()));
                Document resource = requireArtifact(database, oldScope);
                String ownerNode = oldClaim.owner().nodeId();
                var first = observers.get(ownerNode);
                assertThat(first).isNotNull();
                assertThat(WorkloadClaimFence.from(claims.read(key).filter(reading -> reading.leased()).orElseThrow().claim()))
                        .isEqualTo(WorkloadClaimFence.from(oldClaim));
                first.arm(oldScope);
                owned.first().lifecycle(PIPELINE, LifecycleVerb.RESUME);
                long heldDeadline = System.nanoTime() + SETUP_WAIT.toNanos();
                var held = first.awaitHeld(SETUP_WAIT);
                StopReservation marker = actual.readStopReservation(PIPELINE).orElseThrow();
                assertThat(marker.legacy()).isFalse();
                assertThat(marker.counterPolicy()).isEqualTo(StopReservation.CounterPolicy.CONTINUE);
                assertThat(marker.source().scope()).isEqualTo(oldScope);
                assertThat(marker.source().clusterId()).isEqualTo(clusterId);
                assertThat(marker.writerAuthority().standalone()).isFalse();
                WorkloadClaim heldClaim;
                long heldGeneration;
                if (cut == RebuildHandoffJdiSession.Cut.PRE_ADMISSION) {
                    assertThat(marker.phase()).isEqualTo(StopReservation.Phase.REPLACEMENT_PENDING);
                    assertThat(marker.successor()).isNull();
                    assertThat(marker.writerAuthority().claim()).isEqualTo(WorkloadClaimFence.from(oldClaim));
                    assertThat(marker.writerAuthority().executionGeneration()).isEqualTo(oldScope.executionGeneration());
                    heldClaim = oldClaim;
                    heldGeneration = oldScope.executionGeneration();
                } else {
                    assertThat(marker.phase()).isEqualTo(StopReservation.Phase.SUCCESSOR_ADMITTED);
                    heldGeneration = Math.incrementExact(oldScope.executionGeneration());
                    heldClaim = claims.read(key).filter(reading -> reading.leased()).orElseThrow().claim();
                    assertThat(heldClaim.key()).isEqualTo(oldClaim.key());
                    assertThat(heldClaim.owner()).isEqualTo(oldClaim.owner());
                    assertThat(heldClaim.claimGeneration()).isEqualTo(oldClaim.claimGeneration());
                    assertThat(heldClaim.topologyRevision()).isEqualTo(oldClaim.topologyRevision());
                    assertThat(heldClaim.executionGeneration()).isEqualTo(heldGeneration);
                    assertThat(heldClaim.contextExecutionGeneration()).isEqualTo(heldGeneration);
                    assertThat(heldClaim.executionClaimGeneration()).isEqualTo(heldClaim.claimGeneration());
                    assertThat(heldClaim.executionNodeIds()).containsExactlyInAnyOrder(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                    assertThat(heldClaim.failureClaimGeneration()).isZero();
                    assertThat(heldClaim.failureAfterMemberLoss()).isFalse();
                    assertNodeSession(claims, clusterId, heldClaim);
                    assertThat(marker.writerAuthority().claim()).isEqualTo(WorkloadClaimFence.from(heldClaim));
                    assertThat(marker.writerAuthority().executionGeneration()).isEqualTo(heldGeneration);
                    assertThat(marker.successor()).isNotNull();
                    assertThat(marker.successor().scope()).isEqualTo(held.scope());
                    assertThat(marker.successor().scope().pipelineIncarnationId()).isEqualTo(oldScope.pipelineIncarnationId());
                    assertThat(marker.successor().scope().executionGeneration()).isEqualTo(heldGeneration);
                    assertThat(marker.successor().job()).isNull();
                    assertThat(held.submittedJob()).isNull();
                    assertThat(marker.successor().submissionBootId()).isEqualTo(held.submissionBootId());
                    assertThat(marker.successor().submissionBootId()).isNotBlank();
                }
                assertThat(marker.originalDesired().targetState()).isEqualTo(PipelineState.RUNNING);
                assertThat(marker.originalDesired().rebuiltAtStateEpoch()).isNull();
                assertThat(marker.originalDesired().purgeState()).isFalse();
                assertThat(held.cut()).isEqualTo(cut);
                assertThat(held.pipelineId()).isEqualTo(PIPELINE);
                if (cut == RebuildHandoffJdiSession.Cut.PRE_ADMISSION) {
                    assertThat(held.token()).isEqualTo(marker.token());
                    assertThat(held.scope()).isEqualTo(oldScope);
                    assertThat(held.binding().signature()).contains("Ljava/util/Set;");
                } else {
                    assertThat(held.token()).as("Engine submission has no reservation-token argument").isNull();
                    assertThat(held.binding().type()).isEqualTo("io.tapstate.runtime.engine.Engine");
                    assertThat(held.binding().name()).isEqualTo("submit");
                    String peer = ownerNode.equals(TwoMemberCluster.NODE_A) ? TwoMemberCluster.NODE_B : TwoMemberCluster.NODE_A;
                    Map<String, Object> negative = new LinkedHashMap<>();
                    negative.put("held", held.evidence());
                    negative.put("marker", markerEvidence(marker));
                    negative.put("advancedClaim", claimEvidence(heldClaim));
                    try {
                        assertHeldOldCurrent(database, latest, claims, key, heldClaim, oldScope,
                                marker.successor().scope(), owned.memberOtherThan(ownerNode),
                                observers.get(peer).server().baseUrl(), heldDeadline, negative);
                    } finally {
                        negative.put("action", "held-gen2-old-current-observation-refused");
                        report.addFork(Map.copyOf(negative));
                    }
                }
                assertThat(actual.read(PIPELINE).map(value -> StateJson.parse(value.stateJson())))
                        .as(cut == RebuildHandoffJdiSession.Cut.PRE_ADMISSION
                                ? "the old native job and capture completed before admission is held"
                                : "the admitted successor has not reached native submission or binding")
                        .contains(PipelineState.STOPPED);
                var oldJobProof = first.observedJob(oldScope).orElseThrow();
                assertThat(oldJobProof.job()).isEqualTo(marker.source().oldJob());
                assertThat(oldJobProof.scope()).isEqualTo(marker.source().scope());
                assertThat(oldJobProof.returnedAtNanos()).isLessThanOrEqualTo(held.atNanos());
                assertThat(desired.read(PIPELINE)).contains(marker.originalDesired());
                assertThat(generation(database)).isEqualTo(heldGeneration);
                Document owed = requireOwedConsumer(database, owned.first());
                var carrier = Await.answered("a qualified known floor remains readable before the real controller crash", SETUP_WAIT,
                        () -> latest.readContinuation(PIPELINE).filter(saved -> saved.continuation().token().equals(marker.token())
                                && oldScope.equals(saved.continuation().sourceScope()) && saved.receipt().knownBaseline()
                                && hasKnownDelivery(saved.continuation().baselineFacts())));
                ObservationContinuation savedFloor = carrier.continuation();
                assertFloorAtLeast(delivery(paused.observation().facts()), delivery(savedFloor.baselineFacts()));
                Map<String, Object> heldTarget = targetHoldReceipt(target);
                mutateDuringCrash(source);
                Map<String, Object> changedSource = sourceChangeReceipt(source);
                assertThat(requireOwedConsumer(database, owned.first())).isEqualTo(owed);
                assertThat(targetHoldReceipt(target)).as("the terminal old job cannot consume these held-window changes")
                        .isEqualTo(heldTarget);
                assertThat(actual.readStopReservation(PIPELINE)).contains(marker);
                assertThat(generation(database)).isEqualTo(heldGeneration);
                report.addFork(Map.of("action", cut == RebuildHandoffJdiSession.Cut.PRE_ADMISSION
                                ? "qualified-claimed-pre-admission-boundary" : "qualified-claimed-post-admission-boundary",
                        "held", held.evidence(), "oldClaim", claimEvidence(oldClaim), "artifact", Map.of("id", resource.getString("_id"),
                                "incarnation", resource.getString("pipelineIncarnationId")), "marker", markerEvidence(marker),
                        "oldJobProof", oldJobProof.evidence(), "owedConsumer", owed.toJson(),
                        "carrier", continuationEvidence(carrier), "heldTarget", heldTarget));
                if (cut == RebuildHandoffJdiSession.Cut.POST_ADMISSION_PRE_SUBMIT) {
                    assertThat(WorkloadClaimFence.from(claims.read(key).filter(reading -> reading.leased()).orElseThrow().claim()))
                            .isEqualTo(WorkloadClaimFence.from(heldClaim));
                    report.addFork(Map.of("action", "actual-held-advanced-claim", "claim", claimEvidence(heldClaim),
                            "contextExecutionGeneration", heldClaim.contextExecutionGeneration(),
                            "executionClaimGeneration", heldClaim.executionClaimGeneration(),
                            "failureClaimGeneration", heldClaim.failureClaimGeneration(),
                            "failureAfterMemberLoss", heldClaim.failureAfterMemberLoss(),
                            "successorScope", RebuildHandoffJdiSession.scopeEvidence(marker.successor().scope()),
                            "submissionBootId", held.submissionBootId(), "nativeJob", "NOT_SUBMITTED_AT_HELD_ENTRY"));
                }
                report.addFork(Map.of("action", "actual-source-changes-before-controller-kill", "source", changedSource,
                        "targetUnchanged", targetHoldReceipt(target), "timing", "OLD_JOB_TERMINAL_OWNER_HELD_BEFORE_OS_KILL"));
                long killedPid = first.server().pid();
                first.killHeldProcess();
                assertThat(first.server().isAlive()).isFalse();
                report.addFork(Map.of("action", "owned-controller-os-killed", "node", ownerNode,
                        "pid", killedPid, "retainedServerOutput", first.retainedOutput().toString()));
                String survivorNode = ownerNode.equals(TwoMemberCluster.NODE_A) ? TwoMemberCluster.NODE_B : TwoMemberCluster.NODE_A;
                var restarted = observers.get(survivorNode);
                ControlPlane survivor = owned.memberOtherThan(ownerNode);
                long expectedGeneration = Math.incrementExact(heldGeneration);
                ObservationStore.Scope expected = new ObservationStore.Scope(oldScope.pipelineIncarnationId(), expectedGeneration);
                MatchTrace recoveryTrace = new MatchTrace();
                long recoveryDeadline = System.nanoTime() + SETUP_WAIT.toNanos();
                Matched firstKnown;
                try {
                    firstKnown = Await.answered("the actual survivor publishes the same floor plus its own raw native facts", SETUP_WAIT,
                            () -> {
                                pending.observe(database, latest, survivor, restarted.server().baseUrl(), expected, oldScope, recoveryDeadline);
                                return matched(latest, restarted, expected, savedFloor, null, recoveryTrace);
                            });
                } finally {
                    report.addFork(Map.of("action", "claimed-recovery-diagnostic", "sampling", recoveryTrace.evidence(),
                            "newCurrentWindow", pending.evidence(), "durableGeneration", generation(database)));
                }
                assertCumulativeExactly(firstKnown, savedFloor);
                WorkloadClaim successor = Await.answered("the survivor really owns the restored execution", SETUP_WAIT,
                        () -> claims.read(key).filter(reading -> reading.leased()
                                && reading.claim().executionGeneration() == expectedGeneration
                                && reading.claim().owner().nodeId().equals(survivorNode)).map(reading -> reading.claim()));
                assertThat(successor.claimGeneration()).isGreaterThan(oldClaim.claimGeneration());
                assertThat(successor.executionNodeIds()).containsExactly(survivorNode);
                assertNodeSession(claims, clusterId, successor);
                assertThat(survivor.pipelineControllerOf(PIPELINE)).contains(survivorNode);
                assertThat(survivor.executionGenerationOf(PIPELINE)).contains(expectedGeneration);
                assertThat(successor.contextExecutionGeneration()).isEqualTo(expectedGeneration);
                assertThat(successor.executionClaimGeneration()).isEqualTo(successor.claimGeneration());
                var actualNewJob = restarted.observedJob(expected).orElseThrow(
                        () -> new AssertionError("the survivor has no genuine native Job lookup for the restored scope"));
                assertThat(actualNewJob.scope()).isEqualTo(expected);
                assertThat(actualNewJob.job()).isEqualTo(firstKnown.raw().job());
                assertThat(firstKnown.raw().job().clusterId()).isEqualTo(clusterId);
                assertThat(firstKnown.raw().job().bootId()).isNotEqualTo(marker.source().oldJob().bootId());
                if (marker.successor() != null) {
                    assertThat(firstKnown.raw().job().bootId()).isNotEqualTo(marker.successor().submissionBootId());
                }
                assertThat(firstKnown.raw().scope()).isEqualTo(expected);
                assertThat(requireArtifact(database, expected).getString("pipelineIncarnationId"))
                        .isEqualTo(resource.getString("pipelineIncarnationId"));
                assertThat(claims.advanceUnderClaim(oldClaim, oldClaim.topologyRevision()))
                        .as("the killed controller's exact old claim cannot allocate a later execution").isEmpty();
                assertThat(generation(database)).isEqualTo(expectedGeneration);
                report.addFork(Map.of("action", "actual-survivor-claim-and-native-floor", "claim", claimEvidence(successor),
                        "job", RebuildHandoffJdiSession.jobEvidence(firstKnown.raw().job()),
                        "scope", RebuildHandoffJdiSession.scopeEvidence(expected), "rawBinding", restarted.rawBinding().evidence(),
                        "rawNative", factsEvidence(firstKnown.raw().facts()),
                        "cumulative", factsEvidence(firstKnown.publicValue().observation().facts())));
                Await.until("the actual matching handoff receipt retires its reservation", SETUP_WAIT,
                        () -> actual.readStopReservation(PIPELINE).isEmpty()
                                && actual.read(PIPELINE).map(value -> StateJson.parse(value.stateJson()))
                                        .filter(PipelineState.RUNNING::equals).isPresent(),
                        () -> "marker=" + actual.readStopReservation(PIPELINE));
                Await.until("the complete snapshot and held-window CDC reach the physical target", DELIVERY_WAIT,
                        () -> {
                            if (actual.read(PIPELINE).map(value -> StateJson.parse(value.stateJson()))
                                    .filter(PipelineState.FAILED::equals).isPresent()) {
                                throw new AssertionError("the recovered claimed pipeline failed before physical delivery: " + survivor.metrics(PIPELINE));
                            }
                            return target.getCollection(TABLE).countDocuments() == ROWS
                                    && row(target, INSERTED, "inserted-during-crash") && row(target, UPDATED, "updated-during-crash")
                                    && target.getCollection(TABLE).find(new Document("id", (long) DELETED)).first() == null
                                    && snapshotConfirmed(database, survivor);
                        }, () -> "targetRows=" + target.getCollection(TABLE).countDocuments() + ", actual=" + actual.read(PIPELINE));
                String coverageSha = assertFullTargetContent(target);
                Instant targetConfirmedAt = Instant.now();
                MatchTrace anchorTrace = new MatchTrace();
                Matched quiet;
                try {
                    quiet = Await.answered("the actual complete native workload after physical target delivery", SETUP_WAIT,
                            () -> matched(latest, restarted, expected, savedFloor, targetConfirmedAt, anchorTrace)
                                    .filter(value -> {
                                        assertCumulativeExactly(value, savedFloor);
                                        boolean complete = hasCompletedRecoveryWorkload(value.raw());
                                        anchorTrace.stage(complete ? "NATIVE_WORKLOAD_COMPLETE" : "NATIVE_WORKLOAD_INCOMPLETE");
                                        return complete;
                                    }));
                } finally { report.addFork(Map.of("action", "claimed-complete-native-anchor-diagnostic", "sampling", anchorTrace.evidence())); }
                assertCumulativeExactly(quiet, savedFloor);
                report.addFork(Map.of("action", "post-delivery-complete-native-anchor", "targetConfirmedAt", targetConfirmedAt.toString(),
                        "anchor", matchedEvidence(quiet)));
                MatchTrace repeatedTrace = new MatchTrace();
                Matched repeated;
                try {
                    repeated = Await.answered("a later unchanged native frame keeps this claimed floor added once", SETUP_WAIT,
                            () -> matched(latest, restarted, expected, savedFloor, quiet.publicValue().observation().observedAt(), repeatedTrace)
                                    .filter(value -> {
                                        assertCumulativeExactly(value, savedFloor);
                                        boolean unchanged = sameNativeTotals(quiet.raw(), value.raw());
                                        repeatedTrace.stage(unchanged ? "UNCHANGED" : "NATIVE_TOTALS_CHANGED");
                                        return unchanged;
                                    }));
                } finally { report.addFork(repeatedTrace.evidence()); }
                assertCumulativeExactly(repeated, savedFloor);
                assertSameTotals(delivery(quiet.publicValue().observation().facts()), delivery(repeated.publicValue().observation().facts()));
                assertThat(desired.read(PIPELINE)).contains(marker.originalDesired());
                assertThat(generation(database)).isEqualTo(expectedGeneration);
                assertThat(survivor.errorCount(PIPELINE)).contains(0L);
                assertThat(WorkloadClaimFence.from(claims.read(key).filter(reading -> reading.leased()).orElseThrow().claim()))
                        .isEqualTo(WorkloadClaimFence.from(successor));
                report.addFork(Map.of("action", "claimed-crash-recovered-with-fixed-native-pair", "scope", RebuildHandoffJdiSession.scopeEvidence(expected),
                        "job", RebuildHandoffJdiSession.jobEvidence(repeated.raw().job()), "claim", claimEvidence(successor),
                        "rawNative", factsEvidence(repeated.raw().facts()), "cumulative", factsEvidence(repeated.publicValue().observation().facts()),
                        "targetIdCoverageSha256", coverageSha, "sourceCheckpoint", sourceCheckpoint(database)));
                survivor.stop(PIPELINE, false);
                Await.until("the recovered claimed execution really finishes its stop", SETUP_WAIT,
                        () -> actual.read(PIPELINE).map(value -> StateJson.parse(value.stateJson()))
                                .filter(PipelineState.STOPPED::equals).isPresent(), () -> "actual=" + actual.read(PIPELINE));
            }
            try { closeOwned(observers.values(), cluster); }
            finally { observers.clear(); cluster = null; }
            assertThat(PipelineBenchmarkLiveRunIT.artifact(jar)).isEqualTo(application);
            assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(expectedSha);
            assertThat(inputHashes(harnessRoot)).isEqualTo(inputs);
            for (String connector : List.of("mysql", "mongodb")) {
                assertThat(PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(connector))).isEqualTo(connectors.get(connector));
            }
            List<String> unverified = new ArrayList<>(List.of(cut == RebuildHandoffJdiSession.Cut.PRE_ADMISSION
                            ? "POST_ADMISSION_AND_SUBMIT_PRE_BIND_CLAIMED_CRASH"
                            : "PRE_ADMISSION_AND_SUBMIT_PRE_BIND_CLAIMED_CRASH_IN_THIS_RUN",
                    "OLD_CALLBACK_WINDOWS", "NEGATIVE_UNKNOWN_BASELINE_MATRIX", "ALL_TELEMETRY_SURFACE_IDENTITIES", "FORMAL_PERFORMANCE_ACCEPTANCE"));
            if (!pending.qualified) { unverified.add("NEW_GENERATION_BEFORE_FIRST_CURRENT_PUBLICATION_WINDOW"); }
            report.completeDiagnostic(Map.of("correctness", cut == RebuildHandoffJdiSession.Cut.PRE_ADMISSION
                            ? "CLAIMED_PRE_ADMISSION_CRASH_SURVIVOR_KNOWN_FLOOR"
                            : "CLAIMED_POST_ADMISSION_PRE_SUBMIT_CRASH_SURVIVOR_KNOWN_FLOOR",
                    "performanceAcceptanceEligible", false, "newCurrentWindow", pending.evidence(), "unverified", List.copyOf(unverified)));
        } catch (Exception | Error failure) {
            primary = failure;
            try { report.fail(failure); } catch (RuntimeException reporting) { if (reporting != failure) { failure.addSuppressed(reporting); } }
            throw failure;
        } finally {
            try { closeOwned(observers.values(), cluster); }
            catch (Exception | Error cleanup) {
                if (primary == null) {
                    try { report.fail(cleanup); } catch (RuntimeException reporting) { if (reporting != cleanup) { cleanup.addSuppressed(reporting); } }
                    throw cleanup;
                }
                if (cleanup != primary) { primary.addSuppressed(cleanup); }
            }
        }
    }

    private static void assertHeldOldCurrent(MongoDatabase database, MongoObservationStore latest,
            MongoWorkloadClaimStore claims, WorkloadClaimKey key, WorkloadClaim heldClaim,
            ObservationStore.Scope old, ObservationStore.Scope expected, ControlPlane control, URI base,
            long deadline, Map<String, Object> evidence) throws Exception {
        evidence.put("status", "UNQUALIFIED");
        evidence.put("authority", RebuildHandoffJdiSession.scopeEvidence(expected));
        evidence.put("reader", base.toString());
        var before = latest.readStored(PIPELINE);
        evidence.put("retainedBefore", before.map(value -> Map.of("scope", value.scope()
                        .map(RebuildHandoffJdiSession::scopeEvidence).orElseGet(Map::of),
                "state", value.observation().state().name(), "observedAt", value.observation().observedAt().toString()))
                .orElseGet(Map::of));
        var beforeGeneration = claims.currentGeneration(key.clusterId(), PIPELINE);
        evidence.put("durableGenerationBefore", beforeGeneration.isPresent() ? beforeGeneration.getAsLong() : "ABSENT");
        assertThat(beforeGeneration).hasValue(expected.executionGeneration());
        assertThat(WorkloadClaimFence.from(claims.read(key).filter(reading -> reading.leased()).orElseThrow().claim()))
                .isEqualTo(WorkloadClaimFence.from(heldClaim));
        requireArtifact(database, expected);
        assertThat(before).as("the actual held GEN2 boundary must still retain its old GEN1 latest").isPresent();
        assertThat(before.orElseThrow().scope()).contains(old);
        long left = deadline - System.nanoTime();
        assertThat(left).as("the mandatory held-boundary read shares the original cut wait deadline").isPositive();
        HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
        HttpRequest request = HttpRequest.newBuilder(base.resolve("/api/pipelines/" + PIPELINE + "/status"))
                .timeout(Duration.ofNanos(Math.min(left, Duration.ofSeconds(20).toNanos())))
                .header("Authorization", "Bearer " + control.credential()).GET().build();
        HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
        evidence.put("httpStatus", response.statusCode());
        evidence.put("reply", response.body());
        var afterGeneration = claims.currentGeneration(key.clusterId(), PIPELINE);
        evidence.put("durableGenerationAfter", afterGeneration.isPresent() ? afterGeneration.getAsLong() : "ABSENT");
        var after = latest.readStored(PIPELINE);
        evidence.put("retainedAfter", after.map(value -> Map.of("scope", value.scope()
                        .map(RebuildHandoffJdiSession::scopeEvidence).orElseGet(Map::of),
                "state", value.observation().state().name(), "observedAt", value.observation().observedAt().toString()))
                .orElseGet(Map::of));
        var decoded = ControlPlane.interpretState(response.statusCode(), response.body(), PIPELINE);
        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(decoded).as("only monitor.no-observation is decoded as an absent state").isEmpty();
        assertThat(afterGeneration).hasValue(expected.executionGeneration());
        assertThat(WorkloadClaimFence.from(claims.read(key).filter(reading -> reading.leased()).orElseThrow().claim()))
                .isEqualTo(WorkloadClaimFence.from(heldClaim));
        requireArtifact(database, expected);
        assertThat(after).as("the same old latest remains physical evidence across this negative read").contains(before.orElseThrow());
        evidence.put("status", "QUALIFIED_HELD_GEN2_AUTHORITY_OLD_GEN1_LATEST_404");
    }

    private static void closeOwned(Collection<RebuildHandoffJdiSession> observers, TwoMemberCluster cluster) throws Exception {
        Throwable cleanup = null;
        for (var observer : observers) {
            try { observer.close(); } catch (Exception | Error failure) {
                if (cleanup == null) { cleanup = failure; } else if (failure != cleanup) { cleanup.addSuppressed(failure); }
            }
        }
        if (cluster != null) {
            try { cluster.close(); } catch (RuntimeException | Error failure) {
                if (cleanup == null) { cleanup = failure; } else if (failure != cleanup) { cleanup.addSuppressed(failure); }
            }
        }
        if (cleanup instanceof Exception exception) { throw exception; }
        if (cleanup instanceof Error error) { throw error; }
    }

    private static Optional<Matched> matched(MongoObservationStore latest, RebuildHandoffJdiSession observer,
            ObservationStore.Scope expected, ObservationContinuation floor, Instant after) {
        return matched(latest, observer, expected, floor, after, null);
    }

    private static Optional<Matched> matched(MongoObservationStore latest, RebuildHandoffJdiSession observer,
            ObservationStore.Scope expected, ObservationContinuation floor, Instant after, MatchTrace trace) {
        var current = latest.readStored(PIPELINE);
        if (trace != null) { trace.publicValue(current); }
        if (current.isEmpty()) { return rejected(trace, "PUBLIC_ABSENT"); }
        ObservationStore.Stored publicValue = current.orElseThrow();
        if (publicValue.scope().filter(expected::equals).isEmpty()) { return rejected(trace, "PUBLIC_SCOPE_MISMATCH"); }
        if (publicValue.observation().state() != PipelineState.RUNNING) { return rejected(trace, "PUBLIC_NOT_RUNNING"); }
        if (!hasKnownDelivery(publicValue.observation().facts())) { return rejected(trace, "PUBLIC_DELIVERY_UNKNOWN"); }
        if (after != null && !publicValue.observation().observedAt().isAfter(after)) { return rejected(trace, "PUBLIC_TIME_NOT_ADVANCED"); }
        var raw = observer.rawAt(expected, publicValue.observation().observedAt()).filter(value -> hasKnownDelivery(value.facts()));
        if (raw.isEmpty()) { return rejected(trace, "RAW_ABSENT_OR_UNKNOWN"); }
        var privateValue = latest.readContinuation(PIPELINE);
        if (privateValue.isEmpty()) { return rejected(trace, "CONTINUATION_ABSENT"); }
        var measured = raw.orElseThrow(); var saved = privateValue.orElseThrow();
        HandoffIdentity identity = new HandoffIdentity(PIPELINE, floor.token(), StopReservation.CounterPolicy.CONTINUE,
                floor.sourceScope(), expected, measured.job());
        if (!saved.receipt().knownBaseline()) { return rejected(trace, "BASELINE_UNKNOWN"); }
        if (!saved.receipt().matches(identity)) { return rejected(trace, "RECEIPT_MISMATCH"); }
        if (!latest.readStored(PIPELINE).filter(publicValue::equals).isPresent()) { return rejected(trace, "PUBLIC_CHANGED_DURING_READ"); }
        Matched result = new Matched(publicValue, saved, measured);
        if (trace != null) { trace.qualified(result); }
        return Optional.of(result);
    }

    private static Optional<Matched> rejected(MatchTrace trace, String stage) {
        if (trace != null) { trace.stage(stage); }
        return Optional.empty();
    }

    private static Map<String, Object> matchedEvidence(Matched value) {
        return Map.of("scope", RebuildHandoffJdiSession.scopeEvidence(value.raw().scope()),
                "job", RebuildHandoffJdiSession.jobEvidence(value.raw().job()),
                "observedAt", value.publicValue().observation().observedAt().toString(),
                "rawNative", factsEvidence(value.raw().facts()),
                "cumulative", factsEvidence(value.publicValue().observation().facts()));
    }

    private static Map<String, Object> continuationEvidence(ObservationStore.StoredContinuation stored) {
        ObservationContinuation value = stored.continuation();
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("token", value.token());
        if (value.sourceScope() != null) { evidence.put("sourceScope", RebuildHandoffJdiSession.scopeEvidence(value.sourceScope())); }
        value.target().ifPresent(target -> evidence.put("target", targetEvidence(target)));
        value.baselineOrigin().ifPresent(origin -> evidence.put("baselineOrigin", targetEvidence(origin)));
        evidence.put("knownBaseline", value.knownBaseline());
        evidence.put("baselineFacts", factsEvidence(value.baselineFacts()));
        evidence.put("producerStates", value.producerStates().stream().map(state -> Map.of(
                "name", state.name(), "nativeStart", state.nativeStart().toString(),
                "offsetPoints", state.offsets().size(), "publishedPoints", state.published().size())).toList());
        evidence.put("receipt", Map.of("revision", stored.receipt().revision(), "digest", stored.receipt().digest(),
                "encodingVersion", stored.receipt().encodingVersion(), "knownBaseline", stored.receipt().knownBaseline()));
        return evidence;
    }

    private static Map<String, Object> targetEvidence(ObservationContinuation.Target target) {
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("scope", RebuildHandoffJdiSession.scopeEvidence(target.scope()));
        target.realJob().ifPresent(job -> evidence.put("job", RebuildHandoffJdiSession.jobEvidence(job)));
        evidence.put("realJobAvailability", target.realJob().isPresent() ? "PRESENT" : "ABSENT");
        return evidence;
    }

    private static void assertCumulativeExactly(Matched reading, ObservationContinuation floor) {
        Map<Series, MetricPoint> base = points(delivery(floor.baselineFacts()));
        Map<Series, MetricPoint> raw = points(reading.raw().facts());
        Map<Series, MetricPoint> published = points(delivery(reading.publicValue().observation().facts()));
        assertSameTotals(delivery(floor.baselineFacts()), delivery(reading.privateValue().continuation().baselineFacts()));
        assertThat(raw.keySet()).containsAll(base.keySet());
        assertThat(published.keySet()).isEqualTo(raw.keySet());
        for (var entry : raw.entrySet()) {
            Series series = entry.getKey(); MetricPoint old = base.get(series);
            MetricPoint nativePoint = entry.getValue(); MetricPoint cumulative = published.get(series);
            assertThat(cumulative).as("the same measured series is publicly present").isNotNull();
            if (old == null) {
                assertThat(cumulative.startTime().toEpochMilli()).isEqualTo(nativePoint.startTime().toEpochMilli());
                assertThat(cumulative.value()).isEqualTo(nativePoint.value());
                assertThat(cumulative.histogram()).isEqualTo(nativePoint.histogram());
                continue;
            }
            assertThat(cumulative.startTime()).as("a rebuilt physical execution does not reset logical counting").isEqualTo(old.startTime());
            assertThat(nativePoint.startTime()).isNotNull().isAfter(old.startTime());
            if (old.histogram() == null) {
                assertThat(cumulative.value()).as("known floor plus actual native value, added once")
                        .isEqualTo(Math.addExact(old.value(), nativePoint.value()));
            } else {
                HistogramValue before = old.histogram(), measured = nativePoint.histogram(), after = cumulative.histogram();
                assertThat(after.bounds()).isEqualTo(before.bounds()).isEqualTo(measured.bounds());
                assertThat(after.count()).isEqualTo(Math.addExact(before.count(), measured.count()));
                double sum = before.sum() + measured.sum();
                assertThat(after.sum()).isCloseTo(sum, offset(Math.max(1e-8, Math.abs(sum) * 1e-9)));
                for (int i = 0; i < before.bucketCounts().size(); i++) {
                    assertThat(after.bucketCounts().get(i)).isEqualTo(Math.addExact(before.bucketCounts().get(i), measured.bucketCounts().get(i)));
                }
            }
        }
    }

    private static boolean sameNativeTotals(RebuildHandoffJdiSession.Raw before, RebuildHandoffJdiSession.Raw after) {
        return before.scope().equals(after.scope()) && before.job().equals(after.job())
                && points(before.facts()).keySet().equals(points(after.facts()).keySet())
                && points(before.facts()).entrySet().stream().allMatch(entry -> {
                    MetricPoint next = points(after.facts()).get(entry.getKey()); MetricPoint old = entry.getValue();
                    return Objects.equals(old.startTime(), next.startTime()) && Objects.equals(old.value(), next.value())
                            && Objects.equals(old.histogram(), next.histogram());
                });
    }

    /** The unfinished table is read in full on recovery, then its three crash-time changes are replayed. */
    private static boolean hasCompletedRecoveryWorkload(RebuildHandoffJdiSession.Raw raw) {
        Map<String, Long> operations = new HashMap<>();
        HistogramValue delivered = null;
        for (MetricFact fact : delivery(raw.facts())) {
            for (MetricPoint point : fact.points()) {
                if ("tapstate.pipeline.records".equals(fact.name())) {
                    String operation = point.attributes().get(MetricAttributes.OP);
                    assertThat(operation).isIn("read", "insert", "update", "delete");
                    assertThat(operations.put(operation, point.value())).as("one raw delivery counter per operation").isNull();
                    assertThat(point.value()).as("the recovered fixture does not deliver additional %s records", operation)
                            .isLessThanOrEqualTo("read".equals(operation) ? ROWS : 1L);
                } else if (fact.type() == MetricType.HISTOGRAM) {
                    assertThat(delivered).as("one raw delivery histogram for the recovered table").isNull();
                    delivered = point.histogram();
                    assertThat(delivered.count()).isLessThanOrEqualTo(ROWS + 3L);
                }
            }
        }
        return operations.equals(Map.of("read", (long) ROWS, "insert", 1L, "update", 1L, "delete", 1L))
                && delivered != null && delivered.count() == ROWS + 3L;
    }

    private static List<MetricFact> delivery(List<MetricFact> facts) {
        return facts.stream().filter(fact -> RebuildHandoffJdiSession.INSTRUMENTS.contains(fact.name()))
                .map(fact -> new MetricFact(fact.name(), fact.type(), fact.unit(), fact.points().stream()
                        .filter(point -> TABLE.equals(point.attributes().get(MetricAttributes.TABLE_ID))
                                && (fact.type() == MetricType.HISTOGRAM || "out".equals(point.attributes().get(MetricAttributes.DIRECTION)))).toList()))
                .filter(fact -> !fact.points().isEmpty()).sorted(Comparator.comparing(MetricFact::name)).toList();
    }
    private static boolean hasKnownDelivery(List<MetricFact> facts) {
        var selected = delivery(facts);
        return selected.size() == RebuildHandoffJdiSession.INSTRUMENTS.size() && selected.stream().allMatch(fact ->
                fact.points().stream().allMatch(point -> point.startTime() != null
                        && (point.histogram() == null ? point.value() > 0 : point.histogram().count() > 0)));
    }
    private static Map<Series, MetricPoint> points(List<MetricFact> facts) {
        Map<Series, MetricPoint> points = new HashMap<>();
        facts.forEach(fact -> fact.points().forEach(point -> points.put(new Series(fact.name(), point.attributes()), point)));
        return points;
    }
    private static void assertSameTotals(List<MetricFact> before, List<MetricFact> after) {
        Map<Series, MetricPoint> old = points(before), next = points(after);
        assertThat(next.keySet()).isEqualTo(old.keySet());
        old.forEach((series, point) -> {
            assertThat(next.get(series).startTime()).isEqualTo(point.startTime());
            assertThat(next.get(series).value()).isEqualTo(point.value());
            assertThat(next.get(series).histogram()).isEqualTo(point.histogram());
        });
    }
    private static void assertFloorAtLeast(List<MetricFact> before, List<MetricFact> after) {
        Map<Series, MetricPoint> old = points(before), next = points(after);
        assertThat(next.keySet()).containsAll(old.keySet());
        old.forEach((series, point) -> {
            MetricPoint kept = next.get(series); assertThat(kept.startTime()).isEqualTo(point.startTime());
            if (point.histogram() == null) { assertThat(kept.value()).isGreaterThanOrEqualTo(point.value()); }
            else {
                HistogramValue oldHistogram = point.histogram(), keptHistogram = kept.histogram();
                assertThat(keptHistogram.bounds()).isEqualTo(oldHistogram.bounds());
                assertThat(keptHistogram.count()).isGreaterThanOrEqualTo(oldHistogram.count());
                double tolerance = Math.max(1e-8, Math.abs(oldHistogram.sum()) * 1e-9);
                assertThat(keptHistogram.sum()).isGreaterThanOrEqualTo(oldHistogram.sum() - tolerance);
                for (int i = 0; i < oldHistogram.bucketCounts().size(); i++) {
                    assertThat(keptHistogram.bucketCounts().get(i)).isGreaterThanOrEqualTo(oldHistogram.bucketCounts().get(i));
                }
            }
        });
    }

    private static long generation(MongoDatabase database) {
        Document current = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                .find(new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", PIPELINE)).first();
        assertThat(current).as("the unique durable generation document exists").isNotNull();
        return ((Number) current.get("executionGeneration")).longValue();
    }
    private static boolean snapshotConfirmed(MongoDatabase database, ControlPlane control) {
        Document cursor = requireConsumer(database, control);
        return cursor.get("snapshotCompletedTables") instanceof List<?> tables && tables.contains(TABLE);
    }
    private static Map<String, Object> sourceCheckpoint(MongoDatabase database) {
        return Map.of("meta", database.getCollection(MongoStorePort.SRS_META).find().limit(16).into(new ArrayList<>()).stream()
                        .map(Document::toJson).toList(),
                "consumers", database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS).find().limit(16).into(new ArrayList<>()).stream()
                        .map(Document::toJson).toList());
    }
    private static boolean row(MongoDatabase database, long id, String payload) {
        Document row = database.getCollection(TABLE).find(new Document("id", id)).first();
        return row != null && payload.equals(row.getString("payload"));
    }
    private static String assertFullTargetContent(MongoDatabase database) throws Exception {
        BitSet actual = new BitSet(INSERTED + 1); int count = 0;
        try (var rows = database.getCollection(TABLE).find().batchSize(1_024).iterator()) {
            while (rows.hasNext()) {
                Document row = rows.next();
                assertThat(++count).as("the real target cursor is bounded by the complete source set").isLessThanOrEqualTo(ROWS);
                assertThat(row.get("id")).isInstanceOf(Number.class);
                long id = ((Number) row.get("id")).longValue();
                assertThat(id).isBetween(1L, (long) INSERTED).isNotEqualTo(DELETED);
                assertThat(actual.get((int) id)).as("one physical row per source primary key").isFalse();
                actual.set((int) id);
                assertThat(row.getString("payload")).isEqualTo(id == UPDATED ? "updated-during-crash"
                        : id == INSERTED ? "inserted-during-crash" : PAYLOAD);
            }
        }
        BitSet expected = new BitSet(INSERTED + 1); expected.set(1, INSERTED + 1); expected.clear(DELETED);
        assertThat(count).isEqualTo(ROWS); assertThat(actual).isEqualTo(expected);
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(actual.toByteArray()));
    }

    private static Map<String, Object> markerEvidence(StopReservation marker) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("token", marker.token()); result.put("phase", marker.phase().name()); result.put("policy", marker.counterPolicy().name());
        result.put("sourceEpoch", marker.sourceEpoch()); result.put("reservedEpoch", marker.reservedEpoch());
        result.put("sourceScope", RebuildHandoffJdiSession.scopeEvidence(marker.source().scope()));
        result.put("oldJob", RebuildHandoffJdiSession.jobEvidence(marker.source().oldJob()));
        result.put("writerGeneration", marker.writerAuthority().executionGeneration());
        if (marker.successor() != null) {
            result.put("admittedScope", RebuildHandoffJdiSession.scopeEvidence(marker.successor().scope()));
            result.put("submissionBootId", marker.successor().submissionBootId());
        }
        return result;
    }
    private static List<Map<String, Object>> factsEvidence(List<MetricFact> facts) {
        return delivery(facts).stream().map(fact -> {
            Map<String, Object> value = new LinkedHashMap<>(); value.put("name", fact.name()); value.put("type", fact.type().name());
            value.put("unit", fact.unit()); value.put("points", fact.points().stream().map(point -> {
                Map<String, Object> sample = new LinkedHashMap<>(); sample.put("attributes", point.attributes());
                sample.put("startTime", point.startTime().toString()); sample.put("observedAt", point.observedAt().toString());
                if (point.histogram() == null) { sample.put("value", point.value()); }
                else { sample.put("histogram", Map.of("count", point.histogram().count(), "sum", point.histogram().sum(),
                        "bounds", point.histogram().bounds(), "bucketCounts", point.histogram().bucketCounts())); }
                return sample;
            }).toList()); return value;
        }).toList();
    }

    private static void seed(Map<String, Object> source) throws Exception {
        try (Connection connection = SharedMySql.connect(source); Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE bulk_orders (id BIGINT PRIMARY KEY, payload VARCHAR(64))");
            statement.execute("INSERT INTO bulk_orders VALUES (1, REPEAT('x', 64))");
            for (int count = 1; count < ROWS; count *= 2) {
                statement.execute("INSERT INTO bulk_orders (id, payload) SELECT id + " + count + ", payload FROM bulk_orders");
            }
        }
    }
    private static void mutateDuringCrash(Map<String, Object> source) throws Exception {
        try (Connection connection = SharedMySql.connect(source); Statement statement = connection.createStatement()) {
            assertThat(statement.executeUpdate("UPDATE bulk_orders SET payload='updated-during-crash' WHERE id=" + UPDATED)).isEqualTo(1);
            assertThat(statement.executeUpdate("DELETE FROM bulk_orders WHERE id=" + DELETED)).isEqualTo(1);
            assertThat(statement.executeUpdate("INSERT INTO bulk_orders VALUES (" + INSERTED + ", 'inserted-during-crash')")).isEqualTo(1);
        }
    }
    private static Map<String, String> resources(Map<String, Object> source, String targetUri) {
        return Map.of("source.tap.yml", """
                version: tapstate/v1
                kind: source
                id: bulk_source
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s }
                mode: cdc
                tables: [ bulk_orders ]
                """.formatted(source.get("host"), source.get("port"), source.get("database"), source.get("username"), source.get("password")),
                "target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: bulk_target
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetUri), "pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: handoff_bulk
                source: bulk_source
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: bulk_orders
                  sync:
                    - source: bulk_target
                """);
    }
    private static String required(String name) {
        String value = System.getProperty(PREFIX + name);
        if (value == null || value.isBlank()) { throw new AssertionError("real rebuild crash witness requires -D" + PREFIX + name); }
        return value;
    }
    private static Document requireArtifact(MongoDatabase database, ObservationStore.Scope scope) {
        Document resource = database.getCollection(MongoStorePort.ARTIFACTS).find(new Document("_id", PIPELINE))
                .projection(new Document("_id", 1).append("pipelineIncarnationId", 1)).first();
        assertThat(resource).isNotNull();
        assertThat(resource.getString("pipelineIncarnationId")).isEqualTo(scope.pipelineIncarnationId());
        return resource;
    }

    private static Document requireConsumer(MongoDatabase database, ControlPlane control) {
        var positions = control.positionRead(PIPELINE);
        assertThat(positions.pipelineId()).isEqualTo(PIPELINE);
        var chains = positions.chains().stream().filter(chain -> "bulk_source".equals(chain.sourceId())
                && chain.tables().contains(TABLE)).toList();
        assertThat(chains).hasSize(1);
        String chain = chains.getFirst().chainId();
        String consumer = SrsConsumerId.of(PIPELINE, "bulk_source").value();
        Document cursor = database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                .find(new Document("_id", new Document("chain", chain).append("pipeline", consumer)))
                .projection(new Document("miningChainId", 1).append("pipelineId", 1).append("ownerPipelineId", 1)
                        .append("sourceNodeId", 1).append("snapshotEpoch", 1).append("cdcStartPosition", 1)
                        .append("snapshotCompletedTables", 1)).first();
        assertThat(cursor).as("the actual source consumer must exist").isNotNull();
        assertThat(cursor.getString("miningChainId")).isEqualTo(chain);
        assertThat(cursor.getString("pipelineId")).isEqualTo(consumer);
        assertThat(cursor.getString("ownerPipelineId")).isEqualTo(PIPELINE);
        assertThat(cursor.getString("sourceNodeId")).isEqualTo("bulk_source");
        return cursor;
    }

    private static Document requireOwedConsumer(MongoDatabase database, ControlPlane control) {
        Document cursor = requireConsumer(database, control);
        assertThat(cursor.get("snapshotCompletedTables") == null || cursor.get("snapshotCompletedTables") instanceof List<?>).isTrue();
        assertThat(cursor.get("snapshotCompletedTables") instanceof List<?> tables && tables.contains(TABLE))
                .as("this actual consumer still owes the entire table before the crash").isFalse();
        assertThat(cursor.get("snapshotEpoch")).isInstanceOf(Number.class);
        assertThat(((Number) cursor.get("snapshotEpoch")).longValue()).isPositive();
        assertThat(cursor.getString("cdcStartPosition")).isNotBlank();
        return cursor;
    }

    private static void assertNodeSession(MongoWorkloadClaimStore claims, String clusterId, WorkloadClaim claim) {
        var session = claims.read(new WorkloadClaimKey(clusterId, WorkloadClaimType.NODE_SESSION, claim.owner().nodeId())).orElseThrow();
        assertThat(session.leased()).isTrue();
        assertThat(session.claim().owner()).isEqualTo(claim.owner());
    }

    private static Map<String, Object> claimEvidence(WorkloadClaim claim) {
        return Map.of("clusterId", claim.key().clusterId(), "resourceId", claim.key().resourceId(),
                "ownerNodeId", claim.owner().nodeId(), "ownerBootId", claim.owner().bootId(),
                "claimGeneration", claim.claimGeneration(), "executionGeneration", claim.executionGeneration(),
                "topologyRevision", claim.topologyRevision(), "executionNodeIds", claim.executionNodeIds().stream().sorted().toList(),
                "leaseUntil", claim.leaseUntil().toString());
    }

    private static Map<String, Object> targetHoldReceipt(MongoDatabase target) {
        Map<String, Object> receipt = new LinkedHashMap<>();
        receipt.put("rows", target.getCollection(TABLE).countDocuments());
        for (int id : List.of(UPDATED, DELETED, INSERTED)) {
            Document row = target.getCollection(TABLE).find(new Document("id", (long) id)).first();
            receipt.put("id" + id, row == null ? "ABSENT" : row.toJson());
        }
        return Map.copyOf(receipt);
    }

    private static Map<String, Object> sourceChangeReceipt(Map<String, Object> source) throws Exception {
        long actualRows;
        try (Connection connection = SharedMySql.connect(source); Statement sql = connection.createStatement()) {
            try (var result = sql.executeQuery("SELECT COUNT(*) FROM " + TABLE)) {
                assertThat(result.next()).isTrue(); actualRows = result.getLong(1); assertThat(actualRows).isEqualTo(ROWS);
            }
            try (var result = sql.executeQuery("SELECT payload FROM " + TABLE + " WHERE id=" + UPDATED)) {
                assertThat(result.next()).isTrue(); assertThat(result.getString(1)).isEqualTo("updated-during-crash");
            }
            try (var result = sql.executeQuery("SELECT COUNT(*) FROM " + TABLE + " WHERE id=" + DELETED)) {
                assertThat(result.next()).isTrue(); assertThat(result.getLong(1)).isZero();
            }
            try (var result = sql.executeQuery("SELECT payload FROM " + TABLE + " WHERE id=" + INSERTED)) {
                assertThat(result.next()).isTrue(); assertThat(result.getString(1)).isEqualTo("inserted-during-crash");
            }
        }
        return Map.of("rows", actualRows, "updatedId", UPDATED, "deletedId", DELETED, "insertedId", INSERTED,
                "observedAt", Instant.now().toString());
    }

    private static final class PendingProbe {
        private boolean attempted, qualified;
        private Map<String, Object> reading = Map.of("status", "UNVERIFIED_WINDOW_NOT_OBSERVED");
        Map<String, Object> evidence() { return reading; }
        void observe(MongoDatabase database, MongoObservationStore latest, ControlPlane control, URI base,
                ObservationStore.Scope expected, ObservationStore.Scope old, long deadline) {
            if (attempted || generation(database) != expected.executionGeneration()) { return; }
            var before = latest.readStored(PIPELINE);
            if (before.isEmpty() || before.orElseThrow().scope().filter(old::equals).isEmpty()) { return; }
            attempted = true;
            requireArtifact(database, expected);
            long left = deadline - System.nanoTime();
            assertThat(left).as("the optional current-read receipt shares the recovery deadline").isPositive();
            HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
            HttpRequest request = HttpRequest.newBuilder(base.resolve("/api/pipelines/" + PIPELINE + "/status"))
                    .timeout(Duration.ofNanos(Math.min(left, Duration.ofSeconds(20).toNanos())))
                    .header("Authorization", "Bearer " + control.credential()).GET().build();
            try {
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                Optional<PipelineState> decoded = ControlPlane.interpretState(response.statusCode(), response.body(), PIPELINE);
                assertThat(generation(database)).isEqualTo(expected.executionGeneration());
                requireArtifact(database, expected);
                var after = latest.readStored(PIPELINE);
                boolean stillOld = after.isPresent() && after.orElseThrow().scope().filter(old::equals).isPresent();
                if (stillOld) {
                    assertThat(response.statusCode()).as("the current read cannot return the old scoped observation").isEqualTo(404);
                    assertThat(decoded).isEmpty();
                    qualified = true;
                }
                reading = Map.of("status", qualified ? "QUALIFIED" : "UNVERIFIED_PUBLICATION_RACE",
                        "httpStatus", response.statusCode(), "reply", response.body(),
                        "authority", RebuildHandoffJdiSession.scopeEvidence(expected),
                        "retainedBefore", RebuildHandoffJdiSession.scopeEvidence(old), "retainedOldAfterRead", stillOld);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt(); throw new AssertionError("the current-read receipt was interrupted", interrupted);
            } catch (java.io.IOException failure) { throw new AssertionError("the current-read receipt failed", failure); }
        }
    }

    private static Map<String, Object> inputHashes(Path harness) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        for (String name : List.of("RealClaimedRebuildHandoffCrashIT", "RebuildHandoffJdiSession", "RealRebuildHandoffCrashIT",
                "TwoMemberCluster", "RealProcessServer", "Await")) {
            Path source = harness.resolve("e2e/src/test/java/io/tapstate/e2e/" + name + ".java");
            result.put(name + ".source", PipelineBenchmarkLiveRunIT.sha256(source));
            String resource = "/io/tapstate/e2e/" + name + ".class";
            try (var bytes = RealClaimedRebuildHandoffCrashIT.class.getResourceAsStream(resource)) {
                if (bytes == null) { throw new AssertionError("the executing harness class is absent: " + name); }
                result.put(name + ".class", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.readAllBytes())));
            }
        }
        for (String name : List.of("RealClaimedRebuildHandoffCrashIT$Series", "RealClaimedRebuildHandoffCrashIT$Matched",
                "RealClaimedRebuildHandoffCrashIT$MatchTrace", "RealClaimedRebuildHandoffCrashIT$PendingProbe",
                "RebuildHandoffJdiSession$OwnedLauncher", "RebuildHandoffJdiSession$Cut", "RebuildHandoffJdiSession$Site",
                "RebuildHandoffJdiSession$Image", "RebuildHandoffJdiSession$Binding", "RebuildHandoffJdiSession$Held",
                "RebuildHandoffJdiSession$Raw", "RebuildHandoffJdiSession$RawKey", "RebuildHandoffJdiSession$JobProof",
                "RebuildHandoffJdiSession$JobCall")) {
            try (var bytes = RealClaimedRebuildHandoffCrashIT.class.getResourceAsStream("/io/tapstate/e2e/" + name + ".class")) {
                if (bytes == null) { throw new AssertionError("the executing nested harness class is absent: " + name); }
                result.put(name + ".class", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes.readAllBytes())));
            }
        }
        return Map.copyOf(result);
    }

}
