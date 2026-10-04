package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoAuthStores;
import io.tapstate.adapters.mongostore.MongoClusterIdentityStore;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.adapters.mongostore.MongoStateStore;
import io.tapstate.adapters.mongostore.MongoDesiredStore;
import io.tapstate.adapters.mongostore.MongoWorkloadClaimStore;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.ObservationContinuation;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.HandoffIdentity;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.offset;
import static io.tapstate.e2e.NativeTelemetryPositiveCalibrationIT.flag;
import static io.tapstate.e2e.NativeTelemetryPositiveCalibrationIT.has;
import static io.tapstate.e2e.NativeTelemetryPositiveCalibrationIT.matchingProduced;
import static io.tapstate.e2e.NativeTelemetryPositiveCalibrationIT.positiveProduced;
import static io.tapstate.e2e.NativeTelemetryPositiveCalibrationIT.positiveScrape;

/** Actual warm admission and telemetry from the submitting member of one owned two-member cluster. */
@RequiresDocker
class NativeClaimedTelemetryPositiveCalibrationIT {
    private static final String PREFIX = "tapstate.e2e.native-claimed-telemetry-calibration.";
    private static final Duration WAIT = Duration.ofMinutes(2);
    private static final int MAX_RECORDS = 512;
    private static final int MAX_BYTES = 2 * 1024 * 1024;
    private static final int SNAPSHOT_ROWS = 524_288;
    private static final Duration SETUP_WAIT = Duration.ofMinutes(3), DELIVERY_WAIT = Duration.ofMinutes(12);
    private static final Set<String> DELIVERY = Set.of("tapstate.pipeline.records", "tapstate.pipeline.bytes",
            "tapstate.pipeline.record.delivery.duration");
    private static final String PIPELINE = "native_claimed_copy";
    private static final String SOURCE = "native_claimed_source";
    private static final String TABLE = "native_claimed_orders";
    private record Positive(NativeTelemetryIdentityJdiSession.Boundary boundary, String body, String logNode) { }

    @Test
    void aRealWarmClaimedSubmissionOffersProducesAndScrapesUnderItsActualOwner() throws Exception {
        runClaimedPositive(false);
    }

    @Test
    void aRealClaimedResetReplacementKeepsItsActualAdmissionAndSubmittedJobBridge() throws Exception {
        runClaimedPositive(true);
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    void aRealClaimedPartialSnapshotContinuesItsExactFloorThroughAnActualReplacement() throws Exception {
        runClaimedPositive(true, true);
    }

    private static void runClaimedPositive(boolean replacement) throws Exception {
        runClaimedPositive(replacement, false);
    }

    private static void runClaimedPositive(boolean replacement, boolean continuing) throws Exception {
        Assumptions.assumeTrue(List.of("jar", "sha256", "output").stream()
                .anyMatch(name -> System.getProperty(PREFIX + name) != null),
                "the claimed positive calibration needs named immutable inputs");
        Path jar = Path.of(required("jar")).toAbsolutePath().normalize();
        String sha = required("sha256");
        assertThat(sha).matches("[0-9a-f]{64}");
        assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
        BenchmarkCaptureCalibrationLiveRunIT.requireConnectors();
        Path harness = PipelineBenchmarkLiveRunIT.harnessRoot();
        Path requestedOutput = Path.of(required("output")).toAbsolutePath().normalize();
        Path output = continuing ? requestedOutput.resolveSibling(requestedOutput.getFileName() + ".continue.json")
                : replacement ? requestedOutput.resolveSibling(requestedOutput.getFileName() + ".replacement.json") : requestedOutput;
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, harness);
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        Map<String, Object> inputs = inputHashes(harness);
        Map<String, Map<String, Object>> connectors = new LinkedHashMap<>();
        for (String connector : List.of("mysql", "postgres", "mongodb")) {
            connectors.put(connector, PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(connector)));
        }
        String namespace = "native_claimed_" + UUID.randomUUID().toString().replace("-", "");
        String storeUri = SharedMongo.replicaSetUrl(namespace + "_control");
        String targetUri = SharedMongo.replicaSetUrl(namespace + "_target");
        String operatorDatabase = namespace + "_operator";
        Map<String, Object> settings = SharedMySql.settings(namespace + "_source");
        Map<String, NativeTelemetryIdentityJdiSession> sessions = new LinkedHashMap<>();
        Map<String, Integer> scrapePorts = new LinkedHashMap<>();
        TwoMemberCluster cluster = null;
        Throwable primary = null;
        try (var source = SharedMySql.connect(settings); var mongo = MongoClients.create(storeUri)) {
            try (var sql = source.createStatement()) {
                sql.execute("CREATE TABLE " + TABLE
                        + " (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL, payload VARCHAR(32) NOT NULL)");
                if (continuing) {
                    sql.execute("INSERT INTO " + TABLE + " (id,amount,payload) VALUES (1,100,'native-1')");
                    for (int count = 1; count < SNAPSHOT_ROWS; count *= 2) {
                        sql.execute("INSERT INTO " + TABLE + " (id,amount,payload) SELECT id+" + count
                                + ",(id+" + count + ")*100,CONCAT('native-',id+" + count + ") FROM " + TABLE);
                    }
                } else {
                    sql.execute("INSERT INTO " + TABLE
                            + " (id,amount,payload) VALUES (1,100,'native-1'),(2,200,'native-2'),(3,300,'native-3')");
                }
            }
            MongoDatabase database = mongo.getDatabase(new ConnectionString(storeUri).getDatabase());
            Map<String, String> resources = resources(settings, targetUri);
            report.begin(Map.of("purpose", continuing ? "NATIVE_CLAIMED_PARTIAL_SNAPSHOT_CONTINUE_POSITIVE"
                            : replacement ? "NATIVE_CLAIMED_RESET_REPLACEMENT_POSITIVE" : "NATIVE_WARM_CLAIMED_POSITIVE_TELEMETRY",
                    "application", PipelineBenchmarkLiveRunIT.artifact(jar), "expectedJarSha256", sha,
                    "harness", inputs, "connectors", connectors, "clusterMembers", 2,
                    "fixtureResourceSha256", digest(JsonWriter.write(resources).getBytes(StandardCharsets.UTF_8)),
                    "performanceAcceptanceEligible", false, "warmOnly", !replacement),
                    PipelineBenchmarkLiveRunIT.environment(), List.of());
            String clusterId;
            try (var setup = RealProcessServer.start(storeUri, operatorDatabase, jar)) {
                ControlPlane control = new ControlPlane(setup.baseUrl());
                control.bootstrapAndLogin("benchmark", "benchmark-password");
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                control.apply(Map.of(SOURCE + ".tap.yml", resources.get(SOURCE + ".tap.yml")));
                Map<String, Object> connection = new LinkedHashMap<>(settings);
                connection.put("highPerformance", false);
                control.discoverSchema(SOURCE, "mysql", connection);
                control.apply(resources);
                clusterId = new MongoClusterIdentityStore(database.getCollection(MongoAuthStores.CLUSTER_IDENTITY))
                        .find().orElseThrow().clusterId();
                assertThat(clusterId).isNotBlank();
                assertThat(database.getCollection(MongoStorePort.WORKLOAD_CLAIMS).countDocuments(
                        new Document("clusterId", clusterId).append("resourceType", "PIPELINE_ACTUATION")
                                .append("resourceId", PIPELINE)))
                        .as("setup applies resources and has issued no START or execution admission").isZero();
            }
            for (String node : List.of(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B)) {
                scrapePorts.put(node, RealProcessServer.reservePort());
            }
            cluster = TwoMemberCluster.start(storeUri, operatorDatabase, jar, clusterId,
                    "benchmark", "benchmark-password", List.of(), List.of(), (node, address, arguments, jvm) -> {
                        try {
                            NativeTelemetryIdentityJdiSession.OwnedLauncher launcher = (artifact, debug) -> {
                                        List<String> options = new ArrayList<>(jvm);
                                        options.addAll(debug);
                                        return RealProcessServer.launchingWithJvmArguments(storeUri, operatorDatabase,
                                                artifact, address, httpPort -> {
                                                    List<String> application = new ArrayList<>(arguments.apply(httpPort));
                                                    application.add("--tapstate.metrics.export.prometheus.host=127.0.0.1");
                                                    application.add("--tapstate.metrics.export.prometheus.port=" + scrapePorts.get(node));
                                                    application.add("--tapstate.metrics.history.sample-interval=PT2S");
                                                    return List.copyOf(application);
                                                }, List.copyOf(options));
                                    };
                            var observer = continuing
                                    ? NativeTelemetryIdentityJdiSession.startWithContinuationObservation(jar, sha, PIPELINE, launcher)
                                    : replacement ? NativeTelemetryIdentityJdiSession.startWithReplacementObservation(jar, sha, PIPELINE, launcher)
                                    : NativeTelemetryIdentityJdiSession.start(jar, sha, PIPELINE, launcher);
                            sessions.put(node, observer);
                            return observer.server();
                        } catch (RuntimeException | Error failure) {
                            throw failure;
                        } catch (Exception failure) {
                            throw new AssertionError("the owned observed member could not start", failure);
                        }
                    });
            assertThat(cluster.awaitBothMembers()).containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            assertThat(cluster.second().clusterMemberNodeIds()).containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            assertThat(cluster.first().clusterId()).isEqualTo(clusterId);
            assertThat(cluster.second().clusterId()).isEqualTo(clusterId);
            assertThat(sessions).containsOnlyKeys(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            var claims = new MongoWorkloadClaimStore(database.getCollection(MongoStorePort.WORKLOAD_CLAIMS));
            Map<String, WorkloadClaim> memberBoots = nodeSessions(claims, clusterId);
            WorkloadClaimKey key = new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE);
            var previousGeneration = claims.currentGeneration(clusterId, PIPELINE);
            report.addFork(Map.of("action", "before-real-claimed-start", "memberBoots", memberEvidence(memberBoots),
                    "generationStatus", previousGeneration.isPresent() ? "PRESENT" : "ABSENT",
                    "generation", previousGeneration.isPresent() ? previousGeneration.getAsLong() : "ABSENT"));
            var latest = new MongoObservationStore(mongo,
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
            var actual = new MongoStateStore(database.getCollection(MongoStorePort.PIPELINE_STATE));
            Map<String, List<Map<String, Object>>> records = new LinkedHashMap<>();
            for (String node : sessions.keySet()) { records.put(node, new ArrayList<>()); }
            long setupDeadline = System.nanoTime() + (continuing ? SETUP_WAIT : WAIT).toNanos();
            long nativeDeadline = setupDeadline;
            cluster.first().lifecycle(PIPELINE, LifecycleVerb.START);
            try (var target = MongoClients.create(targetUri)) {
                MongoDatabase targetDatabase = target.getDatabase(new ConnectionString(targetUri).getDatabase());
                if (continuing) {
                    Await.until("real claimed partial snapshot has known counters and histogram", remaining(setupDeadline),
                            () -> {
                                captureContinueBoundaries(sessions, records, report, "claimed-partial-snapshot");
                                long rows = targetDatabase.getCollection(TABLE).countDocuments();
                                return rows > 0 && rows < SNAPSHOT_ROWS && latest.readStored(PIPELINE)
                                        .filter(value -> value.scope().isPresent()
                                                && value.scope().orElseThrow().executionGeneration() == 1L
                                                && value.observation().state() == PipelineState.RUNNING
                                                && hasKnownDelivery(value.observation().facts())).isPresent();
                            }, () -> "target rows=" + targetDatabase.getCollection(TABLE).countDocuments());
                } else {
                    Await.until("the real claimed snapshot reaches Mongo", remaining(setupDeadline),
                            () -> targetDatabase.getCollection(TABLE).countDocuments() == 3,
                            () -> "target rows=" + targetDatabase.getCollection(TABLE).countDocuments());
                    assertRows(targetDatabase, 3);
                    try (var sql = source.createStatement()) {
                        sql.execute("INSERT INTO " + TABLE + " (id,amount,payload) VALUES (4,400,'native-4')");
                    }
                    Await.until("a real source CDC insert reaches Mongo", remaining(setupDeadline),
                            () -> targetDatabase.getCollection(TABLE).countDocuments() == 4,
                            () -> "target rows=" + targetDatabase.getCollection(TABLE).countDocuments());
                    assertRows(targetDatabase, 4);
                }
            }
            NativeTelemetryIdentityJdiSession.ClaimedSubmission priorSubmission = null;
            io.tapstate.spi.store.ObservationStore.Scope priorScope = null;
            Map<Map<String, String>, Instant> priorStarts = Map.of();
            List<MetricFact> pausedFacts = List.of();
            if (replacement) {
                var first = Await.answered("the actual first claimed execution before replacement", remaining(setupDeadline),
                        () -> latest.readStored(PIPELINE).filter(value -> value.scope().isPresent()
                                && value.scope().orElseThrow().executionGeneration() == 1L
                                && value.observation().state() == PipelineState.RUNNING
                                && !counterStarts(value.observation()).isEmpty()));
                WorkloadClaim firstClaim = claims.read(key).filter(reading -> reading.leased()
                        && reading.claim().executionGeneration() == first.scope().orElseThrow().executionGeneration())
                        .orElseThrow().claim();
                Document firstClaimDocument = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS).find(
                        new Document("clusterId", clusterId).append("resourceType", "PIPELINE_ACTUATION")
                                .append("resourceId", PIPELINE)).first();
                assertThat(firstClaimDocument).isNotNull();
                var firstReceipt = new NativeTelemetryIdentityJdiSession.AuthorityReceipt(PIPELINE, clusterId,
                        first.scope().orElseThrow().pipelineIncarnationId(), firstClaim.executionGeneration(),
                        JsonWriter.write(firstClaimDocument.get("_id")), Instant.now().toString());
                for (var observer : sessions.values()) { observer.recordAuthority(firstReceipt); }
                var firstSubmission = sessions.get(firstClaim.owner().nodeId()).claimedSubmission(firstReceipt).orElseThrow();
                assertThat(withoutLease(firstSubmission.claim())).isEqualTo(claimTuple(firstClaim));
                priorSubmission = firstSubmission;
                priorScope = first.scope().orElseThrow();
                priorStarts = counterStarts(first.observation());
                assertThat(memberBoots.get(firstClaim.owner().nodeId()).owner()).isEqualTo(firstClaim.owner());
                report.addFork(Map.of("action", continuing ? "actual-first-claimed-execution-before-continue" : "actual-first-claimed-execution-before-reset",
                        "claim", firstSubmission.claim(), "scope", firstReceipt.scope(), "job", firstSubmission.job().job()));
                if (continuing) {
                    assertThat(hasKnownDelivery(first.observation().facts())).isTrue();
                    cluster.first().lifecycle(PIPELINE, LifecycleVerb.PAUSE);
                    TwoMemberCluster owned = cluster;
                    Await.until("actual claimed execution is paused while its snapshot remains unfinished", remaining(setupDeadline),
                            () -> {
                                captureContinueBoundaries(sessions, records, report, "claimed-partial-paused");
                                return actual.read(PIPELINE).filter(checkpoint -> StateJson.parse(checkpoint.stateJson()) == PipelineState.PAUSED).isPresent()
                                        && owned.first().state(PIPELINE).filter(PipelineState.PAUSED::equals).isPresent();
                            }, () -> "actual=" + actual.read(PIPELINE));
                    try (var target = MongoClients.create(targetUri)) {
                        long pausedRows = target.getDatabase(new ConnectionString(targetUri).getDatabase())
                                .getCollection(TABLE).countDocuments();
                        assertThat(pausedRows).as("PAUSE caught a genuine unfinished snapshot").isBetween(1L, SNAPSHOT_ROWS - 1L);
                        report.addFork(Map.of("action", "actual-claimed-partial-snapshot-paused", "physicalRows", pausedRows,
                                "sourceRows", SNAPSHOT_ROWS, "scope", firstReceipt.scope(), "claim", claimTuple(firstClaim)));
                    }
                    assertThat(snapshotConfirmed(database, cluster.first())).as("the current source consumer still owes the snapshot table").isFalse();
                    var paused = Await.answered("known scoped claimed PAUSED frame", remaining(setupDeadline),
                            () -> latest.readStored(PIPELINE).filter(value -> value.scope().equals(first.scope())
                                    && value.observation().state() == PipelineState.PAUSED && hasKnownDelivery(value.observation().facts())));
                    assertFloorAtLeast(first.observation().facts(), paused.observation().facts());
                    priorStarts = counterStarts(paused.observation());
                    pausedFacts = paused.observation().facts();
                    nativeDeadline = System.nanoTime() + WAIT.toNanos();
                    cluster.first().lifecycle(PIPELINE, LifecycleVerb.RESUME);
                } else {
                    cluster.first().stop(PIPELINE, true);
                    cluster.first().lifecycle(PIPELINE, LifecycleVerb.START);
                }
            }
            long deadline = nativeDeadline;
            var current = Await.answered("a real scoped positive claimed observation", remaining(deadline),
                    () -> {
                        if (continuing) { captureContinueBoundaries(sessions, records, report, "claimed-continued-admission"); }
                        return latest.readStored(PIPELINE).filter(value -> value.scope().isPresent()
                            && value.observation().state() == PipelineState.RUNNING
                            && (!replacement || value.scope().orElseThrow().executionGeneration() == 2L)
                            && value.observation().facts().stream()
                            .filter(fact -> fact.name().equals("tapstate.pipeline.records"))
                            .flatMap(fact -> fact.points().stream()).anyMatch(point -> point.value() != null
                                    && point.value() > 0 && "out".equals(point.attributes().get("direction"))));
                    });
            var scope = current.scope().orElseThrow();
            WorkloadClaim claim = claims.read(key).filter(reading -> reading.leased()
                    && reading.claim().executionGeneration() == scope.executionGeneration()).orElseThrow().claim();
            assertThat(claim.executionNodeIds()).containsExactlyInAnyOrder(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            if (previousGeneration.isPresent()) {
                assertThat(claim.executionGeneration()).isEqualTo(Math.addExact(previousGeneration.getAsLong(), replacement ? 2 : 1));
            } else {
                assertThat(claim.executionGeneration())
                        .as("only actual first start and the requested replacement allocate executions").isEqualTo(replacement ? 2L : 1L);
            }
            assertThat(memberBoots.get(claim.owner().nodeId()).owner()).isEqualTo(claim.owner());
            Document artifact = database.getCollection(MongoStorePort.ARTIFACTS).find(new Document("_id", PIPELINE)).first();
            assertThat(artifact).isNotNull();
            assertThat(artifact.getString("pipelineIncarnationId")).isEqualTo(scope.pipelineIncarnationId());
            Document claimDocument = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS).find(
                    new Document("clusterId", clusterId).append("resourceType", "PIPELINE_ACTUATION")
                            .append("resourceId", PIPELINE)).first();
            assertThat(claimDocument).isNotNull();
            var receipt = new NativeTelemetryIdentityJdiSession.AuthorityReceipt(PIPELINE, clusterId,
                    scope.pipelineIncarnationId(), scope.executionGeneration(), JsonWriter.write(claimDocument.get("_id")),
                    Instant.now().toString());
            for (var observer : sessions.values()) { observer.recordAuthority(receipt); }
            String emittingNode = claim.owner().nodeId();
            var owner = sessions.get(emittingNode);
            report.addFork(Map.of("action", "actual-claimed-current-before-submission-lookup", "scope", receipt.scope(),
                    "claim", claimTuple(claim), "observationState", current.observation().state().name(),
                    "observationAt", current.observation().observedAt().toString()));
            NativeTelemetryIdentityJdiSession.ClaimedReplacement replacementProof = null;
            NativeTelemetryIdentityJdiSession.ClaimedSubmission submission;
            if (replacement) {
                var captured = owner.claimedReplacement(receipt);
                assertThat(captured).as("the actual adopted replacement admission and same submit Job are captured").isPresent();
                replacementProof = captured.orElseThrow();
                submission = replacementProof.submission();
            } else {
                var captured = owner.claimedSubmission(receipt);
                assertThat(captured).as("the actual ordinary admission and same submit Job are captured").isPresent();
                submission = captured.orElseThrow();
            }
            if (replacement) {
                assertThat(priorSubmission).isNotNull();
                assertThat(replacementProof).isNotNull();
                assertReplacementSlot(replacementProof, priorSubmission, receipt, continuing);
                report.addFork(Map.of("action", "actual-adopted-claimed-replacement-before-positive-export",
                        "successorAdmissionObject", replacementProof.successorAdmissionObjectId(),
                        "reservationObject", replacementProof.reservationObjectId(),
                        "advancedClaimObject", replacementProof.advancedClaimObjectId(),
                        "returnedExecutionObject", submission.admissionObjectId(),
                        "returnedExecutionJobObject", submission.job().executionObjectId(),
                        "reservation", replacementProof.reservation(), "advancedClaim", submission.claim(),
                        "nativeJob", submission.job().job(), "scope", receipt.scope()));
                assertThat(priorScope).isNotNull();
                assertThat(scope.pipelineIncarnationId()).isEqualTo(priorScope.pipelineIncarnationId());
                assertThat(scope.executionGeneration()).isEqualTo(Math.addExact(priorScope.executionGeneration(), 1));
                assertThat(submission.job().job().get("jobId")).isNotEqualTo(priorSubmission.job().job().get("jobId"));
                if (!continuing) {
                    var resetStarts = counterStarts(current.observation());
                    assertThat(resetStarts).isNotEmpty();
                    var originalStarts = priorStarts;
                    assertThat(resetStarts.keySet().stream().anyMatch(originalStarts::containsKey))
                            .as("the old and replacement samples share at least one actual output-counter series").isTrue();
                    for (var point : resetStarts.entrySet()) {
                        Instant prior = priorStarts.get(point.getKey());
                        if (prior != null) {
                            assertThat(point.getValue()).as("the actual RESET replacement begins this counter after the old start").isAfter(prior);
                        }
                    }
                    try (var sql = source.createStatement()) {
                        sql.execute("INSERT INTO " + TABLE + " (id,amount,payload) VALUES (5,500,'native-5')");
                    }
                    try (var target = MongoClients.create(targetUri)) {
                        var targetDatabase = target.getDatabase(new ConnectionString(targetUri).getDatabase());
                        Await.until("CDC reaches the actual replacement Job's physical target", remaining(deadline),
                                () -> targetDatabase.getCollection(TABLE).countDocuments() == 5,
                                () -> "target rows=" + targetDatabase.getCollection(TABLE).countDocuments());
                        assertRows(targetDatabase, 5);
                    }
                }
            }
            ObservationContinuation continueFloor = null;
            if (continuing) {
                var proof = replacementProof;
                var originalScope = priorScope;
                var carrier = Await.answered("actual known CONTINUE carrier for the admitted old scope and token", remaining(deadline),
                        () -> {
                            captureContinueBoundaries(sessions, records, report, "claimed-continued-carrier");
                            return latest.readContinuation(PIPELINE).filter(value -> value.receipt().knownBaseline()
                                && proof.reservation().get("token").equals(value.continuation().token())
                                && value.continuation().sourceScope().equals(originalScope)
                                && hasKnownDelivery(value.continuation().baselineFacts()));
                        });
                continueFloor = carrier.continuation();
                assertFloorAtLeast(pausedFacts, continueFloor.baselineFacts());
                assertThat(counterStarts(current.observation())).containsAllEntriesOf(priorStarts);
                var desired = new MongoDesiredStore(database.getCollection(MongoStorePort.PIPELINE_DESIRED)).read(PIPELINE).orElseThrow();
                Map<?, ?> original = (Map<?, ?>) proof.reservation().get("originalDesired");
                assertThat(original.get("pipelineId")).isEqualTo(desired.pipelineId());
                assertThat(original.get("targetState")).isEqualTo(desired.targetState().name());
                assertThat(original.get("revision")).isEqualTo(desired.revision());
                assertThat(original.get("assemblyRevision")).isEqualTo(desired.assemblyRevision());
                assertThat(original.get("reassemble")).isEqualTo(desired.reassemble());
                assertThat(original.get("purgeState")).isEqualTo(desired.purgeState());
                assertThat(original.get("rebuiltAtStateEpoch")).isEqualTo(desired.rebuiltAtStateEpoch());
                report.addFork(Map.of("action", "actual-claimed-continuation-floor", "token", continueFloor.token(),
                        "sourceScope", Map.of("incarnation", continueFloor.sourceScope().pipelineIncarnationId(),
                                "generation", continueFloor.sourceScope().executionGeneration()),
                        "floor", factEvidence(continueFloor.baselineFacts()), "receiptDigest", carrier.receipt().digest()));
            }
            var frozenFloor = continueFloor;
            var matchedJob = submission.job().job();
            assertThat(withoutLease(submission.claim())).isEqualTo(claimTuple(claim));
            assertThat(submission.claim().get("leaseUntil")).isInstanceOf(String.class);
            Instant.parse((String) submission.claim().get("leaseUntil"));
            assertThat(submission.members()).containsExactlyInAnyOrder(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            assertThat(submission.job().scope()).isEqualTo(receipt.scope());
            assertThat(submission.job().job().get("clusterId")).isEqualTo(clusterId);
            assertThat(submission.job().executionObjectId()).isPositive();
            HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
            URI scrape = URI.create("http://127.0.0.1:" + scrapePorts.get(emittingNode) + "/metrics");
            Positive positive = Await.answered("actual claimed Job, scoped log, offer, visibility and produced scrape",
                    remaining(deadline), () -> {
                        try {
                            String fresh = NativeTelemetryPositiveCalibrationIT.scrape(http, scrape, deadline);
                            NativeTelemetryIdentityJdiSession.Boundary emitter = null;
                            for (var entry : sessions.entrySet()) {
                                var captured = entry.getValue().boundary(continuing ? "claimed-continue-positive" : "claimed-warm-positive");
                                Map<String, Object> evidence = new LinkedHashMap<>(captured.evidence());
                                evidence.put("observedNodeId", entry.getKey());
                                report.addFork(evidence);
                                records.get(entry.getKey()).addAll(captured.records());
                                if (has(records.get(entry.getKey()), "LOG", receipt)) {
                                    assertThat(captured.bindings())
                                            .as("the actual log member has the validated native log method")
                                            .containsKey(NativeTelemetryIdentityJdiSession.Target.LOG);
                                }
                                if (entry.getKey().equals(emittingNode)) {
                                    emitter = captured;
                                    assertThat(captured.unverified())
                                            .as("the actual emitter has no missing decoder or binding").isEmpty();
                                }
                            }
                            List<Map<String, Object>> observed = records.get(emittingNode);
                            Optional<String> logNode = submission.members().stream()
                                    .filter(node -> has(records.get(node), "LOG", receipt)).findFirst();
                            Map<String, Object> predicates = new LinkedHashMap<>();
                            predicates.put("job", has(observed, "JOB", receipt));
                            predicates.put("scopedLogOnAdmittedMember", logNode.isPresent());
                            predicates.put("acceptedOffer", flag(observed, "OFFER", "accepted", receipt));
                            predicates.put("visible", flag(observed, "VISIBLE", "included", receipt));
                            predicates.put("matchingProduced", matchingProduced(observed, fresh, receipt));
                            predicates.put("positiveScrape", positiveScrape(fresh, PIPELINE));
                            if (continuing) {
                                var matched = matchedContinuation(latest, observed, scope, matchedJob, frozenFloor, null);
                                matched.ifPresent(value -> assertCumulativeExactly(value, frozenFloor));
                                predicates.put("exactFloorPlusActualRaw", matched.isPresent());
                            }
                            predicates.put("ownerInvocationDrainComplete", emitter != null
                                    && emitter.decodedAndAuthorityBound());
                            boolean complete = predicates.values().stream().allMatch(Boolean.TRUE::equals);
                            Map<String, Object> response = new LinkedHashMap<>();
                            response.put("action", "actual-claimed-positive-predicates-and-scrape");
                            response.put("observedAt", Instant.now().toString());
                            response.put("exportEmittingNode", emittingNode);
                            response.put("logEmittingNode", logNode.orElse("ABSENT"));
                            response.put("scope", receipt.scope()); response.put("predicates", predicates);
                            response.put("prometheusEndpoint", scrape.toString()); response.put("body", fresh);
                            response.put("bodySha256", digest(fresh.getBytes(StandardCharsets.UTF_8)));
                            Map<String, Integer> sizes = new LinkedHashMap<>();
                            records.forEach((node, values) -> sizes.put(node, values.size()));
                            response.put("retainedRecords", sizes);
                            report.addFork(response);
                            for (var values : sizes.entrySet()) {
                                assertThat(values.getValue()).as("%s retained native records", values.getKey())
                                        .isLessThanOrEqualTo(MAX_RECORDS);
                            }
                            return complete ? Optional.of(new Positive(emitter, fresh, logNode.orElseThrow())) : Optional.empty();
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError("the claimed positive read was interrupted", interrupted);
                        } catch (Exception failure) {
                            throw new AssertionError("the actual claimed positive read failed", failure);
                        }
                    }
            );
            var boundary = positive.boundary();
            String body = positive.body();
            List<Map<String, Object>> observed = records.get(emittingNode);
            assertThat(boundary).isNotNull();
            assertThat(has(observed, "JOB", receipt)).isTrue();
            assertThat(owner.capturedJob(receipt)).isPresent();
            String logNode = positive.logNode();
            assertThat(submission.members()).contains(logNode);
            assertThat(has(records.get(logNode), "LOG", receipt))
                    .as("a genuine scoped log on an actual admitted member is required; quiet is unverified").isTrue();
            assertThat(flag(observed, "OFFER", "accepted", receipt)).isTrue();
            assertThat(flag(observed, "VISIBLE", "included", receipt)).isTrue();
            assertThat(observed.stream().anyMatch(record -> "PRODUCE".equals(record.get("target")) && positiveProduced(record))).isTrue();
            assertThat(matchingProduced(observed, body, receipt)).isTrue();
            assertThat(positiveScrape(body, PIPELINE)).isTrue();
            assertThat(boundary.decodedAndAuthorityBound()).isTrue();
            if (replacement) { assertReplacementBridge(observed, replacementProof); }
            else { assertBridge(observed, submission); }
            if (continuing) {
                verifyContinuedSnapshotAndCdc(source, targetUri, database, latest, actual, scope, matchedJob, frozenFloor,
                        sessions, records, report, emittingNode, cluster.first());
            }
            WorkloadClaim after = claims.read(key).filter(reading -> reading.leased()).orElseThrow().claim();
            assertThat(claimTuple(after)).isEqualTo(withoutLease(submission.claim()));
            Map<String, WorkloadClaim> afterBoots = nodeSessions(claims, clusterId);
            for (String node : memberBoots.keySet()) {
                assertThat(afterBoots.get(node).owner()).isEqualTo(memberBoots.get(node).owner());
                assertThat(afterBoots.get(node).claimGeneration()).isEqualTo(memberBoots.get(node).claimGeneration());
            }
            ControlPlane emittingControl = emittingNode.equals(TwoMemberCluster.NODE_A) ? cluster.first() : cluster.second();
            ControlPlane logControl = logNode.equals(TwoMemberCluster.NODE_A) ? cluster.first() : cluster.second();
            Map<String, Object> logEvidence = new LinkedHashMap<>(NativeTelemetryPositiveCalibrationIT.assertScopedLogRead(
                    records.get(logNode), http, logControl, sessions.get(logNode).server().baseUrl(), receipt));
            logEvidence.put("observedNodeId", logNode);
            logEvidence.put("nodeSessionOwner", memberEvidence(afterBoots).get(logNode));
            report.addFork(logEvidence);
            Map<String, Object> ownerEvidence = new LinkedHashMap<>(Map.of(
                    "action", "actual-claimed-admission-and-current-owner", "nodeId", emittingNode,
                    "claim", submission.claim(), "admissionObject", submission.admissionObjectId(),
                    "job", submission.job().job(), "scope", receipt.scope(), "memberBootsBefore", memberEvidence(memberBoots),
                    "memberBootsAfter", memberEvidence(afterBoots), "leaseUntilAtAdmission", submission.claim().get("leaseUntil"),
                    "leaseUntilAfter", after.leaseUntil().toString()));
            ownerEvidence.put("leaseUntilBefore", claim.leaseUntil().toString());
            ownerEvidence.put("exportEmittingNode", emittingNode);
            ownerEvidence.put("logEmittingNode", logNode);
            report.addFork(ownerEvidence);
            report.addFork(Map.of("action", "actual-fresh-prometheus-response", "body", body,
                    "bodySha256", digest(body.getBytes(StandardCharsets.UTF_8)), "observedNodeId", emittingNode));
            emittingControl.stop(PIPELINE, false);
            Await.until("the warm copy stops before its members close", WAIT,
                    () -> emittingControl.state(PIPELINE).filter(PipelineState.STOPPED::equals).isPresent(),
                    () -> String.valueOf(emittingControl.state(PIPELINE)));
            assertThat(claims.currentGeneration(clusterId, PIPELINE)).hasValue(claim.executionGeneration());
            for (String node : List.of(TwoMemberCluster.NODE_B, TwoMemberCluster.NODE_A)) {
                var terminal = sessions.get(node).shutdownAndFinish();
                Map<String, Object> evidence = new LinkedHashMap<>(terminal.evidence());
                evidence.put("observedNodeId", node);
                report.addFork(evidence);
                if (continuing) {
                    records.get(node).addAll(terminal.records());
                    assertThat(records.get(node).size()).as("all retained CONTINUE native records, including close").isLessThanOrEqualTo(MAX_RECORDS);
                }
                assertThat(terminal.invocationDrainComplete()).isTrue();
                assertThat(terminal.ownedVmDeath()).isTrue();
                assertThat(terminal.ownedVmDisconnected()).isTrue();
                if (node.equals(emittingNode)) {
                    assertThat(terminal.unverified()).isEmpty();
                    assertThat(terminal.decodedAndAuthorityBound()).isTrue();
                }
            }
            assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
            assertThat(inputHashes(harness)).isEqualTo(inputs);
            for (var connector : connectors.entrySet()) {
                assertThat(PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(connector.getKey())))
                        .isEqualTo(connector.getValue());
            }
            report.completeDiagnostic(Map.of("correctness", continuing
                            ? "ACTUAL_CLAIMED_PARTIAL_SNAPSHOT_CONTINUE_EXACT_FLOOR_AND_RAW"
                            : replacement ? "ACTUAL_CLAIMED_RESET_REPLACEMENT_ADMISSION_AND_EMITTER_TELEMETRY"
                            : "ACTUAL_WARM_CLAIMED_ADMISSION_AND_EMITTER_TELEMETRY",
                    "warmOnly", !replacement, "performanceAcceptanceEligible", false, "unverified", List.of(
                            "REPLACEMENT_CRASH_HANDOFF", continuing ? "CLAIMED_OWNER_TAKEOVER" : "CLAIMED_PARTIAL_SNAPSHOT_CONTINUE", "OLD_CALLBACK_WINDOWS", "NEGATIVE_UNKNOWN_BASELINE_MATRIX",
                            "NONEMITTING_MEMBER_NEGATIVE_CALIBRATION", "FORMAL_PERFORMANCE_ACCEPTANCE")));
        } catch (Exception | Error failure) {
            primary = failure;
            try { report.fail(failure); } catch (RuntimeException reporting) { failure.addSuppressed(reporting); }
            throw failure;
        } finally {
            Throwable cleanup = null;
            for (String node : List.of(TwoMemberCluster.NODE_B, TwoMemberCluster.NODE_A)) {
                var observer = sessions.get(node);
                if (observer != null) {
                    try { observer.close(); } catch (Exception | Error failure) { cleanup = add(cleanup, failure); }
                }
            }
            if (cluster != null) {
                try { cluster.close(); } catch (Exception | Error failure) { cleanup = add(cleanup, failure); }
            }
            for (int port : scrapePorts.values()) { RealProcessServer.releasePort(port); }
            if (cleanup != null) {
                if (primary != null) { if (primary != cleanup) { primary.addSuppressed(cleanup); } }
                else {
                    try { report.fail(cleanup); } catch (RuntimeException reporting) { cleanup.addSuppressed(reporting); }
                    if (cleanup instanceof Exception exception) { throw exception; }
                    throw (Error) cleanup;
                }
            }
        }
    }

    private record Series(String name, Map<String, String> attributes) { }
    private record ContinuedReading(ObservationStore.Stored publicValue, ObservationStore.StoredContinuation privateValue,
            Map<String, Object> raw, List<MetricFact> rawFacts) { }

    private static void captureContinueBoundaries(Map<String, NativeTelemetryIdentityJdiSession> sessions,
            Map<String, List<Map<String, Object>>> records, BenchmarkLiveReport report, String phase) {
        try {
            for (var session : sessions.entrySet()) {
                var boundary = session.getValue().boundary(phase);
                Map<String, Object> evidence = new LinkedHashMap<>(boundary.evidence());
                evidence.put("observedNodeId", session.getKey());
                report.addFork(evidence);
                records.get(session.getKey()).addAll(boundary.records());
                assertThat(records.get(session.getKey()).size()).as("all retained CONTINUE native records on %s", session.getKey())
                        .isLessThanOrEqualTo(MAX_RECORDS);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("the claimed continuation evidence read was interrupted", interrupted);
        } catch (Exception failure) {
            throw new AssertionError("the claimed continuation evidence read failed", failure);
        }
    }

    private static Optional<ContinuedReading> matchedContinuation(MongoObservationStore latest,
            List<Map<String, Object>> records, ObservationStore.Scope scope, Map<String, Object> job,
            ObservationContinuation floor, Instant after) {
        var publicReading = latest.readStored(PIPELINE).filter(value -> value.scope().filter(scope::equals).isPresent()
                && value.observation().state() == PipelineState.RUNNING && hasKnownDelivery(value.observation().facts())
                && (after == null || value.observation().observedAt().isAfter(after)));
        if (publicReading.isEmpty()) { return Optional.empty(); }
        var publicValue = publicReading.orElseThrow();
        Map<String, Object> identity = Map.of("incarnation", scope.pipelineIncarnationId(), "generation", scope.executionGeneration());
        var raw = records.stream().filter(value -> "RAW_CONTINUATION".equals(value.get("target"))
                && Boolean.TRUE.equals(value.get("normalReturn")) && !value.containsKey("decoderStatus")
                && Boolean.TRUE.equals(value.get("publicationPresent")) && identity.equals(value.get("scope"))
                && job.equals(value.get("job")) && publicValue.observation().observedAt().toString().equals(value.get("observedAt")))
                .reduce((first, second) -> {
                    assertThat(first.get("facts")).as("one actual scope and publication time has one complete raw frame")
                            .isEqualTo(second.get("facts"));
                    return first;
                });
        if (raw.isEmpty()) { return Optional.empty(); }
        List<MetricFact> facts = decodedFacts(raw.orElseThrow().get("facts"));
        if (!hasKnownDelivery(facts)) { return Optional.empty(); }
        var privateValue = latest.readContinuation(PIPELINE);
        var actualJob = new StopReservation.JobIdentity((String) job.get("clusterId"),
                ((Number) job.get("jobId")).longValue(), (String) job.get("bootId"));
        var handoff = new HandoffIdentity(PIPELINE, floor.token(), StopReservation.CounterPolicy.CONTINUE,
                floor.sourceScope(), scope, actualJob);
        if (privateValue.isEmpty() || !privateValue.orElseThrow().receipt().knownBaseline()
                || !privateValue.orElseThrow().receipt().matches(handoff)
                || latest.readStored(PIPELINE).filter(publicValue::equals).isEmpty()) { return Optional.empty(); }
        return Optional.of(new ContinuedReading(publicValue, privateValue.orElseThrow(), raw.orElseThrow(), facts));
    }

    private static List<MetricFact> decodedFacts(Object value) {
        assertThat(value).isInstanceOf(List.class);
        List<MetricFact> facts = new ArrayList<>();
        for (Object rawFact : (List<?>) value) {
            assertThat(rawFact).isInstanceOf(Map.class);
            Map<?, ?> fact = (Map<?, ?>) rawFact;
            List<MetricPoint> points = new ArrayList<>();
            for (Object rawPoint : (List<?>) fact.get("points")) {
                Map<?, ?> point = (Map<?, ?>) rawPoint;
                Map<String, String> attributes = new LinkedHashMap<>();
                ((Map<?, ?>) point.get("attributes")).forEach((key, item) -> {
                    assertThat(key).isInstanceOf(String.class); assertThat(item).isInstanceOf(String.class);
                    attributes.put((String) key, (String) item);
                });
                Instant start = point.get("startTime") == null ? null : Instant.parse((String) point.get("startTime"));
                Instant at = Instant.parse((String) point.get("observedAt"));
                HistogramValue histogram = null;
                if (point.get("histogram") instanceof Map<?, ?> distribution) {
                    List<Double> bounds = ((List<?>) distribution.get("bounds")).stream().map(item -> ((Number) item).doubleValue()).toList();
                    List<Long> buckets = ((List<?>) distribution.get("buckets")).stream().map(item -> ((Number) item).longValue()).toList();
                    histogram = new HistogramValue(((Number) distribution.get("count")).longValue(),
                            ((Number) distribution.get("sum")).doubleValue(), bounds, buckets);
                }
                Long count = point.get("value") == null ? null : ((Number) point.get("value")).longValue();
                points.add(new MetricPoint(attributes, start, at, count, histogram));
            }
            facts.add(new MetricFact((String) fact.get("name"), MetricType.valueOf((String) fact.get("type")),
                    (String) fact.get("unit"), points));
        }
        return List.copyOf(facts);
    }

    private static List<MetricFact> delivery(List<MetricFact> facts) {
        return facts.stream().filter(fact -> DELIVERY.contains(fact.name()))
                .map(fact -> new MetricFact(fact.name(), fact.type(), fact.unit(), fact.points().stream()
                        .filter(point -> TABLE.equals(point.attributes().get(MetricAttributes.TABLE_ID))
                                && (fact.type() == MetricType.HISTOGRAM || "out".equals(point.attributes().get("direction")))).toList()))
                .filter(fact -> !fact.points().isEmpty()).sorted(Comparator.comparing(MetricFact::name)).toList();
    }

    private static boolean hasKnownDelivery(List<MetricFact> facts) {
        var selected = delivery(facts);
        return selected.size() == DELIVERY.size() && selected.stream().allMatch(fact -> fact.points().stream()
                .allMatch(point -> point.startTime() != null && (point.histogram() == null ? point.value() > 0 : point.histogram().count() > 0)));
    }

    private static Map<Series, MetricPoint> points(List<MetricFact> facts) {
        Map<Series, MetricPoint> result = new LinkedHashMap<>();
        delivery(facts).forEach(fact -> fact.points().forEach(point -> {
            assertThat(result.put(new Series(fact.name(), point.attributes()), point)).isNull();
        }));
        return Map.copyOf(result);
    }

    private static void assertCumulativeExactly(ContinuedReading reading, ObservationContinuation floor) {
        var base = points(floor.baselineFacts()); var raw = points(reading.rawFacts());
        var published = points(reading.publicValue().observation().facts());
        assertSameTotals(floor.baselineFacts(), reading.privateValue().continuation().baselineFacts());
        assertThat(raw.keySet()).containsAll(base.keySet()); assertThat(published.keySet()).isEqualTo(raw.keySet());
        for (var entry : raw.entrySet()) {
            MetricPoint old = base.get(entry.getKey()), nativePoint = entry.getValue(), cumulative = published.get(entry.getKey());
            assertThat(cumulative).isNotNull();
            if (old == null) {
                assertThat(cumulative.startTime().toEpochMilli()).isEqualTo(nativePoint.startTime().toEpochMilli());
                assertThat(cumulative.value()).isEqualTo(nativePoint.value());
                assertThat(cumulative.histogram()).isEqualTo(nativePoint.histogram());
                continue;
            }
            assertThat(cumulative.startTime()).as("CONTINUE retains the original logical start").isEqualTo(old.startTime());
            assertThat(nativePoint.startTime()).isNotNull().isAfter(old.startTime());
            if (old.histogram() == null) {
                assertThat(cumulative.value()).as("the actual native value is added to the known floor exactly once")
                        .isEqualTo(Math.addExact(old.value(), nativePoint.value()));
            } else {
                HistogramValue before = old.histogram(), measured = nativePoint.histogram(), after = cumulative.histogram();
                assertThat(after.bounds()).isEqualTo(before.bounds()).isEqualTo(measured.bounds());
                assertThat(after.count()).isEqualTo(Math.addExact(before.count(), measured.count()));
                double sum = before.sum() + measured.sum();
                assertThat(after.sum()).isCloseTo(sum, offset(Math.max(1e-8, Math.abs(sum) * 1e-9)));
                for (int index = 0; index < before.bucketCounts().size(); index++) {
                    assertThat(after.bucketCounts().get(index)).isEqualTo(Math.addExact(before.bucketCounts().get(index), measured.bucketCounts().get(index)));
                }
            }
        }
    }

    private static void assertSameTotals(List<MetricFact> before, List<MetricFact> after) {
        var old = points(before); var next = points(after); assertThat(next.keySet()).isEqualTo(old.keySet());
        old.forEach((series, point) -> {
            assertThat(next.get(series).startTime()).isEqualTo(point.startTime());
            assertThat(next.get(series).value()).isEqualTo(point.value());
            assertThat(next.get(series).histogram()).isEqualTo(point.histogram());
        });
    }

    private static void assertFloorAtLeast(List<MetricFact> before, List<MetricFact> floor) {
        var old = points(before); var kept = points(floor); assertThat(kept.keySet()).containsAll(old.keySet());
        old.forEach((series, point) -> {
            MetricPoint next = kept.get(series); assertThat(next.startTime()).isEqualTo(point.startTime());
            if (point.histogram() == null) { assertThat(next.value()).isGreaterThanOrEqualTo(point.value()); }
            else {
                assertThat(next.histogram().bounds()).isEqualTo(point.histogram().bounds());
                assertThat(next.histogram().count()).isGreaterThanOrEqualTo(point.histogram().count());
                double tolerance = Math.max(1e-8, Math.abs(point.histogram().sum()) * 1e-9);
                assertThat(next.histogram().sum()).isGreaterThanOrEqualTo(point.histogram().sum() - tolerance);
                for (int index = 0; index < point.histogram().bucketCounts().size(); index++) {
                    assertThat(next.histogram().bucketCounts().get(index)).isGreaterThanOrEqualTo(point.histogram().bucketCounts().get(index));
                }
            }
        });
    }

    private static boolean sameNativeTotals(ContinuedReading first, ContinuedReading next) {
        var old = points(first.rawFacts()); var after = points(next.rawFacts());
        return old.keySet().equals(after.keySet()) && old.entrySet().stream().allMatch(entry -> {
            MetricPoint point = entry.getValue(), current = after.get(entry.getKey());
            return Objects.equals(point.startTime(), current.startTime()) && Objects.equals(point.value(), current.value())
                    && Objects.equals(point.histogram(), current.histogram());
        });
    }

    private static List<Map<String, Object>> factEvidence(List<MetricFact> facts) {
        return facts.stream().map(fact -> {
            List<Map<String, Object>> points = fact.points().stream().map(point -> {
                Map<String, Object> out = new LinkedHashMap<>(); out.put("attributes", point.attributes());
                out.put("startTime", point.startTime() == null ? null : point.startTime().toString());
                out.put("observedAt", point.observedAt().toString()); out.put("value", point.value());
                if (point.histogram() != null) {
                    HistogramValue histogram = point.histogram();
                    out.put("histogram", Map.of("count", histogram.count(), "sum", histogram.sum(),
                            "bounds", histogram.bounds(), "buckets", histogram.bucketCounts()));
                }
                return java.util.Collections.unmodifiableMap(out);
            }).toList();
            return Map.<String, Object>of("name", fact.name(), "type", fact.type().name(), "unit", fact.unit(), "points", points);
        }).toList();
    }

    private static void verifyContinuedSnapshotAndCdc(java.sql.Connection source, String targetUri, MongoDatabase database,
            MongoObservationStore latest, MongoStateStore actual, ObservationStore.Scope scope, Map<String, Object> job,
            ObservationContinuation floor, Map<String, NativeTelemetryIdentityJdiSession> sessions,
            Map<String, List<Map<String, Object>>> records, BenchmarkLiveReport report, String emittingNode, ControlPlane control) throws Exception {
        long deadline = System.nanoTime() + DELIVERY_WAIT.toNanos();
        try (var target = MongoClients.create(targetUri)) {
            MongoDatabase targetDatabase = target.getDatabase(new ConnectionString(targetUri).getDatabase());
            Await.until("the real claimed continued snapshot finishes its physical delivery and durable ACK", remaining(deadline),
                    () -> {
                        captureContinueBoundaries(sessions, records, report, "claimed-continued-delivery");
                        assertThat(actual.read(PIPELINE).map(value -> StateJson.parse(value.stateJson()))
                                .filter(PipelineState.FAILED::equals))
                                .as("an actual source or sink failure remains visible").isEmpty();
                        return targetDatabase.getCollection(TABLE).countDocuments() == SNAPSHOT_ROWS
                                && snapshotConfirmed(database, control);
                    }, () -> "target rows=" + targetDatabase.getCollection(TABLE).countDocuments());
            try (var sql = source.createStatement()) {
                assertThat(sql.executeUpdate("UPDATE " + TABLE + " SET amount=777,payload='native-updated' WHERE id=7")).isEqualTo(1);
                assertThat(sql.executeUpdate("DELETE FROM " + TABLE + " WHERE id=11")).isEqualTo(1);
                assertThat(sql.executeUpdate("INSERT INTO " + TABLE + " VALUES (" + (SNAPSHOT_ROWS + 1L)
                        + "," + (SNAPSHOT_ROWS + 1L) * 100 + ",'native-" + (SNAPSHOT_ROWS + 1L) + "')")).isEqualTo(1);
            }
            Await.until("actual claimed resumed CDC update, delete and insert reach Mongo", remaining(deadline),
                    () -> {
                        captureContinueBoundaries(sessions, records, report, "claimed-continued-cdc");
                        Document updated = targetDatabase.getCollection(TABLE).find(new Document("id", 7L)).first();
                        Document inserted = targetDatabase.getCollection(TABLE).find(new Document("id", SNAPSHOT_ROWS + 1L)).first();
                        return targetDatabase.getCollection(TABLE).countDocuments() == SNAPSHOT_ROWS
                                && updated != null && ((Number) updated.get("amount")).longValue() == 777
                                && "native-updated".equals(updated.getString("payload"))
                                && targetDatabase.getCollection(TABLE).find(new Document("id", 11L)).first() == null
                                && inserted != null && ((Number) inserted.get("amount")).longValue() == (SNAPSHOT_ROWS + 1L) * 100
                                && ("native-" + (SNAPSHOT_ROWS + 1L)).equals(inserted.getString("payload"));
                    }, () -> "target rows=" + targetDatabase.getCollection(TABLE).countDocuments());
            String contentHash = assertBulkTargetContent(targetDatabase);
            Instant targetConfirmed = Instant.now();
            var quiet = Await.answered("a matched actual raw frame after all physical CDC delivery", remaining(deadline),
                    () -> {
                        captureContinueBoundaries(sessions, records, report, "claimed-continued-final-raw");
                        return matchedContinuation(latest, records.get(emittingNode), scope, job, floor, targetConfirmed)
                                .map(value -> { assertCumulativeExactly(value, floor); return value; })
                                .filter(value -> finalNativeDelivery(value.rawFacts()));
                    });
            assertCumulativeExactly(quiet, floor);
            var repeated = Await.answered("a later unchanged native frame keeps the floor added only once", remaining(deadline),
                    () -> {
                        captureContinueBoundaries(sessions, records, report, "claimed-continued-repeat-raw");
                        return matchedContinuation(latest, records.get(emittingNode), scope, job, floor,
                                quiet.publicValue().observation().observedAt())
                                .map(value -> { assertCumulativeExactly(value, floor); return value; })
                                .filter(value -> sameNativeTotals(quiet, value));
                    });
            assertCumulativeExactly(repeated, floor);
            assertSameTotals(quiet.publicValue().observation().facts(), repeated.publicValue().observation().facts());
            assertThat(actual.read(PIPELINE).map(value -> StateJson.parse(value.stateJson()))).contains(PipelineState.RUNNING);
            assertThat(control.errorCount(PIPELINE)).contains(0L);
            report.addFork(Map.of("action", "actual-claimed-continued-snapshot-and-cdc-qualified",
                    "physicalRows", SNAPSHOT_ROWS, "targetContentSha256", contentHash,
                    "scope", Map.of("incarnation", scope.pipelineIncarnationId(), "generation", scope.executionGeneration()),
                    "job", job, "floor", factEvidence(floor.baselineFacts()), "rawNative", repeated.raw(),
                    "cumulative", factEvidence(repeated.publicValue().observation().facts()),
                    "firstFinalObservedAt", quiet.publicValue().observation().observedAt().toString(),
                    "repeatedObservedAt", repeated.publicValue().observation().observedAt().toString()));
        }
    }

    private static boolean finalNativeDelivery(List<MetricFact> facts) {
        Map<String, Long> operations = new LinkedHashMap<>();
        for (MetricFact fact : delivery(facts)) {
            if (!"tapstate.pipeline.records".equals(fact.name())) { continue; }
            for (MetricPoint point : fact.points()) {
                assertThat(operations.put(point.attributes().get("op"), point.value()))
                        .as("the actual final raw delivery has one point per operation").isNull();
            }
        }
        // Physical Mongo delivery can precede the native writer's counter and latency completion.
        // The fixed quiet anchor must include all known work from this actual replacement Job.
        return operations.equals(Map.of("read", (long) SNAPSHOT_ROWS, "insert", 1L, "update", 1L, "delete", 1L))
                && delivery(facts).stream().filter(fact -> "tapstate.pipeline.record.delivery.duration".equals(fact.name()))
                        .flatMap(fact -> fact.points().stream())
                        .anyMatch(point -> point.histogram().count() == SNAPSHOT_ROWS + 3L);
    }

    private static boolean snapshotConfirmed(MongoDatabase database, ControlPlane control) {
        var positions = control.positionRead(PIPELINE);
        assertThat(positions.pipelineId()).isEqualTo(PIPELINE);
        var chains = positions.chains().stream().filter(chain -> SOURCE.equals(chain.sourceId())
                && chain.tables().contains(TABLE)).toList();
        assertThat(chains).as("the public position response names this source and table's actual chain").hasSize(1);
        String chainId = chains.getFirst().chainId();
        String consumerId = SrsConsumerId.of(PIPELINE, SOURCE).value();
        Document cursor = database.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                .find(new Document("_id", new Document("chain", chainId).append("pipeline", consumerId)))
                .projection(new Document("miningChainId", 1).append("pipelineId", 1)
                        .append("ownerPipelineId", 1).append("sourceNodeId", 1).append("snapshotCompletedTables", 1)).first();
        if (cursor == null) { return false; }
        assertThat(cursor.getString("miningChainId")).isEqualTo(chainId);
        assertThat(cursor.getString("pipelineId")).isEqualTo(consumerId);
        assertThat(cursor.getString("ownerPipelineId")).isEqualTo(PIPELINE);
        assertThat(cursor.getString("sourceNodeId")).isEqualTo(SOURCE);
        Object completed = cursor.get("snapshotCompletedTables");
        assertThat(completed == null || completed instanceof List<?>).isTrue();
        return completed instanceof List<?> tables && tables.contains(TABLE);
    }

    private static String assertBulkTargetContent(MongoDatabase target) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        long count = 0, expected = 1;
        try (var cursor = target.getCollection(TABLE).find().sort(new Document("id", 1)).batchSize(512).iterator()) {
            while (cursor.hasNext()) {
                Document row = cursor.next(); if (expected == 11) { expected++; }
                long id = ((Number) row.get("id")).longValue();
                long amount = id == 7 ? 777 : id * 100;
                String payload = id == 7 ? "native-updated" : "native-" + id;
                if (id != expected || ((Number) row.get("amount")).longValue() != amount || !payload.equals(row.getString("payload"))) {
                    throw new AssertionError("the actual continued target has different content at id " + id + ", expected " + expected);
                }
                digest.update((id + ":" + amount + ":" + payload + "\n").getBytes(StandardCharsets.UTF_8));
                expected++; count++;
            }
        }
        assertThat(count).isEqualTo(SNAPSHOT_ROWS); assertThat(expected).isEqualTo(SNAPSHOT_ROWS + 2L);
        return HexFormat.of().formatHex(digest.digest());
    }

    private static Map<Map<String, String>, Instant> counterStarts(io.tapstate.core.lifecycle.Observation observation) {
        Map<Map<String, String>, Instant> result = new LinkedHashMap<>();
        observation.facts().stream().filter(fact -> fact.name().equals("tapstate.pipeline.records"))
                .flatMap(fact -> fact.points().stream()).filter(point -> "out".equals(point.attributes().get("direction")))
                .forEach(point -> { assertThat(point.startTime()).isNotNull(); result.put(point.attributes(), point.startTime()); });
        return Map.copyOf(result);
    }

    private static Map<String, WorkloadClaim> nodeSessions(MongoWorkloadClaimStore claims, String clusterId) {
        Map<String, WorkloadClaim> result = new LinkedHashMap<>();
        for (String node : List.of(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B)) {
            var reading = claims.read(new WorkloadClaimKey(clusterId, WorkloadClaimType.NODE_SESSION, node)).orElseThrow();
            assertThat(reading.leased()).isTrue();
            assertThat(reading.claim().owner().nodeId()).isEqualTo(node);
            result.put(node, reading.claim());
        }
        return Map.copyOf(result);
    }

    private static Map<String, Object> claimTuple(WorkloadClaim claim) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", Map.of("clusterId", claim.key().clusterId(), "type", claim.key().type().name(),
                "resourceId", claim.key().resourceId()));
        result.put("owner", Map.of("nodeId", claim.owner().nodeId(), "bootId", claim.owner().bootId()));
        result.put("claimGeneration", claim.claimGeneration()); result.put("executionGeneration", claim.executionGeneration());
        result.put("topologyRevision", claim.topologyRevision());
        result.put("contextExecutionGeneration", claim.contextExecutionGeneration());
        result.put("executionClaimGeneration", claim.executionClaimGeneration());
        result.put("executionNodeIds", claim.executionNodeIds().stream().sorted().toList());
        result.put("failureClaimGeneration", claim.failureClaimGeneration());
        result.put("failureAfterMemberLoss", claim.failureAfterMemberLoss());
        return Map.copyOf(result);
    }

    private static Map<String, Object> memberEvidence(Map<String, WorkloadClaim> claims) {
        Map<String, Object> result = new LinkedHashMap<>();
        claims.forEach((node, claim) -> result.put(node, Map.of("owner", claimTuple(claim).get("owner"),
                "claimGeneration", claim.claimGeneration(), "leaseUntil", claim.leaseUntil().toString())));
        return Map.copyOf(result);
    }

    private static Map<String, Object> withoutLease(Map<String, Object> claim) {
        Map<String, Object> result = new LinkedHashMap<>(claim);
        result.remove("leaseUntil");
        return Map.copyOf(result);
    }

    private static void assertBridge(List<Map<String, Object>> records,
            NativeTelemetryIdentityJdiSession.ClaimedSubmission submission) {
        assertThat(records.stream().anyMatch(record -> "ADMISSION".equals(record.get("target"))
                && Boolean.TRUE.equals(record.get("normalReturn")) && !record.containsKey("decoderStatus")
                && Boolean.TRUE.equals(record.get("claimed")) && Boolean.TRUE.equals(record.get("allowed"))
                && Long.valueOf(submission.admissionObjectId()).equals(record.get("admissionObject"))
                && submission.claim().equals(record.get("claim")))).isTrue();
        assertThat(records.stream().anyMatch(record -> "SUBMIT".equals(record.get("target"))
                && Boolean.TRUE.equals(record.get("normalReturn")) && !record.containsKey("decoderStatus")
                && Boolean.TRUE.equals(record.get("registered"))
                && Long.valueOf(submission.admissionObjectId()).equals(record.get("admissionObject"))
                && Long.valueOf(submission.job().executionObjectId()).equals(record.get("executionJobObject"))
                && submission.job().job().equals(record.get("job")))).isTrue();
    }

    private static void assertReplacementSlot(NativeTelemetryIdentityJdiSession.ClaimedReplacement proof,
            NativeTelemetryIdentityJdiSession.ClaimedSubmission previous,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt receipt, boolean continuing) {
        assertThat(proof.successorAdmissionObjectId()).isPositive();
        assertThat(proof.reservationObjectId()).isPositive();
        assertThat(proof.advancedClaimObjectId()).isPositive();
        Map<String, Object> reservation = proof.reservation();
        assertThat(reservation).containsEntry("pipelineId", PIPELINE).containsEntry("phase", "SUCCESSOR_ADMITTED")
                .containsEntry("counterPolicy", continuing ? "CONTINUE" : "RESET");
        assertThat(reservation.get("token")).isInstanceOf(String.class);
        assertThat((String) reservation.get("token")).isNotBlank().hasSizeLessThanOrEqualTo(256);
        assertThat(reservation.get("source")).isInstanceOf(Map.class);
        Map<?, ?> source = (Map<?, ?>) reservation.get("source");
        assertThat(source.get("scope")).isEqualTo(previous.job().scope());
        assertThat(source.get("oldJob")).isEqualTo(previous.job().job());
        Map<?, ?> slot = (Map<?, ?>) reservation.get("successor");
        assertThat(slot.get("scope")).isEqualTo(receipt.scope());
        assertThat(slot.get("job")).as("the adoption argument predates native binding").isNull();
        assertThat(slot.get("submissionBootId")).isEqualTo(proof.submission().job().job().get("bootId"));
        Map<?, ?> desired = (Map<?, ?>) reservation.get("originalDesired");
        assertThat(desired.get("targetState")).isEqualTo("RUNNING");
        assertThat(desired.get("purgeState")).isEqualTo(!continuing);
        if (continuing) { assertThat(desired.get("rebuiltAtStateEpoch")).isNull(); }
        if (!continuing) { assertThat(desired.get("reassemble")).isEqualTo(true); }
        Map<?, ?> writer = (Map<?, ?>) reservation.get("writerAuthority");
        assertThat(writer.get("executionGeneration")).isEqualTo(proof.submission().claim().get("executionGeneration"));
        Map<?, ?> fence = (Map<?, ?>) writer.get("claim");
        for (String name : List.of("key", "owner", "claimGeneration", "executionGeneration", "topologyRevision")) {
            assertThat(fence.get(name)).as("the actual advanced claim and marker authority agree on %s", name)
                    .isEqualTo(proof.submission().claim().get(name));
        }
    }

    private static void assertReplacementBridge(List<Map<String, Object>> records,
            NativeTelemetryIdentityJdiSession.ClaimedReplacement proof) {
        var submission = proof.submission();
        assertThat(records.stream().anyMatch(record -> "REPLACEMENT_ADMISSION".equals(record.get("target"))
                && Boolean.TRUE.equals(record.get("normalReturn")) && !record.containsKey("decoderStatus")
                && Boolean.TRUE.equals(record.get("claimed")) && Boolean.TRUE.equals(record.get("allowed"))
                && "ABSENT".equals(record.get("returnedAdmittedClaimStatus"))
                && "SUCCESSOR_ADMISSION_ARGUMENT".equals(record.get("claimSource"))
                && Long.valueOf(proof.successorAdmissionObjectId()).equals(record.get("successorAdmissionObject"))
                && Long.valueOf(proof.reservationObjectId()).equals(record.get("reservationObject"))
                && Long.valueOf(proof.advancedClaimObjectId()).equals(record.get("advancedClaimObject"))
                && Long.valueOf(submission.admissionObjectId()).equals(record.get("admissionObject"))
                && proof.reservation().equals(record.get("reservation"))
                && submission.claim().equals(record.get("claim")))).isTrue();
        assertThat(records.stream().anyMatch(record -> "SUBMIT".equals(record.get("target"))
                && Boolean.TRUE.equals(record.get("normalReturn")) && !record.containsKey("decoderStatus")
                && "REPLACEMENT".equals(record.get("admissionKind")) && Boolean.TRUE.equals(record.get("registered"))
                && Long.valueOf(proof.successorAdmissionObjectId()).equals(record.get("successorAdmissionObject"))
                && Long.valueOf(submission.admissionObjectId()).equals(record.get("admissionObject"))
                && Long.valueOf(submission.job().executionObjectId()).equals(record.get("executionJobObject"))
                && proof.reservation().equals(record.get("reservation"))
                && submission.job().job().equals(record.get("job")))).isTrue();
    }

    private static void assertRows(MongoDatabase database, int expected) {
        List<Document> rows = database.getCollection(TABLE).find().sort(new Document("id", 1)).limit(5).into(new ArrayList<>());
        assertThat(rows).hasSize(expected);
        for (int index = 0; index < expected; index++) {
            long id = index + 1L;
            assertThat(((Number) rows.get(index).get("id")).longValue()).isEqualTo(id);
            assertThat(((Number) rows.get(index).get("amount")).longValue()).isEqualTo(id * 100);
            assertThat(rows.get(index).getString("payload")).isEqualTo("native-" + id);
        }
    }

    private static Map<String, String> resources(Map<String, Object> settings, String targetUri) {
        return Map.of(SOURCE + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: native_claimed_source
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s, highPerformance: false }
                mode: cdc
                tables: [ native_claimed_orders ]
                """.formatted(settings.get("host"), settings.get("port"), settings.get("database"),
                        settings.get("username"), settings.get("password")),
                "native_claimed_target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: native_claimed_target
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(targetUri), PIPELINE + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: native_claimed_copy
                source: native_claimed_source
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: native_claimed_orders
                  sync: [ { source: native_claimed_target } ]
                """);
    }

    private static Map<String, Object> inputHashes(Path root) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        for (Class<?> type : List.of(NativeClaimedTelemetryPositiveCalibrationIT.class,
                NativeTelemetryIdentityJdiSession.class, NativeTelemetryMirror.class, TwoMemberCluster.class,
                RealProcessServer.class, NativeTelemetryPositiveCalibrationIT.class)) {
            result.put(type.getSimpleName() + "Source", PipelineBenchmarkLiveRunIT.artifact(root.resolve(
                    "e2e/src/test/java/io/tapstate/e2e/" + type.getSimpleName() + ".java")));
            try (InputStream stream = type.getResourceAsStream(type.getSimpleName() + ".class")) {
                assertThat(stream).isNotNull();
                byte[] bytes = stream.readNBytes(MAX_BYTES + 1);
                assertThat(bytes.length).isLessThanOrEqualTo(MAX_BYTES);
                result.put(type.getSimpleName() + "ExecutingClassSha256", digest(bytes));
            }
        }
        return Map.copyOf(result);
    }

    private static Throwable add(Throwable primary, Throwable extra) {
        if (primary == null) { return extra; }
        if (primary != extra) { primary.addSuppressed(extra); }
        return primary;
    }

    private static Duration remaining(long deadline) {
        long left = deadline - System.nanoTime();
        assertThat(left).as("the claimed positive proof uses its original two-minute deadline").isPositive();
        return Duration.ofNanos(left);
    }

    private static String digest(byte[] bytes) throws Exception {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
    }

    private static String required(String name) {
        String value = System.getProperty(PREFIX + name);
        if (value == null || value.isBlank()) { throw new IllegalArgumentException("calibration requires " + PREFIX + name); }
        return value;
    }
}
