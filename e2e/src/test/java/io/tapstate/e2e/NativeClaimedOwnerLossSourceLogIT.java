package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoArtifactStore;
import io.tapstate.adapters.mongostore.MongoAuthStores;
import io.tapstate.adapters.mongostore.MongoClusterIdentityStore;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStateStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.adapters.mongostore.MongoWorkloadClaimStore;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.SourceResource;
import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.SrsConsumerId;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
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
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static io.tapstate.e2e.NativeTelemetryPositiveCalibrationIT.flag;
import static io.tapstate.e2e.NativeTelemetryPositiveCalibrationIT.matchingProduced;
import static io.tapstate.e2e.NativeTelemetryPositiveCalibrationIT.positiveScrape;

/** A real warm shared source and its controller are killed together before a fresh survivor submission. */
@RequiresDocker
class NativeClaimedOwnerLossSourceLogIT {
    private static final String PREFIX = "tapstate.e2e.native-claimed-owner-loss-source-log.";
    private static final String Q = "q", SOURCE = "owner_loss_source", TABLE = "owner_loss_orders";
    private static final Duration WAIT = Duration.ofMinutes(2);
    private static final int MAX_RECORDS = 512, MAX_BYTES = 2 * 1024 * 1024;
    private record Shared(String chain, WorkloadClaimKey captureKey, String consumer) { }
    private record Ready(WorkloadClaim pipeline, WorkloadClaim capture, long epoch,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt receipt) { }
    private record Bridge(NativeTelemetryIdentityJdiSession.ClaimedSubmission submission,
            NativeTelemetryIdentityJdiSession.ClaimedReplacement replacement, long submitReturn) { }
    private record Positive(NativeTelemetryIdentityJdiSession.Boundary boundary, String scrape) { }

    @Test
    @Timeout(value = 20, unit = TimeUnit.MINUTES)
    void aWarmCommonOwnerCrashProducesAFreshSurvivorJobAndSourceLogUnderItsActualNewScope() throws Exception {
        Assumptions.assumeTrue(List.of("jar", "sha256", "output").stream()
                .anyMatch(key -> System.getProperty(PREFIX + key) != null), "named immutable native inputs are required");
        Path jar = Path.of(required("jar")).toAbsolutePath().normalize();
        String sha = required("sha256");
        assertThat(sha).matches("[0-9a-f]{64}");
        assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
        BenchmarkCaptureCalibrationLiveRunIT.requireConnectors();
        Path harness = PipelineBenchmarkLiveRunIT.harnessRoot();
        Path output = Path.of(required("output")).toAbsolutePath().normalize();
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, harness);
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        Map<String, Object> application = PipelineBenchmarkLiveRunIT.artifact(jar);
        Map<String, Object> connectorInputs = connectorInputs();
        Map<String, Object> harnessInputs = harnessInputs(harness);
        String name = "owner_loss_" + UUID.randomUUID().toString().replace("-", "");
        String storeUri = SharedMongo.replicaSetUrl(name + "_control");
        String targetUri = SharedMongo.replicaSetUrl(name + "_target");
        String operator = name + "_operator";
        Map<String, Object> settings = SharedMySql.settings(name + "_source");
        Map<String, NativeTelemetryIdentityJdiSession> sessions = new LinkedHashMap<>();
        Map<String, List<Map<String, Object>>> records = new LinkedHashMap<>();
        Map<String, Integer> scrapePorts = new LinkedHashMap<>();
        TwoMemberCluster cluster = null;
        Throwable primary = null;
        try (var sqlConnection = SharedMySql.connect(settings); var mongo = MongoClients.create(storeUri);
                var targetClient = MongoClients.create(targetUri)) {
            try (var sql = sqlConnection.createStatement()) {
                sql.execute("CREATE TABLE " + TABLE + " (id BIGINT PRIMARY KEY, amount BIGINT NOT NULL, payload VARCHAR(32) NOT NULL)");
                sql.execute("INSERT INTO " + TABLE + " VALUES (1,100,'owner-1'),(2,200,'owner-2'),(3,300,'owner-3')");
            }
            MongoDatabase db = mongo.getDatabase(new ConnectionString(storeUri).getDatabase());
            MongoDatabase target = targetClient.getDatabase(new ConnectionString(targetUri).getDatabase());
            Map<String, String> resources = resources(settings, targetUri);
            report.begin(Map.of("purpose", "ACTUAL_WARM_COMMON_OWNER_OS_KILL_AND_FRESH_SOURCE_LOG",
                    "application", application, "expectedJarSha256", sha, "connectors", connectorInputs,
                    "harness", harnessInputs, "pipeline", Q, "clusterMembers", 2, "performanceAcceptanceEligible", false),
                    PipelineBenchmarkLiveRunIT.environment(), List.of());
            String clusterId;
            try (var setup = RealProcessServer.start(storeUri, operator, jar)) {
                ControlPlane control = new ControlPlane(setup.baseUrl());
                control.bootstrapAndLogin("benchmark", "benchmark-password");
                control.registerConnector("mysql", ConnectorJars.bytesFor("mysql"));
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                control.apply(Map.of(SOURCE + ".tap.yml", resources.get(SOURCE + ".tap.yml")));
                Map<String, Object> discovery = new LinkedHashMap<>(settings); discovery.put("highPerformance", false);
                control.discoverSchema(SOURCE, "mysql", discovery);
                control.apply(resources);
                clusterId = new MongoClusterIdentityStore(db.getCollection(MongoAuthStores.CLUSTER_IDENTITY))
                        .find().orElseThrow().clusterId();
                assertThat(db.getCollection(MongoStorePort.WORKLOAD_CLAIMS).countDocuments(new Document("clusterId", clusterId)
                        .append("resourceType", "PIPELINE_ACTUATION").append("resourceId", Q))).isZero();
            }
            for (String node : List.of(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B)) {
                scrapePorts.put(node, RealProcessServer.reservePort()); records.put(node, new ArrayList<>());
            }
            cluster = TwoMemberCluster.start(storeUri, operator, jar, clusterId, "benchmark", "benchmark-password",
                    List.of(), List.of(), (node, address, arguments, jvm) -> {
                        try {
                            var observer = NativeTelemetryIdentityJdiSession.startWithReplacementObservation(jar, sha, Q,
                                    (artifact, debug) -> {
                                        List<String> options = new ArrayList<>(jvm); options.addAll(debug);
                                        return RealProcessServer.launchingWithJvmArguments(storeUri, operator, artifact, address, httpPort -> {
                                            List<String> args = new ArrayList<>(arguments.apply(httpPort));
                                            args.add("--tapstate.metrics.export.prometheus.host=127.0.0.1");
                                            args.add("--tapstate.metrics.export.prometheus.port=" + scrapePorts.get(node));
                                            args.add("--tapstate.metrics.history.sample-interval=PT2S");
                                            return List.copyOf(args);
                                        }, List.copyOf(options));
                                    });
                            sessions.put(node, observer); return observer.server();
                        } catch (RuntimeException | Error failure) { throw failure; }
                        catch (Exception failure) { throw new AssertionError("the actual observed member could not start", failure); }
                    });
            TwoMemberCluster owned = cluster;
            assertThat(owned.awaitBothMembers()).containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            assertThat(owned.second().clusterMemberNodeIds()).containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            assertThat(owned.first().clusterId()).isEqualTo(clusterId);
            assertThat(owned.second().clusterId()).isEqualTo(clusterId);
            var claims = new MongoWorkloadClaimStore(db.getCollection(MongoStorePort.WORKLOAD_CLAIMS));
            var states = new MongoStateStore(db.getCollection(MongoStorePort.PIPELINE_STATE));
            var latest = new MongoObservationStore(mongo, db.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                    db.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
            WorkloadClaimKey pipelineKey = new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, Q);
            Map<String, WorkloadClaim> boots = nodeSessions(claims, clusterId);
            var sourceResource = new MongoArtifactStore(mongo, db.getCollection(MongoStorePort.ARTIFACTS)).get(SOURCE).orElseThrow();
            assertThat(sourceResource).isInstanceOf(SourceResource.class);
            SourceResource typedSource = (SourceResource) sourceResource;
            CaptureConfig config = new CaptureConfig(typedSource.connector(), typedSource.config(), List.of(TABLE));
            String srsKey = typedSource.srs() == null ? null : typedSource.srs().key();
            Shared shared = new Shared(MiningChainId.resolve(config, srsKey).value(),
                    new WorkloadClaimKey(clusterId, WorkloadClaimType.CAPTURE, CaptureId.of(config, srsKey).value()),
                    SrsConsumerId.of(Q, SOURCE).value());
            Document artifactBefore = artifact(db);
            long deadline = System.nanoTime() + WAIT.toNanos();
            owned.first().lifecycle(Q, LifecycleVerb.START);
            WorkloadClaim initial = Await.answered("the actual first leased pipeline admission", left(deadline),
                    () -> claims.read(pipelineKey).filter(value -> value.leased() && value.claim().executionGeneration() > 0)
                            .map(value -> value.claim()));
            assertThat(initial.executionNodeIds()).containsExactlyInAnyOrder(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            assertThat(initial.contextExecutionGeneration()).isEqualTo(initial.executionGeneration());
            assertThat(initial.executionClaimGeneration()).isEqualTo(initial.claimGeneration());
            NativeTelemetryIdentityJdiSession.AuthorityReceipt beforeReceipt = authority(db, initial, artifactBefore);
            for (var observer : sessions.values()) { observer.recordAuthority(beforeReceipt); }
            String ownerNode = initial.owner().nodeId();
            String survivorNode = ownerNode.equals(TwoMemberCluster.NODE_A) ? TwoMemberCluster.NODE_B : TwoMemberCluster.NODE_A;
            var owner = sessions.get(ownerNode); var survivor = sessions.get(survivorNode);
            ControlPlane ownerControl = ownerNode.equals(TwoMemberCluster.NODE_A) ? owned.first() : owned.second();
            ControlPlane survivorControl = owned.memberOtherThan(ownerNode);
            Await.until("the actual shared snapshot reaches Mongo", left(deadline), () -> {
                capture(owner, ownerNode, records, report, "owner-loss-warm-snapshot");
                return target.getCollection(TABLE).countDocuments() == 3 && current(latest, beforeReceipt).isPresent();
            }, () -> "targetRows=" + target.getCollection(TABLE).countDocuments());
            assertRows(target, 3);
            try (var sql = sqlConnection.createStatement()) { sql.execute("INSERT INTO " + TABLE + " VALUES (4,400,'owner-4')"); }
            Await.until("actual pre-kill CDC row4 and durable source root", left(deadline), () -> {
                capture(owner, ownerNode, records, report, "owner-loss-warm-cdc");
                return target.getCollection(TABLE).countDocuments() == 4 && durableRoot(db, shared, null).isPresent();
            }, () -> "targetRows=" + target.getCollection(TABLE).countDocuments());
            assertRows(target, 4);
            Ready warm = ready(db, claims, pipelineKey, shared, initial, beforeReceipt, boots, ownerControl);
            assertThat(warm.capture().owner()).as("this actual fixture kills the pipeline and shared CAPTURE common owner")
                    .isEqualTo(warm.pipeline().owner());
            Bridge warmBridge = Await.answered("the real warm returned admission and submitted Job lookup", left(deadline), () -> {
                capture(owner, ownerNode, records, report, "owner-loss-warm-native-bridge");
                return bridgeIfPresent(owner, beforeReceipt, records.get(ownerNode), -1);
            });
            assertThat(warmBridge.submission().members()).containsExactlyInAnyOrder(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            assertThat(withoutLease(warmBridge.submission().claim())).isEqualTo(tuple(warm.pipeline()));
            HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
            URI warmScrape = URI.create("http://127.0.0.1:" + scrapePorts.get(ownerNode) + "/metrics");
            Positive warmPositive = positive(owner, ownerNode, beforeReceipt, records, report, http, warmScrape, -1, deadline);
            List<Map<String, Object>> warmSourceLogs = records.get(ownerNode).stream()
                    .filter(record -> sourceLog(record, beforeReceipt)).toList();
            assertThat(warmSourceLogs).as("a true native source stream caller must precede the fault").isNotEmpty();
            report.addFork(scopedSourceLogRead(warmSourceLogs, http, ownerControl, owner.server().baseUrl(), beforeReceipt, deadline));
            assertThat(current(latest, beforeReceipt).orElseThrow().observation().facts().stream()
                    .filter(fact -> "tapstate.pipeline.records".equals(fact.name())).flatMap(fact -> fact.points().stream())
                    .anyMatch(point -> "out".equals(point.attributes().get("direction")) && point.value() != null && point.value() > 0))
                    .as("the actual warm stored scope carries positive output records").isTrue();
            assertThat(warmPositive.boundary().decodedAndAuthorityBound()).isTrue();
            ready(db, claims, pipelineKey, shared, warm.pipeline(), beforeReceipt, boots, ownerControl);
            assertThat(artifact(db)).isEqualTo(artifactBefore);
            var cutoff = capture(survivor, survivorNode, records, report, "owner-loss-survivor-before-fault");
            long cutoffEvent = cutoff.events();
            report.addFork(Map.of("action", "actual-warm-common-owner-before-kill", "pipelineClaim", tuple(warm.pipeline()),
                    "captureClaim", tuple(warm.capture()), "captureEpoch", warm.epoch(), "chain", shared.chain(),
                    "ownerNode", ownerNode, "ownedPid", owner.server().pid(), "survivorCutoffEvent", cutoffEvent,
                    "job", warmBridge.submission().job().job(), "scope", beforeReceipt.scope()));
            assertThat(states.read(Q).map(cp -> StateJson.parse(cp.stateJson()))).contains(PipelineState.RUNNING);
            Instant faultAt = Instant.now();
            Map<String, Object> mark = owner.markOwnedCrash();
            report.addFork(Map.of("action", "caller-owned-crash-mark", "mark", mark, "callerFaultAt", faultAt.toString()));
            long killedPid = owner.server().pid(); owner.server().kill();
            assertThat(owner.server().isAlive()).isFalse();
            var crashed = owner.finishOwnedCrash();
            report.addFork(crashed.evidence());
            if (crashed.observed() != null) { append(ownerNode, crashed.observed(), records); }
            assertThat(crashed.ownedPid()).isEqualTo(killedPid);
            assertThat(crashed.ownedFaultObserved()).isTrue(); assertThat(crashed.artifactUnchanged()).isTrue();
            assertThat(crashed.pumpStopped()).isTrue(); assertThat(crashed.evidenceFailure()).isNull();
            assertThat(crashed.observerFailure()).isNull(); assertThat(crashed.cleanupFailure()).isNull();
            assertThat(crashed.observed()).isNotNull(); assertThat(crashed.unpairedCalls()).isNotNull();
            assertThat(crashed.observed().queueDrained()).isFalse();
            boolean[] actualFailed = {false};
            var failedReceipt = new java.util.concurrent.atomic.AtomicReference<Map<String, Object>>(Map.of());
            WorkloadClaim next = Await.answered("a real survivor admission follows the actual FAILED process", left(deadline), () -> {
                states.read(Q).ifPresent(checkpoint -> {
                    if (StateJson.parse(checkpoint.stateJson()) == PipelineState.FAILED) {
                        actualFailed[0] = true;
                        failedReceipt.set(Map.of("kind", "ACTUAL_CHECKPOINT_NONATOMIC", "pipelineId", Q,
                                "state", "FAILED", "epoch", checkpoint.epoch(), "touchTime", checkpoint.touchTime().toString()));
                    }
                });
                if (!actualFailed[0]) {
                    Document failed = db.getCollection(MongoStorePort.PIPELINE_EVENTS).find(new Document("pipelineId", Q)
                            .append("pipelineIncarnationId", beforeReceipt.incarnation()).append("executionGeneration", beforeReceipt.generation())
                            .append("kind", "STATE_CHANGED").append("afterState", "FAILED")
                            .append("occurredAt", new Document("$gte", Date.from(faultAt))))
                            .projection(new Document("_id", 1).append("pipelineId", 1).append("pipelineIncarnationId", 1)
                                    .append("executionGeneration", 1).append("kind", 1).append("afterState", 1).append("occurredAt", 1))
                            .limit(1).first();
                    actualFailed[0] = failed != null;
                    if (failed != null) { failedReceipt.set(Map.of("kind", "ACTUAL_RETAINED_STATE_CHANGE", "row", failed.toJson())); }
                }
                capture(survivor, survivorNode, records, report, "owner-loss-recovery-admission");
                return !actualFailed[0] ? Optional.empty() : claims.read(pipelineKey)
                        .filter(value -> value.leased() && value.claim().executionGeneration() > warm.pipeline().executionGeneration()
                                && value.claim().owner().nodeId().equals(survivorNode)).map(value -> value.claim());
            });
            report.addFork(Map.of("action", "actual-failed-process-receipt", "observed", actualFailed[0], "proof", failedReceipt.get()));
            assertThat(actualFailed[0]).as("a new RUNNING claim cannot replace missing proof of FAILED").isTrue();
            assertThat(next.owner()).isEqualTo(boots.get(survivorNode).owner());
            NativeTelemetryIdentityJdiSession.AuthorityReceipt afterReceipt = authority(db, next, artifactBefore);
            survivor.recordAuthority(afterReceipt);
            Bridge recovered = Await.answered("actual fresh admission then submit then fresh Job entry and return", left(deadline), () -> {
                capture(survivor, survivorNode, records, report, "owner-loss-fresh-job");
                return bridgeIfPresent(survivor, afterReceipt, records.get(survivorNode), cutoffEvent);
            });
            assertThat(recovered.submission().job().job().get("jobId")).isNotEqualTo(warmBridge.submission().job().job().get("jobId"));
            assertThat(withoutLease(recovered.submission().claim())).isEqualTo(tuple(next));
            Ready recoveredReady = Await.answered("actual new CAPTURE holder and epoch before row5", left(deadline), () -> {
                var p = claims.read(pipelineKey).filter(value -> value.leased()).map(value -> value.claim());
                var c = claims.read(shared.captureKey()).filter(value -> value.leased()
                        && value.claim().owner().equals(next.owner()) && value.claim().claimGeneration() > warm.capture().claimGeneration())
                        .map(value -> value.claim());
                var root = durableRoot(db, shared, warm.epoch());
                return p.isPresent() && c.isPresent() && root.isPresent() && current(latest, afterReceipt).isPresent()
                        ? Optional.of(ready(db, claims, pipelineKey, shared, next, afterReceipt, boots, survivorControl)) : Optional.empty();
            });
            assertThat(recoveredReady.epoch()).isGreaterThan(warm.epoch());
            assertThat(claims.advanceUnderClaim(warm.pipeline(), warm.pipeline().topologyRevision())).isEmpty();
            report.addFork(Map.of("action", "actual-survivor-before-new-input", "scope", afterReceipt.scope(),
                    "pipelineClaim", tuple(recoveredReady.pipeline()), "captureClaim", tuple(recoveredReady.capture()),
                    "captureEpoch", recoveredReady.epoch(), "freshSubmitReturnOrder", recovered.submitReturn(),
                    "oldClaimAdvance", "REFUSED", "actualFailedObserved", true,
                    "admissionMode", recovered.replacement() == null ? "ORDINARY" : "REPLACEMENT"));
            try (var sql = sqlConnection.createStatement()) { sql.execute("INSERT INTO " + TABLE + " VALUES (5,500,'owner-5')"); }
            Await.until("row5 reaches the actual new shared source execution", left(deadline), () -> {
                capture(survivor, survivorNode, records, report, "owner-loss-new-cdc");
                return target.getCollection(TABLE).countDocuments() == 5 && durableRoot(db, shared, warm.epoch()).isPresent();
            }, () -> "targetRows=" + target.getCollection(TABLE).countDocuments());
            assertRows(target, 5);
            URI newScrape = URI.create("http://127.0.0.1:" + scrapePorts.get(survivorNode) + "/metrics");
            Positive recoveredPositive = positive(survivor, survivorNode, afterReceipt, records, report, http,
                    newScrape, recovered.submitReturn(), deadline);
            List<Map<String, Object>> recoveredLogs = records.get(survivorNode).stream()
                    .filter(record -> sourceLog(record, afterReceipt) && order(record, "entryOrder") > recovered.submitReturn()).toList();
            assertThat(recoveredLogs).isNotEmpty();
            report.addFork(scopedSourceLogRead(recoveredLogs, http, survivorControl, survivor.server().baseUrl(), afterReceipt, deadline));
            ready(db, claims, pipelineKey, shared, next, afterReceipt, boots, survivorControl);
            assertThat(artifact(db)).isEqualTo(artifactBefore);
            assertThat(recoveredPositive.boundary().decodedAndAuthorityBound()).isTrue();
            survivorControl.stop(Q, false);
            Await.until("the actual recovered pipeline stops", left(deadline), () -> states.read(Q)
                    .filter(cp -> StateJson.parse(cp.stateJson()) == PipelineState.STOPPED).isPresent(), () -> "state=" + states.read(Q));
            var terminal = survivor.shutdownAndFinish(); append(survivorNode, terminal, records); report.addFork(terminal.evidence());
            assertThat(terminal.invocationDrainComplete()).isTrue(); assertThat(terminal.ownedVmDeath()).isTrue();
            assertThat(terminal.ownedVmDisconnected()).isTrue(); assertThat(terminal.decodedAndAuthorityBound()).isTrue();
            assertThat(PipelineBenchmarkLiveRunIT.artifact(jar)).isEqualTo(application);
            assertThat(connectorInputs()).isEqualTo(connectorInputs); assertThat(harnessInputs(harness)).isEqualTo(harnessInputs);
            report.completeDiagnostic(Map.of("qualification", "WARM_COMMON_OWNER_KILL_FRESH_SURVIVOR_SOURCE_LOG_AND_TELEMETRY",
                    "acceptanceEvaluated", false, "taskAcceptanceEligible", false, "performanceAcceptanceEligible", false,
                    "oldHeapCleanup", "UNKNOWN", "oldCrashDrain", "INCOMPLETE",
                    "unverified", List.of("OLD_CALLBACK_ZERO_WINDOWS", "UNKNOWN_BASELINE_MATRIX", "REPLACEMENT_CRASH_CUTS", "FORMAL_PERFORMANCE")));
        } catch (Exception | Error failure) {
            primary = failure; try { report.fail(failure); } catch (RuntimeException recording) { failure.addSuppressed(recording); }
            throw failure;
        } finally {
            Throwable cleanup = null;
            for (var observer : sessions.values()) {
                try { observer.close(); } catch (Exception | Error problem) { cleanup = add(cleanup, problem); }
            }
            if (cluster != null) { try { cluster.close(); } catch (Exception | Error problem) { cleanup = add(cleanup, problem); } }
            for (int port : scrapePorts.values()) { RealProcessServer.releasePort(port); }
            if (cleanup != null) {
                if (primary != null) { primary.addSuppressed(cleanup); }
                else {
                    try { report.fail(cleanup); } catch (RuntimeException recording) { cleanup.addSuppressed(recording); }
                    if (cleanup instanceof Exception problem) { throw problem; } throw (Error) cleanup;
                }
            }
        }
    }

    private static Optional<ObservationStore.Stored> current(MongoObservationStore latest,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt receipt) {
        return latest.readStored(Q).filter(value -> value.scope().filter(scope -> scope.pipelineIncarnationId().equals(receipt.incarnation())
                && scope.executionGeneration() == receipt.generation()).isPresent() && value.observation().state() == PipelineState.RUNNING);
    }
    private static Document artifact(MongoDatabase db) {
        return Optional.ofNullable(db.getCollection(MongoStorePort.ARTIFACTS).find(new Document("_id", Q))
                .projection(new Document("pipelineIncarnationId", 1).append("contentHash", 1)).first()).orElseThrow();
    }
    private static NativeTelemetryIdentityJdiSession.AuthorityReceipt authority(MongoDatabase db, WorkloadClaim claim, Document artifact) {
        Document actual = db.getCollection(MongoStorePort.WORKLOAD_CLAIMS).find(new Document("clusterId", claim.key().clusterId())
                .append("resourceType", "PIPELINE_ACTUATION").append("resourceId", Q)).projection(new Document("_id", 1)).first();
        assertThat(actual).isNotNull();
        return new NativeTelemetryIdentityJdiSession.AuthorityReceipt(Q, claim.key().clusterId(), artifact.getString("pipelineIncarnationId"),
                claim.executionGeneration(), JsonWriter.write(actual.get("_id")), Instant.now().toString());
    }
    private static Ready ready(MongoDatabase db, MongoWorkloadClaimStore claims, WorkloadClaimKey key, Shared shared,
            WorkloadClaim expected, NativeTelemetryIdentityJdiSession.AuthorityReceipt receipt,
            Map<String, WorkloadClaim> boots, ControlPlane control) {
        WorkloadClaim pipeline = claims.read(key).filter(value -> value.leased()).orElseThrow().claim();
        WorkloadClaim capture = claims.read(shared.captureKey()).filter(value -> value.leased()).orElseThrow().claim();
        assertThat(WorkloadClaimFence.from(pipeline)).isEqualTo(WorkloadClaimFence.from(expected));
        assertThat(pipeline.owner()).isEqualTo(boots.get(pipeline.owner().nodeId()).owner());
        WorkloadClaim session = claims.read(new WorkloadClaimKey(key.clusterId(), WorkloadClaimType.NODE_SESSION,
                pipeline.owner().nodeId())).filter(value -> value.leased()).orElseThrow().claim();
        assertThat(session.owner()).isEqualTo(pipeline.owner());
        assertThat(session.claimGeneration()).isEqualTo(boots.get(pipeline.owner().nodeId()).claimGeneration());
        assertThat(capture.owner()).isEqualTo(boots.get(capture.owner().nodeId()).owner());
        assertThat(artifact(db).getString("pipelineIncarnationId")).isEqualTo(receipt.incarnation());
        assertThat(claims.currentGeneration(key.clusterId(), Q)).hasValue(receipt.generation());
        var positions = control.positionRead(Q);
        var chains = positions.chains().stream().filter(chain -> SOURCE.equals(chain.sourceId()) && chain.tables().contains(TABLE)).toList();
        assertThat(chains).hasSize(1); assertThat(chains.getFirst().chainId()).isEqualTo(shared.chain());
        Document cursor = db.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS)
                .find(new Document("_id", new Document("chain", shared.chain()).append("pipeline", shared.consumer())))
                .projection(new Document("miningChainId", 1).append("ownerPipelineId", 1).append("sourceNodeId", 1).append("snapshotCompletedTables", 1)).first();
        assertThat(cursor).isNotNull(); assertThat(cursor.getString("miningChainId")).isEqualTo(shared.chain());
        assertThat(cursor.getString("ownerPipelineId")).isEqualTo(Q); assertThat(cursor.getString("sourceNodeId")).isEqualTo(SOURCE);
        assertThat(cursor.get("snapshotCompletedTables")).isInstanceOf(List.class);
        assertThat(((List<?>) cursor.get("snapshotCompletedTables")).stream().anyMatch(TABLE::equals)).isTrue();
        Document root = durableRoot(db, shared, null).orElseThrow();
        long epoch = ((Number) root.get("epoch")).longValue();
        assertThat(WorkloadClaimFence.from(claims.read(key).filter(value -> value.leased()).orElseThrow().claim()))
                .isEqualTo(WorkloadClaimFence.from(pipeline));
        assertThat(WorkloadClaimFence.from(claims.read(shared.captureKey()).filter(value -> value.leased()).orElseThrow().claim()))
                .isEqualTo(WorkloadClaimFence.from(capture));
        return new Ready(pipeline, capture, epoch, receipt);
    }
    private static Optional<Document> durableRoot(MongoDatabase db, Shared shared, Long afterEpoch) {
        Document root = db.getCollection(MongoStorePort.SRS_META).find(new Document("_id", shared.chain()))
                .projection(new Document("epoch", 1).append("sourceReadOffset", 1).append("sourceReadDurable", 1)).first();
        return root == null || !(root.get("epoch") instanceof Number epoch) || epoch.longValue() <= 0
                || afterEpoch != null && epoch.longValue() <= afterEpoch || root.get("sourceReadOffset") == null
                || !Boolean.TRUE.equals(root.get("sourceReadDurable")) ? Optional.empty() : Optional.of(root);
    }
    private static Map<String, WorkloadClaim> nodeSessions(MongoWorkloadClaimStore claims, String clusterId) {
        Map<String, WorkloadClaim> out = new LinkedHashMap<>();
        for (String node : List.of(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B)) {
            WorkloadClaim actual = claims.read(new WorkloadClaimKey(clusterId, WorkloadClaimType.NODE_SESSION, node))
                    .filter(value -> value.leased()).orElseThrow().claim();
            assertThat(actual.owner().nodeId()).isEqualTo(node); out.put(node, actual);
        }
        return Map.copyOf(out);
    }
    private static NativeTelemetryIdentityJdiSession.Boundary capture(NativeTelemetryIdentityJdiSession observer, String node,
            Map<String, List<Map<String, Object>>> records, BenchmarkLiveReport report, String phase) {
        try {
            var value = observer.boundary(phase); append(node, value, records);
            Map<String, Object> evidence = new LinkedHashMap<>(value.evidence()); evidence.put("observedNodeId", node);
            report.addFork(evidence); return value;
        } catch (InterruptedException problem) { Thread.currentThread().interrupt(); throw new AssertionError("native capture interrupted", problem); }
        catch (Exception problem) { throw new AssertionError("native capture failed", problem); }
    }
    private static void append(String node, NativeTelemetryIdentityJdiSession.Boundary boundary, Map<String, List<Map<String, Object>>> records) {
        records.get(node).addAll(boundary.records());
        assertThat(records.get(node).size()).as("all actual retained records on %s", node).isLessThanOrEqualTo(MAX_RECORDS);
        assertThat(JsonWriter.write(records.get(node)).getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(MAX_BYTES);
    }
    private static Optional<Bridge> bridgeIfPresent(NativeTelemetryIdentityJdiSession observer,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt receipt, List<Map<String, Object>> records, long cutoff) {
        var ordinary = observer.claimedSubmission(receipt); var replacement = observer.claimedReplacement(receipt);
        if (ordinary.isEmpty() && replacement.isEmpty()) { return Optional.empty(); }
        assertThat(ordinary.isPresent() && replacement.isPresent()).isFalse();
        var submission = ordinary.orElseGet(() -> replacement.orElseThrow().submission());
        var admissions = records.stream().filter(record -> good(record) && Boolean.TRUE.equals(record.get("allowed"))
                && Boolean.TRUE.equals(record.get("claimed")) && order(record, "entryOrder") > cutoff
                && Long.valueOf(submission.admissionObjectId()).equals(record.get("admissionObject"))
                && submission.claim().equals(record.get("claim"))
                && ("ADMISSION".equals(record.get("target")) || "REPLACEMENT_ADMISSION".equals(record.get("target")))).toList();
        var submits = records.stream().filter(record -> good(record) && order(record, "entryOrder") > cutoff
                && "SUBMIT".equals(record.get("target")) && Boolean.TRUE.equals(record.get("registered"))
                && Long.valueOf(submission.admissionObjectId()).equals(record.get("admissionObject"))
                && Long.valueOf(submission.job().executionObjectId()).equals(record.get("executionJobObject"))
                && submission.job().job().equals(record.get("job"))).toList();
        if (admissions.isEmpty() || submits.isEmpty()) { return Optional.empty(); }
        long submitted = order(submits.getFirst(), "returnOrder");
        long submitEntry = order(submits.getFirst(), "entryOrder");
        boolean freshJob = records.stream().anyMatch(record -> good(record) && "JOB".equals(record.get("target"))
                && receipt.scope().equals(record.get("scope")) && submission.job().job().equals(record.get("job"))
                && order(record, "returnOrder") > order(record, "entryOrder")
                && (cutoff < 0 ? order(record, "entryOrder") > submitEntry && order(record, "returnOrder") < submitted
                        : order(record, "entryOrder") > submitted));
        if (!freshJob) { return Optional.empty(); }
        assertThat(order(admissions.getFirst(), "returnOrder")).isLessThan(order(submits.getFirst(), "entryOrder"));
        return Optional.of(new Bridge(submission, replacement.orElse(null), submitted));
    }
    private static Bridge bridge(NativeTelemetryIdentityJdiSession observer, NativeTelemetryIdentityJdiSession.AuthorityReceipt receipt,
            List<Map<String, Object>> records, long cutoff) { return bridgeIfPresent(observer, receipt, records, cutoff).orElseThrow(); }
    private static Positive positive(NativeTelemetryIdentityJdiSession observer, String node,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt receipt, Map<String, List<Map<String, Object>>> records,
            BenchmarkLiveReport report, HttpClient http, URI scrape, long cutoff, long deadline) {
        return Await.answered("actual scoped source stream log and matching offer/visible/produce/fresh scrape", left(deadline), () -> {
            try {
                String body = NativeTelemetryPositiveCalibrationIT.scrape(http, scrape, deadline);
                var boundary = capture(observer, node, records, report, "owner-loss-positive");
                var fresh = records.get(node).stream().filter(record -> order(record, "entryOrder") > cutoff).toList();
                boolean source = fresh.stream().anyMatch(record -> sourceLog(record, receipt));
                boolean accepted = flag(fresh, "OFFER", "accepted", receipt);
                boolean visible = flag(fresh, "VISIBLE", "included", receipt);
                boolean produced = matchingProduced(fresh, body, receipt);
                Map<String, Object> predicates = Map.of("sourceStreamLog", source, "acceptedOffer", accepted, "visible", visible,
                        "matchingProduced", produced, "positiveFreshScrape", positiveScrape(body, Q),
                        "decodedInvocationDrain", boundary.decodedAndAuthorityBound());
                report.addFork(Map.of("action", "owned-source-positive-predicates", "node", node, "scope", receipt.scope(),
                        "predicates", predicates, "scrapeBody", body, "scrapeSha256", digest(body.getBytes(StandardCharsets.UTF_8))));
                return predicates.values().stream().allMatch(Boolean.TRUE::equals) ? Optional.of(new Positive(boundary, body)) : Optional.empty();
            } catch (InterruptedException problem) { Thread.currentThread().interrupt(); throw new AssertionError("positive native read interrupted", problem); }
            catch (Exception problem) { throw new AssertionError("positive native read failed", problem); }
        });
    }
    private static Map<String, Object> scopedSourceLogRead(List<Map<String, Object>> observed, HttpClient http,
            ControlPlane control, URI base, NativeTelemetryIdentityJdiSession.AuthorityReceipt receipt, long deadline) throws Exception {
        Duration remaining = left(deadline);
        var reply = http.send(HttpRequest.newBuilder(base.resolve("/api/pipelines/" + Q + "/logs?scope=current"))
                .timeout(remaining.compareTo(Duration.ofSeconds(20)) < 0 ? remaining : Duration.ofSeconds(20))
                .header("Authorization", "Bearer " + control.credential()).GET().build(), HttpResponse.BodyHandlers.ofString());
        assertThat(reply.statusCode()).isEqualTo(200);
        assertThat(reply.body().getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(MAX_BYTES);
        Object parsed = JsonReader.parse(reply.body()); assertThat(parsed).isInstanceOf(Map.class);
        Object lines = ((Map<?, ?>) parsed).get("lines"); assertThat(lines).isInstanceOf(List.class);
        assertThat(((List<?>) lines).stream().filter(Map.class::isInstance).map(Map.class::cast).anyMatch(line ->
                observed.stream().anyMatch(record -> sourceLog(record, receipt)
                        && line.get("timestampMillis") instanceof Number time && record.get("timestampMillis") instanceof Number captured
                        && time.longValue() == captured.longValue() && line.get("level").equals(record.get("level"))
                        && line.get("message").equals(record.get("message")))))
                .as("the current node-local endpoint serves the actual source stream scoped line").isTrue();
        left(deadline);
        return Map.of("action", "actual-source-current-http-log", "scope", receipt.scope(), "body", reply.body(),
                "bodySha256", digest(reply.body().getBytes(StandardCharsets.UTF_8)));
    }
    private static boolean good(Map<String, Object> record) { return Boolean.TRUE.equals(record.get("normalReturn")) && !record.containsKey("decoderStatus"); }
    private static long order(Map<String, Object> record, String key) { return record.get(key) instanceof Number value ? value.longValue() : -1; }
    private static boolean sourceLog(Map<String, Object> record, NativeTelemetryIdentityJdiSession.AuthorityReceipt receipt) {
        if (!good(record) || !"LOG".equals(record.get("target")) || !receipt.scope().equals(record.get("scope"))
                || !(record.get("callers") instanceof List<?> encoded) || encoded.size() != 2
                || !(encoded.getFirst() instanceof List<?> strings) || !(encoded.get(1) instanceof List<?> callers) || callers.size() % 9 != 0) { return false; }
        for (int at = 0; at < callers.size(); at += 9) {
            if ("io.tapstate.adapters.pdk.PdkCapturePort".equals(decoded(strings, callers.get(at)))
                    && "streamLoop".equals(decoded(strings, callers.get(at + 1)))
                    && "EXACT_ARTIFACT_METHOD".equals(decoded(strings, callers.get(at + 7)))
                    && decoded(strings, callers.get(at + 8)) != null
                    && decoded(strings, callers.get(at + 8)).matches("[0-9a-f]{64}")) { return true; }
        }
        return false;
    }
    private static String decoded(List<?> strings, Object value) {
        return value instanceof Integer index && index >= 0 && index < strings.size() && strings.get(index) instanceof String text ? text : null;
    }
    private static void assertRows(MongoDatabase db, int size) {
        List<Document> rows = db.getCollection(TABLE).find().sort(new Document("id", 1)).limit(6).into(new ArrayList<>());
        assertThat(rows).hasSize(size);
        for (int at = 0; at < size; at++) {
            Document row = rows.get(at); long id = at + 1L;
            assertThat(((Number) row.get("id")).longValue()).isEqualTo(id);
            assertThat(((Number) row.get("amount")).longValue()).isEqualTo(id * 100);
            assertThat(row.getString("payload")).isEqualTo("owner-" + id);
        }
    }
    private static Map<String, Object> tuple(WorkloadClaim claim) {
        return Map.ofEntries(Map.entry("key", Map.of("clusterId", claim.key().clusterId(), "type", claim.key().type().name(), "resourceId", claim.key().resourceId())),
                Map.entry("owner", Map.of("nodeId", claim.owner().nodeId(), "bootId", claim.owner().bootId())),
                Map.entry("claimGeneration", claim.claimGeneration()), Map.entry("executionGeneration", claim.executionGeneration()),
                Map.entry("topologyRevision", claim.topologyRevision()), Map.entry("contextExecutionGeneration", claim.contextExecutionGeneration()),
                Map.entry("executionClaimGeneration", claim.executionClaimGeneration()), Map.entry("executionNodeIds", claim.executionNodeIds().stream().sorted().toList()),
                Map.entry("failureClaimGeneration", claim.failureClaimGeneration()), Map.entry("failureAfterMemberLoss", claim.failureAfterMemberLoss()));
    }
    private static Map<String, Object> withoutLease(Map<String, Object> input) {
        Map<String, Object> copy = new LinkedHashMap<>(input); copy.remove("leaseUntil"); return Map.copyOf(copy);
    }
    private static Map<String, Object> connectorInputs() throws Exception {
        Path directory = ConnectorJars.pathFor("mysql").getParent(); Map<String, Object> out = new LinkedHashMap<>();
        try (var files = Files.list(directory)) {
            List<Path> selected = files.filter(Files::isRegularFile).filter(path -> path.getFileName().toString().endsWith(".jar"))
                    .sorted().limit(6).toList();
            assertThat(selected).as("the native input directory contains the five frozen connector artifacts").hasSize(5);
            for (Path file : selected) { out.put(file.getFileName().toString(), PipelineBenchmarkLiveRunIT.artifact(file)); }
        }
        return Map.copyOf(out);
    }
    private static Map<String, Object> harnessInputs(Path root) throws Exception {
        Map<String, Object> out = new LinkedHashMap<>();
        for (Class<?> type : List.of(NativeClaimedOwnerLossSourceLogIT.class, NativeTelemetryIdentityJdiSession.class,
                NativeTelemetryMirror.class, NativeTelemetryPositiveCalibrationIT.class, TwoMemberCluster.class, RealProcessServer.class)) {
            out.put(type.getSimpleName() + ".source", PipelineBenchmarkLiveRunIT.artifact(root.resolve("e2e/src/test/java/io/tapstate/e2e/" + type.getSimpleName() + ".java")));
            try (InputStream input = type.getResourceAsStream(type.getSimpleName() + ".class")) {
                assertThat(input).isNotNull(); byte[] bytes = input.readNBytes(MAX_BYTES + 1);
                assertThat(bytes.length).isLessThanOrEqualTo(MAX_BYTES); out.put(type.getSimpleName() + ".classSha256", digest(bytes));
            }
            Class<?>[] nested = type.getDeclaredClasses(); assertThat(nested.length).isLessThanOrEqualTo(64);
            for (Class<?> child : nested) {
                try (InputStream input = child.getResourceAsStream("/" + child.getName().replace('.', '/') + ".class")) {
                    assertThat(input).isNotNull(); byte[] bytes = input.readNBytes(MAX_BYTES + 1);
                    assertThat(bytes.length).isLessThanOrEqualTo(MAX_BYTES); out.put(child.getName() + ".classSha256", digest(bytes));
                }
            }
        }
        return Map.copyOf(out);
    }
    private static Map<String, String> resources(Map<String, Object> settings, String target) {
        return Map.of(SOURCE + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: owner_loss_source
                connector: mysql
                config: { host: %s, port: %s, database: %s, username: %s, password: %s, highPerformance: false }
                mode: cdc
                tables: [ owner_loss_orders ]
                """.formatted(settings.get("host"), settings.get("port"), settings.get("database"), settings.get("username"), settings.get("password")),
                "owner_loss_target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: owner_loss_target
                connector: mongodb
                config: { uri: "%s" }
                """.formatted(target), Q + ".tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: q
                source: owner_loss_source
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: owner_loss_orders
                  sync: [ { source: owner_loss_target } ]
                """);
    }
    private static Duration left(long deadline) {
        long remaining = deadline - System.nanoTime(); assertThat(remaining).as("the original native calibration deadline remains").isPositive();
        return Duration.ofNanos(remaining);
    }
    private static String required(String key) {
        String value = System.getProperty(PREFIX + key); if (value == null || value.isBlank()) { throw new AssertionError("missing native input " + key); }
        return value;
    }
    private static String digest(byte[] input) throws Exception { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(input)); }
    private static Throwable add(Throwable first, Throwable next) { if (first == null) { return next; } if (first != next) { first.addSuppressed(next); } return first; }
}
