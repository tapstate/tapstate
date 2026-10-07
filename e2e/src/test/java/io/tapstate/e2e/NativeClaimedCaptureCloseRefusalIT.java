package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.*;
import io.tapstate.adapters.pdk.ConnectorError;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.SourceResource;
import io.tapstate.runtime.srs.CaptureId;
import io.tapstate.runtime.srs.MiningChainId;
import io.tapstate.spi.capture.CaptureConfig;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.MessageDigest;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static org.assertj.core.api.Assertions.assertThat;

/** A real claimed Mongo reader keeps ordinary stop honest while its actual delivery cannot end. */
@RequiresDocker
@EnabledIfSystemProperty(named = "tapstate.e2e.native-claimed-capture-close.jar", matches = ".+")
class NativeClaimedCaptureCloseRefusalIT {
    private static final String PREFIX = "tapstate.e2e.native-claimed-capture-close.";
    private static final String Q = "native_close_q", SOURCE = "native_close_source", TABLE = "native_close_orders";
    private static final Duration WAIT = Duration.ofMinutes(2);

    @Test @Timeout(value = 20, unit = TimeUnit.MINUTES)
    void aRealClaimedMongoDeliveryKeepsOrdinaryStopPendingUntilItsSameNativeHandleEnds() throws Exception {
        Path jar = Path.of(required("jar")).toRealPath(); String sha = required("sha256");
        assertThat(sha).matches("[0-9a-f]{64}"); assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
        BenchmarkCaptureCalibrationLiveRunIT.requireConnectors();
        Path harness = PipelineBenchmarkLiveRunIT.harnessRoot(), output = Path.of(required("output")).toAbsolutePath().normalize();
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, harness);
        Map<String, Object> input = inputHashes(harness), connectors = connectorInputs();
        String suffix = "native_close_" + UUID.randomUUID().toString().replace("-", "");
        String controlUri = SharedMongo.replicaSetUrl(suffix + "_control"), sourceUri = SharedMongo.replicaSetUrl(suffix + "_source");
        String targetUri = SharedMongo.replicaSetUrl(suffix + "_target"), operator = suffix + "_operator";
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        report.begin(Map.of("purpose", "ORDINARY_CLAIMED_NATIVE_SOURCE_CLOSE_REFUSAL", "application", PipelineBenchmarkLiveRunIT.artifact(jar),
                "harness", input, "connectors", connectors, "performanceAcceptanceEligible", false),
                PipelineBenchmarkLiveRunIT.environment(), List.of());
        Map<String, NativeCaptureCloseJdiSession> sessions = new LinkedHashMap<>(); TwoMemberCluster cluster = null;
        NativeCaptureCloseJdiSession held = null; Throwable primary = null;
        try (var store = MongoClients.create(controlUri); var source = MongoClients.create(sourceUri); var target = MongoClients.create(targetUri)) {
            MongoDatabase db = store.getDatabase(new ConnectionString(controlUri).getDatabase());
            MongoDatabase src = source.getDatabase(new ConnectionString(sourceUri).getDatabase());
            MongoDatabase dst = target.getDatabase(new ConnectionString(targetUri).getDatabase());
            src.getCollection(TABLE).insertMany(List.of(row(1), row(2), row(3)));
            Map<String, String> resources = resources(sourceUri, src.getName(), targetUri, dst.getName());
            String clusterId;
            try (var setup = RealProcessServer.start(controlUri, operator, jar)) {
                ControlPlane control = new ControlPlane(setup.baseUrl()); control.bootstrapAndLogin("benchmark", "benchmark-password");
                control.registerConnector("mongodb", ConnectorJars.bytesFor("mongodb"));
                control.apply(Map.of(SOURCE + ".tap.yml", resources.get(SOURCE + ".tap.yml")));
                control.discoverSchema(SOURCE, "mongodb", Map.of("uri", sourceUri, "database", src.getName()));
                control.apply(resources);
                clusterId = new MongoClusterIdentityStore(db.getCollection(MongoAuthStores.CLUSTER_IDENTITY)).find().orElseThrow().clusterId();
                assertThat(db.getCollection(MongoStorePort.WORKLOAD_CLAIMS).countDocuments(new Document("clusterId", clusterId)
                        .append("resourceType", "PIPELINE_ACTUATION").append("resourceId", Q))).isZero();
            }
            cluster = TwoMemberCluster.start(controlUri, operator, jar, clusterId, "benchmark", "benchmark-password",
                    List.of(), List.of(), (node, address, arguments, jvm) -> {
                        try {
                            var observer = NativeCaptureCloseJdiSession.start(jar, sha, Q, SOURCE, TABLE, (artifact, debug) -> {
                                List<String> options = new ArrayList<>(jvm); options.addAll(debug);
                                return RealProcessServer.launchingWithJvmArguments(controlUri, operator, artifact, address,
                                        port -> arguments.apply(port), List.copyOf(options));
                            });
                            sessions.put(node, observer); return observer.server();
                        } catch (RuntimeException | Error failure) { throw failure; }
                        catch (Exception failure) { throw new AssertionError("owned observed member could not start", failure); }
                    });
            var claims = new MongoWorkloadClaimStore(db.getCollection(MongoStorePort.WORKLOAD_CLAIMS));
            var states = new MongoStateStore(db.getCollection(MongoStorePort.PIPELINE_STATE));
            var desired = new MongoDesiredStore(db.getCollection(MongoStorePort.PIPELINE_DESIRED));
            var latest = new MongoObservationStore(store, db.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                    db.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
            SourceResource sourceResource = (SourceResource) new MongoArtifactStore(store, db.getCollection(MongoStorePort.ARTIFACTS))
                    .get(SOURCE).orElseThrow();
            CaptureConfig config = new CaptureConfig(sourceResource.connector(), sourceResource.config(), List.of(TABLE));
            String srsKey = sourceResource.srs() == null ? null : sourceResource.srs().key();
            String captureId = CaptureId.of(config, srsKey).value(), chainId = MiningChainId.resolve(config, srsKey).value();
            WorkloadClaimKey pKey = new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, Q);
            WorkloadClaimKey cKey = new WorkloadClaimKey(clusterId, WorkloadClaimType.CAPTURE, captureId);
            assertThat(claims.read(pKey)).isEmpty();
            assertThat(claims.read(cKey)).isEmpty();
            for (var session : sessions.entrySet()) {
                session.getValue().enableObservation();
                report.addFork(Map.of("action", "actual-owned-observer-enabled-before-first-START",
                        "nodeId", session.getKey(), "ownedPid", session.getValue().server().pid(),
                        "output", session.getValue().server().output().toString(),
                        "evidence", session.getValue().evidence()));
            }
            long deadline = System.nanoTime() + WAIT.toNanos();
            cluster.first().lifecycle(Q, io.tapstate.core.lifecycle.LifecycleVerb.START);
            WorkloadClaim pipeline = Await.answered("actual claimed ordinary admission", remaining(deadline), () -> live(claims, pKey).filter(value -> value.executionGeneration() > 0
                    && value.contextExecutionGeneration() == value.executionGeneration()
                    && value.executionClaimGeneration() == value.claimGeneration()));
            WorkloadClaim capture = Await.answered("actual physical capture lease", remaining(deadline), () -> live(claims, cKey));
            assertThat(capture.owner()).as("UNSELECTED fixture unless the genuine reader and pipeline are co-owned").isEqualTo(pipeline.owner());
            String owner = pipeline.owner().nodeId(); held = sessions.get(owner);
            assertThat(held).isNotNull();
            ControlPlane control = owner.equals(TwoMemberCluster.NODE_A) ? cluster.first() : cluster.second();
            for (String node : List.of(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B)) {
                var session = live(claims, new WorkloadClaimKey(clusterId, WorkloadClaimType.NODE_SESSION, node)).orElseThrow();
                assertThat(session.owner().nodeId()).isEqualTo(node);
                if (node.equals(owner)) { assertThat(session.owner()).isEqualTo(pipeline.owner()); }
            }
            NativeCaptureCloseJdiSession observer = held;
            Map<String, Object> admission = Await.answered("actual returned full admitted claim", remaining(deadline), observer::admission);
            Map<String, Object> nativeJob = Await.answered("actual native Job metadata normal return", remaining(deadline), observer::job);
            assertThat(withoutLease((Map<?, ?>) admission.get("claim"))).isEqualTo(tuple(pipeline));
            Map<?, ?> actualScope = (Map<?, ?>) nativeJob.get("scope");
            ObservationStore.Scope scope = new ObservationStore.Scope((String) actualScope.get("incarnation"),
                    ((Number) actualScope.get("generation")).longValue());
            assertThat(scope.executionGeneration()).isEqualTo(pipeline.executionGeneration()).isPositive();
            Document artifact = artifact(db, scope); assertThat(pipeline.executionNodeIds()).containsExactlyInAnyOrder(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            Await.until("real snapshot documents reach physical target", remaining(deadline), () -> {
                        observer.tail(); return dst.getCollection(TABLE).countDocuments() == 3;
                    },
                    () -> "target=" + dst.getCollection(TABLE).countDocuments());
            src.getCollection(TABLE).insertOne(row(4));
            Await.until("genuine CDC row4 reaches physical target", remaining(deadline), () -> {
                        observer.tail(); return exactRow(dst, 4);
                    }, () -> observer.evidence().toString());
            var positions = control.positionRead(Q);
            assertThat(positions.chains().stream().filter(value -> value.sourceId().equals(SOURCE) && value.tables().contains(TABLE))
                    .map(value -> value.chainId()).toList()).containsExactly(chainId);
            String consumer = SrsConsumerId.of(Q, SOURCE).value();
            Document cursor = db.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS).find(new Document("_id",
                    new Document("chain", chainId).append("pipeline", consumer))).first();
            assertThat(cursor).isNotNull(); assertThat(cursor.getString("ownerPipelineId")).isEqualTo(Q);
            assertThat(cursor.getString("sourceNodeId")).isEqualTo(SOURCE); assertThat(cursor.getString("cdcStartPosition")).isNotBlank();
            var observed = Await.answered("actual current scoped observation after native delivery", remaining(deadline),
                    () -> latest.readStored(Q).filter(value -> value.scope().filter(scope::equals).isPresent()
                            && value.observation().state() == PipelineState.RUNNING));
            assertThat(dst.getCollection(TABLE).countDocuments()).isEqualTo(4L);
            Map<String, Object> tail = Await.answered("actual cdc returned subscription linkage", remaining(deadline), observer::tail);
            report.addFork(Map.of("action", "actual-opened-and-delivered-Mongo-tail", "admission", admission, "job", nativeJob,
                    "capture", tuple(capture), "tail", tail, "sourceCursorSha256", digest(cursor.toJson()),
                    "artifact", artifact.toJson(), "observedAt", observed.observation().observedAt().toString()));
            for (var session : sessions.values()) { session.finishWarmObservation(session == observer, deadline); }
            report.addFork(Map.of("action", "actual-warm-drained-thread-filtered-native-phase", "evidence", observer.evidence()));
            observer.arm(Map.of("incarnation", scope.pipelineIncarnationId(), "generation", scope.executionGeneration()), deadline);
            src.getCollection(TABLE).insertOne(row(5));
            Map<String, Object> heldDelivery = Await.answered("one genuine non-heartbeat active native delivery", remaining(deadline), observer::held);
            assertThat(dst.getCollection(TABLE).countDocuments()).as("the real batch has not crossed handOver ENTRY").isEqualTo(4L);
            String oldState = states.read(Q).orElseThrow().stateJson();
            control.stop(Q, false);
            Map<String, Object> outcome;
            Throwable outcomeFailure = null;
            try {
                outcome = Await.answered("actual final native close refusal or explicitly unresolved strict native stop", remaining(deadline), observer::outcome);
                report.addFork(Map.of("action", "actual-native-close-outcome", "held", heldDelivery, "outcome", outcome));
                var marker = states.readStopReservation(Q).orElseThrow();
                assertThat(marker.phase()).isEqualTo(StopReservation.Phase.STOPPING);
                assertThat(marker.originalDesired().targetState()).isEqualTo(PipelineState.STOPPED);
                assertThat(marker.originalDesired().purgeState()).isFalse();
                assertThat(marker.source().scope()).isEqualTo(scope);
                Map<?, ?> job = (Map<?, ?>) nativeJob.get("job");
                assertThat(marker.source().oldJob()).isEqualTo(new StopReservation.JobIdentity((String) job.get("clusterId"),
                        ((Number) job.get("jobId")).longValue(), (String) job.get("bootId")));
                assertThat(states.read(Q).orElseThrow().stateJson()).isEqualTo(oldState);
                assertThat(states.read(Q).orElseThrow().epoch()).isEqualTo(marker.reservedEpoch());
                assertThat(live(claims, pKey).map(NativeClaimedCaptureCloseRefusalIT::tuple)).contains(tuple(pipeline));
                assertThat(live(claims, new WorkloadClaimKey(clusterId, WorkloadClaimType.NODE_SESSION, owner)).orElseThrow().owner())
                        .isEqualTo(pipeline.owner());
                artifact(db, scope);
                if ("ACTUAL_NATIVE_CLOSE_REFUSAL".equals(outcome.get("status"))) {
                    assertThat(outcome.get("code")).isEqualTo(ConnectorError.CAPTURE_FAILED.code());
                    Await.until("the original captured source fence is no longer leased after native refusal unwinds", remaining(deadline),
                            () -> claims.read(cKey).filter(reading -> reading.leased() && WorkloadClaimFence.from(reading.claim())
                                    .equals(WorkloadClaimFence.from(capture))).isEmpty(), () -> claims.read(cKey).toString());
                    var after = claims.read(cKey);
                    assertThat(after.filter(reading -> reading.leased() && WorkloadClaimFence.from(reading.claim())
                            .equals(WorkloadClaimFence.from(capture)))).as("the actual old source lease is not permitted after its release/fence window").isEmpty();
                    report.addFork(Map.of("action", "actual-old-source-lease-fenced", "previous", tuple(capture),
                            "current", after.map(value -> Map.of("leased", value.leased(), "claim", tuple(value.claim()))).orElseGet(Map::of),
                            "nativeOldEffectRejectionObserved", false));
                }
            } catch (Exception | Error failure) {
                outcomeFailure = failure; throw failure;
            } finally {
                try { observer.release(); }
                catch (RuntimeException | Error cleanup) {
                    if (outcomeFailure != null) { outcomeFailure.addSuppressed(cleanup); } else { throw cleanup; }
                }
            }
            Await.until("the exact subscription and native read/delivery really end", remaining(deadline),
                    () -> {
                        try { return observer.sameHandleEnded(); } catch (Exception unavailable) { throw new AssertionError("same-handle end evidence unavailable", unavailable); }
                    }, () -> observer.evidence().toString());
            Await.until("ordinary stop completes only after actual native end", remaining(deadline),
                    () -> states.readStopReservation(Q).isEmpty() && states.read(Q).filter(value -> StateJson.parse(value.stateJson()) == PipelineState.STOPPED).isPresent(),
                    () -> states.readStopReservation(Q).toString());
            assertThat(desired.read(Q).orElseThrow().targetState()).isEqualTo(PipelineState.STOPPED);
            assertThat(dst.getCollection(TABLE).countDocuments()).isEqualTo(4L);
            assertThat(db.getCollection(MongoStorePort.SRS_CONSUMER_OFFSETS).find(new Document("_id", cursor.get("_id"))).first()).isNotNull();
            long sameHandleAttempts = ((List<?>) observer.evidence().get("records")).stream().filter(value -> {
                Map<?, ?> event = (Map<?, ?>) value; return "ENTRY".equals(event.get("phase")) && "close".equals(event.get("site"))
                        && tail.get("subscriptionId").equals(event.get("receiverId"));
            }).count();
            if ("ACTUAL_NATIVE_CLOSE_REFUSAL".equals(outcome.get("status"))) {
                assertThat(sameHandleAttempts).as("the refused exact native subscription receives an actual retry").isGreaterThanOrEqualTo(2L);
            }
            report.addFork(Map.of("action", "same-exact-native-handle-ended-before-STOP", "outcome", outcome,
                    "actualSameHandleCloseAttempts", sameHandleAttempts, "evidence", observer.evidence()));
            if (!"ACTUAL_NATIVE_CLOSE_REFUSAL".equals(outcome.get("status"))) {
                throw new AssertionError("UNVERIFIED close-refusal qualification: actual stopStrict remained unresolved while held");
            }
            assertThat(inputHashes(harness)).isEqualTo(input); assertThat(connectorInputs()).isEqualTo(connectors);
            assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
            report.completeDiagnostic(Map.of("ordinaryNativeCloseRefusalQualified", true, "acceptanceEvaluated", false,
                    "taskAcceptanceEligible", false, "performanceAcceptanceEligible", false,
                    "unverified", List.of("UNPUBLISHED_RETENTION", "LOCAL_REOPEN", "REMOTE_READER_EXCLUSION", "ACTUAL_OLD_NATIVE_EFFECT_REJECTION",
                            "ALL_TELEMETRY_SURFACES", "FULL_MATRIX", "FORMAL_PERFORMANCE")));
        } catch (Exception | Error failure) {
            for (var session : sessions.entrySet()) {
                try { report.addFork(Map.of("action", "actual-passive-observer-failure-evidence", "nodeId", session.getKey(),
                        "ownedPid", session.getValue().server().pid(), "evidence", session.getValue().evidence())); }
                catch (RuntimeException | Error reporting) { if (reporting != failure) { failure.addSuppressed(reporting); } }
            }
            primary = failure; try { report.fail(failure); } catch (RuntimeException reporting) { if (reporting != failure) { failure.addSuppressed(reporting); } }
            throw failure;
        } finally {
            Throwable cleanup = null;
            if (held != null) {
                try { held.release(); } catch (RuntimeException | Error failure) { cleanup = failure; }
            }
            for (var session : sessions.values()) { try { session.close(); } catch (Exception | Error failure) { if (cleanup == null) { cleanup = failure; } else { cleanup.addSuppressed(failure); } } }
            if (cluster != null) { try { cluster.close(); } catch (RuntimeException | Error failure) { if (cleanup == null) { cleanup = failure; } else { cleanup.addSuppressed(failure); } } }
            if (cleanup != null) { if (primary != null) { primary.addSuppressed(cleanup); } else if (cleanup instanceof Exception e) { throw e; } else if (cleanup instanceof Error e) { throw e; } }
        }
    }

    private static Document row(long id) { return new Document("_id", id).append("id", id).append("amount", id * 100).append("payload", "native-" + id); }
    private static boolean exactRow(MongoDatabase database, long id) {
        Document actual = database.getCollection(TABLE).find(new Document("id", id)).first();
        return actual != null && ((Number) actual.get("amount")).longValue() == id * 100 && ("native-" + id).equals(actual.getString("payload"));
    }
    private static Optional<WorkloadClaim> live(MongoWorkloadClaimStore claims, WorkloadClaimKey key) {
        return claims.read(key).filter(value -> value.leased()).map(value -> value.claim());
    }
    private static Map<String, Object> tuple(WorkloadClaim claim) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("key", Map.of("clusterId", claim.key().clusterId(), "resourceId", claim.key().resourceId(), "type", claim.key().type().name()));
        result.put("owner", Map.of("nodeId", claim.owner().nodeId(), "bootId", claim.owner().bootId()));
        result.put("claimGeneration", claim.claimGeneration()); result.put("executionGeneration", claim.executionGeneration());
        result.put("topologyRevision", claim.topologyRevision()); result.put("contextExecutionGeneration", claim.contextExecutionGeneration());
        result.put("executionClaimGeneration", claim.executionClaimGeneration()); result.put("executionNodeIds", claim.executionNodeIds().stream().sorted().toList());
        result.put("failureClaimGeneration", claim.failureClaimGeneration()); result.put("failureAfterMemberLoss", claim.failureAfterMemberLoss());
        return Map.copyOf(result);
    }
    private static Map<String, Object> withoutLease(Map<?, ?> claim) {
        Map<String, Object> result = new LinkedHashMap<>(); claim.forEach((key, value) -> { if (!key.equals("leaseUntil")) { result.put((String) key, value); } });
        return Map.copyOf(result);
    }
    private static Document artifact(MongoDatabase database, ObservationStore.Scope scope) {
        Document actual = database.getCollection(MongoStorePort.ARTIFACTS).find(new Document("_id", Q))
                .projection(new Document("_id", 1).append("pipelineIncarnationId", 1)).first();
        assertThat(actual).isNotNull(); assertThat(actual.getString("pipelineIncarnationId")).isEqualTo(scope.pipelineIncarnationId()); return actual;
    }
    private static Duration remaining(long deadline) {
        long value = deadline - System.nanoTime(); assertThat(value).as("the original absolute native qualification deadline remains").isPositive();
        return Duration.ofNanos(value);
    }
    private static Map<String, String> resources(String sourceUri, String sourceDatabase, String targetUri, String targetDatabase) {
        return Map.of(SOURCE + ".tap.yml", """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: "%s", database: %s }
                mode: cdc
                tables: [ %s ]
                """.formatted(SOURCE, sourceUri, sourceDatabase, TABLE), "target.tap.yml", """
                version: tapstate/v1
                kind: source
                id: native_close_target
                connector: mongodb
                config: { uri: "%s", database: %s }
                """.formatted(targetUri, targetDatabase), "pipeline.tap.yml", """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                serve:
                  from: %s
                  sync:
                    - source: native_close_target
                """.formatted(Q, SOURCE, TABLE));
    }
    private static String digest(String value) throws Exception { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8))); }
    private static Map<String, Object> connectorInputs() throws Exception {
        Path directory = ConnectorJars.pathFor("mongodb").getParent(); Map<String, Object> result = new LinkedHashMap<>();
        try (var paths = Files.list(directory)) {
            List<Path> selected = paths.filter(Files::isRegularFile).filter(value -> value.getFileName().toString().endsWith(".jar")).sorted().limit(6).toList();
            assertThat(selected).hasSize(5); for (Path path : selected) { result.put(path.getFileName().toString(), PipelineBenchmarkLiveRunIT.artifact(path)); }
        }
        return Map.copyOf(result);
    }
    private static Map<String, Object> inputHashes(Path harness) throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        for (String name : List.of("NativeClaimedCaptureCloseRefusalIT", "NativeCaptureCloseJdiSession", "NativeTelemetryMirror",
                "BenchmarkJdiCostObserver", "NativeTelemetryIdentityJdiSession", "TwoMemberCluster", "RealProcessServer", "Await")) {
            result.put(name + ".source", PipelineBenchmarkLiveRunIT.sha256(harness.resolve("e2e/src/test/java/io/tapstate/e2e/" + name + ".java")));
            try (var data = NativeClaimedCaptureCloseRefusalIT.class.getResourceAsStream("/io/tapstate/e2e/" + name + ".class")) {
                if (data == null) { throw new AssertionError("executing native fixture class absent: " + name); }
                result.put(name + ".class", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data.readAllBytes())));
            }
        }
        for (String name : List.of("Spec", "Image", "Bound", "Call", "Tail")) {
            try (var data = NativeCaptureCloseJdiSession.class.getResourceAsStream("/io/tapstate/e2e/NativeCaptureCloseJdiSession$" + name + ".class")) {
                if (data == null) { throw new AssertionError("executing nested passive fixture class absent: " + name); }
                result.put("NativeCaptureCloseJdiSession$" + name + ".class", HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data.readAllBytes())));
            }
        }
        return Map.copyOf(result);
    }
    private static String required(String key) {
        String value = System.getProperty(PREFIX + key); if (value == null || value.isBlank()) { throw new AssertionError("missing native close qualification input " + key); } return value;
    }
}
