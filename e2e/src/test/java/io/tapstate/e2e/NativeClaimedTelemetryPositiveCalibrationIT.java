package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoAuthStores;
import io.tapstate.adapters.mongostore.MongoClusterIdentityStore;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.adapters.mongostore.MongoWorkloadClaimStore;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
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
    private static final String PIPELINE = "native_claimed_copy";
    private static final String SOURCE = "native_claimed_source";
    private static final String TABLE = "native_claimed_orders";
    private record Positive(NativeTelemetryIdentityJdiSession.Boundary boundary, String body, String logNode) { }

    @Test
    void aRealWarmClaimedSubmissionOffersProducesAndScrapesUnderItsActualOwner() throws Exception {
        Assumptions.assumeTrue(List.of("jar", "sha256", "output").stream()
                .anyMatch(name -> System.getProperty(PREFIX + name) != null),
                "the claimed positive calibration needs named immutable inputs");
        Path jar = Path.of(required("jar")).toAbsolutePath().normalize();
        String sha = required("sha256");
        assertThat(sha).matches("[0-9a-f]{64}");
        assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
        BenchmarkCaptureCalibrationLiveRunIT.requireConnectors();
        Path harness = PipelineBenchmarkLiveRunIT.harnessRoot();
        Path output = Path.of(required("output")).toAbsolutePath().normalize();
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
                sql.execute("INSERT INTO " + TABLE
                        + " (id,amount,payload) VALUES (1,100,'native-1'),(2,200,'native-2'),(3,300,'native-3')");
            }
            MongoDatabase database = mongo.getDatabase(new ConnectionString(storeUri).getDatabase());
            Map<String, String> resources = resources(settings, targetUri);
            report.begin(Map.of("purpose", "NATIVE_WARM_CLAIMED_POSITIVE_TELEMETRY",
                    "application", PipelineBenchmarkLiveRunIT.artifact(jar), "expectedJarSha256", sha,
                    "harness", inputs, "connectors", connectors, "clusterMembers", 2,
                    "fixtureResourceSha256", digest(JsonWriter.write(resources).getBytes(StandardCharsets.UTF_8)),
                    "performanceAcceptanceEligible", false, "warmOnly", true),
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
                            var observer = NativeTelemetryIdentityJdiSession.start(jar, sha, PIPELINE,
                                    (artifact, debug) -> {
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
                                    });
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
            long deadline = System.nanoTime() + WAIT.toNanos();
            cluster.first().lifecycle(PIPELINE, LifecycleVerb.START);
            try (var target = MongoClients.create(targetUri)) {
                MongoDatabase targetDatabase = target.getDatabase(new ConnectionString(targetUri).getDatabase());
                Await.until("the real claimed snapshot reaches Mongo", remaining(deadline),
                        () -> targetDatabase.getCollection(TABLE).countDocuments() == 3,
                        () -> "target rows=" + targetDatabase.getCollection(TABLE).countDocuments());
                assertRows(targetDatabase, 3);
                try (var sql = source.createStatement()) {
                    sql.execute("INSERT INTO " + TABLE + " (id,amount,payload) VALUES (4,400,'native-4')");
                }
                Await.until("a real source CDC insert reaches Mongo", remaining(deadline),
                        () -> targetDatabase.getCollection(TABLE).countDocuments() == 4,
                        () -> "target rows=" + targetDatabase.getCollection(TABLE).countDocuments());
                assertRows(targetDatabase, 4);
            }
            var latest = new MongoObservationStore(mongo,
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                    database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
            var current = Await.answered("a real scoped positive claimed observation", remaining(deadline),
                    () -> latest.readStored(PIPELINE).filter(value -> value.scope().isPresent()
                            && value.observation().state() == PipelineState.RUNNING
                            && value.observation().facts().stream()
                            .filter(fact -> fact.name().equals("tapstate.pipeline.records"))
                            .flatMap(fact -> fact.points().stream()).anyMatch(point -> point.value() != null
                                    && point.value() > 0 && "out".equals(point.attributes().get("direction")))));
            var scope = current.scope().orElseThrow();
            WorkloadClaim claim = claims.read(key).filter(reading -> reading.leased()
                    && reading.claim().executionGeneration() == scope.executionGeneration()).orElseThrow().claim();
            assertThat(claim.executionNodeIds()).containsExactlyInAnyOrder(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            if (previousGeneration.isPresent()) {
                assertThat(claim.executionGeneration()).isEqualTo(Math.addExact(previousGeneration.getAsLong(), 1));
            } else {
                assertThat(claim.executionGeneration())
                        .as("the actual first admission in the fresh coordination store").isEqualTo(1L);
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
            var submission = owner.claimedSubmission(receipt).orElseThrow();
            assertThat(withoutLease(submission.claim())).isEqualTo(claimTuple(claim));
            assertThat(submission.claim().get("leaseUntil")).isInstanceOf(String.class);
            Instant.parse((String) submission.claim().get("leaseUntil"));
            assertThat(submission.members()).containsExactlyInAnyOrder(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            assertThat(submission.job().scope()).isEqualTo(receipt.scope());
            assertThat(submission.job().job().get("clusterId")).isEqualTo(clusterId);
            assertThat(submission.job().executionObjectId()).isPositive();
            HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
            URI scrape = URI.create("http://127.0.0.1:" + scrapePorts.get(emittingNode) + "/metrics");
            Map<String, List<Map<String, Object>>> records = new LinkedHashMap<>();
            for (String node : sessions.keySet()) { records.put(node, new ArrayList<>()); }
            Positive positive = Await.answered("actual claimed Job, scoped log, offer, visibility and produced scrape",
                    remaining(deadline), () -> {
                        try {
                            String fresh = NativeTelemetryPositiveCalibrationIT.scrape(http, scrape, deadline);
                            NativeTelemetryIdentityJdiSession.Boundary emitter = null;
                            for (var entry : sessions.entrySet()) {
                                var captured = entry.getValue().boundary("claimed-warm-positive");
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
            assertBridge(observed, submission);
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
            report.completeDiagnostic(Map.of("correctness", "ACTUAL_WARM_CLAIMED_ADMISSION_AND_EMITTER_TELEMETRY",
                    "warmOnly", true, "performanceAcceptanceEligible", false, "unverified", List.of(
                            "REPLACEMENT_CRASH_HANDOFF", "OLD_CALLBACK_WINDOWS", "NEGATIVE_UNKNOWN_BASELINE_MATRIX",
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
