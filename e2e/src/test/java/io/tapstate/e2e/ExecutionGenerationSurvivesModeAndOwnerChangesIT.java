package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.adapters.mongostore.MongoWorkloadClaimStore;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Real submissions share one durable sequence across discovery modes and a controller process loss. */
@RequiresDocker
class ExecutionGenerationSurvivesModeAndOwnerChangesIT {
    private static final String PREFIX = "tapstate.e2e.execution-mode-owner.";
    private static final Duration WAIT = Duration.ofMinutes(3);
    private static final List<String> LEASE_FIELDS = List.of(
            "ownerNodeId", "ownerBootId", "claimGeneration", "leaseUntil");

    @Test
    void actualStartsAndAnAutomaticTakeoverAdvanceOneDocumentAndRefuseTheOldOwner() throws Exception {
        Assumptions.assumeTrue(List.of("jar", "output").stream()
                .anyMatch(name -> System.getProperty(PREFIX + name) != null),
                "no execution-mode-owner properties supplied; real mode and owner witness is opt-in");
        Path jar = Path.of(required("jar")).toAbsolutePath().normalize();
        assertThat(Files.isRegularFile(jar)).as("the explicitly selected application artifact").isTrue();
        BenchmarkCaptureCalibrationLiveRunIT.requireConnectors();
        Path output = Path.of(required("output"));
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, PipelineBenchmarkLiveRunIT.harnessRoot());
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        try {
            Map<String, Object> application = PipelineBenchmarkLiveRunIT.artifact(jar);
            Map<String, Map<String, Object>> connectors = new LinkedHashMap<>();
            for (String id : List.of("mysql", "postgres", "mongodb")) {
                connectors.put(id, PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(id)));
            }
            var workload = BenchmarkWorkloadDefinitions.byId("copy");
            String pipeline = workload.pipelineIds().getFirst();
            report.begin(Map.of("purpose", "REAL_EXECUTION_MODE_AND_OWNER_SEQUENCE",
                    "application", application, "connectors", connectors, "workload", workload.id(),
                    "clusterProfile", "process-failure-only", "clusterMembers", 2),
                    PipelineBenchmarkLiveRunIT.environment(), List.of());
            try (var fork = BenchmarkForkEnvironment.open(workload, jar, "execution-mode-owner");
                    var client = MongoClients.create(fork.storeUri())) {
                MongoDatabase database = client.getDatabase(new ConnectionString(fork.storeUri()).getDatabase());
                var latest = new MongoObservationStore(client,
                        database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                        database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
                var claims = new MongoWorkloadClaimStore(database.getCollection(MongoStorePort.WORKLOAD_CLAIMS));
                fork.runPhase(workload.phases().getFirst(), true);
                fork.runPhase(workload.phases().get(1), true);
                fork.runPhase(workload.phases().get(2), true);

                Document standalone = standaloneDocument(database, pipeline);
                String clusterId = standalone.getString("clusterId");
                assertThat(clusterId).as("the identity created by the actual standalone boot").isNotBlank();
                Document documentId = new Document(standalone.get("_id", Document.class));
                WorkloadClaimKey key = new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, pipeline);
                long n = generation(standalone);
                assertThat(claims.currentGeneration(clusterId, pipeline)).hasValue(n);
                assertThat(claims.read(key)).as("standalone allocation does not create an owner lease").isEmpty();
                var first = stored(latest, pipeline, n, PipelineState.RUNNING);
                String incarnation = first.scope().orElseThrow().pipelineIncarnationId();
                record(report, "standalone-start", first, standalone, null);
                oneDocument(database, key, documentId, n);

                fork.control().stop(pipeline, false);
                awaitState(fork.control(), pipeline, PipelineState.STOPPED);
                var stopped = stored(latest, pipeline, n, PipelineState.STOPPED);
                assertThat(stopped.scope()).isEqualTo(first.scope());
                record(report, "standalone-stop", stopped, document(database, documentId), null);
                fork.server().close();
                assertThat(fork.server().isAlive()).as("the standalone process ended before cluster admission").isFalse();
                oneDocument(database, key, documentId, n);

                String operatorDatabase = new ConnectionString(fork.operatorStateUri()).getDatabase();
                try (var cluster = TwoMemberCluster.start(fork.storeUri(), operatorDatabase, jar,
                        clusterId, "benchmark", "benchmark-password")) {
                    assertThat(cluster.awaitBothMembers())
                            .containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                    assertThat(cluster.second().clusterMemberNodeIds())
                            .containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
                    assertThat(cluster.first().clusterId()).isEqualTo(clusterId);
                    assertThat(cluster.second().clusterId()).isEqualTo(clusterId);
                    oneDocument(database, key, documentId, n);

                    cluster.first().lifecycle(pipeline, LifecycleVerb.START);
                    long clusterGeneration = Math.addExact(n, 1);
                    WorkloadClaimReading firstOwner = leasedClaim(claims, key, clusterGeneration, null);
                    var clustered = stored(latest, pipeline, clusterGeneration, PipelineState.RUNNING);
                    awaitState(cluster.first(), pipeline, PipelineState.RUNNING);
                    assertThat(clustered.scope().orElseThrow().pipelineIncarnationId()).isEqualTo(incarnation);
                    assertThat(cluster.first().pipelineControllerOf(pipeline))
                            .contains(firstOwner.claim().owner().nodeId());
                    assertThat(cluster.first().executionGenerationOf(pipeline)).contains(clusterGeneration);
                    oneDocument(database, key, documentId, clusterGeneration);
                    assertCdc(fork, client);
                    WorkloadClaimReading readyOwner = leasedClaim(claims, key, clusterGeneration, null);
                    WorkloadClaim oldClaim = readyOwner.claim();
                    assertThat(oldClaim.owner()).as("the original controller still owned the stable cluster run")
                            .isEqualTo(firstOwner.claim().owner());
                    assertThat(oldClaim.claimGeneration()).isEqualTo(firstOwner.claim().claimGeneration());
                    assertThat(oldClaim.topologyRevision()).isEqualTo(firstOwner.claim().topologyRevision());
                    assertThat(cluster.first().pipelineControllerOf(pipeline)).contains(oldClaim.owner().nodeId());
                    record(report, "cluster-start", clustered, document(database, documentId), readyOwner);
                    String oldOwner = oldClaim.owner().nodeId();
                    ControlPlane survivor = cluster.memberOtherThan(oldOwner);
                    RealProcessServer failedOwner = cluster.processCarrying(oldOwner);
                    failedOwner.kill();
                    assertThat(failedOwner.isAlive()).as("the actual controller process was terminated").isFalse();
                    long takeoverGeneration = Math.addExact(n, 2);
                    WorkloadClaimReading successor = leasedClaim(claims, key, takeoverGeneration, oldOwner);
                    var takeover = stored(latest, pipeline, takeoverGeneration, PipelineState.RUNNING);
                    awaitState(survivor, pipeline, PipelineState.RUNNING);
                    assertThat(successor.claim().owner().nodeId()).isNotEqualTo(oldOwner);
                    assertThat(successor.claim().claimGeneration()).isGreaterThan(oldClaim.claimGeneration());
                    assertThat(takeover.scope().orElseThrow().pipelineIncarnationId()).isEqualTo(incarnation);
                    assertThat(survivor.pipelineControllerOf(pipeline)).contains(successor.claim().owner().nodeId());
                    assertThat(survivor.executionGenerationOf(pipeline)).contains(takeoverGeneration);
                    oneDocument(database, key, documentId, takeoverGeneration);
                    assertCdc(fork, client);
                    WorkloadClaimReading readySuccessor = leasedClaim(claims, key, takeoverGeneration, oldOwner);
                    assertThat(readySuccessor.claim().owner()).isEqualTo(successor.claim().owner());
                    assertThat(readySuccessor.claim().claimGeneration()).isEqualTo(successor.claim().claimGeneration());
                    assertThat(readySuccessor.claim().topologyRevision()).isEqualTo(successor.claim().topologyRevision());
                    record(report, "automatic-owner-takeover", takeover, document(database, documentId), readySuccessor);

                    assertThat(claims.advanceUnderClaim(oldClaim, oldClaim.topologyRevision()))
                            .as("the actual superseded claim cannot allocate another execution").isEmpty();
                    assertThat(claims.currentGeneration(clusterId, pipeline)).hasValue(takeoverGeneration);
                    oneDocument(database, key, documentId, takeoverGeneration);
                    var afterRefusal = stored(latest, pipeline, takeoverGeneration, PipelineState.RUNNING);
                    record(report, "old-owner-advance-refused", afterRefusal,
                            document(database, documentId), leasedClaim(claims, key, takeoverGeneration, oldOwner));

                    survivor.stop(pipeline, false);
                    awaitState(survivor, pipeline, PipelineState.STOPPED);
                    stored(latest, pipeline, takeoverGeneration, PipelineState.STOPPED);
                    oneDocument(database, key, documentId, takeoverGeneration);
                }
            }
            assertThat(PipelineBenchmarkLiveRunIT.artifact(jar)).isEqualTo(application);
            for (var connector : connectors.entrySet()) {
                assertThat(PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(connector.getKey())))
                        .as("the %s connector bytes remained fixed", connector.getKey()).isEqualTo(connector.getValue());
            }
            report.completeDiagnostic(Map.of("correctness", "ONE_SEQUENCE_MATCHED_REAL_MODE_AND_OWNER_SUBMISSIONS",
                    "performanceAcceptanceEligible", false,
                    "unverified", List.of("ACTUAL_OLD_EXECUTION_CALLBACK", "COMMAND_LEVEL_ADVANCE_COUNTS",
                            "ALL_TELEMETRY_SURFACE_IDENTITIES", "POST_SWITCH_FULL_TABLE_ORACLE")));
        } catch (Exception | Error failure) {
            try { report.fail(failure); } catch (RuntimeException writeFailure) { failure.addSuppressed(writeFailure); }
            throw failure;
        }
    }

    private static Document standaloneDocument(MongoDatabase database, String pipeline) {
        var documents = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                .find(new Document("resourceType", WorkloadClaimType.PIPELINE_ACTUATION.name())
                        .append("resourceId", pipeline)).limit(2).into(new java.util.ArrayList<Document>());
        assertThat(documents).as("one actual standalone coordination document").hasSize(1);
        Document result = documents.getFirst();
        for (String field : LEASE_FIELDS) {
            assertThat(result.containsKey(field)).as("standalone acquired no %s", field).isFalse();
        }
        return result;
    }

    private static Document document(MongoDatabase database, Document id) {
        Document value = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS).find(new Document("_id", id)).first();
        assertThat(value).as("the original coordination document remains present").isNotNull();
        return value;
    }

    private static long generation(Document document) {
        Object value = document.get("executionGeneration");
        assertThat(value instanceof Long || value instanceof Integer).as("a real durable execution generation").isTrue();
        long generation = ((Number) value).longValue();
        assertThat(generation).isPositive();
        return generation;
    }

    private static void oneDocument(MongoDatabase database, WorkloadClaimKey key, Document id, long expected) {
        assertThat(id).containsEntry("clusterId", key.clusterId())
                .containsEntry("resourceType", key.type().name()).containsEntry("resourceId", key.resourceId());
        assertThat(database.getCollection(MongoStorePort.WORKLOAD_CLAIMS).countDocuments(
                new Document("clusterId", key.clusterId()).append("resourceType", key.type().name())
                        .append("resourceId", key.resourceId()))).isEqualTo(1);
        Document current = document(database, id);
        assertThat(current.get("_id")).isEqualTo(id);
        assertThat(generation(current)).isEqualTo(expected);
    }

    private static WorkloadClaimReading leasedClaim(MongoWorkloadClaimStore claims, WorkloadClaimKey key,
            long generation, String previousOwner) {
        return Await.answered("an actual leased owner at execution " + generation, WAIT,
                () -> claims.read(key).filter(reading -> reading.leased()
                        && reading.claim().executionGeneration() == generation
                        && (previousOwner == null || !previousOwner.equals(reading.claim().owner().nodeId()))));
    }

    private static ObservationStore.Stored stored(MongoObservationStore store, String pipeline,
            long generation, PipelineState state) {
        return Await.answered(pipeline + " scope " + generation + " state " + state, WAIT,
                () -> store.readStored(pipeline).filter(value -> value.scope().map(scope ->
                        scope.executionGeneration() == generation).orElse(false)
                        && value.observation().state() == state));
    }

    private static void awaitState(ControlPlane control, String pipeline, PipelineState state) {
        Await.until(pipeline + " to become " + state, WAIT,
                () -> control.state(pipeline).filter(state::equals).isPresent(),
                () -> "state=" + control.state(pipeline));
    }

    private static void assertCdc(BenchmarkForkEnvironment fork, MongoClient client) throws Exception {
        long expected = Math.addExact(targetAmount(client, fork.externalTargetUri()), 1);
        fork.executeSource("UPDATE bench_copy_orders SET amount=amount+1 WHERE id=1");
        Await.until("the actual owner to deliver a new CDC change", WAIT,
                () -> targetAmount(client, fork.externalTargetUri()) == expected,
                () -> "target amount=" + targetAmount(client, fork.externalTargetUri()) + ", expected=" + expected);
    }

    private static long targetAmount(MongoClient client, String targetUri) {
        Document row = client.getDatabase(new ConnectionString(targetUri).getDatabase())
                .getCollection("bench_copy_orders").find(new Document("id", 1L)).first();
        assertThat(row).as("the actual source row is present at the real target").isNotNull();
        return ((Number) row.get("amount")).longValue();
    }

    private static void record(BenchmarkLiveReport report, String action, ObservationStore.Stored stored,
            Document coordination, WorkloadClaimReading owner) {
        ObservationStore.Scope scope = stored.scope().orElseThrow();
        assertThat(scope.executionGeneration()).isEqualTo(generation(coordination));
        Map<String, Object> evidence = new LinkedHashMap<>(Map.of("action", action,
                "clusterId", coordination.getString("clusterId"), "coordinationId", coordination.get("_id"),
                "executionGeneration", scope.executionGeneration(), "incarnation", scope.pipelineIncarnationId(),
                "state", stored.observation().state().name(), "observedAt", stored.observation().observedAt().toString()));
        evidence.put("leaseFieldsAbsent", LEASE_FIELDS.stream().noneMatch(coordination::containsKey));
        if (owner != null) {
            assertThat(owner.leased()).as("the recorded owner held a server-time lease").isTrue();
            assertThat(owner.claim().executionGeneration()).isEqualTo(scope.executionGeneration());
            assertThat(coordination.getString("ownerNodeId")).isEqualTo(owner.claim().owner().nodeId());
            assertThat(coordination.getString("ownerBootId")).isEqualTo(owner.claim().owner().bootId());
            assertThat(((Number) coordination.get("claimGeneration")).longValue())
                    .isEqualTo(owner.claim().claimGeneration());
            evidence.put("ownerNodeId", owner.claim().owner().nodeId());
            evidence.put("ownerBootId", owner.claim().owner().bootId());
            evidence.put("claimGeneration", owner.claim().claimGeneration());
            evidence.put("topologyRevision", owner.claim().topologyRevision());
            evidence.put("leaseRemainingMillis", owner.leaseRemaining().toMillis());
        }
        report.addFork(evidence);
        System.out.printf("mode-owner-identity action=%s generation=%d owner=%s%n", action,
                scope.executionGeneration(), owner == null ? "UNAVAILABLE" : owner.claim().owner().nodeId());
    }

    private static String required(String name) {
        String value = System.getProperty(PREFIX + name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("mode and owner witness requires -D" + PREFIX + name);
        }
        return value;
    }
}
