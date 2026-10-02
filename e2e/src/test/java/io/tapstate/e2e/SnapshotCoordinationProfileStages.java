package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoStorePort;
import org.bson.Document;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Pairs snapshot and parallel CDC command windows with their factual standalone authority. */
final class SnapshotCoordinationProfileStages implements AutoCloseable {
    private static final List<String> ACTIONS = List.of("setup-before-parallel-cdc-start", "before-bulk-start",
            "partial-snapshot-start", "snapshot-pause", "parallel-cdc-while-snapshot-paused",
            "partial-snapshot-rebuild-resume", "parallel-cdc-after-snapshot-resume",
            "snapshot-completed-with-cdc-ticks", "both-stopped", "snapshot-process-shutdown");
    private static final Set<String> KEY_FIELDS = Set.of("clusterId", "resourceType", "resourceId");

    private record Authority(NativeCoordinationProfile.Key key, long generation, Long guardedWrites) {
        Map<String, Object> evidence() {
            Map<String, Object> value = new LinkedHashMap<>(Map.of("documentState", "PRESENT",
                    "key", Map.of("clusterId", key.clusterId(), "resourceType", key.resourceType(),
                            "resourceId", key.resourceId()), "executionGeneration", generation,
                    "leaseFieldsAbsent", true, "guardCounterState", guardedWrites == null ? "ABSENT" : "RECORDED"));
            if (guardedWrites != null) { value.put("fencedAppends", guardedWrites); }
            return value;
        }
    }

    private final BenchmarkLiveReport report;
    private final boolean enabled;
    private final String bulk;
    private final String fast;
    private final List<String> pipelines;
    private final Map<String, NativeCoordinationProfile.Key> factualKeys = new LinkedHashMap<>();
    private NativeCoordinationProfile profile;
    private MongoClient client;
    private MongoDatabase database;
    private String pendingAction;
    private Map<String, Authority> before;
    private Map<String, Object> readinessBefore;
    private int completed;
    private boolean closed;

    SnapshotCoordinationProfileStages(BenchmarkLiveReport report, boolean enabled, String bulk, String fast) {
        this.report = report;
        this.enabled = enabled;
        this.bulk = bulk;
        this.fast = fast;
        this.pipelines = List.of(bulk, fast);
        assertThat(bulk).isNotEqualTo(fast);
    }

    String openOwned(String uri, Map<String, Object> targetBeforeBoot) {
        if (!enabled) { return uri; }
        assertThat(profile).as("one profiler spans the complete owned snapshot process").isNull();
        profile = NativeCoordinationProfile.openOwned(uri);
        client = MongoClients.create(uri);
        database = client.getDatabase(new ConnectionString(uri).getDatabase());
        begin("setup-before-parallel-cdc-start", targetBeforeBoot);
        assertThat(before).as("the unique control database has no admitted pipeline before boot").isEmpty();
        return profile.nativeUri();
    }

    void begin(String action, Map<String, Object> readiness) {
        if (!enabled) { return; }
        assertThat(closed).isFalse();
        assertThat(pendingAction).as("the previous physical command interval was completed").isNull();
        assertThat(completed).isLessThan(ACTIONS.size());
        assertThat(action).as("snapshot physical intervals follow every actual action").isEqualTo(ACTIONS.get(completed));
        pendingAction = action;
        before = authorities();
        readinessBefore = Map.copyOf(readiness);
    }

    /** Called after the original readiness assertions and asserted logical admission stage. */
    void stage(String action, Map<String, Object> readiness, ExecutionAdmissionJdiSession observer) throws Exception {
        if (!enabled) { return; }
        assertThat(action).isEqualTo(pendingAction);
        assertThat(observer).isNotNull();
        var logical = action.equals("snapshot-process-shutdown") ? observer.shutdownAndFinish() : observer.boundary();
        assertThat(logical.pipelineId()).as("the original exact JDI admission witness is bulk scoped").isEqualTo(bulk);
        assertThat(logical.drained()).isTrue();
        assertThat(logical.leaseObservationEnabled()).isTrue();
        assertThat(logical.leasesDrained()).isTrue();
        assertThat(logical.bindings()).containsOnlyKeys(ExecutionAdmissionJdiSession.Target.values());
        assertThat(logical.leaseBindings()).containsOnlyKeys(ExecutionAdmissionJdiSession.LeaseTarget.values());
        if (action.equals("snapshot-process-shutdown")) { assertThat(logical.fullyDrained()).isTrue(); }

        Map<String, Authority> after = authorities();
        var boundary = profile.boundary(action);
        long allAttempts = 0;
        Map<String, Object> perPipeline = new LinkedHashMap<>();
        for (String pipeline : pipelines) {
            Authority prior = before.get(pipeline);
            Authority current = after.get(pipeline);
            long increment = increments(action, pipeline);
            long attempts = increment == 0 ? 0 : prior == null ? 2 : 1;
            long committed = verifyAuthority(action, pipeline, prior, current, increment);
            var key = current == null ? null : current.key();
            List<NativeCoordinationProfile.Operation> advances = boundary.operations().stream()
                    .filter(operation -> operation.family() == NativeCoordinationProfile.Family.ADVANCE_STANDALONE
                            && key != null && key.equals(operation.key())).toList();
            assertThat(advances).as("%s physical standalone attempts for %s", action, pipeline).hasSize(Math.toIntExact(attempts));
            assertThat(advances).allSatisfy(operation -> {
                assertThat(operation.command()).isIn("findAndModify", "findandmodify");
                assertThat(operation.errorCode()).as("%s completed advance has no server error", action).isNull();
            });
            if (increment == 1 && prior == null) {
                assertThat(advances).extracting(NativeCoordinationProfile.Operation::upsert).containsExactly(false, true);
            } else { assertThat(advances).allSatisfy(operation -> assertThat(operation.upsert()).isFalse()); }
            if (current != null) {
                assertThat(readiness.get(pipeline)).isInstanceOf(Map.class);
                Map<?, ?> ready = (Map<?, ?>) readiness.get(pipeline);
                assertThat(ready.get("storedObservationState")).isEqualTo("PRESENT");
                assertThat(ready.get("executionGeneration")).isEqualTo(current.generation());
                assertThat(ready.get("incarnation")).isInstanceOf(String.class);
                assertThat((String) ready.get("incarnation")).isNotBlank();
                if (prior != null) {
                    assertThat(readinessBefore.get(pipeline)).isInstanceOf(Map.class);
                    assertThat(ready.get("incarnation")).isEqualTo(((Map<?, ?>) readinessBefore.get(pipeline)).get("incarnation"));
                }
                if (increment == 1) {
                    assertThat(ready.get("state")).isEqualTo("RUNNING");
                    assertThat(ready.get("targetRows")).isInstanceOf(Long.class);
                    assertThat((Long) ready.get("targetRows")).isPositive();
                }
            }
            var guards = boundary.operations().stream().filter(operation ->
                    operation.family() == NativeCoordinationProfile.Family.CLAIM_WRITE_GUARD
                            && key != null && key.equals(operation.key())).toList();
            List<Long> permitted = new ArrayList<>();
            if (prior != null) { permitted.add(prior.generation()); }
            if (current != null && !permitted.contains(current.generation())) { permitted.add(current.generation()); }
            assertThat(guards).allSatisfy(operation -> {
                assertThat(operation.command()).isEqualTo("update");
                assertThat(operation.upsert()).isFalse();
                assertThat(operation.errorCode()).as("%s completed guard has no server error", action).isNull();
                assertThat(operation.guardedExecutionGeneration()).isNotNull().isPositive().isIn(permitted);
            });
            if (action.equals("partial-snapshot-rebuild-resume") && pipeline.equals(bulk)) {
                assertThat(guards).extracting(NativeCoordinationProfile.Operation::guardedExecutionGeneration)
                        .as("the actual handoff guarded both its source and admitted successor authority")
                        .contains(prior.generation(), current.generation());
            }
            if (action.equals("both-stopped")) {
                assertThat(guards).as("%s reserves and completes the actual stop", pipeline).hasSize(2);
            }
            perPipeline.put(pipeline, Map.of("authorityBefore", authorityEvidence(prior),
                    "authorityAfter", authorityEvidence(current), "authoritativeCommittedGenerationIncrement", committed,
                    "expectedPhysicalAdvanceAttempts", attempts, "physicalClaimWriteGuardAttempts", guards.size(),
                    "claimWriteGuardPermittedGenerations", List.copyOf(permitted)));
            allAttempts = Math.addExact(allAttempts, attempts);
        }
        assertThat(boundary.count(NativeCoordinationProfile.Family.ADVANCE_STANDALONE))
                .as("%s includes both pipelines and every native namespace advance", action).isEqualTo(allAttempts);
        for (var operation : boundary.operations()) {
            if (operation.family() == NativeCoordinationProfile.Family.ADVANCE_STANDALONE
                    || operation.family() == NativeCoordinationProfile.Family.CLAIM_WRITE_GUARD) {
                assertThat(operation.key()).as("%s mutates only a factual A/B workload key", action).isIn(factualKeys.values());
            }
        }
        for (var family : NativeCoordinationProfile.Family.values()) {
            if (family == NativeCoordinationProfile.Family.READ || family == NativeCoordinationProfile.Family.CONTROL
                    || family == NativeCoordinationProfile.Family.SCHEMA_WRITE
                    || family == NativeCoordinationProfile.Family.ADVANCE_STANDALONE
                    || family == NativeCoordinationProfile.Family.CLAIM_WRITE_GUARD) { continue; }
            assertThat(boundary.count(family)).as("%s native namespace %s operations", action, family).isZero();
        }
        Map<String, Object> evidence = new LinkedHashMap<>(boundary.evidence());
        evidence.put("nativeAppName", profile.nativeAppName());
        evidence.put("controlDatabase", profile.databaseName());
        evidence.put("coordinationNamespace", profile.databaseName() + "." + MongoStorePort.WORKLOAD_CLAIMS);
        evidence.put("profileLimits", Map.of("cappedBytes", NativeCoordinationProfile.Limits.DEFAULT.cappedBytes(),
                "maxRecords", NativeCoordinationProfile.Limits.DEFAULT.maxRecords()));
        evidence.put("pipelines", perPipeline);
        evidence.put("readinessBefore", readinessBefore);
        evidence.put("readinessAfter", Map.copyOf(readiness));
        evidence.put("expectedPhysicalAdvanceAttempts", allAttempts);
        evidence.put("physicalClaimWriteGuardAttempts", boundary.count(NativeCoordinationProfile.Family.CLAIM_WRITE_GUARD));
        evidence.put("claimWriteGuardAttemptsProveCommit", false);
        evidence.put("claimWriteGuardChangesGeneration", false);
        evidence.put("drainedJdiStage", action);
        evidence.put("pairedJdiBoundaryAtNanos", logical.atNanos());
        evidence.put("jdiObservedPipelineId", logical.pipelineId());
        evidence.put("applicationSha256", logical.artifactSha256());
        evidence.put("ownedVmDeath", logical.ownedVmDeath());
        evidence.put("ownedVmDisconnected", logical.ownedVmDisconnected());
        evidence.put("physicalMongoCommandsInferred", false);
        evidence.put("concreteJobIdCaptured", false);
        if (increments(action, bulk) == 1) {
            evidence.put("bulkSubmissionEvidence", "EXISTING_EXACT_SUBMIT_BINDING_AND_DRAINED_JDI_STAGE");
        }
        if (increments(action, fast) == 1) {
            evidence.put("parallelSubmissionEvidence", "ACTUAL_SCOPED_OBSERVATION_AND_REAL_TARGET_READINESS");
        }
        report.addFork(evidence);
        pendingAction = null;
        completed++;
    }

    private long increments(String action, String pipeline) {
        return (action.equals("before-bulk-start") && pipeline.equals(fast))
                || (Set.of("partial-snapshot-start", "partial-snapshot-rebuild-resume").contains(action)
                && pipeline.equals(bulk)) ? 1 : 0;
    }

    private long verifyAuthority(String action, String pipeline, Authority prior, Authority current, long increment) {
        if (prior == null && increment == 0) {
            assertThat(current).as("%s did not admit %s", action, pipeline).isNull();
            return 0;
        }
        assertThat(current).as("%s retains the factual authority for %s", action, pipeline).isNotNull();
        assertThat(current.generation()).as("%s actual committed generation for %s", action, pipeline)
                .isEqualTo(Math.addExact(prior == null ? 0 : prior.generation(), increment));
        if (prior == null) { factualKeys.put(pipeline, current.key()); }
        else { assertThat(current.key()).isEqualTo(prior.key()).isEqualTo(factualKeys.get(pipeline)); }
        return current.generation() - (prior == null ? 0 : prior.generation());
    }

    void requireComplete() {
        if (!enabled) { return; }
        assertThat(pendingAction).as("no physical interval remains open").isNull();
        assertThat(completed).as("all snapshot and final owned shutdown intervals were captured").isEqualTo(ACTIONS.size());
    }

    private Map<String, Authority> authorities() {
        Map<String, Authority> values = new LinkedHashMap<>();
        for (String pipeline : pipelines) {
            List<Document> documents = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                    .find(new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", pipeline))
                    .limit(2).into(new ArrayList<>());
            assertThat(documents).as("%s has at most one factual authority document", pipeline).hasSizeLessThanOrEqualTo(1);
            if (documents.isEmpty()) { continue; }
            Document value = documents.getFirst();
            assertThat(value.get("_id")).isInstanceOf(Document.class);
            Document id = (Document) value.get("_id");
            assertThat(id.keySet()).containsExactlyInAnyOrderElementsOf(KEY_FIELDS);
            for (String field : KEY_FIELDS) {
                assertThat(id.get(field)).isInstanceOf(String.class);
                assertThat(id.getString(field)).isNotBlank().isEqualTo(value.getString(field));
            }
            assertThat(id.getString("resourceType")).isEqualTo("PIPELINE_ACTUATION");
            assertThat(id.getString("resourceId")).isEqualTo(pipeline);
            for (String field : List.of("ownerNodeId", "ownerBootId", "claimGeneration", "leaseUntil")) {
                assertThat(value.containsKey(field)).as("standalone acquired no claim or lease field %s", field).isFalse();
            }
            Object raw = value.get("executionGeneration");
            assertThat(raw instanceof Long || raw instanceof Integer).as("the authority generation is integral").isTrue();
            long generation = ((Number) raw).longValue();
            assertThat(generation).isPositive();
            Object guard = value.get("fencedAppends");
            assertThat(guard == null || guard instanceof Long || guard instanceof Integer).isTrue();
            Long guardedWrites = guard == null ? null : ((Number) guard).longValue();
            if (guardedWrites != null) { assertThat(guardedWrites).isNotNegative(); }
            values.put(pipeline, new Authority(new NativeCoordinationProfile.Key(id.getString("clusterId"),
                    id.getString("resourceType"), id.getString("resourceId")), generation, guardedWrites));
        }
        return Map.copyOf(values);
    }

    private static Map<String, Object> authorityEvidence(Authority value) {
        return value == null ? Map.of("documentState", "ABSENT") : value.evidence();
    }

    @Override public void close() {
        if (closed) { return; }
        closed = true;
        try { if (profile != null) { profile.close(); } }
        finally { if (client != null) { client.close(); } }
    }
}
