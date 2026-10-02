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

/** Pairs completed native command intervals with factual standalone generation documents. */
final class ExecutionCoordinationProfileStages implements AutoCloseable {
    private static final List<String> ACTIONS = List.of("before-first-start", "first-start", "running-ticks",
            "pause-and-paused-ticks", "ordinary-resume", "stop", "stop-start", "stop-before-process-restart",
            "first-process-shutdown", "new-process-boot-without-start", "new-process-start", "stop-before-recreate",
            "delete-recreate-without-start", "recreate-start", "final-stop", "second-process-shutdown");
    private static final Set<String> STARTS = Set.of("first-start", "stop-start", "new-process-start", "recreate-start");
    private static final Set<String> KEY_FIELDS = Set.of("clusterId", "resourceType", "resourceId");

    private record Authority(NativeCoordinationProfile.Key key, long generation) {
        Map<String, Object> evidence() {
            return Map.of("documentState", "PRESENT", "key", Map.of("clusterId", key.clusterId(),
                    "resourceType", key.resourceType(), "resourceId", key.resourceId()),
                    "executionGeneration", generation, "leaseFieldsAbsent", true);
        }
    }

    private final BenchmarkLiveReport report;
    private final boolean enabled;
    private final String pipeline;
    private NativeCoordinationProfile profile;
    private MongoClient client;
    private MongoDatabase database;
    private String originalUri;
    private String pendingAction;
    private Authority before;
    private NativeCoordinationProfile.Key factualKey;
    private int completed;
    private boolean closed;

    ExecutionCoordinationProfileStages(BenchmarkLiveReport report, boolean enabled, String pipeline) {
        this.report = report;
        this.enabled = enabled;
        this.pipeline = pipeline;
    }

    String openOwned(String uri) {
        assertThat(enabled).as("only explicit direct counts may enable the profiler").isTrue();
        assertThat(profile).as("one profiler spans both owned application boots").isNull();
        profile = NativeCoordinationProfile.openOwned(uri);
        originalUri = uri;
        client = MongoClients.create(uri);
        database = client.getDatabase(new ConnectionString(uri).getDatabase());
        begin("before-first-start");
        return profile.nativeUri();
    }

    String nativeUri(String uri) {
        if (!enabled) { return uri; }
        assertThat(uri).as("the restored boot retains the same owned control database").isEqualTo(originalUri);
        assertThat(profile).isNotNull();
        return profile.nativeUri();
    }

    void begin(String action) {
        if (!enabled) { return; }
        assertThat(closed).isFalse();
        assertThat(pendingAction).as("the previous physical command interval was completed").isNull();
        assertThat(completed).isLessThan(ACTIONS.size());
        assertThat(action).as("every physical interval is captured in lifecycle order").isEqualTo(ACTIONS.get(completed));
        pendingAction = action;
        before = authority();
        if (completed <= 1) {
            assertThat(before).as("%s reads a genuinely absent pre-start authority document", action).isNull();
        } else {
            assertThat(before).as("%s reads the persisted authority before the action", action).isNotNull();
            assertThat(before.key()).isEqualTo(factualKey);
        }
    }

    /** Called only after the existing readiness assertions and corresponding JDI stage have drained. */
    void stage(String action, Map<String, Object> readiness) {
        if (!enabled) { return; }
        assertThat(action).isEqualTo(pendingAction);
        Authority after = authority();
        long increment = STARTS.contains(action) ? 1 : 0;
        long attempts = action.equals("first-start") ? 2 : increment;
        if (action.equals("before-first-start")) {
            assertThat(after).as("setup did not allocate an execution").isNull();
        } else {
            assertThat(after).as("%s retains its authoritative document", action).isNotNull();
            if (action.equals("first-start")) {
                assertThat(after.generation()).as("the first committed standalone generation").isEqualTo(1);
                factualKey = after.key();
            } else {
                assertThat(after.key()).isEqualTo(before.key()).isEqualTo(factualKey);
                assertThat(after.generation()).as("%s committed authority increment", action)
                        .isEqualTo(Math.addExact(before.generation(), increment));
            }
        }

        var boundary = profile.boundary(action);
        assertThat(boundary.count(NativeCoordinationProfile.Family.ADVANCE_STANDALONE))
                .as("%s physical standalone attempts in the entire native claim namespace", action).isEqualTo(attempts);
        if (after != null) {
            assertThat(boundary.count(NativeCoordinationProfile.Family.ADVANCE_STANDALONE, after.key()))
                    .as("%s physical standalone attempts for the factual workload key", action).isEqualTo(attempts);
        }
        for (var family : NativeCoordinationProfile.Family.values()) {
            if (family == NativeCoordinationProfile.Family.READ || family == NativeCoordinationProfile.Family.CONTROL
                    || family == NativeCoordinationProfile.Family.SCHEMA_WRITE
                    || family == NativeCoordinationProfile.Family.ADVANCE_STANDALONE) { continue; }
            assertThat(boundary.count(family)).as("%s native namespace %s operations", action, family).isZero();
        }
        var advances = boundary.operations().stream()
                .filter(operation -> operation.family() == NativeCoordinationProfile.Family.ADVANCE_STANDALONE).toList();
        assertThat(advances).allSatisfy(operation -> {
            assertThat(operation.key()).isEqualTo(factualKey);
            assertThat(operation.command()).isIn("findAndModify", "findandmodify");
            assertThat(operation.errorCode()).as("%s completed physical advance has no server error", action).isNull();
        });
        if (action.equals("first-start")) {
            assertThat(advances).extracting(NativeCoordinationProfile.Operation::upsert).containsExactly(false, true);
        } else {
            assertThat(advances).allSatisfy(operation -> assertThat(operation.upsert()).isFalse());
        }
        long committedIncrement = after == null ? 0 : after.generation() - (before == null ? 0 : before.generation());
        assertThat(committedIncrement).as("%s factual committed authority increment", action).isEqualTo(increment);
        if (increment == 1) {
            assertThat(readiness.get("state")).isEqualTo("RUNNING");
            assertThat(readiness.get("executionGeneration")).isEqualTo(after.generation());
            assertThat(readiness.get("counterState")).isEqualTo("RECORDED");
        }

        Map<String, Object> evidence = new LinkedHashMap<>(boundary.evidence());
        evidence.put("pipelineId", pipeline);
        evidence.put("expectedPhysicalAdvanceAttempts", attempts);
        evidence.put("authorityBefore", before == null ? Map.of("documentState", "ABSENT") : before.evidence());
        evidence.put("authorityAfter", after == null ? Map.of("documentState", "ABSENT") : after.evidence());
        evidence.put("authoritativeCommittedGenerationIncrement", committedIncrement);
        evidence.put("drainedJdiStage", action);
        evidence.put("readiness", Map.copyOf(readiness));
        evidence.put("physicalMongoCommandsInferred", false);
        evidence.put("concreteJobIdCaptured", false);
        if (increment == 1) {
            evidence.put("jobSubmissionEvidence", "EXISTING_EXACT_SUBMIT_BINDING_AND_DRAINED_JDI_STAGE");
        }
        report.addFork(evidence);
        pendingAction = null;
        completed++;
    }

    void requireComplete() {
        if (!enabled) { return; }
        assertThat(pendingAction).as("no physical command interval remains open").isNull();
        assertThat(completed).as("all native lifecycle and final shutdown intervals were captured").isEqualTo(ACTIONS.size());
    }

    private Authority authority() {
        List<Document> documents = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                .find(new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", pipeline))
                .limit(2).into(new ArrayList<>());
        assertThat(documents).as("the actual pipeline has at most one authority document").hasSizeLessThanOrEqualTo(1);
        if (documents.isEmpty()) { return null; }
        Document value = documents.getFirst();
        assertThat(value.get("_id")).as("the authoritative workload key is a document").isInstanceOf(Document.class);
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
        assertThat(raw instanceof Long || raw instanceof Integer).as("the factual authority generation is integral").isTrue();
        long generation = ((Number) raw).longValue();
        assertThat(generation).isPositive();
        return new Authority(new NativeCoordinationProfile.Key(id.getString("clusterId"),
                id.getString("resourceType"), id.getString("resourceId")), generation);
    }

    @Override
    public void close() {
        if (closed) { return; }
        closed = true;
        try {
            if (profile != null) { profile.close(); }
        } finally {
            if (client != null) { client.close(); }
        }
    }
}
