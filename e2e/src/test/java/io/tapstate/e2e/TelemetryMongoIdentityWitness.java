package io.tapstate.e2e;

import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.control.core.MonitorError;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.spi.store.ObservationStore;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Reads actual emitted envelopes and public projections under the generation that admitted each run. */
final class TelemetryMongoIdentityWitness {
    private static final int MAX_SCOPES = 8;
    private static final int MAX_ROWS = 512;
    private static final int PAGE_LIMIT = 100;
    private static final int MAX_PAGES = 8;
    private static final Duration HTTP_BOUND = Duration.ofSeconds(20);
    private static final Set<String> LIFECYCLE_KINDS = Set.of(
            "STATE_CHANGED", "EXECUTION_RESTARTED", "EXECUTION_RECOVERED");
    static final String HISTORY_ARGUMENT = "--tapstate.metrics.history.sample-interval=PT2S";
    static final String HISTORY_JVM_ARGUMENT = "-Dtapstate.metrics.history.sample-interval=PT2S";

    record RetainedCursor(Instant from, Instant to, String cursor, String incarnation,
            List<String> eventIds) { }
    private record Emitted(List<Document> raw, List<Document> events,
            List<Document> positive, List<Document> transitions, Instant to) { }
    private record RawPair(String leftId, String rightId, Instant start, Instant end, long delta) { }
    private record Selection(List<String> rawIds, List<String> eventIds, Instant intervalEnd,
            List<RawPair> pairs, PipelineState capturedState) { }

    private final MongoDatabase database;
    private final MongoObservationStore latest;
    private final BenchmarkLiveReport report;
    private final String pipeline;
    private final Instant from;
    private final Duration bound;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(HTTP_BOUND).build();
    private final Map<ObservationStore.Scope, Selection> issued = new LinkedHashMap<>();
    private Document coordinationId;
    private String clusterId;
    private long deadline;

    TelemetryMongoIdentityWitness(MongoDatabase database, MongoObservationStore latest,
            BenchmarkLiveReport report, String pipeline, Instant from, Duration bound) {
        this.database = database;
        this.latest = latest;
        this.report = report;
        this.pipeline = pipeline;
        this.from = from;
        this.bound = bound;
    }

    static Map<String, Object> inputHashes(Path root) {
        Map<String, Object> hashes = new LinkedHashMap<>();
        for (String name : List.of("TelemetryMongoIdentityWitness",
                "StandaloneExecutionGenerationIsDurableIT", "ExecutionGenerationSurvivesModeAndOwnerChangesIT")) {
            hashes.put(name, PipelineBenchmarkLiveRunIT.artifact(root.resolve(
                    "e2e/src/test/java/io/tapstate/e2e/" + name + ".java")));
        }
        try (InputStream bytes = TelemetryMongoIdentityWitness.class.getResourceAsStream(
                "TelemetryMongoIdentityWitness.class")) {
            if (bytes == null) { throw new AssertionError("executing witness class bytes are unavailable"); }
            hashes.put("executingHelperClassSha256", digest(bytes.readAllBytes()));
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        }
        hashes.put("historySampleInterval", "PT2S");
        return Map.copyOf(hashes);
    }

    void capture(String action, ObservationStore.Stored expected, ControlPlane control, URI base,
            boolean requirePositiveRaw) {
        deadline = System.nanoTime() + bound.toNanos();
        ObservationStore.Scope scope = requireActualScope(expected);
        assertThat(issued.size() < MAX_SCOPES || issued.containsKey(scope))
                .as("the finite lifecycle witness holds at most %s issued scopes", MAX_SCOPES).isTrue();
        issued.putIfAbsent(scope, new Selection(List.of(), List.of(), null, List.of(), null));
        PipelineState state = expected.observation().state();
        Selection priorCapture = issued.get(scope);
        PipelineState beforeState = priorCapture.capturedState();
        boolean changedState = beforeState != null && beforeState != state;
        ObservationStore.Stored comparable = expected;
        if (requirePositiveRaw && knownOutStart(comparable) == null) {
            comparable = Await.answered("a real known output point for " + action, remaining(),
                    () -> latest.readStored(pipeline).filter(actual -> {
                        Long output = actual.observation().metrics().get("records.out");
                        return actual.scope().filter(scope::equals).isPresent()
                                && actual.observation().state() == state
                                && output != null && output > 0 && knownOutStart(actual) != null;
                    }));
            requireActualScope(comparable);
        }
        Instant expectedStart = knownOutStart(comparable);
        Instant freshFrom = requirePositiveRaw ? comparable.observation().observedAt() : null;
        if (requirePositiveRaw) {
            assertThat(freshFrom).as("fresh raw evidence starts at the actual expected observation").isNotNull();
        }
        var last = new java.util.concurrent.atomic.AtomicReference<Emitted>();
        Emitted emitted;
        try {
            emitted = Await.answered("real scoped raw and lifecycle output for " + action, remaining(), () -> {
            Instant to = Instant.now();
            List<Document> raw = rows(MongoStorePort.PIPELINE_RATE_HISTORY, "observedAt", to,
                    "_id", "pipelineId", "pipelineIncarnationId", "executionGeneration",
                    "observedAt", "countingSince", "counters", "gapFrom");
            List<Document> events = rows(MongoStorePort.PIPELINE_EVENTS, "occurredAt", to,
                    "_id", "pipelineId", "pipelineIncarnationId", "executionGeneration",
                    "occurredAt", "kind", "beforeState", "afterState");
            raw.forEach(this::requireIssued);
            events.forEach(this::requireIssued);
            List<Document> scopedRaw = raw.stream().filter(row -> owner(row).equals(scope)).toList();
            List<Document> allPositive = scopedRaw.stream().filter(TelemetryMongoIdentityWitness::positive).toList();
            requireKnownStarts(allPositive, expectedStart);
            List<Document> positive = qualifiedPair(scopedRaw, freshFrom);
            List<Document> transitions = events.stream().filter(row -> owner(row).equals(scope))
                    .filter(row -> LIFECYCLE_KINDS.contains(row.getString("kind")))
                    .filter(row -> state.name().equals(row.getString("afterState")))
                    .filter(row -> !changedState || "STATE_CHANGED".equals(row.getString("kind"))
                            && beforeState.name().equals(row.getString("beforeState"))
                            && !priorCapture.eventIds().contains(String.valueOf(row.get("_id"))))
                    .toList();
            Emitted actual = new Emitted(raw, events, positive, transitions, to);
            last.set(actual);
            return (requirePositiveRaw && positive.size() < 2) || transitions.isEmpty()
                    ? java.util.Optional.empty() : java.util.Optional.of(actual);
            });
        } catch (AssertionError failure) {
            Map<String, Object> missing = new LinkedHashMap<>();
            missing.put("action", "mongo-telemetry-" + action + "-missing");
            missing.put("status", "MISSING_EVIDENCE");
            missing.put("expectedIncarnation", scope.pipelineIncarnationId());
            missing.put("expectedExecutionGeneration", scope.executionGeneration());
            missing.put("expectedAfterState", state.name());
            missing.put("positiveRawRequired", requirePositiveRaw);
            Emitted actual = last.get();
            if (actual != null) {
                missing.put("lastScannedAt", actual.to().toString());
                missing.put("rawCount", actual.raw().size());
                missing.put("eventCount", actual.events().size());
                missing.put("matchingPositiveRaw", actual.positive().stream()
                        .map(TelemetryMongoIdentityWitness::rawEvidence).toList());
                missing.put("matchingTransitionCount", actual.transitions().size());
                missing.put("actualEvents", actual.events().stream()
                        .map(TelemetryMongoIdentityWitness::eventEvidence).toList());
            }
            missing.put("diagnostic", failure.getMessage());
            report.addFork(missing);
            throw failure;
        }
        requireActualScope(expected);
        requireRetained(scope, emitted);
        List<Document> selected = emitted.positive().size() < 2
                ? emitted.positive() : emitted.positive().subList(0, 2);
        Selection previous = issued.get(scope);
        Selection selection = new Selection(
                java.util.stream.Stream.concat(previous.rawIds().stream(), ids(selected).stream()).distinct().toList(),
                java.util.stream.Stream.concat(previous.eventIds().stream(),
                        ids(emitted.transitions()).stream()).distinct().toList(),
                previous.intervalEnd() != null || selected.size() < 2 ? previous.intervalEnd()
                        : selected.getLast().getDate("observedAt").toInstant(),
                selected.size() < 2 ? previous.pairs() : java.util.stream.Stream.concat(previous.pairs().stream(),
                        java.util.stream.Stream.of(rawPair(selected))).distinct().toList(), state);
        Map<ObservationStore.Scope, Selection> retainedSelections = new LinkedHashMap<>(issued);
        retainedSelections.put(scope, selection);

        List<Map<?, ?>> segments = publicRows(control, base, "metrics/history", emitted.to(), "segments");
        List<Map<?, ?>> events = publicRows(control, base, "events", emitted.to(), "events");
        Set<Instant> ends = intervalEnds(segments);
        Set<String> publicEventIds = events.stream().map(row -> String.valueOf(row.get("id")))
                .collect(Collectors.toSet());
        for (var retained : retainedSelections.entrySet()) {
            if (retained.getKey().pipelineIncarnationId().equals(scope.pipelineIncarnationId())) {
                if (retained.getValue().intervalEnd() != null) {
                    assertThat(ends).as("public raw history retains the actual earlier execution point")
                            .contains(retained.getValue().intervalEnd());
                }
                assertThat(publicEventIds).as("public events retain earlier executions of this resource")
                        .containsAll(retained.getValue().eventIds());
                for (RawPair pair : retained.getValue().pairs()) { requirePublicPair(segments, pair); }
            }
        }
        requirePublicBoundaries(segments, emitted.raw(), scope.pipelineIncarnationId());
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("action", "mongo-telemetry-" + action);
        evidence.put("pipelineId", pipeline);
        evidence.put("incarnation", scope.pipelineIncarnationId());
        evidence.put("executionGeneration", scope.executionGeneration());
        evidence.put("coordinationId", coordinationId);
        evidence.put("clusterId", clusterId);
        evidence.put("state", state.name());
        evidence.put("from", from.toString());
        evidence.put("to", emitted.to().toString());
        evidence.put("positiveRawRequired", requirePositiveRaw);
        evidence.put("requestedObservationAt", expected.observation().observedAt().toString());
        evidence.put("comparisonObservationAt", comparable.observation().observedAt().toString());
        evidence.put("receiptOnly", beforeState == state);
        if (beforeState != null) { evidence.put("expectedBeforeState", beforeState.name()); }
        if (freshFrom != null) { evidence.put("freshRawFrom", freshFrom.toString()); }
        if (expectedStart != null) { evidence.put("expectedCountingSinceMillis", expectedStart.toEpochMilli()); }
        evidence.put("selectedRaw", selected.stream().map(TelemetryMongoIdentityWitness::rawEvidence).toList());
        evidence.put("selectedEvents", emitted.transitions().stream()
                .map(TelemetryMongoIdentityWitness::eventEvidence).toList());
        evidence.put("retainedRawCount", emitted.raw().size());
        evidence.put("retainedEventCount", emitted.events().size());
        evidence.put("historySegments", segments);
        evidence.put("publicEventIds", events.stream().map(row -> row.get("id")).toList());
        evidence.put("performanceAcceptanceEligible", false);
        report.addFork(evidence);
        issued.put(scope, selection);
    }

    /** Qualifies identity and retained public output without claiming quiet-stage counter or raw-pair evidence. */
    void captureIdentityOnly(String action, ObservationStore.Stored expected, ControlPlane control, URI base) {
        deadline = System.nanoTime() + bound.toNanos();
        ObservationStore.Scope scope = requireActualScope(expected);
        assertThat(issued.size() < MAX_SCOPES || issued.containsKey(scope))
                .as("the finite lifecycle witness holds at most %s issued scopes", MAX_SCOPES).isTrue();
        issued.putIfAbsent(scope, new Selection(List.of(), List.of(), null, List.of(), null));
        Selection previous = issued.get(scope);
        PipelineState state = expected.observation().state();
        PipelineState beforeState = previous.capturedState();
        boolean changedState = beforeState != null && beforeState != state;
        var last = new java.util.concurrent.atomic.AtomicReference<Emitted>();
        Emitted emitted;
        try {
            emitted = Await.answered("actual emitted envelope identities and lifecycle event for " + action,
                    remaining(), () -> {
                        Instant to = Instant.now();
                        List<Document> raw = rows(MongoStorePort.PIPELINE_RATE_HISTORY, "observedAt", to,
                                "_id", "pipelineId", "pipelineIncarnationId", "executionGeneration",
                                "observedAt", "countingSince", "counters", "gapFrom");
                        List<Document> events = rows(MongoStorePort.PIPELINE_EVENTS, "occurredAt", to,
                                "_id", "pipelineId", "pipelineIncarnationId", "executionGeneration",
                                "occurredAt", "kind", "beforeState", "afterState");
                        raw.forEach(this::requireIssued);
                        events.forEach(this::requireIssued);
                        List<Document> transitions = events.stream().filter(row -> owner(row).equals(scope))
                                .filter(row -> LIFECYCLE_KINDS.contains(row.getString("kind")))
                                .filter(row -> state.name().equals(row.getString("afterState")))
                                .filter(row -> !changedState || "STATE_CHANGED".equals(row.getString("kind"))
                                        && beforeState.name().equals(row.getString("beforeState"))
                                        && !previous.eventIds().contains(String.valueOf(row.get("_id"))))
                                .toList();
                        Emitted actual = new Emitted(raw, events, List.of(), transitions, to);
                        last.set(actual);
                        return transitions.isEmpty() ? java.util.Optional.empty() : java.util.Optional.of(actual);
                    });
        } catch (AssertionError failure) {
            try {
                Map<String, Object> missing = new LinkedHashMap<>();
                missing.put("action", "mongo-telemetry-" + action + "-missing");
                missing.put("status", "MISSING_EVIDENCE");
                missing.put("qualification", "ACTUAL_ENVELOPE_IDENTITIES_AND_RETAINED_PUBLIC_PROJECTIONS");
                missing.put("expectedIncarnation", scope.pipelineIncarnationId());
                missing.put("expectedExecutionGeneration", scope.executionGeneration());
                missing.put("expectedAfterState", state.name());
                missing.put("positiveRawRequired", false);
                missing.put("positiveRawQualification", "UNVERIFIED_AT_THIS_STAGE");
                missing.put("outputPointQualification", "UNVERIFIED_AT_THIS_STAGE");
                missing.put("unverifiedSurfaces", List.of("FRESH_POSITIVE_RAW_PAIR", "CURRENT_OUTPUT_POINT_START_AND_TOTALS"));
                Emitted actual = last.get();
                if (actual != null) {
                    missing.put("lastScannedAt", actual.to().toString());
                    missing.put("rawCount", actual.raw().size());
                    missing.put("eventCount", actual.events().size());
                    missing.put("ownedRawEnvelopes", actual.raw().stream().filter(row -> owner(row).equals(scope))
                            .map(TelemetryMongoIdentityWitness::rawIdentityEvidence).toList());
                    missing.put("matchingTransitionCount", actual.transitions().size());
                    missing.put("actualEvents", actual.events().stream()
                            .map(TelemetryMongoIdentityWitness::eventEvidence).toList());
                }
                missing.put("diagnostic", failure.getMessage());
                missing.put("performanceAcceptanceEligible", false);
                report.addFork(missing);
            } catch (RuntimeException | Error diagnosticFailure) {
                if (failure != diagnosticFailure) { failure.addSuppressed(diagnosticFailure); }
            }
            throw failure;
        }
        requireActualScope(expected);
        requireRetained(scope, emitted);
        List<Document> ownedRaw = emitted.raw().stream().filter(row -> owner(row).equals(scope)).toList();
        Selection selection = new Selection(
                java.util.stream.Stream.concat(previous.rawIds().stream(), ids(ownedRaw).stream()).distinct().toList(),
                java.util.stream.Stream.concat(previous.eventIds().stream(), ids(emitted.transitions()).stream())
                        .distinct().toList(), previous.intervalEnd(), previous.pairs(), state);
        Map<ObservationStore.Scope, Selection> retained = new LinkedHashMap<>(issued);
        retained.put(scope, selection);
        List<Map<?, ?>> segments = publicRows(control, base, "metrics/history", emitted.to(), "segments");
        List<Map<?, ?>> events = publicRows(control, base, "events", emitted.to(), "events");
        Set<Instant> ends = intervalEnds(segments);
        Set<String> publicEventIds = events.stream().map(row -> String.valueOf(row.get("id")))
                .collect(Collectors.toSet());
        for (var earlier : retained.entrySet()) {
            if (!earlier.getKey().pipelineIncarnationId().equals(scope.pipelineIncarnationId())) { continue; }
            if (earlier.getValue().intervalEnd() != null) {
                assertThat(ends).as("public history retains a previously qualified actual raw point")
                        .contains(earlier.getValue().intervalEnd());
            }
            assertThat(publicEventIds).as("public events retain each actually emitted event of this incarnation")
                    .containsAll(earlier.getValue().eventIds());
            for (RawPair pair : earlier.getValue().pairs()) { requirePublicPair(segments, pair); }
        }
        requirePublicBoundaries(segments, emitted.raw(), scope.pipelineIncarnationId());
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("action", "mongo-telemetry-" + action);
        evidence.put("qualification", "ACTUAL_ENVELOPE_IDENTITIES_AND_RETAINED_PUBLIC_PROJECTIONS");
        evidence.put("pipelineId", pipeline);
        evidence.put("incarnation", scope.pipelineIncarnationId());
        evidence.put("executionGeneration", scope.executionGeneration());
        evidence.put("coordinationId", coordinationId);
        evidence.put("clusterId", clusterId);
        evidence.put("state", state.name());
        evidence.put("from", from.toString());
        evidence.put("to", emitted.to().toString());
        evidence.put("requestedObservationAt", expected.observation().observedAt().toString());
        evidence.put("positiveRawRequired", false);
        evidence.put("positiveRawQualification", "UNVERIFIED_AT_THIS_STAGE");
        evidence.put("outputPointQualification", "UNVERIFIED_AT_THIS_STAGE");
        evidence.put("unverifiedSurfaces", List.of("FRESH_POSITIVE_RAW_PAIR", "CURRENT_OUTPUT_POINT_START_AND_TOTALS"));
        evidence.put("receiptOnly", beforeState == state);
        if (beforeState != null) { evidence.put("expectedBeforeState", beforeState.name()); }
        evidence.put("ownedRawEnvelopeState", ownedRaw.isEmpty() ? "ABSENT" : "EMITTED");
        evidence.put("ownedRawEnvelopes", ownedRaw.stream().map(TelemetryMongoIdentityWitness::rawIdentityEvidence).toList());
        evidence.put("selectedEvents", emitted.transitions().stream()
                .map(TelemetryMongoIdentityWitness::eventEvidence).toList());
        evidence.put("retainedRawCount", emitted.raw().size());
        evidence.put("retainedEventCount", emitted.events().size());
        evidence.put("retainedStrictRawPairs", selection.pairs().size());
        evidence.put("historySegments", segments);
        evidence.put("publicEventIds", events.stream().map(row -> row.get("id")).toList());
        evidence.put("performanceAcceptanceEligible", false);
        report.addFork(evidence);
        issued.put(scope, selection);
    }

    RetainedCursor beforeRecreation(ControlPlane control, URI base) {
        deadline = System.nanoTime() + bound.toNanos();
        Instant to = Instant.now();
        String incarnation = artifactIncarnation();
        Map<?, ?> page = get(control, base, query("metrics/history", from, to, 1, null), 200);
        assertThat(page.get("nextCursor")).as("pagination is witnessed by a genuinely issued cursor")
                .isInstanceOf(String.class);
        String cursor = (String) page.get("nextCursor");
        assertThat(cursor).isNotBlank();
        Map<?, ?> following = get(control, base, query("metrics/history", from, to, 1, cursor), 200);
        Set<Instant> firstPoints = intervalEnds(mapRows(page, "segments"));
        Set<Instant> followingPoints = intervalEnds(mapRows(following, "segments"));
        assertThat(firstPoints).as("the cursor's original page contains a real point").isNotEmpty();
        assertThat(followingPoints).as("the unchanged issued cursor returns its next real point").isNotEmpty();
        for (String field : List.of("from", "to", "effectiveFrom", "effectiveTo", "retentionCutoff")) {
            assertThat(following.get(field)).as("the issued cursor keeps its frozen %s", field)
                    .isEqualTo(page.get(field));
        }
        List<String> events = publicRows(control, base, "events", to, "events").stream()
                .map(row -> String.valueOf(row.get("id"))).toList();
        assertThat(events).as("the old incarnation has actual retained events").isNotEmpty();
        report.addFork(Map.of("action", "mongo-telemetry-before-recreation", "pipelineId", pipeline,
                "incarnation", incarnation, "from", from.toString(), "to", to.toString(), "limit", 1,
                "cursorSha256", digest(cursor.getBytes(StandardCharsets.UTF_8)), "eventIds", events));
        return new RetainedCursor(from, to, cursor, incarnation, events);
    }

    void recreatedBeforeStart(RetainedCursor previous, ControlPlane control, URI base) {
        deadline = System.nanoTime() + bound.toNanos();
        String incarnation = artifactIncarnation();
        assertThat(incarnation).isNotEqualTo(previous.incarnation());
        Map<?, ?> history = get(control, base,
                query("metrics/history", previous.from(), previous.to(), PAGE_LIMIT, null), 200);
        assertThat(intervalEnds(mapRows(history, "segments")))
                .as("a fixed old window exposes no former-incarnation history points").isEmpty();
        Map<?, ?> events = get(control, base,
                query("events", previous.from(), previous.to(), PAGE_LIMIT, null), 200);
        assertThat(events.get("completeness")).isEqualTo("BEST_EFFORT");
        assertThat(mapRows(events, "events")).as("the fixed old window has no new-incarnation events").isEmpty();
        Map<?, ?> refused = get(control, base,
                query("metrics/history", previous.from(), previous.to(), 1, previous.cursor()), 400);
        assertThat(refused.get("code")).isEqualTo(MonitorError.INVALID_CURSOR.code());
        assertThat(refused.get("params")).isInstanceOf(Map.class);
        assertThat(((Map<?, ?>) refused.get("params")).get("reason")).isEqualTo("QUERY_MISMATCH");
        assertThat(((Map<?, ?>) refused.get("params")).get("operation")).isEqualTo("pipeline.metrics.history");
        report.addFork(Map.of("action", "mongo-telemetry-recreated-before-start", "pipelineId", pipeline,
                "oldIncarnation", previous.incarnation(), "newIncarnation", incarnation,
                "oldCursorStatus", 400, "oldCursorCode", MonitorError.INVALID_CURSOR.code(),
                "oldCursorReason", "QUERY_MISMATCH", "historyPoints", 0, "events", 0));
    }

    private ObservationStore.Scope requireActualScope(ObservationStore.Stored expected) {
        ObservationStore.Scope scope = expected.scope().orElseThrow(() ->
                new AssertionError("the real current observation has no execution scope"));
        List<Document> documents = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                .find(new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", pipeline))
                .projection(new Document("_id", 1).append("clusterId", 1).append("executionGeneration", 1))
                .maxTime(remaining().toNanos(), java.util.concurrent.TimeUnit.NANOSECONDS)
                .limit(2).into(new ArrayList<>());
        assertThat(documents).as("one actual authoritative coordination document").hasSize(1);
        Document actual = documents.getFirst();
        assertThat(actual.get("executionGeneration") instanceof Long
                || actual.get("executionGeneration") instanceof Integer).isTrue();
        assertThat(((Number) actual.get("executionGeneration")).longValue()).isPositive()
                .isEqualTo(scope.executionGeneration());
        assertThat(artifactIncarnation()).isEqualTo(scope.pipelineIncarnationId());
        if (coordinationId == null) {
            coordinationId = new Document(actual.get("_id", Document.class));
            clusterId = actual.getString("clusterId");
            assertThat(clusterId).isNotBlank();
        }
        assertThat(actual.get("_id")).isEqualTo(coordinationId);
        assertThat(actual.getString("clusterId")).isEqualTo(clusterId);
        assertThat(latest.readStored(pipeline).orElseThrow().scope()).contains(scope);
        return scope;
    }

    private String artifactIncarnation() {
        Document artifact = database.getCollection(MongoStorePort.ARTIFACTS)
                .find(new Document("_id", pipeline))
                .projection(new Document("pipelineIncarnationId", 1))
                .maxTime(remaining().toNanos(), java.util.concurrent.TimeUnit.NANOSECONDS).first();
        assertThat(artifact).as("the current real pipeline artifact exists").isNotNull();
        String incarnation = artifact.getString("pipelineIncarnationId");
        assertThat(incarnation).as("the incarnation came from the actual artifact").isNotBlank();
        return incarnation;
    }

    private List<Document> rows(String collection, String time, Instant to, String... fields) {
        Document projection = new Document();
        for (String field : fields) { projection.append(field, 1); }
        List<Document> rows = database.getCollection(collection)
                .find(new Document("pipelineId", pipeline).append(time,
                        new Document("$gte", Date.from(from)).append("$lt", Date.from(to))))
                .projection(projection).sort(new Document(time, 1).append("_id", 1))
                .maxTime(remaining().toNanos(), java.util.concurrent.TimeUnit.NANOSECONDS)
                .limit(MAX_ROWS + 1).into(new ArrayList<>());
        assertThat(rows.size()).as("the diagnostic envelope scan remains bounded").isLessThanOrEqualTo(MAX_ROWS);
        return rows;
    }

    private void requireIssued(Document row) {
        assertThat(row.getString("pipelineId")).isEqualTo(pipeline);
        assertThat(issued.keySet()).as("every emitted envelope belongs to an actually captured execution")
                .contains(owner(row));
    }

    private void requireRetained(ObservationStore.Scope scope, Emitted emitted) {
        Set<String> raw = Set.copyOf(ids(emitted.raw()));
        Set<String> events = Set.copyOf(ids(emitted.events()));
        for (var previous : issued.entrySet()) {
            if (previous.getKey().pipelineIncarnationId().equals(scope.pipelineIncarnationId())) {
                assertThat(raw).as("same-incarnation stop/start retains earlier raw rows")
                        .containsAll(previous.getValue().rawIds());
                assertThat(events).as("same-incarnation stop/start retains earlier lifecycle events")
                        .containsAll(previous.getValue().eventIds());
            }
        }
    }

    private List<Map<?, ?>> publicRows(ControlPlane control, URI base, String path,
            Instant to, String field) {
        List<Map<?, ?>> output = new ArrayList<>();
        String cursor = null;
        Set<String> seen = new java.util.HashSet<>();
        for (int index = 0; index < MAX_PAGES; index++) {
            Map<?, ?> page = get(control, base, query(path, from, to, PAGE_LIMIT, cursor), 200);
            if (path.equals("events")) { assertThat(page.get("completeness")).isEqualTo("BEST_EFFORT"); }
            output.addAll(mapRows(page, field));
            assertThat(output.size()).as("public evidence stays within a fixed page budget")
                    .isLessThanOrEqualTo(MAX_ROWS);
            Object next = page.get("nextCursor");
            if (next == null) { return List.copyOf(output); }
            assertThat(next).isInstanceOf(String.class);
            cursor = (String) next;
            assertThat(cursor).isNotBlank();
            assertThat(seen.add(cursor)).as("the public cursor advances without a loop").isTrue();
        }
        throw new AssertionError("public telemetry evidence exceeded the fixed page budget");
    }

    private String query(String path, Instant queryFrom, Instant to, int limit, String cursor) {
        return "/api/pipelines/" + encode(pipeline) + "/" + path
                + "?from=" + encode(queryFrom.toString()) + "&to=" + encode(to.toString())
                + "&limit=" + limit + (path.equals("metrics/history") ? "&resolution=raw" : "")
                + (cursor == null ? "" : "&cursor=" + encode(cursor));
    }

    private Map<?, ?> get(ControlPlane control, URI base, String path, int expectedStatus) {
        Duration left = remaining();
        HttpRequest request = HttpRequest.newBuilder(base.resolve(path))
                .timeout(left.compareTo(HTTP_BOUND) < 0 ? left : HTTP_BOUND)
                .header("Authorization", "Bearer " + control.credential()).GET().build();
        try {
            HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).as("the ordinary authenticated telemetry query status")
                    .isEqualTo(expectedStatus);
            Object parsed = JsonReader.parse(response.body());
            assertThat(parsed).isInstanceOf(Map.class);
            return (Map<?, ?>) parsed;
        } catch (IOException failure) {
            throw new UncheckedIOException(failure);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while reading actual telemetry evidence", interrupted);
        }
    }

    private static List<Map<?, ?>> mapRows(Map<?, ?> page, String field) {
        assertThat(page.get(field)).isInstanceOf(List.class);
        List<Map<?, ?>> rows = new ArrayList<>();
        for (Object value : (List<?>) page.get(field)) {
            assertThat(value).isInstanceOf(Map.class);
            rows.add((Map<?, ?>) value);
        }
        return List.copyOf(rows);
    }

    private static Set<Instant> intervalEnds(List<Map<?, ?>> segments) {
        Set<Instant> ends = new java.util.HashSet<>();
        for (Map<?, ?> segment : segments) {
            for (Map<?, ?> point : mapRows(segment, "points")) {
                assertThat(point.get("intervalEnd")).isInstanceOf(String.class);
                ends.add(Instant.parse((String) point.get("intervalEnd")));
            }
        }
        return Set.copyOf(ends);
    }

    private static ObservationStore.Scope owner(Document row) {
        String incarnation = row.getString("pipelineIncarnationId");
        assertThat(incarnation).as("a newly emitted envelope has a real incarnation").isNotBlank();
        Object generation = row.get("executionGeneration");
        assertThat(generation instanceof Long || generation instanceof Integer).isTrue();
        long value = ((Number) generation).longValue();
        assertThat(value).as("a newly emitted envelope is never unfenced").isPositive();
        return new ObservationStore.Scope(incarnation, value);
    }

    private static boolean positive(Document row) {
        return row.get("counters") instanceof Document counters
                && counters.get("records.out") instanceof Number records && records.longValue() > 0;
    }

    private static Instant knownOutStart(ObservationStore.Stored expected) {
        Instant latest = null;
        for (var fact : expected.observation().facts()) {
            if (!"tapstate.pipeline.records".equals(fact.name())) { continue; }
            for (var point : fact.points()) {
                if (!"out".equals(point.attributes().get("direction")) || point.startTime() == null) { continue; }
                if (latest == null || point.startTime().isAfter(latest)) { latest = point.startTime(); }
            }
        }
        return latest == null ? null : Instant.ofEpochMilli(latest.toEpochMilli());
    }

    private static void requireKnownStarts(List<Document> rows, Instant expectedStart) {
        if (rows.isEmpty()) { return; }
        assertThat(expectedStart).as("positive output has its actual known point start").isNotNull();
        for (Document row : rows) {
            assertThat(row.get("countingSince")).as("the positive raw row retains its known start").isInstanceOf(Date.class);
            assertThat(row.getDate("countingSince").toInstant())
                    .as("raw countingSince equals the actual latest known out point start at BSON precision")
                    .isEqualTo(expectedStart);
        }
    }

    private static List<Document> qualifiedPair(List<Document> rows, Instant freshFrom) {
        for (int i = 1; i < rows.size(); i++) {
            Document left = rows.get(i - 1), right = rows.get(i);
            if (!positive(left) || !positive(right)) { continue; }
            Instant a = left.getDate("observedAt").toInstant(), b = right.getDate("observedAt").toInstant();
            if (freshFrom != null && (a.isBefore(freshFrom) || b.isBefore(freshFrom))) { continue; }
            if (!a.isBefore(b) || boundary(left, right) != null) { continue; }
            return List.of(left, right);
        }
        return List.of();
    }

    private static RawPair rawPair(List<Document> rows) {
        Document left = rows.getFirst(), right = rows.getLast();
        return new RawPair(String.valueOf(left.get("_id")), String.valueOf(right.get("_id")),
                left.getDate("observedAt").toInstant(), right.getDate("observedAt").toInstant(),
                counter(right, "records.out") - counter(left, "records.out"));
    }

    private static long counter(Document row, String name) {
        Object value = row.get("counters") instanceof Document counters ? counters.get(name) : null;
        assertThat(value).as("a selected interval has its actual raw %s counter", name).isInstanceOf(Number.class);
        return ((Number) value).longValue();
    }

    private static void requirePublicPair(List<Map<?, ?>> segments, RawPair expected) {
        List<Map<?, ?>> matching = new ArrayList<>();
        for (Map<?, ?> segment : segments) {
            for (Map<?, ?> point : mapRows(segment, "points")) {
                if (expected.start().toString().equals(point.get("intervalStart"))
                        && expected.end().toString().equals(point.get("intervalEnd"))) { matching.add(point); }
            }
        }
        assertThat(matching).as("the public raw point belongs to its actual adjacent selected rows").hasSize(1);
        assertThat(matching.getFirst().get("recordsOut")).isInstanceOf(Map.class);
        Object delta = ((Map<?, ?>) matching.getFirst().get("recordsOut")).get("delta");
        assertThat(delta).isNotNull();
        assertThat(new BigDecimal(String.valueOf(delta)).compareTo(BigDecimal.valueOf(expected.delta())))
                .as("public recordsOut is computed only from the actual known raw pair").isZero();
    }

    private static String boundary(Document left, Document right) {
        Instant a = left.getDate("observedAt").toInstant(), b = right.getDate("observedAt").toInstant();
        if (right.get("gapFrom") instanceof Date || Duration.between(a, b).compareTo(Duration.ofSeconds(4)) >= 0) {
            return "GAP";
        }
        if (left.get("countingSince") instanceof Date oldStart && right.get("countingSince") instanceof Date newStart
                && !oldStart.equals(newStart) || decreased(left, right, "records.out") || decreased(left, right, "bytes.out")) {
            return "COUNTER_RESET";
        }
        return owner(left).equals(owner(right)) ? null : "CONTINUATION";
    }

    private static boolean decreased(Document left, Document right, String name) {
        Object a = left.get("counters") instanceof Document counters ? counters.get(name) : null;
        Object b = right.get("counters") instanceof Document counters ? counters.get(name) : null;
        return a instanceof Number before && b instanceof Number after && after.longValue() < before.longValue();
    }

    private static void requirePublicBoundaries(List<Map<?, ?>> segments, List<Document> raw, String incarnation) {
        List<Document> current = raw.stream().filter(row -> incarnation.equals(owner(row).pipelineIncarnationId())).toList();
        for (int i = 1; i < current.size(); i++) {
            Document left = current.get(i - 1), right = current.get(i);
            String reason = boundary(left, right);
            if (reason == null) { continue; }
            Instant at = right.getDate("observedAt").toInstant();
            // A raw boundary emits its real right-hand sample as a singleton, with no rate across it.
            List<Map<?, ?>> matching = segments.stream()
                    .filter(segment -> at.toString().equals(segment.get("intervalStart"))
                            && reason.equals(segment.get("startReason")))
                    .toList();
            assertThat(matching).as("the actual %s boundary at %s starts a public segment", reason, at)
                    .isNotEmpty();
            assertThat(matching.stream().anyMatch(segment -> {
                Map<?, ?> first = mapRows(segment, "points").getFirst();
                return at.toString().equals(first.get("intervalStart"))
                        && at.toString().equals(first.get("intervalEnd"))
                        && first.get("recordsOut") == null && first.get("bytesOut") == null;
            })).as("the actual boundary has a baseline point without invented counter rates").isTrue();
            assertThat(segments.stream().anyMatch(segment ->
                    Instant.parse(String.valueOf(segment.get("intervalStart"))).isBefore(at)
                            && Instant.parse(String.valueOf(segment.get("intervalEnd"))).isAfter(at)))
                    .as("no public segment merges the actual raw boundary").isFalse();
            assertThat(segments.stream().flatMap(segment -> mapRows(segment, "points").stream()).anyMatch(point ->
                    Instant.parse(String.valueOf(point.get("intervalStart"))).isBefore(at)
                            && Instant.parse(String.valueOf(point.get("intervalEnd"))).isAfter(at)))
                    .as("no public rate interval crosses the actual raw boundary").isFalse();
        }
    }

    private static List<String> ids(List<Document> rows) {
        rows.forEach(row -> assertThat(row.get("_id")).as("the actual stored envelope id exists").isNotNull());
        return rows.stream().map(row -> String.valueOf(row.get("_id"))).toList();
    }

    private static Map<String, Object> rawEvidence(Document row) {
        return Map.of("id", String.valueOf(row.get("_id")), "observedAt", row.getDate("observedAt").toInstant().toString(),
                "countingSince", row.getDate("countingSince").toInstant().toString(),
                "recordsOut", row.get("counters", Document.class).get("records.out"),
                "incarnation", row.getString("pipelineIncarnationId"), "executionGeneration", row.get("executionGeneration"));
    }

    private static Map<String, Object> rawIdentityEvidence(Document row) {
        Map<String, Object> evidence = new LinkedHashMap<>();
        evidence.put("id", String.valueOf(row.get("_id")));
        evidence.put("observedAt", row.getDate("observedAt").toInstant().toString());
        evidence.put("incarnation", row.getString("pipelineIncarnationId"));
        evidence.put("executionGeneration", row.get("executionGeneration"));
        assertThat(row.get("countingSince") == null || row.get("countingSince") instanceof Date)
                .as("an actually emitted raw start is a BSON date when present").isTrue();
        evidence.put("countingSinceState", row.get("countingSince") instanceof Date ? "RECORDED" : "ABSENT");
        if (row.get("countingSince") instanceof Date start) {
            evidence.put("countingSince", start.toInstant().toString());
        }
        assertThat(row.get("counters") == null || row.get("counters") instanceof Document)
                .as("actually emitted raw counters form a document when present").isTrue();
        Object output = row.get("counters") instanceof Document counters ? counters.get("records.out") : null;
        assertThat(output == null || output instanceof Number).as("an emitted output counter is numeric when present").isTrue();
        evidence.put("recordsOutState", output instanceof Number ? "RECORDED" : "ABSENT");
        if (output instanceof Number) { evidence.put("recordsOut", output); }
        return Map.copyOf(evidence);
    }

    private static Map<String, Object> eventEvidence(Document row) {
        Map<String, Object> result = new LinkedHashMap<>();
        for (String key : List.of("pipelineIncarnationId", "executionGeneration", "kind", "beforeState", "afterState")) {
            if (row.get(key) != null) { result.put(key, row.get(key)); }
        }
        result.put("id", String.valueOf(row.get("_id")));
        result.put("occurredAt", row.getDate("occurredAt").toInstant().toString());
        return Map.copyOf(result);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private Duration remaining() {
        long nanos = deadline - System.nanoTime();
        assertThat(nanos).as("one witness stage shares the caller's original wait budget").isPositive();
        return Duration.ofNanos(nanos);
    }

    private static String digest(byte[] bytes) {
        try { return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)); }
        catch (NoSuchAlgorithmException missing) { throw new IllegalStateException("SHA-256 unavailable", missing); }
    }
}
