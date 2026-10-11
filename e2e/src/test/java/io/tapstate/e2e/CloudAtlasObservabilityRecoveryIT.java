package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assumptions;

import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Cloud Cookie observations over real Atlas data and SDK validation, with a controlled issuer. */
@RequiresDocker
class CloudAtlasObservabilityRecoveryIT {
    private static final Duration WAIT = Duration.ofSeconds(120);
    private static final String SOURCE = "src_atlas_observed";
    private static final String TARGET = "tgt_atlas_observed";
    private static final String PIPELINE = "cloud_atlas_observed";
    private static final String TABLE = "probe";
    private static final String INVALID_PASSWORD = "owned-invalid-atlas-password-sentinel";

    @Test
    void aCloudCookieReadsAllObservationSurfacesDuringAtlasFailureAndRecovery() throws Exception {
        String baseUri = System.getenv("TAPSTATE_ATLAS_TEST_URI");
        Assumptions.assumeTrue(baseUri != null && !baseUri.isBlank(), "a controlled Atlas URI is required");
        RealConnectorGate.require("mongodb-atlas");
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String sourceDatabase = "ts_plan_obs_source_" + suffix;
        String targetDatabase = "ts_plan_obs_target_" + suffix;
        String sourceUri = inDatabase(baseUri, sourceDatabase);
        String targetUri = inDatabase(baseUri, targetDatabase);
        String storeUri = SharedMongo.replicaSetUrl("ts_plan_obs_meta_" + suffix);
        Instant from = Instant.now().minusSeconds(1);
        try (var source = MongoClients.create(sourceUri);
             var target = MongoClients.create(targetUri);
             AutoCloseable sourceCleanup = () -> source.getDatabase(sourceDatabase).drop();
             AutoCloseable targetCleanup = () -> target.getDatabase(targetDatabase).drop();
             AtlasRuntime runtime = new AtlasRuntime(AtlasRuntime.Mode.CLOUD, storeUri);
             ServerHandle server = runtime.launch(Tiers.REAL_PROCESS,
                     List.of("--tapstate.metrics.history.sample-interval=10s"))) {
            MongoCollection<Document> input = source.getDatabase(sourceDatabase).getCollection(TABLE);
            MongoCollection<Document> output = target.getDatabase(targetDatabase).getCollection(TABLE);
            input.insertMany(List.of(new Document("_id", "first").append("value", 11),
                    new Document("_id", "second").append("value", 22)));
            ControlPlane control = runtime.control(server, true);
            List<String> credentials = List.of(baseUri, sourceUri, targetUri, INVALID_PASSWORD,
                    control.credential(), new String(new ConnectionString(baseUri).getCredential().getPassword()));
            boolean applied = false;
            Throwable primaryFailure = null;
            try {
                control.apply(Map.of("source.tap.yml", sourceYaml(sourceUri),
                        "target.tap.yml", targetYaml(targetUri), "pipeline.tap.yml", pipelineYaml()));
                applied = true;
                control.discoverSchema(SOURCE, "mongodb-atlas", Map.of("isUri", true, "uri", sourceUri));
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until("the two Atlas snapshot rows to arrive", WAIT,
                        () -> rows(output).equals(rows(input)), () -> diagnostic(control, output, credentials));
                String snapshotAck = Await.answered("the snapshot ACK", WAIT,
                        () -> control.durablePosition(PIPELINE, TABLE));
                AtomicReference<Map<?, ?>> latestHistory = new AtomicReference<>(Map.of());
                Await.until("the real retained output-counter baseline", WAIT, () -> {
                            Map<?, ?> history = read(control, historyPath(from, Instant.now().minusSeconds(1)), credentials);
                            latestHistory.set(history);
                            return hasOutputRate(history);
                        }, () -> historyDiagnostic(latestHistory.get()));
                input.updateOne(Filters.eq("_id", "first"), Updates.set("value", 111));
                Await.until("a CDC update and its ACK to arrive", WAIT,
                        () -> rows(output).equals(rows(input))
                                && control.durablePosition(PIPELINE, TABLE)
                                        .filter(position -> !position.equals(snapshotAck)).isPresent(),
                        () -> "the target or ACK has not advanced");
                AtomicReference<Instant> frozenTo = new AtomicReference<>();
                Await.until("a retained target-acknowledged output interval", WAIT, () -> {
                            Instant to = Instant.now().minusSeconds(1);
                            Map<?, ?> history = read(control, historyPath(from, to), credentials);
                            latestHistory.set(history);
                            if (pointCount(history) < 2 || !hasOutputDelta(history)) return false;
                            frozenTo.set(to);
                            return true;
                        },
                        () -> historyDiagnostic(latestHistory.get()));

                String fixedHistory = historyPath(from, frozenTo.get());
                Map<?, ?> retained = read(control, fixedHistory, credentials);
                assertThat(pointCount(retained)).isGreaterThanOrEqualTo(2);
                List<String> observationPaths = List.of(path("status"), path("metrics"), path("snapshot"),
                        path("logs"), fixedHistory, path("explain"));
                assertCookieBoundary(server, control, observationPaths);

                Map<String, Long> beforeFailure = rows(output);
                String previousAck = control.durablePosition(PIPELINE, TABLE).orElseThrow();
                control.stop(PIPELINE, false);
                awaitState(control, PipelineState.STOPPED);
                control.apply(Map.of("source.tap.yml", sourceYaml(wrongPassword(sourceUri))));
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until("the real Atlas authentication refusal and fresh explanation", WAIT,
                        () -> {
                            Map<?, ?> status = read(control, path("status"), credentials);
                            Map<?, ?> explain = read(control, path("explain"), credentials);
                            return "FAILED".equals(status.get("state"))
                                    && status.get("failure") instanceof Map<?, ?> failure
                                    && "connector.capture-failed".equals(failure.get("code"))
                                    && "CODED_FAILURE".equals(explain.get("kind"))
                                    && "FRESH".equals(explain.get("freshness"));
                        }, () -> "no matching fresh coded failure yet");
                Map<?, ?> failureStatus = read(control, path("status"), credentials);
                Map<?, ?> failureExplain = read(control, path("explain"), credentials);
                for (String observation : observationPaths) read(control, observation, credentials);
                assertThat(read(control, fixedHistory, credentials).get("segments")).isEqualTo(retained.get("segments"));
                assertThat(rows(output)).isEqualTo(beforeFailure);
                assertThat(((List<?>) failureExplain.get("evidence")).stream().anyMatch(value ->
                        value instanceof Map<?, ?> evidence && "status".equals(evidence.get("source"))
                                && "failure".equals(evidence.get("field"))
                                && evidence.get("value") instanceof Map<?, ?> failure
                                && ((Map<?, ?>) failureStatus.get("failure")).get("code").equals(failure.get("code"))))
                        .as("status and explain must report the same canonical failure").isTrue();

                control.stop(PIPELINE, false);
                awaitState(control, PipelineState.STOPPED);
                control.apply(Map.of("source.tap.yml", sourceYaml(sourceUri)));
                input.insertOne(new Document("_id", "after-recovery").append("value", 33));
                Instant recoveryStartedAt = Instant.now();
                control.lifecycle(PIPELINE, LifecycleVerb.START);
                Await.until("recovery to consume the retained position and clear the failure", WAIT,
                        () -> {
                            Map<?, ?> status = read(control, path("status"), credentials);
                            Map<?, ?> explain = read(control, path("explain"), credentials);
                            return rows(output).equals(rows(input)) && rows(output).size() == 3
                                    && "RUNNING".equals(status.get("state")) && status.get("failure") == null
                                    && "FRESH".equals(explain.get("freshness"))
                                    && !"CODED_FAILURE".equals(explain.get("kind"))
                                    && Long.valueOf(0).equals(control.snapshotRowsRead(PIPELINE).get(TABLE))
                                    && control.durablePosition(PIPELINE, TABLE)
                                            .filter(position -> !position.equals(previousAck)).isPresent();
                        }, () -> "recovery, target data, or ACK is incomplete");
                assertThat(control.snapshotRowsRead(PIPELINE)).containsEntry(TABLE, 0L);
                Await.until("a retained sample after recovery", WAIT,
                        () -> hasPointAfter(read(control, historyPath(from, Instant.now().minusSeconds(1)), credentials),
                                recoveryStartedAt), () -> "no later retained sample yet");
                for (String observation : observationPaths) read(control, observation, credentials);
                assertCookieBoundary(server, control, observationPaths);
                runtime.assertAuthenticationBoundary();
                writeEvidence(sourceDatabase, targetDatabase, pointCount(retained), failureStatus, failureExplain,
                        rows(output).size());
            } catch (Exception | Error failure) {
                primaryFailure = failure;
                throw failure;
            } finally {
                if (applied) {
                    try { control.stop(PIPELINE, false); }
                    catch (RuntimeException | Error cleanupFailure) {
                        if (primaryFailure == null) throw cleanupFailure;
                        primaryFailure.addSuppressed(cleanupFailure);
                    }
                }
            }
        }
    }

    private static void assertCookieBoundary(ServerHandle server, ControlPlane control, List<String> paths)
            throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        for (String path : paths) {
            assertThat(control.sourceRequest("GET", path, null, null, 200).headers().firstValue("Set-Cookie")).isEmpty();
            for (boolean bearer : List.of(false, true)) {
                var request = HttpRequest.newBuilder(server.baseUrl().resolve(path)).timeout(Duration.ofSeconds(20));
                if (bearer) request.header("Authorization", "Bearer " + control.credential());
                var response = client.send(request.GET().build(), HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode()).isEqualTo(401);
                assertThat(((Map<?, ?>) JsonReader.parse(response.body())).get("code"))
                        .isEqualTo("control.unauthenticated");
            }
        }
    }

    private static Map<?, ?> read(ControlPlane control, String path, List<String> credentials) {
        var response = control.sourceRequest("GET", path, null, null, 200);
        assertThat(credentials.stream().noneMatch(response.body()::contains))
                .as("observation responses must not contain credentials").isTrue();
        assertThat(JsonReader.parse(response.body())).isInstanceOf(Map.class);
        return (Map<?, ?>) JsonReader.parse(response.body());
    }

    private static int pointCount(Map<?, ?> history) {
        return ((List<?>) history.get("segments")).stream().mapToInt(segment ->
                ((List<?>) ((Map<?, ?>) segment).get("points")).size()).sum();
    }

    private static boolean hasOutputDelta(Map<?, ?> history) {
        return ((List<?>) history.get("segments")).stream().flatMap(segment ->
                ((List<?>) ((Map<?, ?>) segment).get("points")).stream()).anyMatch(point ->
                ((Map<?, ?>) point).get("recordsOut") instanceof Map<?, ?> rate
                        && rate.get("delta") instanceof Number delta && delta.doubleValue() > 0);
    }

    private static boolean hasOutputRate(Map<?, ?> history) {
        return ((List<?>) history.get("segments")).stream().flatMap(segment ->
                ((List<?>) ((Map<?, ?>) segment).get("points")).stream()).anyMatch(point ->
                ((Map<?, ?>) point).get("recordsOut") instanceof Map<?, ?> rate
                        && rate.get("delta") instanceof Number);
    }

    private static String historyDiagnostic(Map<?, ?> history) {
        Object segments = history.get("segments");
        List<Map<String, Object>> readings = new java.util.ArrayList<>();
        if (segments instanceof List<?> values) {
            for (Object value : values) {
                Map<?, ?> segment = (Map<?, ?>) value;
                for (Object item : (List<?>) segment.get("points")) {
                    Map<?, ?> point = (Map<?, ?>) item;
                    Object delta = point.get("recordsOut") instanceof Map<?, ?> rate ? rate.get("delta") : "absent";
                    readings.add(Map.of("reason", segment.get("startReason"), "end", point.get("intervalEnd"),
                            "delta", delta == null ? "absent" : delta));
                }
            }
        }
        return JsonWriter.write(Map.of("status", String.valueOf(history.get("status")), "readings", readings));
    }

    private static boolean hasPointAfter(Map<?, ?> history, Instant boundary) {
        return ((List<?>) history.get("segments")).stream().flatMap(segment ->
                ((List<?>) ((Map<?, ?>) segment).get("points")).stream()).anyMatch(point ->
                Instant.parse(String.valueOf(((Map<?, ?>) point).get("intervalEnd"))).isAfter(boundary));
    }

    private static String diagnostic(ControlPlane control, MongoCollection<Document> output,
            List<String> credentials) {
        Map<?, ?> status = read(control, path("status"), credentials);
        Map<?, ?> metrics = read(control, path("metrics"), credentials);
        Map<?, ?> counts = metrics.get("metrics") instanceof Map<?, ?> values ? values : Map.of();
        Object code = status.get("failure") instanceof Map<?, ?> failure ? failure.get("code") : "none";
        return "state=" + status.get("state") + ", failureCode=" + code + ", targetRows=" + output.countDocuments()
                + ", recordsIn=" + counts.get("records.in") + ", recordsOut=" + counts.get("records.out");
    }

    private static void awaitState(ControlPlane control, PipelineState expected) {
        Await.until("pipeline state " + expected, WAIT,
                () -> control.state(PIPELINE).filter(expected::equals).isPresent(), () -> "state not observed");
    }

    private static Map<String, Long> rows(MongoCollection<Document> collection) {
        Map<String, Long> rows = new TreeMap<>();
        for (Document row : collection.find()) rows.put(row.getString("_id"), ((Number) row.get("value")).longValue());
        return rows;
    }

    private static String path(String surface) { return "/api/pipelines/" + PIPELINE + "/" + surface; }

    private static String historyPath(Instant from, Instant to) {
        return path("metrics/history") + "?from=" + encoded(from.toString()) + "&to=" + encoded(to.toString())
                + "&resolution=raw&limit=1000&table=" + TABLE;
    }

    private static String encoded(String value) { return URLEncoder.encode(value, StandardCharsets.UTF_8); }

    private static String inDatabase(String uri, String database) {
        int slash = uri.indexOf('/', uri.indexOf("://") + 3);
        assertThat(slash).as("the controlled Atlas URI has a database path").isPositive();
        int query = uri.indexOf('?', slash);
        return uri.substring(0, slash + 1) + database + (query < 0 ? "" : uri.substring(query));
    }

    private static String wrongPassword(String uri) {
        int start = uri.indexOf("://") + 3;
        int separator = uri.indexOf(':', start);
        int at = uri.indexOf('@', start);
        assertThat(separator > start && at > separator).as("the controlled URI carries credentials").isTrue();
        return uri.substring(0, separator + 1) + INVALID_PASSWORD + uri.substring(at);
    }

    private static String sourceYaml(String uri) { return sourceYaml(SOURCE, uri, "mode: cdc\ntables: [probe]\n"); }
    private static String targetYaml(String uri) { return sourceYaml(TARGET, uri, ""); }
    private static String sourceYaml(String id, String uri, String suffix) {
        return "version: tapstate/v1\nkind: source\nid: " + id + "\nconnector: mongodb-atlas\nconfig: "
                + JsonWriter.write(Map.of("isUri", true, "uri", uri)) + "\n" + suffix;
    }

    private static String pipelineYaml() {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                settings: { read_mode: snapshot_and_cdc }
                transforms:
                  - { id: all_rows, from: [%s], type: filter, expr: "true" }
                serve:
                  from: all_rows
                  sync:
                    - source: %s
                """.formatted(PIPELINE, SOURCE, TABLE, TARGET);
    }

    private static void writeEvidence(String source, String target, int points, Map<?, ?> status,
            Map<?, ?> explain, int rows) throws Exception {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("completedAt", Instant.now().toString());
        record.put("scope", "Cloud Cookie and real Atlas data; controlled SDK issuer; local system metadata");
        record.put("sourceDatabase", source); record.put("targetDatabase", target);
        record.put("surfaces", List.of("status", "metrics", "snapshot", "logs", "history", "explain"));
        record.put("cookieReadsSucceeded", true); record.put("missingCookieAndBearerRejected", true);
        record.put("retainedHistoryPoints", points); record.put("pastHistoryPreservedDuringFailure", true);
        record.put("failureCode", ((Map<?, ?>) status.get("failure")).get("code"));
        record.put("failureKind", explain.get("kind")); record.put("failureFreshness", explain.get("freshness"));
        record.put("recoveryRows", rows); record.put("sourceTargetMatched", true);
        record.put("recoveryAckAdvanced", true); record.put("recoverySnapshotRowsRead", 0);
        Path directory = Path.of("target", "live-acceptance"); Files.createDirectories(directory);
        Files.writeString(directory.resolve("cloud-atlas-observability-recovery.json"), JsonWriter.write(record));
    }
}
