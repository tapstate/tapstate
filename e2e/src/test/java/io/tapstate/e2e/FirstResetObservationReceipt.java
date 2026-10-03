package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import org.bson.Document;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/** Bounded raw receipts around the original first-reset gate; empty measurements are not zeros. */
final class FirstResetObservationReceipt {
    private static final String OUTPUT = "tapstate.e2e.observability-first-reset.output-dir";
    private static final int MAX_BYTES = 2 * 1024 * 1024;

    private FirstResetObservationReceipt() { }

    static void captureIfRequested(String stage, String storeUri, String pipeline, ControlPlane control,
            Instant queryFrom, Throwable originalFailure) throws Exception {
        String selected = System.getProperty(OUTPUT);
        if (selected == null) { return; }
        if (selected.isBlank()) { throw new AssertionError("empty first-reset receipt output directory"); }
        Path directory = Path.of(selected);
        if (!directory.isAbsolute()) { throw new AssertionError("first-reset receipt output directory must be absolute"); }
        Path output = directory.resolve(stage + ".json");
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, PipelineBenchmarkLiveRunIT.harnessRoot());
        Map<String, Object> receipt = new LinkedHashMap<>();
        receipt.put("kind", "FIRST_RESET_OBSERVATION_DIAGNOSTIC"); receipt.put("stage", stage);
        receipt.put("capturedAt", Instant.now().toString()); receipt.put("pipelineId", pipeline);
        receipt.put("originalQueryFrom", queryFrom.toString()); receipt.put("performanceAcceptanceEligible", false);
        receipt.put("qualification", "UNVERIFIED_NON_ATOMIC_READ_RECEIPTS");
        if (originalFailure != null) {
            receipt.put("originalFailure", Map.of("type", originalFailure.getClass().getName(),
                    "message", String.valueOf(originalFailure.getMessage())));
        }
        String response = control.metrics(pipeline);
        if (response.getBytes(StandardCharsets.UTF_8).length > MAX_BYTES) {
            throw new AssertionError("public metrics receipt byte budget exceeded");
        }
        receipt.put("publicMetricsResponse", response);
        int separator = response.indexOf(' ');
        if (separator < 0) { throw new AssertionError("public metrics receipt has no HTTP status"); }
        receipt.put("publicMetricsHttpStatus", Integer.parseInt(response.substring(0, separator)));
        Object body = JsonReader.parse(response.substring(separator + 1));
        receipt.put("publicMetricsBody", body);
        List<Map<String, Object>> outFacts = new ArrayList<>();
        if (body instanceof Map<?, ?> map && map.get("facts") instanceof List<?> facts) {
            for (Object raw : facts) {
                if (!(raw instanceof Map<?, ?> fact) || !(fact.get("points") instanceof List<?> points)) {
                    throw new AssertionError("public fact receipt shape unavailable");
                }
                List<Object> outPoints = points.stream().filter(point -> point instanceof Map<?, ?> value
                        && value.get("attributes") instanceof Map<?, ?> attributes
                        && "out".equals(attributes.get("direction"))).map(point -> (Object) point).toList();
                if (!outPoints.isEmpty()) {
                    Map<String, Object> kept = new LinkedHashMap<>();
                    for (String key : List.of("name", "type", "unit")) {
                        if (fact.containsKey(key)) { kept.put(key, fact.get(key)); }
                    }
                    kept.put("points", outPoints); outFacts.add(kept);
                }
            }
        }
        receipt.put("publicOutFacts", outFacts);

        ConnectionString connection = new ConnectionString(storeUri);
        if (connection.getDatabase() == null) { throw new AssertionError("receipt store URI lacks a database"); }
        try (var client = MongoClients.create(connection)) {
            var database = client.getDatabase(connection.getDatabase());
            Document artifact = database.getCollection(MongoStorePort.ARTIFACTS)
                    .withTimeout(5, TimeUnit.SECONDS).find(new Document("_id", pipeline).append("kind", "pipeline"))
                    .projection(new Document("_id", 1).append("kind", 1).append("contentHash", 1)
                            .append("pipelineIncarnationId", 1).append("legacyHistoryVisible", 1)).first();
            receipt.put("artifact", json(artifact));
            List<Document> authorities = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                    .withTimeout(5, TimeUnit.SECONDS)
                    .find(new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", pipeline))
                    .projection(new Document("_id", 1).append("clusterId", 1).append("resourceType", 1)
                            .append("resourceId", 1).append("executionGeneration", 1)
                            .append("claimGeneration", 1).append("ownerNodeId", 1).append("ownerBootId", 1))
                    .limit(2).into(new ArrayList<>());
            receipt.put("authorityCandidates", authorities.stream().map(FirstResetObservationReceipt::json).toList());
            receipt.put("authoritySelection", authorities.size() == 1 ? "ONE_ACTUAL_CANDIDATE" : "UNVERIFIED");
            List<Document> raw = database.getCollection(MongoStorePort.PIPELINE_RATE_HISTORY)
                    .withTimeout(5, TimeUnit.SECONDS).find(new Document("pipelineId", pipeline))
                    .projection(new Document("_id", 1).append("pipelineId", 1).append("pipelineIncarnationId", 1)
                            .append("executionGeneration", 1).append("observedAt", 1).append("counters", 1)
                            .append("countingSince", 1).append("gapFrom", 1))
                    .sort(new Document("observedAt", -1).append("_id", -1)).limit(2).into(new ArrayList<>());
            receipt.put("latestTwoRawSamples", raw.stream().map(FirstResetObservationReceipt::json).toList());
            receipt.put("rawScopeQualification", "RECORDED_NOT_REBASED_OR_DEFAULTED");
        }
        receipt.put("completedAt", Instant.now().toString());
        byte[] bytes = JsonWriter.write(receipt).getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAX_BYTES) { throw new AssertionError("first-reset receipt byte budget exceeded"); }
        Files.createDirectories(directory);
        Files.write(output, bytes, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
    }

    private static Object json(Document document) { return document == null ? null : JsonReader.parse(document.toJson()); }
}
