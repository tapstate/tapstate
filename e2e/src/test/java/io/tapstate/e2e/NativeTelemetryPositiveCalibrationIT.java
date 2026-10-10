package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoObservationStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Requires genuine copy output and passive identity records from one immutable native process. */
@RequiresDocker
class NativeTelemetryPositiveCalibrationIT {
    private static final String PREFIX = "tapstate.e2e.native-telemetry-calibration.";
    private static final Duration WAIT = Duration.ofMinutes(2);
    private static final int MAX_RECORDS = 512;
    private static final int MAX_RESPONSE_BYTES = 2 * 1024 * 1024;

    @Test
    void nativeCopyOffersProduceAndScrapeUnderItsActualAuthority() throws Exception {
        Assumptions.assumeTrue(List.of("jar", "sha256", "output").stream()
                .anyMatch(name -> System.getProperty(PREFIX + name) != null),
                "the native positive telemetry calibration needs named immutable inputs");
        Path jar = Path.of(required("jar")).toAbsolutePath().normalize();
        String sha = required("sha256");
        assertThat(sha).matches("[0-9a-f]{64}");
        assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
        BenchmarkCaptureCalibrationLiveRunIT.requireConnectors();
        Path output = Path.of(required("output"));
        PipelineBenchmarkLiveRunIT.requireSafeOutput(output, PipelineBenchmarkLiveRunIT.harnessRoot());
        int port;
        try (ServerSocket reserved = new ServerSocket(0, 1,
                java.net.InetAddress.getByName("127.0.0.1"))) { port = reserved.getLocalPort(); }
        URI scrape = URI.create("http://127.0.0.1:" + port + "/metrics");
        var workload = BenchmarkWorkloadDefinitions.byId("copy");
        String pipeline = workload.pipelineIds().getFirst();
        Map<String, Map<String, Object>> connectorInputs = new java.util.LinkedHashMap<>();
        for (String connector : List.of("mysql", "postgres", "mongodb")) {
            connectorInputs.put(connector, PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(connector)));
        }
        BenchmarkLiveReport report = new BenchmarkLiveReport(output);
        Map<String, Object> harnessInputs = inputHashes(PipelineBenchmarkLiveRunIT.harnessRoot());
        AtomicReference<NativeTelemetryIdentityJdiSession> owned = new AtomicReference<>();
        var callerResolver = new NativeLogCallerEvidence.Resolver(NativeLogCallerEvidence.Compatibility.ALLOW_LEGACY_RECORD_LOCAL);
        BenchmarkForkEnvironment fork = null;
        Throwable primary = null;
        try {
            report.begin(Map.of("purpose", "NATIVE_POSITIVE_TELEMETRY_CALIBRATION",
                    "application", PipelineBenchmarkLiveRunIT.artifact(jar), "expectedJarSha256", sha,
                    "harness", harnessInputs,
                    "connectors", connectorInputs, "prometheusEndpoint", scrape.toString(),
                    "historySampleInterval", "PT2S", "performanceAcceptanceEligible", false),
                    PipelineBenchmarkLiveRunIT.environment(), List.of());
            fork = BenchmarkForkEnvironment.open(workload, jar, "native-positive-telemetry",
                    (uri, operator, artifact) -> {
                        var observer = NativeTelemetryIdentityJdiSession.start(uri, operator, artifact, sha,
                                pipeline, List.of("--tapstate.metrics.export.prometheus.host=127.0.0.1",
                                        "--tapstate.metrics.export.prometheus.port=" + port,
                                        "--tapstate.metrics.history.sample-interval=PT2S"));
                        owned.set(observer);
                        return new BenchmarkForkEnvironment.OwnedBoot(observer.server(), null);
                    });
            NativeTelemetryIdentityJdiSession observer = owned.get();
            assertThat(observer).isNotNull();
            var snapshot = fork.runPhase(workload.phases().getFirst(), true);
            var warmup = fork.runPhase(workload.phases().get(1), true);
            assertThat(snapshot.targets()).allSatisfy(target -> assertThat(target.matches()).isTrue());
            assertThat(warmup.targets()).allSatisfy(target -> assertThat(target.matches()).isTrue());
            try (var mongo = MongoClients.create(fork.storeUri())) {
                MongoDatabase database = mongo.getDatabase(new ConnectionString(fork.storeUri()).getDatabase());
                var latest = new MongoObservationStore(mongo,
                        database.getCollection(MongoStorePort.PIPELINE_OBSERVATION),
                        database.getCollection(MongoStorePort.PIPELINE_OBSERVATION_CHUNKS));
                var current = Await.answered("actual positive output in a scoped observation", WAIT,
                        () -> latest.readStored(pipeline).filter(value -> value.scope().isPresent()
                                && value.observation().state() == PipelineState.RUNNING
                                && value.observation().facts().stream()
                                .filter(fact -> fact.name().equals("tapstate.pipeline.records"))
                                .flatMap(fact -> fact.points().stream()).anyMatch(point ->
                                        "out".equals(point.attributes().get("direction"))
                                                && point.value() != null && point.value() > 0)));
                var scope = current.scope().orElseThrow();
                Document artifact = database.getCollection(MongoStorePort.ARTIFACTS)
                        .find(new Document("_id", pipeline)).first();
                List<Document> claims = database.getCollection(MongoStorePort.WORKLOAD_CLAIMS)
                        .find(new Document("resourceType", "PIPELINE_ACTUATION").append("resourceId", pipeline))
                        .limit(2).into(new ArrayList<>());
                assertThat(artifact).isNotNull();
                assertThat(claims).hasSize(1);
                Document claim = claims.getFirst();
                assertThat(artifact.getString("pipelineIncarnationId")).isEqualTo(scope.pipelineIncarnationId());
                assertThat(claim.get("executionGeneration") instanceof Long
                        || claim.get("executionGeneration") instanceof Integer).isTrue();
                assertThat(((Number) claim.get("executionGeneration")).longValue())
                        .isPositive().isEqualTo(scope.executionGeneration());
                var receipt = new NativeTelemetryIdentityJdiSession.AuthorityReceipt(pipeline,
                        claim.getString("clusterId"), artifact.getString("pipelineIncarnationId"),
                        ((Number) claim.get("executionGeneration")).longValue(),
                        JsonWriter.write(claim.get("_id")), Instant.now().toString());
                observer.recordAuthority(receipt);
                List<Map<String, Object>> observed = new ArrayList<>();
                HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(20)).build();
                long deadline = System.nanoTime() + WAIT.toNanos();
                String body = "";
                NativeTelemetryIdentityJdiSession.Boundary boundary = null;
                while (System.nanoTime() - deadline < 0) {
                    observer.check();
                    body = scrape(http, scrape, deadline);
                    boundary = observer.boundary("positive-copy");
                    report.addFork(boundary.evidence());
                    assertThat(boundary.unverified()).as("missing decoder/binding is not a calibrated zero").isEmpty();
                    callerResolver.register(boundary.callerSymbols());
                    observed.addAll(boundary.records());
                    callerResolver.evidence(observed);
                    assertThat(observed.size()).as("positive calibration retains a bounded record set")
                            .isLessThanOrEqualTo(MAX_RECORDS);
                    if (has(observed, "JOB", receipt) && has(observed, "LOG", receipt)
                            && flag(observed, "OFFER", "accepted", receipt)
                            && flag(observed, "VISIBLE", "included", receipt)
                            && matchingProduced(observed, body, receipt)
                            && positiveScrape(body, pipeline)
                            && boundary.decodedAndAuthorityBound()) { break; }
                    java.util.concurrent.TimeUnit.MILLISECONDS.sleep(250);
                }
                assertThat(boundary).isNotNull();
                assertThat(has(observed, "JOB", receipt)).as("the actual native Job was observed").isTrue();
                assertThat(observer.capturedJob(receipt)).as("the captured Job matches the real authority").isPresent();
                assertThat(has(observed, "LOG", receipt))
                        .as("UNVERIFIED: a quiet native copy supplies no positive scoped log").isTrue();
                assertThat(flag(observed, "OFFER", "accepted", receipt)).isTrue();
                assertThat(flag(observed, "VISIBLE", "included", receipt)).isTrue();
                assertThat(observed.stream().anyMatch(record -> "PRODUCE".equals(record.get("target"))
                        && positiveProduced(record))).isTrue();
                assertThat(matchingProduced(observed, body, receipt))
                        .as("the same real included point is visible in the fresh scrape").isTrue();
                assertThat(positiveScrape(body, pipeline)).isTrue();
                assertThat(boundary.decodedAndAuthorityBound())
                        .as("the positive evidence is complete only after its native invocations drain").isTrue();
                var logRead = new java.util.LinkedHashMap<>(assertScopedLogRead(observed, http, fork.control(), observer.server().baseUrl(), receipt));
                logRead.put("nativeLogEvidence", callerResolver.evidence(observed.stream()
                        .filter(record -> "LOG".equals(record.get("target"))).toList()));
                report.addFork(logRead);
                report.addFork(Map.of("action", "positive-visible-calibration", "pipelineId", pipeline,
                        "incarnation", receipt.incarnation(), "generation", receipt.generation(),
                        "coordinationId", receipt.coordinationId(), "ownedPid", observer.server().pid(),
                        "prometheusEndpoint", scrape.toString(), "positiveScrape", true,
                        "scopedLogObserved", true, "retainedRecordCount", observed.size()));
                report.addFork(Map.of("action", "actual-fresh-prometheus-response", "body", body,
                        "bodySha256", digest(body.getBytes(StandardCharsets.UTF_8))));
            }
            fork.control().stop(pipeline, false);
            var terminal = observer.shutdownAndFinish();
            report.addFork(terminal.evidence());
            assertThat(terminal.unverified()).isEmpty();
            assertThat(terminal.decodedAndAuthorityBound()).isTrue();
            assertThat(terminal.invocationDrainComplete()).isTrue();
            assertThat(terminal.ownedVmDeath()).isTrue();
            assertThat(terminal.ownedVmDisconnected()).isTrue();
            assertThat(PipelineBenchmarkLiveRunIT.sha256(jar)).isEqualTo(sha);
            for (var input : connectorInputs.entrySet()) {
                assertThat(PipelineBenchmarkLiveRunIT.artifact(ConnectorJars.pathFor(input.getKey())))
                        .isEqualTo(input.getValue());
            }
            assertThat(inputHashes(PipelineBenchmarkLiveRunIT.harnessRoot())).isEqualTo(harnessInputs);
            report.completeDiagnostic(Map.of("correctness", "POSITIVE_NATIVE_COPY_TELEMETRY_CALIBRATED",
                    "performanceAcceptanceEligible", false, "unverified", List.of(
                            "POSITIVE_FAILURE_ACCOUNT", "POSITIVE_FOLDER_EVICTION", "RESET_RECREATE",
                            "CLUSTER_EMITTING_MEMBER", "ALL_TELEMETRY_SURFACE_IDENTITIES")));
        } catch (Exception | Error failure) {
            primary = failure;
            if (owned.get() != null) { recordFailureEvidence(report, owned.get(), "owned-native-copy", failure); }
            try { report.fail(failure); } catch (RuntimeException reporting) { failure.addSuppressed(reporting); }
            throw failure;
        } finally {
            Throwable cleanup = null;
            if (owned.get() != null) {
                try { owned.get().close(); } catch (Exception | Error failure) { cleanup = failure; }
            }
            if (fork != null) {
                try { fork.close(); } catch (Exception | Error failure) {
                    if (cleanup == null) { cleanup = failure; } else if (cleanup != failure) { cleanup.addSuppressed(failure); }
                }
            }
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

    static void recordFailureEvidence(BenchmarkLiveReport report, NativeTelemetryIdentityJdiSession observer,
            String node, Throwable primary) {
        try {
            report.addFork(Map.of("action", "incomplete-native-evidence-at-failure", "observedNodeId", node,
                    "evidence", observer.failureEvidence(), "performanceAcceptanceEligible", false));
        } catch (Exception | Error recording) {
            if (recording != primary) { primary.addSuppressed(recording); }
        }
    }

    static boolean has(List<Map<String, Object>> records, String target,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt receipt) {
        return records.stream().anyMatch(record -> target.equals(record.get("target"))
                && receipt.scope().equals(record.get("scope")) && Boolean.TRUE.equals(record.get("normalReturn"))
                && !record.containsKey("decoderStatus"));
    }
    static boolean flag(List<Map<String, Object>> records, String target, String field,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt receipt) {
        return records.stream().anyMatch(record -> target.equals(record.get("target"))
                && receipt.scope().equals(record.get("scope")) && Boolean.TRUE.equals(record.get("normalReturn"))
                && Boolean.TRUE.equals(record.get(field)) && !record.containsKey("decoderStatus"));
    }
    static boolean positiveProduced(Map<String, Object> record) {
        if (record.containsKey("decoderStatus") || !(record.get("metrics") instanceof List<?> metrics)
                || !(record.get("includedFrames") instanceof List<?> included) || included.isEmpty()) { return false; }
        return metrics.stream().filter(Map.class::isInstance).map(Map.class::cast).anyMatch(metric ->
                "tapstate.pipeline.records".equals(metric.get("name")) && "LONG_SUM".equals(metric.get("type"))
                        && metric.get("points") instanceof List<?> points && points.stream()
                        .filter(Map.class::isInstance).map(Map.class::cast).anyMatch(point ->
                                point.get("value") instanceof Number count && count.longValue() > 0
                                        && point.get("attributes") instanceof Map<?, ?> attributes
                                        && "out".equals(attributes.get("direction"))));
    }
    static boolean positiveScrape(String body, String pipeline) {
        return body.lines().filter(line -> line.startsWith("tapstate_pipeline_records_total{"))
                .filter(line -> line.contains("tapstate_pipeline_id=\"" + pipeline + "\"")
                        && line.contains("direction=\"out\"")).anyMatch(line -> {
                            int close = line.indexOf('}');
                            if (close < 0) { return false; }
                            String value = line.substring(close + 1).trim().split("\\s+")[0];
                            return new java.math.BigDecimal(value).signum() > 0;
                        });
    }
    static boolean matchingProduced(List<Map<String, Object>> records, String body,
            NativeTelemetryIdentityJdiSession.AuthorityReceipt receipt) {
        for (Map<String, Object> record : records) {
            if (!"PRODUCE".equals(record.get("target")) || !positiveProduced(record)
                    || !(record.get("includedFrames") instanceof List<?> included)
                    || included.stream().filter(Map.class::isInstance).map(Map.class::cast)
                    .noneMatch(frame -> acceptedVisibleFrame(frame, record, records, receipt))) { continue; }
            for (Object value : (List<?>) record.get("metrics")) {
                Map<?, ?> metric = (Map<?, ?>) value;
                if (!"tapstate.pipeline.records".equals(metric.get("name"))
                        || !"LONG_SUM".equals(metric.get("type"))) { continue; }
                for (Object rawPoint : (List<?>) metric.get("points")) {
                    Map<?, ?> point = (Map<?, ?>) rawPoint;
                    Map<?, ?> attributes = (Map<?, ?>) point.get("attributes");
                    if (!receipt.pipelineId().equals(attributes.get("tapstate.pipeline.id"))
                            || !"out".equals(attributes.get("direction"))
                            || !(point.get("value") instanceof Number number) || number.longValue() <= 0) { continue; }
                    if (included.stream().filter(Map.class::isInstance).map(Map.class::cast)
                            .noneMatch(frame -> acceptedVisibleFrame(frame, record, records, receipt)
                                    && sameFactPoint(frame, metric, point))) { continue; }
                    for (String line : body.lines().filter(row ->
                            row.startsWith("tapstate_pipeline_records_total{")).toList()) {
                        if (attributes.entrySet().stream().anyMatch(attribute -> !hasLabel(line,
                                attribute.getKey().toString().replace('.', '_'), attribute.getValue().toString()))) { continue; }
                        int close = line.indexOf('}');
                        if (close >= 0 && new java.math.BigDecimal(
                                line.substring(close + 1).trim().split("\\s+")[0])
                                .compareTo(java.math.BigDecimal.valueOf(number.longValue())) == 0) { return true; }
                    }
                }
            }
        }
        return false;
    }
    private static boolean acceptedVisibleFrame(Map<?, ?> frame, Map<String, Object> produced,
            List<Map<String, Object>> records, NativeTelemetryIdentityJdiSession.AuthorityReceipt receipt) {
        if (!receipt.scope().equals(frame.get("scope"))) { return false; }
        return List.of("OFFER", "VISIBLE").stream().allMatch(target -> records.stream().anyMatch(record ->
                target.equals(record.get("target")) && !record.containsKey("decoderStatus")
                        && produced.get("receiver").equals(record.get("receiver"))
                        && Boolean.TRUE.equals(record.get(target.equals("OFFER") ? "accepted" : "included"))
                        && List.of("scope", "state", "observedAt", "facts").stream().allMatch(key ->
                                java.util.Objects.equals(frame.get(key), record.get(key)))));
    }
    private static boolean sameFactPoint(Map<?, ?> frame, Map<?, ?> metric, Map<?, ?> produced) {
        if (!(frame.get("facts") instanceof List<?> facts)) { return false; }
        for (Object rawFact : facts) {
            Map<?, ?> fact = (Map<?, ?>) rawFact;
            if (!metric.get("name").equals(fact.get("name")) || !metric.get("unit").equals(fact.get("unit"))
                    || !"COUNTER".equals(fact.get("type"))) { continue; }
            for (Object rawPoint : (List<?>) fact.get("points")) {
                Map<?, ?> point = (Map<?, ?>) rawPoint;
                if (point.get("attributes").equals(produced.get("attributes"))
                        && point.get("value") instanceof Number value
                        && produced.get("value") instanceof Number actual && value.longValue() == actual.longValue()
                        && point.get("startTime") instanceof String started
                        && point.get("observedAt") instanceof String observed
                        && produced.get("startEpochNanos") instanceof Number actualStart
                        && produced.get("epochNanos") instanceof Number actualTime
                        && nanos(started) == actualStart.longValue() && nanos(observed) == actualTime.longValue()) { return true; }
            }
        }
        return false;
    }
    private static long nanos(String text) {
        Instant instant = Instant.parse(text);
        return Math.addExact(Math.multiplyExact(instant.getEpochSecond(), 1_000_000_000L), instant.getNano());
    }
    private static boolean hasLabel(String line, String name, String value) {
        String escaped = value.replace("\\", "\\\\").replace("\n", "\\n").replace("\"", "\\\"");
        String label = name + "=\"" + escaped + "\"";
        return line.startsWith("tapstate_pipeline_records_total{" + label) || line.contains("," + label);
    }
    static Map<String, Object> assertScopedLogRead(List<Map<String, Object>> observed, HttpClient http,
            ControlPlane control, URI base, NativeTelemetryIdentityJdiSession.AuthorityReceipt receipt) throws Exception {
        var response = http.send(HttpRequest.newBuilder(base.resolve(
                "/api/pipelines/" + receipt.pipelineId() + "/logs?scope=current")).timeout(Duration.ofSeconds(20))
                .header("Authorization", "Bearer " + control.credential()).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body().getBytes(StandardCharsets.UTF_8).length).isLessThanOrEqualTo(MAX_RESPONSE_BYTES);
        Object parsed = JsonReader.parse(response.body());
        assertThat(parsed).isInstanceOf(Map.class);
        Object rawLines = ((Map<?, ?>) parsed).get("lines");
        assertThat(rawLines).isInstanceOf(List.class);
        assertThat(((List<?>) rawLines).stream().filter(Map.class::isInstance).map(Map.class::cast)
                .anyMatch(line -> observed.stream().anyMatch(record ->
                        "LOG".equals(record.get("target")) && receipt.scope().equals(record.get("scope"))
                                && !record.containsKey("decoderStatus")
                                && Boolean.TRUE.equals(record.get("normalReturn"))
                                && line.get("timestampMillis") instanceof Number time
                                && record.get("timestampMillis") instanceof Number captured
                                && time.longValue() == captured.longValue()
                                && line.get("level").equals(record.get("level"))
                                && line.get("message").equals(record.get("message")))))
                .as("the actual emitting node serves the captured scoped line").isTrue();
        return Map.of("action", "actual-current-log-response", "body", response.body(),
                "bodySha256", digest(response.body().getBytes(StandardCharsets.UTF_8)),
                "incarnation", receipt.incarnation(), "generation", receipt.generation());
    }
    static String scrape(HttpClient http, URI endpoint, long deadline) throws Exception {
        long left = deadline - System.nanoTime();
        assertThat(left).as("the calibration uses one original wait budget").isPositive();
        Duration timeout = Duration.ofNanos(Math.min(left, Duration.ofSeconds(20).toNanos()));
        var response = http.send(HttpRequest.newBuilder(endpoint).timeout(timeout).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        String body = response.body();
        assertThat(body.getBytes(StandardCharsets.UTF_8).length)
                .as("the actual scrape stays within the evidence response budget").isLessThanOrEqualTo(MAX_RESPONSE_BYTES);
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(body).doesNotContain("pipelineIncarnationId", "executionGeneration",
                "pipeline_incarnation_id", "pipeline_execution_generation");
        return body;
    }
    private static Map<String, Object> inputHashes(Path root) throws Exception {
        Map<String, Object> inputs = new java.util.LinkedHashMap<>();
        for (Class<?> type : List.of(NativeTelemetryPositiveCalibrationIT.class,
                NativeTelemetryIdentityJdiSession.class, NativeTelemetryMirror.class)) {
            inputs.put(type.getSimpleName() + "Source", PipelineBenchmarkLiveRunIT.artifact(root.resolve(
                    "e2e/src/test/java/io/tapstate/e2e/" + type.getSimpleName() + ".java")));
            try (InputStream stream = type.getResourceAsStream(type.getSimpleName() + ".class")) {
                assertThat(stream).as("the actually executing class bytes are available").isNotNull();
                byte[] bytes = stream.readNBytes(MAX_RESPONSE_BYTES + 1);
                assertThat(bytes.length).as("the executing class stays within its evidence byte budget")
                        .isLessThanOrEqualTo(MAX_RESPONSE_BYTES);
                inputs.put(type.getSimpleName() + "ExecutingClassSha256", digest(bytes));
            }
        }
        inputs.putAll(callerEvidenceHashes(root, MAX_RESPONSE_BYTES));
        return Map.copyOf(inputs);
    }

    static Map<String, Object> callerEvidenceHashes(Path root, int maxClassBytes) throws Exception {
        Map<String, Object> inputs = new java.util.LinkedHashMap<>();
        inputs.put("NativeLogCallerEvidenceSource", PipelineBenchmarkLiveRunIT.artifact(root.resolve(
                "e2e/src/test/java/io/tapstate/e2e/NativeLogCallerEvidence.java")));
        List<Class<?>> types = new ArrayList<>(List.of(NativeLogCallerEvidence.class));
        for (int at = 0; at < types.size(); at++) {
            Class<?> type = types.get(at);
            types.addAll(List.of(type.getDeclaredClasses()));
            String name = type.getName().substring(type.getPackageName().length() + 1);
            try (InputStream stream = type.getResourceAsStream(name + ".class")) {
                assertThat(stream).as("the executing caller evidence class bytes are available").isNotNull();
                byte[] bytes = stream.readNBytes(maxClassBytes + 1);
                assertThat(bytes.length).isLessThanOrEqualTo(maxClassBytes);
                inputs.put(name + "ExecutingClassSha256", digest(bytes));
            }
        }
        return Map.copyOf(inputs);
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
