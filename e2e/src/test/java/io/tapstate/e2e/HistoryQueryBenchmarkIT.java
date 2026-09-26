package io.tapstate.e2e;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.MongoRateHistoryStore;
import io.tapstate.adapters.mongostore.MongoStorePort;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Repeatable real-process HTTP and Mongo scan fixture for history query comparisons.
 *
 * <p>Run the same compiled test with {@code -Dtapstate.e2e.history-benchmark.jar=/path/to/app-boot.jar}
 * to launch another build, and supply the same {@code -Dtapstate.e2e.history-benchmark.anchor=...}
 * to keep the seeded timestamps and request windows identical between runs.
 */
class HistoryQueryBenchmarkIT {

    private static final String DATABASE = "history_query_benchmark";
    private static final String PIPELINE = "history_benchmark";
    private static final String BOOT_JAR_PROPERTY = "tapstate.e2e.history-benchmark.jar";
    private static final String ANCHOR_PROPERTY = "tapstate.e2e.history-benchmark.anchor";
    private static final int HOT_READS = 5;
    private static final HttpClient HTTP = HttpClient.newHttpClient();

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: history_source
            connector: mongodb
            config: { uri: "mongodb://127.0.0.1:27017/unused" }
            mode: cdc
            tables: [ orders ]
            """;
    private static final String TARGET = """
            version: tapstate/v1
            kind: source
            id: history_target
            connector: mongodb
            config: { uri: "mongodb://127.0.0.1:27017/unused" }
            """;
    private static final String PIPELINE_DSL = """
            version: tapstate/v1
            kind: pipeline
            id: history_benchmark
            source: history_source
            serve:
              from: /.*/
              sync:
                - id: sink
                  source: history_target
                  write_mode: upsert
                  ddl: apply
            """;

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void theSameFifteenDayFixtureMeasuresOneHourOneDayAndFifteenDays() throws Exception {
        String storeUri = SharedMongo.replicaSetUrl(DATABASE);
        try (MongoClient mongo = MongoClients.create(storeUri)) {
            MongoDatabase database = mongo.getDatabase(DATABASE);
            database.drop();
            String configuredJar = System.getProperty(BOOT_JAR_PROPERTY);
            Path jar = configuredJar == null || configuredJar.isBlank() ? null : Path.of(configuredJar);
            try (RealProcessServer server = jar == null
                    ? RealProcessServer.start(storeUri) : RealProcessServer.start(storeUri, jar)) {
                ControlPlane control = new ControlPlane(server.baseUrl());
                control.bootstrapAndLogin("history-benchmark", "history-benchmark-password");
                control.apply(Map.of("source.tap.yml", SOURCE, "target.tap.yml", TARGET,
                        "pipeline.tap.yml", PIPELINE_DSL));

                // Keep the upper edge off the minute boundary and three minutes behind wall time. The
                // latter leaves room for the live server's retention cutoff to move while tests run.
                String configuredAnchor = System.getProperty(ANCHOR_PROPERTY);
                Instant to = configuredAnchor == null || configuredAnchor.isBlank()
                        ? Instant.now().truncatedTo(ChronoUnit.MINUTES)
                                .minus(Duration.ofMinutes(3)).plusSeconds(17)
                        : Instant.parse(configuredAnchor);
                Instant missingBucket = Instant.ofEpochSecond(
                        Math.floorDiv(to.minus(Duration.ofMinutes(30)).getEpochSecond(), 1_800) * 1_800);
                Document artifact = database.getCollection(MongoStorePort.ARTIFACTS)
                        .find(new Document("_id", PIPELINE)).first();
                assertThat(artifact).as("the applied pipeline artifact").isNotNull();
                String incarnation = artifact.getString("pipelineIncarnationId");
                int samples = seed(database, to, missingBucket, incarnation);
                System.out.printf("history-query-fixture jar=%s os=%s/%s java=%s mongo=7.0"
                                + " anchor=%s missingBucket=%s seededSamples=%d historyScope=%s"
                                + " concurrency=1 hotReads=%d%n",
                        jar == null ? "reactor" : jar,
                        System.getProperty("os.name"), System.getProperty("os.arch"),
                        System.getProperty("java.version"), to, missingBucket, samples,
                        incarnation == null ? "legacy" : "incarnation", HOT_READS);

                for (Window window : List.of(
                        new Window("1h", Duration.ofHours(1), "raw", "PT1M"),
                        new Window("1d", Duration.ofDays(1), "PT30M", "PT30M"),
                        new Window("15d", Duration.ofDays(15), "PT6H", "PT6H"))) {
                    URI uri = query(server.baseUrl(), to.minus(window.span()), to, window.resolution());
                    Reading cold = profiledGet(database, uri, control.credential());
                    assertResponse(cold, window, missingBucket);
                    List<Reading> hot = new ArrayList<>();
                    for (int i = 0; i < HOT_READS; i++) {
                        Reading reading = profiledGet(database, uri, control.credential());
                        assertResponse(reading, window, missingBucket);
                        hot.add(reading);
                    }
                    report(window, cold, hot);
                }
            }
        }
    }

    private static int seed(MongoDatabase database, Instant to, Instant missingBucket, String incarnation) {
        MongoCollection<Document> history = database.getCollection(MongoStorePort.PIPELINE_RATE_HISTORY);
        Instant first = to.truncatedTo(ChronoUnit.MINUTES).minus(Duration.ofDays(15))
                .plus(Duration.ofMinutes(10));
        Instant last = to.truncatedTo(ChronoUnit.MINUTES);
        List<Document> samples = new ArrayList<>((int) Duration.between(first, last).toMinutes() + 1);
        for (Instant at = first; !at.isAfter(last); at = at.plus(Duration.ofMinutes(1))) {
            if (!at.isBefore(missingBucket) && at.isBefore(missingBucket.plus(Duration.ofMinutes(30)))) {
                continue;
            }
            long minutes = Duration.between(first, at).toMinutes();
            Document sample = MongoRateHistoryStore.toDocument(new RateSample(PIPELINE, at,
                    Map.of("records.out", minutes * 60, "bytes.out", minutes * 600),
                    Map.of("orders", minutes % 11), first));
            // The fixture keeps the same logical samples on both builds. New runs carry their current
            // internal owner; a pre-identity reference build reads the same samples as legacy history.
            if (incarnation != null) {
                sample.append("pipelineIncarnationId", incarnation).append("executionGeneration", 1L);
            }
            samples.add(sample);
        }
        history.insertMany(samples);
        assertThat(history.countDocuments()).isEqualTo(samples.size());
        return samples.size();
    }

    private static URI query(URI base, Instant from, Instant to, String resolution) {
        String path = "/api/pipelines/" + PIPELINE + "/metrics/history?from=" + encoded(from)
                + "&to=" + encoded(to) + "&resolution=" + resolution + "&limit=1000&table=orders";
        return base.resolve(path);
    }

    private static String encoded(Instant time) {
        return URLEncoder.encode(time.toString(), StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unchecked")
    private static Reading profiledGet(MongoDatabase database, URI uri, String credential) throws Exception {
        database.runCommand(new Document("profile", 0));
        database.getCollection("system.profile").drop();
        database.runCommand(new Document("profile", 2).append("slowms", 0));
        HttpRequest request = HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(40))
                .header("Authorization", "Bearer " + credential).GET().build();
        long start = System.nanoTime();
        HttpResponse<byte[]> answer;
        long elapsedNanos;
        try {
            answer = HTTP.send(request, HttpResponse.BodyHandlers.ofByteArray());
            elapsedNanos = System.nanoTime() - start;
        } finally {
            database.runCommand(new Document("profile", 0));
        }
        assertThat(answer.statusCode()).as("history HTTP status and body: %s",
                new String(answer.body(), StandardCharsets.UTF_8)).isEqualTo(200);
        Object parsed = JsonReader.parse(new String(answer.body(), StandardCharsets.UTF_8));
        assertThat(parsed).isInstanceOf(Map.class);
        List<Document> operations = database.getCollection("system.profile")
                .find(new Document("ns", DATABASE + "." + MongoStorePort.PIPELINE_RATE_HISTORY))
                .into(new ArrayList<>());
        assertThat(operations).as("Mongo profiler entries for the history collection").isNotEmpty();
        long keys = operations.stream().mapToLong(operation -> count(operation, "keysExamined")).sum();
        long documents = operations.stream().mapToLong(operation -> count(operation, "docsExamined")).sum();
        assertThat(keys).as("indexed history query").isPositive();
        assertThat(documents).as("history documents examined").isPositive().isLessThanOrEqualTo(25_000);
        return new Reading((Map<String, Object>) parsed, elapsedNanos, answer.body().length,
                operations.size(), keys, documents);
    }

    private static long count(Document document, String field) {
        Object value = document.get(field);
        return value instanceof Number number ? number.longValue() : 0;
    }

    @SuppressWarnings("unchecked")
    private static void assertResponse(Reading reading, Window window, Instant missingBucket) {
        Map<String, Object> response = reading.response();
        assertThat(response).containsEntry("pipelineId", PIPELINE)
                .containsEntry("effectiveResolution", window.effectiveResolution())
                .containsEntry("status", "OK")
                .containsEntry("consistency", "EVENTUAL")
                .containsEntry("nextCursor", null);
        List<Map<String, Object>> segments = (List<Map<String, Object>>) response.get("segments");
        assertThat(segments).isNotEmpty();
        List<Map<String, Object>> gaps = (List<Map<String, Object>>) response.get("gaps");
        assertThat(gaps).as("the 30-minute missing bucket must remain visible").anySatisfy(gap -> {
            assertThat(gap).containsEntry("reason", "SAMPLE_GAP");
            assertThat(Instant.parse(String.valueOf(gap.get("intervalStart"))))
                    .isBefore(missingBucket.plusSeconds(60));
            assertThat(Instant.parse(String.valueOf(gap.get("intervalEnd")))).isAfter(missingBucket);
        });
    }

    private static void report(Window window, Reading cold, List<Reading> hot) {
        List<Long> latencies = hot.stream().map(Reading::elapsedNanos).sorted().toList();
        System.out.printf("history-query-window=%s resolution=%s coldMs=%.3f hotP50Ms=%.3f hotP95Ms=%.3f"
                        + " coldCommands=%d coldKeys=%d coldDocs=%d coldResponseBytes=%d"
                        + " hotCommands=%d hotKeys=%d hotDocs=%d hotResponseBytes=%d%n",
                window.name(), window.effectiveResolution(), millis(cold.elapsedNanos()),
                millis(latencies.get(latencies.size() / 2)), millis(latencies.get(latencies.size() - 1)),
                cold.commands(), cold.keysExamined(), cold.docsExamined(), cold.responseBytes(),
                median(hot.stream().map(Reading::commands).toList()),
                median(hot.stream().map(Reading::keysExamined).toList()),
                median(hot.stream().map(Reading::docsExamined).toList()),
                median(hot.stream().map(Reading::responseBytes).toList()));
    }

    private static double millis(long nanos) {
        return nanos / 1_000_000.0;
    }

    private static long median(List<? extends Number> values) {
        return values.stream().map(Number::longValue).sorted(Comparator.naturalOrder())
                .toList().get(values.size() / 2);
    }

    private record Window(String name, Duration span, String resolution, String effectiveResolution) {}

    private record Reading(Map<String, Object> response, long elapsedNanos, int responseBytes,
            int commands, long keysExamined, long docsExamined) {}
}
