package io.tapstate.e2e;

import com.hazelcast.core.Hazelcast;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.LifecycleVerb;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.DockerGate;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class FirstSnapshotBatchAppearsInHistoryIT {
    private static final String PIPELINE = "startup_history";
    private static final String TABLE = "orders";
    private static final long ROWS = 4;
    private static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(10);
    private static final Duration WAIT = Duration.ofSeconds(90);

    @Test
    void theFirstConfirmedSnapshotBatchAppearsWhenTheFirstPositiveSampleIsStopped(@TempDir Path directory) throws Exception {
        DockerGate.require();
        Path source = Files.createDirectory(directory.resolve("source"));
        Path target = Files.createDirectory(directory.resolve("target"));
        Path hold = Files.createDirectory(directory.resolve("hold")).resolve("read-" + TABLE);
        Files.createFile(hold);
        var existingMembers = Hazelcast.getAllHazelcastInstances();
        // Two intervals cover acknowledgement, Stop and the final write without crossing a history gap.
        var interval = Duration.ofMinutes(2);

        try (var endpoints = new FileEndpoints(); var storage = PreparedNativeStore.create("e2e", "e2e-password");
                var mongo = MongoClients.create(storage.cacheUri());
                var server = InProcessServer.startPrepared(storage, List.of(
                        "--tapstate.metrics.history.sample-interval=" + interval.toSeconds() + "s"));
                var client = HttpClient.newHttpClient()) {
            endpoints.seed(EndpointAddress.uri(source.toString()), TABLE, SeedRows.generated(ROWS));
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            configure(control, directory, source, target, hold);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            var history = SystemCollections.CACHE_PIPELINE_RATE_HISTORY.on(mongo.getDatabase(storage.cacheDatabase()));
            var filter = new Document("pipelineId", PIPELINE);
            var zeroFilter = new Document(filter).append("startupEvidence.zeroAt", new Document("$exists", true))
                    .append("countingSince", new Document("$exists", false));
            // A pre-execution observation can consume the first sampling slot.
            Await.answered("the retained zero before the held snapshot", interval.plus(WAIT),
                    () -> Optional.ofNullable(history.find(zeroFilter).first()));
            assertThat(server.stopPublishingObservations()).isEqualTo(1);
            server.awaitObservationWorkersIdle();
            var previous = history.find(filter).sort(new Document("observedAt", -1)).first();
            assertThat(previous).isNotNull();
            var counters = previous.get("counters", Document.class);
            assertThat(counters.getLong("records.out")).isZero();
            assertThat(counters.getLong("bytes.out")).isZero();
            assertThat(previous.get("startupEvidence", Document.class)).isNotNull();

            Files.delete(hold);
            var member = Hazelcast.getAllHazelcastInstances().stream()
                    .filter(candidate -> !existingMembers.contains(candidate) && candidate.getJet().getJob(PIPELINE) != null)
                    .findFirst().orElseThrow();
            var engine = new Engine(member);
            var confirmed = Await.answered("the native target acknowledgement without a new history sample", WAIT,
                    () -> engine.captureRun(PIPELINE).flatMap(Engine.Run::metrics).map(Engine.RunMetrics::delivered)
                            .filter(delivered -> delivered.rowsByTableAndOp().values().stream()
                                    .flatMap(table -> table.values().stream()).mapToLong(Long::longValue).sum() == ROWS)
                            .filter(delivered -> delivered.bytesByTable().values().stream().anyMatch(bytes -> bytes > 0)));
            assertThat(endpoints.count(EndpointAddress.uri(target.toString()), TABLE)).isEqualTo(ROWS);
            var positive = new Document(filter).append("countingSince", new Document("$exists", true));
            assertThat(history.countDocuments(positive)).isZero();

            control.stop(PIPELINE, false);
            Await.until("STOPPED is published", WAIT, () -> {
                assertThat(server.publishObservationsOnce()).isEqualTo(1);
                return control.state(PIPELINE).filter(PipelineState.STOPPED::equals).isPresent();
            }, () -> control.logs(PIPELINE));
            server.awaitObservationWorkersIdle();
            var stopped = history.find(positive).first();
            assertThat(stopped).as("the STOPPED history sample holds the first confirmed batch").isNotNull();
            assertThat(stopped.get("startupEvidence", Document.class)).isEqualTo(previous.get("startupEvidence", Document.class));
            var from = previous.getDate("observedAt").toInstant();
            var end = stopped.getDate("observedAt").toInstant();
            assertThat(Duration.between(from, end)).isLessThan(interval.multipliedBy(2));
            long bytes = confirmed.bytesByTable().values().stream().mapToLong(Long::longValue).sum();
            for (String resolution : List.of("raw", "PT5M")) {
                var response = history(client, server.baseUrl(), control.credential(), from, end.plusMillis(1), resolution);
                assertThat((List<?>) response.get("gaps")).isEmpty();
                assertThat(points(response)).noneMatch(point -> point.containsKey("cdcDelivery"));
                assertRates(points(response), resolution, "recordsOut", BigDecimal.valueOf(ROWS));
                assertRates(points(response), resolution, "bytesOut", BigDecimal.valueOf(bytes));
            }

            var previousId = new Document("_id", previous.get("_id"));
            var otherExecution = new Document(previous.get("startupEvidence", Document.class));
            otherExecution.append("executionId", otherExecution.getLong("executionId") ^ 1L);
            for (var update : List.of(new Document("$unset", new Document("startupEvidence", 1)),
                    new Document("$set", new Document("startupEvidence", otherExecution)))) {
                assertThat(history.updateOne(previousId, update).getModifiedCount()).isEqualTo(1);
                for (String resolution : List.of("raw", "PT5M")) {
                    var response = history(client, server.baseUrl(), control.credential(), from, end.plusMillis(1), resolution);
                    assertThat((List<?>) response.get("gaps")).isEmpty();
                    // RAW can also return the measured idle interval that ends at the selected baseline.
                    var startupPoints = points(response).stream()
                            .filter(point -> Instant.parse(point.get("intervalEnd").toString()).isAfter(from)).toList();
                    assertThat(startupPoints).isNotEmpty().allSatisfy(point -> {
                        assertThat(point.get("recordsOut")).as("%s records rate without matching proof", resolution).isNull();
                        assertThat(point.get("bytesOut")).as("%s bytes rate without matching proof", resolution).isNull();
                    });
                }
            }
        }
    }

    @Test
    void theFirstConfirmedSnapshotBatchIsMeasuredAtBothResolutions(@TempDir Path directory) throws Exception {
        DockerGate.require();
        Path source = Files.createDirectory(directory.resolve("source"));
        Path target = Files.createDirectory(directory.resolve("target"));
        Path hold = Files.createDirectory(directory.resolve("hold")).resolve("read-" + TABLE);
        Files.createFile(hold);
        var endpoints = new FileEndpoints();
        endpoints.seed(EndpointAddress.uri(source.toString()), TABLE, SeedRows.generated(ROWS));

        try (var storage = PreparedNativeStore.create("e2e", "e2e-password");
                var mongo = MongoClients.create(storage.cacheUri());
                var server = InProcessServer.startPrepared(storage, List.of(
                        "--tapstate.metrics.history.sample-interval=" + SAMPLE_INTERVAL.toSeconds() + "s"))) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            configure(control, directory, source, target, hold);
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            var history = SystemCollections.CACHE_PIPELINE_RATE_HISTORY.on(mongo.getDatabase(storage.cacheDatabase()));
            var filter = new Document("pipelineId", PIPELINE);
            var baseline = Await.answered("two retained zero observations while the snapshot is held", WAIT, () -> {
                var samples = history.find(new Document(filter).append("startupEvidence.zeroAt", new Document("$exists", true))
                        .append("countingSince", new Document("$exists", false)))
                        .sort(new Document("observedAt", 1)).limit(2).into(new ArrayList<>());
                return samples.size() == 2 && samples.stream().allMatch(sample -> {
                    var counters = sample.get("counters", Document.class);
                    return Long.valueOf(0).equals(counters.getLong("records.out"))
                            && Long.valueOf(0).equals(counters.getLong("bytes.out"));
                }) ? Optional.of(samples) : Optional.empty();
            });
            assertThat(endpoints.count(EndpointAddress.uri(target.toString()), TABLE)).isZero();
            Instant idleFrom = baseline.getFirst().getDate("observedAt").toInstant();
            Instant idleTo = baseline.getLast().getDate("observedAt").toInstant().plusMillis(1);
            try (var client = HttpClient.newHttpClient()) {
                for (String resolution : List.of("raw", "PT5M")) {
                    var idle = history(client, server.baseUrl(), control.credential(), idleFrom, idleTo, resolution);
                    assertThat((List<?>) idle.get("gaps")).isEmpty();
                    var idlePoints = points(idle);
                    assertThat(idlePoints).noneMatch(point -> point.containsKey("cdcDelivery"));
                    for (String key : List.of("recordsOut", "bytesOut")) {
                        var zeroRates = idlePoints.stream().map(point -> point.get(key)).filter(Map.class::isInstance)
                                .map(rate -> (Map<?, ?>) rate).toList();
                        assertThat(zeroRates).as("%s held snapshot %s", resolution, key).isNotEmpty().allSatisfy(rate -> {
                            assertThat(number(rate.get("delta"))).isZero();
                            assertThat(number(rate.get("averageRate"))).isZero();
                        });
                    }
                    var empty = history(client, server.baseUrl(), control.credential(), idleFrom.minusSeconds(3600),
                            idleFrom.minusSeconds(1800), resolution);
                    assertThat(empty.get("status")).isEqualTo("NO_RETAINED_SAMPLES");
                    assertThat(points(empty)).isEmpty();
                }
            }
            Files.delete(hold);

            var first = new AtomicReference<Document>();
            Await.until("the first retained target acknowledgement", WAIT, () -> {
                first.set(history.find(new Document(filter).append("countingSince", new Document("$exists", true)))
                        .sort(new Document("observedAt", 1)).first());
                return first.get() != null;
            }, () -> control.logs(PIPELINE));
            var confirmed = first.get();
            var previous = history.find(new Document(filter).append("observedAt", new Document("$lt", confirmed.getDate("observedAt"))))
                    .sort(new Document("observedAt", -1)).first();
            assertThat(previous).isNotNull();
            var before = previous.get("startupEvidence", Document.class);
            var after = confirmed.get("startupEvidence", Document.class);
            assertThat(before).isNotNull().isEqualTo(after);
            Instant from = previous.getDate("observedAt").toInstant();
            Instant observedAt = confirmed.getDate("observedAt").toInstant();
            assertThat(Duration.between(from, observedAt)).isLessThan(SAMPLE_INTERVAL.multipliedBy(2));
            assertThat(endpoints.count(EndpointAddress.uri(target.toString()), TABLE)).isEqualTo(ROWS);

            try (var client = HttpClient.newHttpClient()) {
                String path = "/api/pipelines/" + PIPELINE + "/metrics";
                Map<?, ?> metrics = (Map<?, ?>) get(client, server.baseUrl(), control.credential(), path).get("metrics");
                BigDecimal records = number(metrics.get("records.out"));
                BigDecimal bytes = number(metrics.get("bytes.out"));
                assertThat(records).isEqualByComparingTo(BigDecimal.valueOf(ROWS));
                assertThat(bytes).isPositive();
                for (String resolution : List.of("raw", "PT5M")) {
                    var response = history(client, server.baseUrl(), control.credential(), from,
                            observedAt.plusMillis(1), resolution);
                    assertThat((List<?>) response.get("gaps")).isEmpty();
                    var points = points(response);
                    assertThat(points).noneMatch(point -> point.containsKey("cdcDelivery"));
                    assertRates(points, resolution, "recordsOut", records);
                    assertRates(points, resolution, "bytesOut", bytes);
                }
                assertThat(history.updateOne(new Document("_id", baseline.getFirst().get("_id")),
                        new Document("$set", new Document("startupEvidence", "unreadable"))).getModifiedCount()).isEqualTo(1);
                for (String resolution : List.of("raw", "PT5M")) {
                    var failure = getResponse(client, server.baseUrl(), control.credential(), historyPath(idleFrom, idleTo, resolution));
                    assertThat(failure.statusCode()).isEqualTo(500);
                    assertThat(failure.body()).contains(IoError.DOCUMENT_UNREADABLE.code());
                }
            }
        }
    }

    private static void configure(ControlPlane control, Path directory, Path source, Path target, Path hold) throws Exception {
        control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
        control.discoverSchema("startup_source", E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
        control.apply(Map.of(
                "source.tap.yml", Workspaces.cdcSourceYaml("startup_source", source, List.of(TABLE),
                        Map.of("hold", hold.getParent().toString(), "start_after_existing_rows", "true")),
                "target.tap.yml", Workspaces.targetYaml("startup_target", target),
                "pipeline.tap.yml", Workspaces.pipelineYaml(PIPELINE, "startup_source", "startup_target", TABLE)));
    }

    private static Map<?, ?> history(HttpClient client, URI base, String token, Instant from, Instant to,
            String resolution) throws Exception {
        return get(client, base, token, historyPath(from, to, resolution));
    }

    private static String historyPath(Instant from, Instant to, String resolution) {
        return "/api/pipelines/" + PIPELINE + "/metrics/history?resolution=" + resolution
                + "&from=" + URLEncoder.encode(from.toString(), StandardCharsets.UTF_8)
                + "&to=" + URLEncoder.encode(to.toString(), StandardCharsets.UTF_8);
    }

    private static Map<?, ?> get(HttpClient client, URI base, String token, String path) throws Exception {
        var response = getResponse(client, base, token, path);
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return (Map<?, ?>) JsonReader.parse(response.body());
    }

    private static HttpResponse<String> getResponse(HttpClient client, URI base, String token, String path) throws Exception {
        var request = HttpRequest.newBuilder(base.resolve(path)).header("Authorization", "Bearer " + token)
                .timeout(Duration.ofSeconds(20)).GET().build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static List<? extends Map<?, ?>> points(Map<?, ?> response) {
        return ((List<?>) response.get("segments")).stream()
                .map(item -> (List<?>) ((Map<?, ?>) item).get("points"))
                .flatMap(List::stream).map(item -> (Map<?, ?>) item).toList();
    }

    private static void assertRates(List<? extends Map<?, ?>> points, String resolution, String key, BigDecimal expected) {
        var rates = points.stream().map(point -> point.get(key)).filter(Map.class::isInstance)
                .map(rate -> (Map<?, ?>) rate).toList();
        assertThat(rates.stream().map(rate -> number(rate.get("delta"))).reduce(BigDecimal.ZERO, BigDecimal::add))
                .as("%s %s delta", resolution, key).isEqualByComparingTo(expected);
        assertThat(rates).as("%s %s throughput", resolution, key).anySatisfy(rate -> {
            assertThat(number(rate.get("averageRate"))).isPositive();
            assertThat(number(rate.get("maxRate"))).isPositive();
        });
    }

    private static BigDecimal number(Object value) {
        assertThat(value).isInstanceOf(Number.class);
        return new BigDecimal(value.toString());
    }
}
