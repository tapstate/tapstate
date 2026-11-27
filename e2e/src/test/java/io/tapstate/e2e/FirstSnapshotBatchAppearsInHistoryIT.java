package io.tapstate.e2e;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.lifecycle.LifecycleVerb;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class FirstSnapshotBatchAppearsInHistoryIT {
    private static final String PIPELINE = "startup_history";
    private static final String TABLE = "orders";
    private static final long ROWS = 4;
    private static final Duration SAMPLE_INTERVAL = Duration.ofSeconds(10);
    private static final Duration WAIT = Duration.ofSeconds(90);

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
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, Files.readAllBytes(E2eConnectorJar.buildInto(directory)));
            control.discoverSchema("startup_source", E2eConnectorJar.CONNECTOR_ID, Map.of("uri", source.toString()));
            control.apply(Map.of(
                    "source.tap.yml", Workspaces.cdcSourceYaml("startup_source", source, List.of(TABLE),
                            Map.of("hold", hold.getParent().toString(), "start_after_existing_rows", "true")),
                    "target.tap.yml", Workspaces.targetYaml("startup_target", target),
                    "pipeline.tap.yml", Workspaces.pipelineYaml(PIPELINE, "startup_source", "startup_target", TABLE)));
            control.lifecycle(PIPELINE, LifecycleVerb.START);
            var history = SystemCollections.CACHE_PIPELINE_RATE_HISTORY.on(mongo.getDatabase(storage.cacheDatabase()));
            var filter = new Document("pipelineId", PIPELINE);
            Await.until("a retained zero baseline while the snapshot is held", WAIT,
                    () -> history.find(new Document(filter).append("startupEvidence.zeroAt", new Document("$exists", true))
                            .append("countingSince", new Document("$exists", false))).first() != null,
                    () -> history.find(filter).sort(new Document("observedAt", -1)).limit(3)
                            .into(new java.util.ArrayList<>()) + control.logs(PIPELINE));
            assertThat(endpoints.count(EndpointAddress.uri(target.toString()), TABLE)).isZero();
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
                    var points = ((List<?>) response.get("segments")).stream()
                            .map(item -> (List<?>) ((Map<?, ?>) item).get("points"))
                            .flatMap(List::stream).map(item -> (Map<?, ?>) item).toList();
                    assertThat(points).noneMatch(point -> point.containsKey("cdcDelivery"));
                    assertRates(points, resolution, "recordsOut", records);
                    assertRates(points, resolution, "bytesOut", bytes);
                }
            }
        }
    }

    private static Map<?, ?> history(HttpClient client, URI base, String token, Instant from, Instant to,
            String resolution) throws Exception {
        String path = "/api/pipelines/" + PIPELINE + "/metrics/history?resolution=" + resolution
                + "&from=" + URLEncoder.encode(from.toString(), StandardCharsets.UTF_8)
                + "&to=" + URLEncoder.encode(to.toString(), StandardCharsets.UTF_8);
        return get(client, base, token, path);
    }

    private static Map<?, ?> get(HttpClient client, URI base, String token, String path) throws Exception {
        var request = HttpRequest.newBuilder(base.resolve(path)).header("Authorization", "Bearer " + token)
                .timeout(Duration.ofSeconds(20)).GET().build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
        return (Map<?, ?>) JsonReader.parse(response.body());
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
