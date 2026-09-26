package io.tapstate.app;

import io.tapstate.adapters.otel.ExportSettings;
import io.tapstate.adapters.otel.OtelMetricsExport;
import io.tapstate.control.core.PipelineHistoryQueryService.RollupFallback;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.spi.store.HistoryRollupStore.Resolution;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class HistoryRollupQueryFactsTest {

    private static final Instant START = Instant.parse("2026-09-27T10:00:00Z");
    private static final Instant NOW = START.plusSeconds(60);

    @Test
    void completedFallbacksProduceOnlyObservedFixedResolutionCounters() {
        HistoryRollupQueryHealth health = new HistoryRollupQueryHealth();
        assertThat(HistoryRollupQueryFacts.snapshot(health.snapshot(), START, NOW)).isEmpty();

        health.accept(new RollupFallback(Resolution.PT30M, 2, false));
        health.accept(new RollupFallback(Resolution.PT30M, 0, true));
        health.accept(new RollupFallback(Resolution.PT6H, 1, false));

        assertThat(health.snapshot()).containsEntry(Resolution.PT30M,
                new HistoryRollupQueryHealth.Level(2, 2, 1)).containsEntry(Resolution.PT6H,
                new HistoryRollupQueryHealth.Level(1, 1, 0)).hasSize(2);
        List<MetricFact> facts = HistoryRollupQueryFacts.snapshot(health.snapshot(), START, NOW);
        assertThat(facts).extracting(MetricFact::name).containsExactly(
                "tapstate.process.rollup.query.raw_fallback",
                "tapstate.process.rollup.query.full_raw_fallback",
                "tapstate.process.rollup.query.bucket.down_drilled");
        assertThat(facts).allSatisfy(fact -> {
            assertThat(fact.type()).isEqualTo(MetricType.COUNTER);
            assertThat(fact.points()).allSatisfy(point ->
                    assertThat(point.attributes()).containsOnlyKeys(MetricAttributes.ROLLUP_RESOLUTION));
        });
        assertThat(facts.get(0).points()).extracting(point -> point.attributes()
                .get(MetricAttributes.ROLLUP_RESOLUTION)).containsExactly("30m", "6h");
        assertThat(facts.get(1).points()).singleElement().satisfies(point -> {
            assertThat(point.attributes()).isEqualTo(Map.of(MetricAttributes.ROLLUP_RESOLUTION, "30m"));
            assertThat(point.value()).isEqualTo(1L);
        });
    }

    @Test
    void prometheusScrapeStaysQuietUntilFallbackAndHasNoPipelineOrBucketLabels() throws Exception {
        int port;
        try (ServerSocket available = new ServerSocket(0)) {
            port = available.getLocalPort();
        }
        HistoryRollupQueryHealth health = new HistoryRollupQueryHealth();
        try (OtelMetricsExport export = OtelMetricsExport.start(ExportSettings.prometheusOn("127.0.0.1", port))) {
            export.observeProcess("rollup-query", () -> HistoryRollupQueryFacts.snapshot(
                    health.snapshot(), START, NOW));
            assertThat(scrape(port)).doesNotContain("tapstate_process_rollup_query_");

            health.accept(new RollupFallback(Resolution.PT30M, 1, false));
            assertThat(scrape(port))
                    .contains("tapstate_process_rollup_query_raw_fallback_total")
                    .contains("tapstate_process_rollup_query_bucket_down_drilled_total")
                    .contains("resolution=\"30m\"")
                    .doesNotContain("tapstate_process_rollup_query_full_raw_fallback_total")
                    .doesNotContain("tapstate_pipeline_id")
                    .doesNotContain("bucket_start");
        }
    }

    private static String scrape(int port) throws Exception {
        HttpResponse<String> response = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/metrics")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return response.body();
    }
}
