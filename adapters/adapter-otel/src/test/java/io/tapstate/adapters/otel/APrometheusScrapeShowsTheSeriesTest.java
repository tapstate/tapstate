package io.tapstate.adapters.otel;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.PipelineState;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;

import static io.tapstate.adapters.otel.Facts.AT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole path a Prometheus takes: the SDK's scrape endpoint is really brought up on a port, scraped
 * over HTTP, and the text it answers with names the facts under their translated names, carries their
 * attributes as labels, and lays a histogram out bucket by bucket. What a dashboard groups by is what is
 * asserted on: the pipeline, the table, the direction, and the state.
 */
class APrometheusScrapeShowsTheSeriesTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    void aScrapeReadsProcessHealthWithoutAnyPipelineObservation() throws Exception {
        int port = freePort();
        try (OtelMetricsExport export = OtelMetricsExport.start(ExportSettings.prometheusOn("127.0.0.1", port))) {
            export.observeProcess(() -> List.of(MetricFact.single(
                    "tapstate.process.telemetry.degraded", MetricType.GAUGE, "1",
                    MetricPoint.reading(Map.of(MetricAttributes.TELEMETRY_SINK, "latest"), AT, 1))));

            HttpResponse<String> response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/metrics")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());

            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body())
                    .containsPattern("tapstate_process_telemetry_degraded\\{[^}]*sink=\\\"latest\\\"[^}]*\\} 1")
                    .doesNotContain("tapstate_pipeline_state");
        }
    }

    @Test
    void aScrapeShowsTheFactsUnderTheirNamesWithTheirLabelsAndBuckets() throws Exception {
        int port = freePort();
        try (OtelMetricsExport export = OtelMetricsExport.start(ExportSettings.prometheusOn("127.0.0.1", port))) {
            export.offer("orders_sync", PipelineState.RUNNING, AT,
                    List.of(Facts.records("orders_sync", 42, 50), Facts.lag("orders_sync", 7), Facts.delivery("orders_sync")));

            HttpResponse<String> response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/metrics")).GET().build(),
                    HttpResponse.BodyHandlers.ofString());

            assertThat(response.statusCode()).isEqualTo(200);
            String body = response.body();
            body.lines().filter(line -> line.startsWith("tapstate_")).limit(16)
                    .forEach(line -> System.out.println("scraped: " + line));
            assertThat(body)
                    .contains("tapstate_pipeline_records_total{")
                    .contains("direction=\"out\"")
                    .contains("tapstate_table_id=\"orders\"")
                    .contains("tapstate_pipeline_id=\"orders_sync\"")
                    .containsPattern("tapstate_pipeline_records_total\\{[^}]*direction=\"out\"[^}]*\\} 42")
                    .containsPattern("tapstate_pipeline_lag_seconds\\{[^}]*\\} 7")
                    .contains("tapstate_pipeline_record_delivery_duration_seconds_bucket{")
                    .containsPattern("delivery_duration_seconds_bucket\\{[^}]*le=\"1(\\.0)?\"[^}]*\\} 3")
                    .containsPattern("tapstate_pipeline_state\\{[^}]*state=\"running\"[^}]*\\} 1")
                    .containsPattern("tapstate_pipeline_state\\{[^}]*state=\"stopped\"[^}]*\\} 0");
        }
    }

    @Test
    void nothingConfiguredIsRefusedRatherThanStartedForNobody() {
        assertThat(new ExportSettings(null, null, null, null, null).exportsAnything()).isFalse();
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> OtelMetricsExport.start(new ExportSettings(null, null, null, null, null)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("MetricsExport.none()");
    }
}
