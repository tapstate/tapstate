package io.tapstate.adapters.otel;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.PipelineState;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static io.tapstate.adapters.otel.Facts.AT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The push path: an OTLP endpoint that is really listened on receives the facts as a protobuf export
 * request when the SDK's reader flushes. What is asserted is that the wire was crossed with the shape a
 * collector expects; what the bytes say is the Prometheus case's to check, on the same producer.
 */
class AnOtlpEndpointReceivesTheFactsTest {

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Test
    void collectorFailureAndRecoveryAreVisibleWithoutAStoredPipelineObservation() throws Exception {
        AtomicBoolean reject = new AtomicBoolean(true);
        HttpServer collector = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        collector.createContext("/v1/metrics", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(reject.get() ? 503 : 200, -1);
            exchange.close();
        });
        collector.start();
        try {
            int prometheusPort = freePort();
            String endpoint = "http://127.0.0.1:" + collector.getAddress().getPort() + "/v1/metrics";
            try (OtelMetricsExport export = OtelMetricsExport.start(new ExportSettings(
                    endpoint, ExportSettings.Protocol.HTTP_PROTOBUF, Duration.ofMinutes(10),
                    "127.0.0.1", prometheusPort))) {
                export.observeProcess("probe", () -> List.of(MetricFact.single("tapstate.process.probe.alive",
                        MetricType.GAUGE, "1", MetricPoint.reading(Map.of(), AT, 1))));
                export.flush(Duration.ofSeconds(15));
                String failed = scrape(prometheusPort);
                assertThat(failed)
                        .containsPattern("tapstate_process_otlp_export_failure_total\\{[^}]*\\} 1")
                        .containsPattern("tapstate_process_otlp_export_degraded\\{[^}]*\\} 1")
                        .doesNotContain("tapstate_pipeline_state");

                reject.set(false);
                assertThat(export.flush(Duration.ofSeconds(15))).isTrue();
                String recovered = scrape(prometheusPort);
                assertThat(recovered)
                        .containsPattern("tapstate_process_otlp_export_success_total\\{[^}]*\\} 1")
                        .containsPattern("tapstate_process_otlp_export_failure_total\\{[^}]*\\} 1")
                        .containsPattern("tapstate_process_otlp_export_degraded\\{[^}]*\\} 0")
                        .contains("tapstate_process_otlp_export_last_success_age")
                        .doesNotContain("tapstate_pipeline_state");
            }
        } finally {
            collector.stop(0);
        }
    }

    private static String scrape(int port) throws Exception {
        HttpResponse<String> response = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                URI.create("http://127.0.0.1:" + port + "/metrics")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        return response.body();
    }

    @Test
    void aFlushPushesTheFactsToTheEndpointAsProtobuf() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        AtomicReference<String> contentType = new AtomicReference<>();
        AtomicLong bytes = new AtomicLong();
        HttpServer collector = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        collector.createContext("/v1/metrics", exchange -> {
            byte[] body = exchange.getRequestBody().readAllBytes();
            requests.incrementAndGet();
            contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            bytes.addAndGet(body.length);
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        collector.start();
        try {
            String endpoint = "http://127.0.0.1:" + collector.getAddress().getPort() + "/v1/metrics";
            try (OtelMetricsExport export = OtelMetricsExport.start(
                    ExportSettings.otlpTo(endpoint, ExportSettings.Protocol.HTTP_PROTOBUF, Duration.ofMinutes(10)))) {
                export.offer("orders_sync", PipelineState.RUNNING, AT, List.of(Facts.records("orders_sync", 42, 50)));

                assertThat(export.flush(Duration.ofSeconds(15))).as("the push completed").isTrue();
            }
            System.out.printf("otlp requests=%d contentType=%s bytes=%d%n", requests.get(), contentType.get(), bytes.get());
            assertThat(requests.get()).isGreaterThanOrEqualTo(1);
            assertThat(contentType.get()).isEqualTo("application/x-protobuf");
            assertThat(bytes.get()).isPositive();
        } finally {
            collector.stop(0);
        }
    }
}
