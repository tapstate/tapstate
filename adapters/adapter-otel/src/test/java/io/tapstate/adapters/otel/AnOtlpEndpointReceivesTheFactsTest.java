package io.tapstate.adapters.otel;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.lifecycle.PipelineState;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
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
