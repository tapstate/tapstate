package io.tapstate.cli;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** The CLI decodes every shared history fixture without filling, averaging, or dropping a field. */
class PipelineHistoryParityTest {

    private static final List<String> FIXTURES = List.of(
            "history-raw-page-1.golden.json",
            "history-raw-page-2.golden.json",
            "history-auto-page-1.golden.json",
            "history-aggregate-boundaries.golden.json",
            "history-single-metric-missing.golden.json",
            "history-empty.golden.json");

    @Test
    void everyRestFixtureReachesTheCliWithTheSameTypedFields() throws Exception {
        AtomicReference<String> response = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/pipelines/orders/metrics/history", exchange ->
                answer(exchange, response.get()));
        server.start();
        try {
            HttpControlPlaneClient client = new HttpControlPlaneClient();
            URI base = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            HistoryRequest request = new HistoryRequest(
                    "2026-09-20T10:00:00Z", "2026-09-20T11:00:00Z",
                    "auto", 240, List.of("public.orders"), null);
            for (String fixture : FIXTURES) {
                String json = fixture(fixture);
                response.set(json);

                HistoryOutcome outcome = client.history(base, "read-token", "orders", request);

                assertThat(outcome).as(fixture).isInstanceOf(HistoryOutcome.Found.class);
                assertThat(asMap((HistoryOutcome.Found) outcome))
                        .as(fixture)
                        .isEqualTo(JsonReader.parse(json));
            }
        } finally {
            server.stop(0);
        }
    }

    private static Map<String, Object> asMap(HistoryOutcome.Found found) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("pipelineId", found.pipelineId());
        value.put("from", found.from());
        value.put("to", found.to());
        value.put("effectiveFrom", found.effectiveFrom());
        value.put("effectiveTo", found.effectiveTo());
        value.put("retentionCutoff", found.retentionCutoff());
        value.put("effectiveResolution", found.effectiveResolution());
        value.put("status", found.status());
        value.put("consistency", found.consistency());
        value.put("segments", found.segments().stream().map(PipelineHistoryParityTest::segment).toList());
        value.put("gaps", found.gaps().stream().map(gap -> Map.of(
                "intervalStart", gap.intervalStart(),
                "intervalEnd", gap.intervalEnd(),
                "reason", gap.reason())).toList());
        value.put("unavailable", found.unavailable().stream().map(missing -> {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("metric", missing.metric());
            if (missing.table() != null) {
                item.put("table", missing.table());
            }
            return item;
        }).toList());
        value.put("nextCursor", found.nextCursor());
        return value;
    }

    private static Map<String, Object> segment(HistoryOutcome.Segment segment) {
        return Map.of(
                "intervalStart", segment.intervalStart(),
                "intervalEnd", segment.intervalEnd(),
                "startReason", segment.startReason(),
                "points", segment.points().stream().map(PipelineHistoryParityTest::point).toList());
    }

    private static Map<String, Object> point(HistoryOutcome.Point point) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("intervalStart", point.intervalStart());
        value.put("intervalEnd", point.intervalEnd());
        if (point.recordsOut() != null) {
            value.put("recordsOut", rate(point.recordsOut()));
        }
        if (point.bytesOut() != null) {
            value.put("bytesOut", rate(point.bytesOut()));
        }
        value.put("lag", point.lag().stream().map(lag -> Map.of(
                "table", lag.table(),
                "observedAt", lag.observedAt(),
                "last", lag.last(),
                "max", lag.max())).toList());
        return value;
    }

    private static Map<String, Object> rate(HistoryOutcome.Rate rate) {
        return Map.of(
                "delta", rate.delta(),
                "averageRate", rate.averageRate(),
                "maxRate", rate.maxRate());
    }

    private static String fixture(String name) throws IOException {
        try (var input = PipelineHistoryParityTest.class.getResourceAsStream(
                "/golden/observability/" + name)) {
            if (input == null) {
                throw new IOException("missing shared history fixture: " + name);
            }
            return new String(input.readAllBytes(), StandardCharsets.UTF_8).stripTrailing();
        }
    }

    private static void answer(HttpExchange exchange, String body) throws IOException {
        assertThat(exchange.getRequestMethod()).isEqualTo("GET");
        assertThat(exchange.getRequestHeaders().getFirst("Authorization"))
                .isEqualTo("Bearer read-token");
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
