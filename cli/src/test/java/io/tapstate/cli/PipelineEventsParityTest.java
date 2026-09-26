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

/** The CLI transports the same typed event page that REST serves. */
class PipelineEventsParityTest {

    @Test
    void everySharedEventPageKeepsFailureGapAndEmptyShapes() throws Exception {
        AtomicReference<String> response = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/pipelines/orders/events", exchange -> answer(exchange, response.get()));
        server.start();
        try {
            HttpControlPlaneClient client = new HttpControlPlaneClient();
            URI base = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            EventsRequest request = new EventsRequest("2026-09-20T10:00:00Z", "2026-09-20T11:00:00Z", 100, null);
            for (String fixture : List.of("events-failure-recovery.golden.json",
                    "events-known-gap.golden.json", "events-empty.golden.json")) {
                String json = fixture(fixture);
                response.set(json);
                EventsOutcome outcome = client.events(base, "read-token", "orders", request);
                assertThat(outcome).as(fixture).isInstanceOf(EventsOutcome.Found.class);
                assertThat(asMap((EventsOutcome.Found) outcome)).as(fixture).isEqualTo(JsonReader.parse(json));
            }
        } finally {
            server.stop(0);
        }
    }

    private static Map<String, Object> asMap(EventsOutcome.Found page) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("pipelineId", page.pipelineId());
        result.put("from", page.from());
        result.put("to", page.to());
        result.put("effectiveFrom", page.effectiveFrom());
        result.put("effectiveTo", page.effectiveTo());
        result.put("retentionCutoff", page.retentionCutoff());
        result.put("completeness", page.completeness());
        result.put("events", page.events().stream().map(PipelineEventsParityTest::event).toList());
        result.put("knownGaps", page.knownGaps().stream().map(gap -> Map.of(
                "eventId", gap.eventId(), "from", gap.from(), "to", gap.to(),
                "reasons", gap.reasons())).toList());
        result.put("nextCursor", page.nextCursor());
        return result;
    }

    private static Map<String, Object> event(EventsOutcome.Event event) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("id", event.id());
        value.put("occurredAt", event.occurredAt());
        value.put("kind", event.kind());
        value.put("message", event.message());
        if (event.beforeState() != null) value.put("beforeState", event.beforeState());
        if (event.afterState() != null) value.put("afterState", event.afterState());
        if (event.reason() != null) value.put("reason", event.reason());
        if (event.failure() != null) value.put("failure", Map.of(
                "code", event.failure().code(), "params", event.failure().params(),
                "message", event.failure().message()));
        return value;
    }

    private static String fixture(String name) throws IOException {
        try (var input = PipelineEventsParityTest.class.getResourceAsStream(
                "/golden/observability-events/" + name)) {
            if (input == null) throw new IOException("missing shared event fixture: " + name);
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void answer(HttpExchange exchange, String body) throws IOException {
        assertThat(exchange.getRequestMethod()).isEqualTo("GET");
        assertThat(exchange.getRequestHeaders().getFirst("Authorization")).isEqualTo("Bearer read-token");
        assertThat(exchange.getRequestURI().getRawQuery())
                .isEqualTo("from=2026-09-20T10%3A00%3A00Z&to=2026-09-20T11%3A00%3A00Z&limit=100");
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
