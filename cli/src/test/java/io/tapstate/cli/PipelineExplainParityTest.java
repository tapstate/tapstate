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

/** The CLI decodes every shared explanation fixture without dropping or re-deriving a field. */
class PipelineExplainParityTest {

    private static final List<String> FIXTURES = List.of(
            "explain-stale.golden.json",
            "explain-coded-failure.golden.json",
            "explain-reconcile-failures.golden.json",
            "explain-no-movement.golden.json",
            "explain-frontier-stalled.golden.json",
            "explain-no-match.golden.json",
            "explain-unknown.golden.json",
            "explain-start-pending.golden.json");

    @Test
    void everyRestFixtureReachesTheCliWithTheSameTypedFields() throws Exception {
        AtomicReference<String> response = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/api/pipelines/orders/explain", exchange ->
                answer(exchange, response.get()));
        server.start();
        try {
            HttpControlPlaneClient client = new HttpControlPlaneClient();
            URI base = URI.create("http://127.0.0.1:" + server.getAddress().getPort());
            for (String fixture : FIXTURES) {
                String json = fixture(fixture);
                response.set(json);

                ExplainOutcome outcome = client.explain(base, "read-token", "orders");

                assertThat(outcome).as(fixture).isInstanceOf(ExplainOutcome.Found.class);
                assertThat(asMap((ExplainOutcome.Found) outcome))
                        .as(fixture)
                        .isEqualTo(JsonReader.parse(json));
            }
        } finally {
            server.stop(0);
        }
    }

    private static Map<String, Object> asMap(ExplainOutcome.Found found) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("pipelineId", found.pipelineId());
        value.put("state", found.state());
        value.put("kind", found.kind());
        value.put("message", found.message());
        if (found.observedAt() != null) {
            value.put("observedAt", found.observedAt());
            value.put("observedAgeMillis", found.observedAgeMillis());
        }
        value.put("freshness", found.freshness());
        value.put("evidence", found.evidence().stream().map(evidence -> {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("source", evidence.source());
            item.put("field", evidence.field());
            item.put("value", evidence.value());
            return item;
        }).toList());
        value.put("cannotSay", found.cannotSay());
        value.put("next", found.next() == null ? null : Map.of(
                "action", found.next().action(), "message", found.next().message()));
        if (found.pending() != null) {
            value.put("pending", Map.of("reason", found.pending().reason()));
        }
        return value;
    }

    private static String fixture(String name) throws IOException {
        try (var input = PipelineExplainParityTest.class.getResourceAsStream(
                "/golden/observability/" + name)) {
            if (input == null) {
                throw new IOException("missing shared explanation fixture: " + name);
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
