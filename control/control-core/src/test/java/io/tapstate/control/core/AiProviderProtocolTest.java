package io.tapstate.control.core;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import org.junit.jupiter.api.Test;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import static org.assertj.core.api.Assertions.*;

class AiProviderProtocolTest {
    @Test void reasoningChatChecksUseCompletionTokensWhileLegacyRelaysKeepMaxTokens() throws Exception {
        AtomicReference<Map<?, ?>> received = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/chat/completions", exchange -> {
            Map<?, ?> body = (Map<?, ?>) JsonReader.parse(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            received.set(body);
            boolean accepted = !"gpt-6.1-sol".equals(body.get("model")) || body.containsKey("max_completion_tokens");
            byte[] response = JsonWriter.write(accepted ? Map.of("choices", List.of(Map.of("message", body.containsKey("tools") ? Map.of("tool_calls", List.of(Map.of("function", Map.of("name", "connection_check", "arguments", "{\"ok\":true}")))) : Map.of("content", "OK"))))
                    : Map.of("error", Map.of("code", "unsupported_parameter", "param", "max_tokens"))).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(accepted ? 200 : 400, response.length);
            try (var output = exchange.getResponseBody()) { output.write(response); }
        });
        server.start();
        try {
            var client = new OpenAiCompatibleClient(base -> URI.create(base + "/chat/completions"));
            String base = "http://127.0.0.1:" + server.getAddress().getPort() + "/v1";
            client.check(new AiProviderSettings("openai-compatible", base, "gpt-6.1-sol", "test-key"));
            assertThat(received.get().containsKey("max_completion_tokens")).isTrue();
            assertThat(received.get().containsKey("max_tokens")).isFalse();
            client.check(new AiProviderSettings("openai-compatible", base, "relay-custom-model", "test-key"));
            assertThat(received.get().containsKey("max_tokens")).isTrue();
            assertThat(received.get().containsKey("max_completion_tokens")).isFalse();
        } finally { server.stop(0); }
    }
    @Test void responsesStreamsACompleteToolProposalUsingTheSelectedProtocol() throws Exception {
        String arguments = JsonWriter.write(Map.of("explanation", "Keep orders.", "candidateWizard", Map.of("root", Map.of("table", "orders"))));
        Map<String, Object> call = Map.of("type", "function_call", "id", "call-1", "name", "propose_pipeline", "arguments", arguments, "status", "completed");
        String stream = event(Map.of("type", "response.output_item.added", "item", call))
                + event(Map.of("type", "response.function_call_arguments.delta", "item_id", "call-1", "delta", arguments))
                + event(Map.of("type", "response.completed", "response", Map.of("status", "completed", "output", List.of(call))));
        AtomicReference<Map<?, ?>> received = new AtomicReference<>();
        withEndpoint("/v1/responses", stream, (settings, client) -> {
            StringBuilder text = new StringBuilder();
            var result = client.propose(settings, List.of(Map.of("role", "user", "content", "Keep orders")), Map.of(), text::append);
            assertThat(result).hasValue(Map.of("root", Map.of("table", "orders")));
            assertThat(text.toString()).isEqualTo("Keep orders.");
            Map<?, ?> body = received.get();
            assertThat(body.get("max_output_tokens")).isEqualTo(8192L);
            assertThat(body.get("store")).isEqualTo(false);
            assertThat(body.get("input")).isInstanceOf(List.class);
            assertThat(body.containsKey("messages")).isFalse();
            Map<?, ?> tool = (Map<?, ?>) ((List<?>) body.get("tools")).getFirst();
            assertThat(tool.get("name")).isEqualTo("propose_pipeline");
            assertThat(tool.get("strict")).isEqualTo(false);
            assertThat(body.get("tool_choice")).isEqualTo(Map.of("type", "function", "name", "propose_pipeline"));
        }, received);
    }

    @Test void responsesRejectsIncompleteAndMismatchedToolStreams() throws Exception {
        String args = "{\"candidateWizard\":{}}";
        var call = Map.of("type", "function_call", "id", "call-1", "name", "propose_pipeline", "arguments", args);
        for (String stream : List.of(
                event(Map.of("type", "response.output_text.delta", "delta", "partial")),
                event(Map.of("type", "response.incomplete", "response", Map.of("status", "incomplete"))),
                event(Map.of("type", "response.completed", "response", Map.of("status", "completed", "output", List.of(call, call)))),
                event(Map.of("type", "response.output_item.added", "item", call))
                        + event(Map.of("type", "response.function_call_arguments.delta", "item_id", "wrong", "delta", args)))) {
            withEndpoint("/v1/responses", stream, (settings, client) -> assertThatThrownBy(() ->
                    client.propose(settings, List.of(), Map.of(), ignored -> {}))
                    .isInstanceOf(io.tapstate.core.common.TapstateException.class), new AtomicReference<>());
        }
    }

    @Test void responsesPlainTextIsNotDuplicatedAndCannotReturnAToolWhenDisabled() throws Exception {
        var message = Map.of("type", "message", "content", List.of(Map.of("type", "output_text", "text", "Hello")));
        String stream = event(Map.of("type", "response.output_text.delta", "delta", "Hello"))
                + event(Map.of("type", "response.completed", "response", Map.of("status", "completed", "output", List.of(message))));
        withEndpoint("/v1/responses", stream, (settings, client) -> {
            StringBuilder text = new StringBuilder();
            assertThat(client.propose(settings, List.of(), Map.of("proposalEnabled", false), text::append)).isEmpty();
            assertThat(text.toString()).isEqualTo("Hello");
        }, new AtomicReference<>());
        var call = Map.of("type", "function_call", "name", "propose_pipeline", "arguments", "{\"candidateWizard\":{}}");
        withEndpoint("/v1/responses", event(Map.of("type", "response.completed", "response", Map.of("status", "completed", "output", List.of(call)))),
                (settings, client) -> assertThatThrownBy(() -> client.propose(settings, List.of(), Map.of("proposalEnabled", false), ignored -> {}))
                        .isInstanceOf(io.tapstate.core.common.TapstateException.class), new AtomicReference<>());
    }

    @Test void discoversDeduplicatedModelsWithoutRequiringAModelOrReturningSecrets() throws Exception {
        String body = JsonWriter.write(Map.of("data", List.of(Map.of("id", "z-model"), Map.of("id", "a-model"),
                Map.of("id", "z-model"), Map.of("id", "test-key"), Map.of("id", ""))));
        withEndpoint("/v1/models", body, (settings, client) ->
                assertThat(client.models(settings)).containsExactly("a-model", "z-model"), new AtomicReference<>());
    }

    @Test void responsesConnectionCheckVerifiesTextAndToolCapabilities() throws Exception {
        java.util.concurrent.atomic.AtomicInteger count = new java.util.concurrent.atomic.AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/responses", exchange -> {
            Map<?, ?> request = (Map<?, ?>) JsonReader.parse(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            count.incrementAndGet();
            Object output = request.containsKey("tools")
                    ? Map.of("type", "function_call", "name", "connection_check", "arguments", "{\"ok\":true}")
                    : Map.of("type", "message", "content", List.of(Map.of("type", "output_text", "text", "OK")));
            byte[] bytes = JsonWriter.write(Map.of("status", "completed", "output", List.of(output))).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (var response = exchange.getResponseBody()) { response.write(bytes); }
        });
        server.start();
        try {
            new OpenAiCompatibleClient(base -> URI.create(base + "/chat/completions")).check(
                    new AiProviderSettings("openai-responses", "http://127.0.0.1:" + server.getAddress().getPort() + "/v1", "gpt-6.1-sol", "key"));
            assertThat(count).hasValue(2);
        } finally { server.stop(0); }
    }

    @Test void malformedConnectionToolArgumentsAreCodedForBothProtocols() throws Exception {
        for (boolean responses : List.of(false, true)) {
            HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/" + (responses ? "responses" : "chat/completions"), exchange -> {
                Map<?, ?> request = (Map<?, ?>) JsonReader.parse(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
                boolean tool = request.containsKey("tools");
                Object message = Map.of("content", "OK");
                if (tool) message = Map.of("tool_calls", List.of(Map.of("function", Map.of("name", "connection_check", "arguments", "{broken"))));
                Object payload = responses ? Map.of("status", "completed", "output", List.of(tool
                        ? Map.of("type", "function_call", "name", "connection_check", "arguments", "{broken")
                        : Map.of("type", "message", "content", List.of(Map.of("type", "output_text", "text", "OK")))))
                        : Map.of("choices", List.of(Map.of("message", message)));
                byte[] bytes = JsonWriter.write(payload).getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, bytes.length);
                try (var output = exchange.getResponseBody()) { output.write(bytes); }
            });
            server.start();
            try {
                var settings = new AiProviderSettings(responses ? "openai-responses" : "openai-compatible",
                        "http://127.0.0.1:" + server.getAddress().getPort() + "/v1", "test", "key");
                assertThatThrownBy(() -> new OpenAiCompatibleClient(base -> URI.create(base + "/chat/completions")).check(settings))
                        .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                                error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_REQUEST_REJECTED));
            } finally { server.stop(0); }
        }
    }

    @Test void nonStreamingFallbackRetainsExplicitTokenParameterNegotiation() throws Exception {
        java.util.concurrent.atomic.AtomicInteger requests = new java.util.concurrent.atomic.AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/chat/completions", exchange -> {
            requests.incrementAndGet();
            Map<?, ?> request = (Map<?, ?>) JsonReader.parse(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            boolean unsupported = request.containsKey("max_tokens");
            String body = unsupported ? JsonWriter.write(Map.of("error", Map.of("code", "unsupported_parameter", "param", "max_tokens")))
                    : Boolean.TRUE.equals(request.get("stream")) ? event(Map.of("error", Map.of("type", "upstream_error")))
                    : JsonWriter.write(Map.of("choices", List.of(Map.of("message", Map.of("content", "Hello")))));
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(unsupported ? 400 : 200, bytes.length);
            try (var output = exchange.getResponseBody()) { output.write(bytes); }
        });
        server.start();
        try {
            var settings = new AiProviderSettings("openai-compatible", "http://127.0.0.1:" + server.getAddress().getPort() + "/v1", "gpt-6.1-sol", "key");
            StringBuilder text = new StringBuilder();
            assertThat(new OpenAiCompatibleClient(base -> URI.create(base + "/chat/completions"))
                    .propose(settings, List.of(), Map.of("proposalEnabled", false), text::append)).isEmpty();
            assertThat(text.toString()).isEqualTo("Hello");
            assertThat(requests).hasValue(6);
        } finally { server.stop(0); }
    }

    @Test void preservesRelayPrefixesThroughTheProductionEndpointPolicyForBothProtocols() throws Exception {
        record Route(String provider, String base, String request, String models) { }
        List<String> paths = new java.util.concurrent.CopyOnWriteArrayList<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            paths.add(exchange.getRequestURI().getPath());
            String raw = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            Map<?, ?> body = raw.isEmpty() ? Map.of() : (Map<?, ?>) JsonReader.parse(raw);
            boolean tool = body.containsKey("tools");
            Object payload;
            if (exchange.getRequestMethod().equals("GET")) payload = Map.of("data", List.of(Map.of("id", "model")));
            else if (body.containsKey("input")) payload = Map.of("status", "completed", "output", List.of(tool
                    ? Map.of("type", "function_call", "name", "connection_check", "arguments", "{\"ok\":true}")
                    : Map.of("type", "message", "content", List.of(Map.of("type", "output_text", "text", "OK")))));
            else payload = Map.of("choices", List.of(Map.of("message", tool
                    ? Map.of("tool_calls", List.of(Map.of("function", Map.of("name", "connection_check", "arguments", "{\"ok\":true}"))))
                    : Map.of("content", "OK"))));
            byte[] bytes = JsonWriter.write(payload).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (var output = exchange.getResponseBody()) { output.write(bytes); }
        });
        server.start();
        try {
            var client = new OpenAiCompatibleClient(base -> {
                URI validated = OpenAiCompatibleEndpointPolicy.requestUri(base,
                        host -> new java.net.InetAddress[]{java.net.InetAddress.getByAddress(new byte[]{8, 8, 8, 8})});
                return URI.create("http://127.0.0.1:" + server.getAddress().getPort() + validated.getRawPath());
            });
            for (var route : List.of(
                    new Route("openai-compatible", "/responses", "/responses/chat/completions", "/responses/models"),
                    new Route("openai-compatible", "/relay/v1", "/relay/v1/chat/completions", "/relay/v1/models"),
                    new Route("openai-compatible", "/relay/v1/chat/completions", "/relay/v1/chat/completions", "/relay/v1/models"),
                    new Route("openai-responses", "/relay/v1", "/relay/v1/responses", "/relay/v1/models"),
                    new Route("openai-responses", "/relay/v1/responses", "/relay/v1/responses", "/relay/v1/models"),
                    new Route("openai-responses", "/relay/v1/chat/completions", "/relay/v1/responses", "/relay/v1/models"))) {
                paths.clear();
                var settings = new AiProviderSettings(route.provider(), "https://relay.example" + route.base(), "model", "key");
                client.check(settings);
                assertThat(client.models(settings)).containsExactly("model");
                assertThat(paths).as(route.provider() + " " + route.base()).containsExactly(route.request(), route.request(), route.models());
            }
        } finally { server.stop(0); }
    }

    private static String event(Map<String, Object> value) { return "data: " + JsonWriter.write(value) + "\n\n"; }

    private static void withEndpoint(String path, String response,
            java.util.function.BiConsumer<AiProviderSettings, OpenAiCompatibleClient> action,
            AtomicReference<Map<?, ?>> received) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(path, exchange -> {
            String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            received.set(body.isEmpty() ? Map.of() : (Map<?, ?>) JsonReader.parse(body));
            byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (var output = exchange.getResponseBody()) { output.write(bytes); }
        });
        server.start();
        try {
            var settings = new AiProviderSettings("openai-responses", "http://127.0.0.1:" + server.getAddress().getPort() + "/v1", "gpt-6.1-sol", "test-key");
            action.accept(settings, new OpenAiCompatibleClient(base -> URI.create(base + "/chat/completions")));
        } finally { server.stop(0); }
    }

}
