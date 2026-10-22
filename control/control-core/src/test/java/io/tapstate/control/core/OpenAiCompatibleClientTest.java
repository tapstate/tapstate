package io.tapstate.control.core;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import static org.assertj.core.api.Assertions.*;

class OpenAiCompatibleClientTest {
    @Test void returnsAPlainConversationWhenNoSchemaIsAvailable() throws Exception {
        String stream = chunk(Map.of("content", "Tell me which Source and table to choose."), null)
                + "data: [DONE]\n\n";
        withServer(200, stream, settings -> {
            List<String> text = new ArrayList<>();
            Optional<Map<String, Object>> result = localClient().propose(settings, List.of(
                    Map.of("role", "user", "content", "How do I start?")),
                    Map.of("proposalEnabled", false, "sources", List.of()), text::add);

            assertThat(result).isEmpty();
            assertThat(String.join("", text)).isEqualTo("Tell me which Source and table to choose.");
        });
    }

    @Test void acceptsOnlyCompletedToolArgumentsAndKeepsTextSeparate() throws Exception {
        String arguments = JsonWriter.write(Map.of("candidateWizard", Map.of("root", Map.of("table", "orders"))));
        String stream = chunk(Map.of("content", "Inspecting selected schema."), null)
                + chunk(Map.of("tool_calls", List.of(Map.of("index", 0, "function", Map.of("name", "propose_pipeline", "arguments", arguments.substring(0, 15))))), null)
                + chunk(Map.of("tool_calls", List.of(Map.of("index", 0, "function", Map.of("arguments", arguments.substring(15))))), "tool_calls")
                + "data: [DONE]\n\n";
        withServer(200, stream, settings -> {
            List<String> text = new ArrayList<>();
            var candidate = localClient().propose(settings, List.of(Map.of("role", "user", "content", "Please propose")), Map.of(), text::add);
            assertThat(String.join("", text)).isEqualTo("Inspecting selected schema.");
            assertThat(candidate).hasValueSatisfying(value -> assertThat(value).containsKey("root").doesNotContainKey("content"));
        });
    }
    @Test void describesTheBuilderMapRuleForCopyingAField() throws Exception {
        String arguments = JsonWriter.write(Map.of("candidateWizard", Map.of("root", Map.of("transforms", List.of(
                Map.of("id", "copy-name", "name", "map", "type", "map", "enabled", true,
                        "config", Map.of("map", Map.of("fields", List.of(Map.of("id", "name-rule",
                                "outputField", "name", "ruleType", "rename", "value", "$Db")),
                                "passthroughUnlisted", true))))))));
        String stream = chunk(Map.of("tool_calls", List.of(Map.of("index", 0, "function", Map.of(
                "name", "propose_pipeline", "arguments", arguments)))), "tool_calls") + "data: [DONE]\n\n";
        AtomicReference<Map<?, ?>> submitted = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/chat/completions", exchange -> {
            submitted.set((Map<?, ?>) JsonReader.parse(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            byte[] response = stream.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (var output = exchange.getResponseBody()) { output.write(response); }
        });
        server.start();
        try {
            var settings = new AiProviderSettings("openai-compatible", "http://127.0.0.1:" + server.getAddress().getPort(), "test", "sk-test");
            String editRequest = "\u4fdd\u7559\u539f\u6709 Db \u5b57\u6bb5\uff0c\u5e76\u65b0\u589e name \u5b57\u6bb5\uff0c\u503c\u53d6\u81ea Db\uff1b\u5176\u4ed6\u914d\u7f6e\u4fdd\u6301\u4e0d\u53d8\u3002";
            var result = localClient().propose(settings, List.of(Map.of("role", "user",
                    "content", editRequest)),
                    Map.of("proposalEnabled", true, "currentWizard", Map.of()), ignored -> { });

            assertThat(result).isPresent();
            Map<?, ?> tool = (Map<?, ?>) ((List<?>) submitted.get().get("tools")).getFirst();
            Map<?, ?> function = (Map<?, ?>) tool.get("function");
            Map<?, ?> parameters = (Map<?, ?>) function.get("parameters");
            Map<?, ?> candidateSchema = (Map<?, ?>) ((Map<?, ?>) parameters.get("properties")).get("candidateWizard");
            assertThat(candidateSchema).as("the provider must receive the Builder candidate contract").isNotNull();
            assertThat(candidateSchema.get("properties")).as("candidateWizard must declare its Builder fields").isNotNull();
            Map<?, ?> rootSchema = (Map<?, ?>) ((Map<?, ?>) candidateSchema.get("properties")).get("root");
            Map<?, ?> transformSchema = (Map<?, ?>) ((Map<?, ?>) rootSchema.get("properties")).get("transforms");
            Map<?, ?> transformItem = (Map<?, ?>) transformSchema.get("items");
            Map<?, ?> configSchema = (Map<?, ?>) ((Map<?, ?>) transformItem.get("properties")).get("config");
            Map<?, ?> mapSchema = (Map<?, ?>) ((Map<?, ?>) configSchema.get("properties")).get("map");
            Map<?, ?> fieldsSchema = (Map<?, ?>) ((Map<?, ?>) mapSchema.get("properties")).get("fields");
            Map<?, ?> ruleSchema = (Map<?, ?>) ((Map<?, ?>) fieldsSchema.get("items")).get("properties");
            assertThat(((List<?>) ((Map<?, ?>) ruleSchema.get("ruleType")).get("enum")).toString())
                    .contains("delete", "rename", "compute", "literal");
            String systemPrompt = ((List<?>) submitted.get().get("messages")).stream()
                    .map(item -> (Map<?, ?>) item).filter(item -> "system".equals(item.get("role")))
                    .map(item -> (String) item.get("content")).findFirst().orElseThrow();
            assertThat(systemPrompt).contains("passthroughUnlisted", "ruleType", "$<sourceField>");
            assertThat(submitted.get().containsKey("tool_choice")).isTrue();
        } finally { server.stop(0); }
    }
    @Test void keepsHowToQuestionsConversationalEvenWhenTheyMentionAnEdit() throws Exception {
        String stream = chunk(Map.of("content", "Use a map transform to add a field."), null) + "data: [DONE]\n\n";
        AtomicReference<Map<?, ?>> submitted = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/chat/completions", exchange -> {
            submitted.set((Map<?, ?>) JsonReader.parse(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            byte[] response = stream.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (var output = exchange.getResponseBody()) { output.write(response); }
        });
        server.start();
        try {
            var settings = new AiProviderSettings("openai-compatible", "http://127.0.0.1:" + server.getAddress().getPort(), "test", "sk-test");
            String question = "\u5982\u4f55\u65b0\u589e name \u5b57\u6bb5\uff1f";
            var result = localClient().propose(settings, List.of(Map.of("role", "user", "content", question)),
                    Map.of("proposalEnabled", true, "currentWizard", Map.of()), ignored -> { });

            assertThat(result).isEmpty();
            assertThat(submitted.get().containsKey("tools")).isTrue();
            assertThat(submitted.get().containsKey("tool_choice")).isFalse();
        } finally { server.stop(0); }
    }
    @Test void keepsAnEmptyToolCandidatePresentForServerValidation() throws Exception {
        String stream = chunk(Map.of("tool_calls", List.of(Map.of("index", 0, "function", Map.of(
                "name", "propose_pipeline", "arguments", "{\"candidateWizard\":{}}")))), "tool_calls")
                + "data: [DONE]\n\n";
        withServer(200, stream, settings -> assertThat(localClient().propose(settings, List.of(), Map.of(), ignored -> { }))
                .hasValueSatisfying(value -> assertThat(value).isEmpty()));
    }
    @Test void keyEchoAcrossTextChunksIsRedactedBeforeItLeavesTheAdapter() throws Exception {
        String arguments = JsonWriter.write(Map.of("candidateWizard", Map.of("root", Map.of("table", "orders"))));
        String stream = chunk(Map.of("content", "Do not echo sk-"), null) + chunk(Map.of("content", "test in the UI."), null)
                + chunk(Map.of("tool_calls", List.of(Map.of("index", 0, "function", Map.of("name", "propose_pipeline", "arguments", arguments)))), "tool_calls") + "data: [DONE]\n\n";
        withServer(200, stream, settings -> {
            List<String> output = new ArrayList<>();
            localClient().propose(settings, List.of(), Map.of(), output::add);
            assertThat(String.join("", output)).doesNotContain("sk-test").contains("[redacted]");
        });
    }
    @Test void escapedCredentialsInDecodedCandidateValuesAndPropertyNamesAreRejected() throws Exception {
        String secret = "sk-quote\"-slash\\-secret";
        for (Map<String, Object> candidate : List.of(Map.<String, Object>of("nested", List.of(Map.of("value", "prefix-" + secret + "-suffix"))),
                Map.<String, Object>of("nested", List.of(Map.of("prefix-" + secret + "-suffix", "value"))))) {
            String arguments = JsonWriter.write(Map.of("candidateWizard", candidate));
            String stream = chunk(Map.of("tool_calls", List.of(Map.of("index", 0, "function", Map.of("name", "propose_pipeline", "arguments", arguments)))), "tool_calls") + "data: [DONE]\n\n";
            withServer(200, stream, settings -> {
                var escapedSettings = new AiProviderSettings(settings.providerId(), settings.baseUrl(), settings.model(), secret);
                assertThatThrownBy(() -> localClient().propose(escapedSettings, List.of(), Map.of(), ignored -> { }))
                        .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(ControlError.AI_INVALID_PROPOSAL))
                        .hasMessageNotContaining(secret);
            });
        }
    }
    @Test void truncatedAndProviderErrorResponsesNeverReturnPartialCandidateOrSecret() throws Exception {
        for (String stream : List.of(chunk(Map.of("content", "Partial answer"), null),
                chunk(Map.of("tool_calls", List.of(Map.of("index", 0, "function", Map.of("name", "propose_pipeline", "arguments", "{\"candidateWizard\":")))), "length") + "data: [DONE]\n\n")) {
            withServer(200, stream, settings -> assertThatThrownBy(() -> localClient().propose(settings, List.of(), Map.of(), ignored -> { }))
                    .isInstanceOf(TapstateException.class).hasMessageNotContaining("sk-test"));
        }
        withServer(401, "sk-test leaked upstream diagnostic", settings -> assertThatThrownBy(() -> localClient().check(settings))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_AUTH_FAILED)).hasMessageNotContaining("sk-test"));
        withServer(503, "provider outage diagnostic", settings -> assertThatThrownBy(() -> localClient().check(settings))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED)));
    }
    @Test void malformedProviderStreamObjectsBecomeCodedProposalFailures() throws Exception {
        for (String malformed : List.of("null", "{\"choices\":[null]}", "{\"choices\":[{\"delta\":null}]")) {
            withServer(200, "data: " + malformed + "\n\ndata: [DONE]\n\n", settings ->
                    assertThatThrownBy(() -> localClient().propose(settings, List.of(), Map.of(), ignored -> { }))
                            .isInstanceOfSatisfying(TapstateException.class,
                                    failure -> assertThat(failure.code()).isEqualTo(ControlError.AI_INVALID_PROPOSAL)));
        }
    }
    private static String chunk(Map<String, Object> delta, String reason) {
        Map<String, Object> choice = new java.util.LinkedHashMap<>(); choice.put("delta", delta); choice.put("finish_reason", reason);
        return "data: " + JsonWriter.write(Map.of("choices", List.of(choice))) + "\n\n";
    }
    private static OpenAiCompatibleClient localClient() {
        return new OpenAiCompatibleClient(baseUrl -> URI.create(baseUrl + "/chat/completions"));
    }
    private static void withServer(int status, String body, java.util.function.Consumer<AiProviderSettings> action) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/chat/completions", exchange -> {
            exchange.getRequestBody().readAllBytes();
            byte[] response = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(status, response.length);
            try (var output = exchange.getResponseBody()) { output.write(response); }
        });
        server.start();
        try { action.accept(new AiProviderSettings("openai-compatible", "http://127.0.0.1:" + server.getAddress().getPort(), "test", "sk-test")); }
        finally { server.stop(0); }
    }
}
