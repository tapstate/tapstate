package io.tapstate.control.core;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.control.core.AiProviderSettings;
import org.junit.jupiter.api.Test;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.time.Duration;
import static org.assertj.core.api.Assertions.*;

class OpenAiCompatibleClientTest {
    @Test void streamsToolExplanationBeforeTheCandidateCompletes() throws Exception {
        CountDownLatch visibleText = new CountDownLatch(1);
        CountDownLatch releaseCandidate = new CountDownLatch(1);
        StringBuffer text = new StringBuffer();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/chat/completions", exchange -> {
            exchange.getRequestBody().readAllBytes();
            exchange.sendResponseHeaders(200, 0);
            try (var output = exchange.getResponseBody()) {
                output.write(chunk(Map.of("content", "Inspecting selected schema."), null).getBytes(StandardCharsets.UTF_8));
                String prefix = "{\"explanation\":\"First paragraph.\\nSecond ";
                output.write(chunk(Map.of("tool_calls", List.of(Map.of("index", 0, "function", Map.of(
                        "name", "propose_pipeline", "arguments", prefix)))), null).getBytes(StandardCharsets.UTF_8));
                output.flush();
                try { releaseCandidate.await(5, TimeUnit.SECONDS); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
                String suffix = "paragraph.\",\"candidateWizard\":{\"root\":{\"table\":\"orders\"}}}";
                output.write((chunk(Map.of("tool_calls", List.of(Map.of("index", 0, "function", Map.of(
                        "arguments", suffix)))), "tool_calls") + "data: [DONE]\n\n").getBytes(StandardCharsets.UTF_8));
            }
        });
        server.start();
        try (var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            var settings = new AiProviderSettings("openai-compatible", "http://127.0.0.1:" + server.getAddress().getPort(), "test", "sk-test");
            var result = executor.submit(() -> localClient().propose(settings,
                    List.of(Map.of("role", "user", "content", "Add a filter")), Map.of(), delta -> {
                        text.append(delta);
                        if (text.toString().contains("First paragraph.\nSecond ")) visibleText.countDown();
                    }));
            try {
                assertThat(visibleText.await(1, TimeUnit.SECONDS))
                        .as("tool explanation must be visible while the provider still holds the candidate open").isTrue();
                assertThat(result.isDone()).isFalse();
            } finally { releaseCandidate.countDown(); }
            assertThat(result.get(5, TimeUnit.SECONDS)).hasValueSatisfying(candidate -> assertThat(candidate).containsKey("root"));
            assertThat(text.toString()).isEqualTo("Inspecting selected schema.\n\nFirst paragraph.\nSecond paragraph.");
        } finally {
            releaseCandidate.countDown();
            server.stop(0);
        }
    }

    @Test void streamsOnlyTheTopLevelExplanationAndDecodesSplitEscapes() throws Exception {
        String arguments = "{\"candidateWizard\":{\"description\":\"not visible\",\"explanation\":\"nested decoy\"},"
                + "\"explanation\":\"Quote: \\\"field\\\"\\n\\uD83D\\uDE80 sk-test\"}";
        StringBuilder stream = new StringBuilder();
        for (int index = 0; index < arguments.length(); index++) {
            stream.append(chunk(Map.of("tool_calls", List.of(Map.of("index", 0, "function", Map.of(
                    "name", "propose_pipeline", "arguments", arguments.substring(index, index + 1))))), null));
        }
        stream.append("data: [DONE]\n\n");
        withServer(200, stream.toString(), settings -> {
            StringBuilder text = new StringBuilder();
            assertThat(localClient().propose(settings, List.of(), Map.of(), text::append)).isPresent();
            assertThat(text.toString()).isEqualTo("Quote: \"field\"\n\uD83D\uDE80 [redacted]");
        });
    }

    @Test void returnsAPlainConversationWhenNoSchemaIsAvailable() throws Exception {
        String stream = chunk(Map.of("content", "Tell me which Source and table to choose."), null)
                + "data: " + JsonWriter.write(Map.of("usage", Map.of("total_tokens", 12))) + "\n\n"
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

    @Test void blankInputCanAskForClarificationInsteadOfForcingAProposal() throws Exception {
        var received = new java.util.concurrent.atomic.AtomicReference<Map<Object, Object>>();
        var server = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/chat/completions", exchange -> {
            received.set(new LinkedHashMap<>((Map<?, ?>) JsonReader.parse(
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8))));
            byte[] bytes = (chunk(Map.of("content", "Which input table should anchor the output?"), "stop")
                    + "data: [DONE]\n\n").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (var output = exchange.getResponseBody()) { output.write(bytes); }
        });
        server.start();
        try {
            var settings = new AiProviderSettings("openai-compatible", "http://127.0.0.1:" + server.getAddress().getPort(), "test", "sk-test");
            List<String> text = new ArrayList<>();
            var result = localClient().propose(settings, List.of(Map.of("role", "user", "content", "Create a pipeline")),
                    Map.of("proposalEnabled", true, "inputSelectionPending", true), text::add);
            assertThat(result).isEmpty();
            assertThat(received.get()).containsKey("tools").doesNotContainKey("tool_choice");
            assertThat(String.join("", text)).contains("Which input table");
            localClient().propose(settings, List.of(Map.of("role", "user", "content", "Explain the pipeline. Do not propose or modify configuration.")),
                    Map.of("proposalEnabled", true), ignored -> { });
            assertThat(received.get()).as("explanation requests must not force a proposal").doesNotContainKey("tool_choice");
            localClient().propose(settings, List.of(Map.of("role", "user", "content", "Create a pipeline")),
                    Map.of("proposalEnabled", false, "scopeSelectionRequired", true), ignored -> { });
            assertThat(received.get()).doesNotContainKey("tools");
        } finally { server.stop(0); }
    }

    @Test void acceptsOnlyCompletedToolArgumentsAndKeepsTextSeparate() throws Exception {
        String arguments = JsonWriter.write(Map.of("explanation", "I will preserve the existing fields.", "candidateWizard", Map.of("root", Map.of("table", "orders"))));
        String stream = chunk(Map.of("content", "Inspecting selected schema."), null)
                + chunk(Map.of("tool_calls", List.of(Map.of("index", 0, "function", Map.of("name", "propose_pipeline", "arguments", arguments.substring(0, 15))))), null)
                + chunk(Map.of("tool_calls", List.of(Map.of("index", 0, "function", Map.of("arguments", arguments.substring(15))))), "tool_calls")
                + "data: [DONE]\n\n";
        withServer(200, stream, settings -> {
            List<String> text = new ArrayList<>();
            var candidate = localClient().propose(settings, List.of(Map.of("role", "user", "content", "Please propose")), Map.of(), text::add);
            assertThat(String.join("", text)).isEqualTo("Inspecting selected schema.\n\nI will preserve the existing fields.");
            assertThat(candidate).hasValueSatisfying(value -> assertThat(value).containsKey("root").doesNotContainKey("content"));
        });
    }
    @Test void acceptsCompleteToolCallsWhenTheProviderReportsStopOrOmitsFinishReason() throws Exception {
        String arguments = JsonWriter.write(Map.of("candidateWizard", Map.of("root", Map.of("table", "orders"))));
        for (String finishReason : java.util.Arrays.asList("stop", null)) {
            String stream = chunk(Map.of("tool_calls", List.of(Map.of("index", 0, "function", Map.of(
                    "name", "propose_pipeline", "arguments", arguments)))), finishReason) + "data: [DONE]\n\n";
            withServer(200, stream, settings -> assertThat(localClient().propose(settings, List.of(), Map.of(), ignored -> { }))
                    .hasValueSatisfying(value -> assertThat(value).containsKey("root")));
        }
    }
    @Test void describesUnionAndMapRulesForBuilderProposals() throws Exception {
        String arguments = JsonWriter.write(Map.of("candidateWizard", Map.of("root", Map.of("transforms", List.of(
                Map.of("id", "copy-name", "name", "map", "type", "map", "enabled", true,
                        "config", Map.of("map", Map.of("fields", List.of(Map.of("id", "name-rule",
                                "outputField", "name", "ruleType", "rename", "value", "$Db")),
                                "passthroughUnlisted", true))))))));
        String stream = chunk(Map.of("tool_calls", List.of(Map.of("index", 0, "function", Map.of(
                "name", "propose_pipeline", "arguments", arguments)))), "tool_calls") + "data: [DONE]\n\n";
        AtomicReference<Map<Object, Object>> submitted = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/chat/completions", exchange -> {
            submitted.set(new LinkedHashMap<>((Map<?, ?>) JsonReader.parse(
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8))));
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
            Map<?, ?> typeSchema = (Map<?, ?>) ((Map<?, ?>) transformItem.get("properties")).get("type");
            assertThat(((List<?>) typeSchema.get("enum")).toString()).contains("union");
            Map<?, ?> configSchema = (Map<?, ?>) ((Map<?, ?>) transformItem.get("properties")).get("config");
            Map<?, ?> unionSchema = (Map<?, ?>) ((Map<?, ?>) configSchema.get("properties")).get("union");
            assertThat(unionSchema).isNotNull();
            Map<?, ?> unionProperties = (Map<?, ?>) unionSchema.get("properties");
            Map<?, ?> inputsSchema = (Map<?, ?>) unionProperties.get("from");
            assertThat(inputsSchema.get("type")).isEqualTo("array");
            assertThat(((Map<?, ?>) inputsSchema.get("items")).get("type")).isEqualTo("string");
            assertThat(unionSchema.get("required")).isEqualTo(List.of("from"));
            Map<?, ?> mapSchema = (Map<?, ?>) ((Map<?, ?>) configSchema.get("properties")).get("map");
            Map<?, ?> fieldsSchema = (Map<?, ?>) ((Map<?, ?>) mapSchema.get("properties")).get("fields");
            Map<?, ?> ruleSchema = (Map<?, ?>) ((Map<?, ?>) fieldsSchema.get("items")).get("properties");
            assertThat(((List<?>) ((Map<?, ?>) ruleSchema.get("ruleType")).get("enum")).toString())
                    .contains("delete", "rename", "compute", "literal");
            String systemPrompt = ((List<?>) submitted.get().get("messages")).stream()
                    .map(item -> (Map<?, ?>) item).filter(item -> "system".equals(item.get("role")))
                    .map(item -> (String) item.get("content")).findFirst().orElseThrow();
            assertThat(systemPrompt).contains("passthroughUnlisted", "ruleType", "$<sourceField>");
            assertThat(systemPrompt).contains("Bundled DSL semantics", "CEL boolean predicate",
                    "A rename consumes its source name", "ctx.emit(record)", "explicit alias map");
            assertThat(systemPrompt).contains("function process(record, ctx)", "record.after",
                    "top-level return", "deletes", "DDL", "Return the complete record", "ctx.emit(record)");
            Map<?, ?> jsSchema = (Map<?, ?>) ((Map<?, ?>) configSchema.get("properties")).get("js");
            Map<?, ?> scriptSchema = (Map<?, ?>) ((Map<?, ?>) jsSchema.get("properties")).get("script");
            assertThat((String) scriptSchema.get("description"))
                    .contains("function process(record, ctx)", "record.after", "new Date().toISOString()");
            assertThat(submitted.get()).containsKey("tool_choice");
        } finally { server.stop(0); }
    }
    @Test void keepsHowToQuestionsConversationalEvenWhenTheyMentionAnEdit() throws Exception {
        String stream = chunk(Map.of("content", "Use a map transform to add a field."), null) + "data: [DONE]\n\n";
        AtomicReference<Map<Object, Object>> submitted = new AtomicReference<>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/chat/completions", exchange -> {
            submitted.set(new LinkedHashMap<>((Map<?, ?>) JsonReader.parse(
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8))));
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
            assertThat(submitted.get()).containsKey("tools").doesNotContainKey("tool_choice");
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
        withServer(403, "sk-test leaked upstream diagnostic", settings -> assertThatThrownBy(() -> localClient().check(settings))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_AUTH_FAILED)).hasMessageNotContaining("sk-test"));
        withServer(503, "provider outage diagnostic", settings -> assertThatThrownBy(() -> localClient().check(settings))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED)));
        withServer(500, "provider outage diagnostic", settings -> assertThatThrownBy(() -> localClient().propose(settings, List.of(), Map.of(), ignored -> { }))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED)));
    }

    @Test void connectionCheckAcceptsAChoicesEnvelopeAndRejectsAnEmptyChoicesList() throws Exception {
        withServer(200, "{\"choices\":[{}]}", settings ->
                assertThatCode(() -> localClient().check(settings)).doesNotThrowAnyException());
        withServer(200, "{\"choices\":[]}", settings -> assertThatThrownBy(() -> localClient().check(settings))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED)));
        withServer(200, "not-json", settings -> assertThatThrownBy(() -> localClient().check(settings))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED)));
        withServer(200, "x".repeat(524_289), settings -> assertThatThrownBy(() -> localClient().check(settings))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED)));
    }

    @Test void interruptedCheckAndProposalRestoreTheInterruptFlagAndReturnCodedFailures() throws Exception {
        var settings = new AiProviderSettings("openai-compatible", "http://127.0.0.1:1", "test", "sk-test");
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> localClient().check(settings))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
        try {
            Thread.currentThread().interrupt();
            assertThatThrownBy(() -> localClient().propose(settings, List.of(), Map.of(), ignored -> { }))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED));
            assertThat(Thread.currentThread().isInterrupted()).isTrue();
        } finally {
            Thread.interrupted();
        }
    }

    @Test void invalidRequestUriFactoryIsReturnedAsACodedProposalFailure() throws Exception {
        withServer(200, "data: [DONE]\n\n", settings -> assertThatThrownBy(() ->
                new OpenAiCompatibleClient(baseUrl -> { throw new IllegalArgumentException("invalid uri"); })
                        .propose(settings, List.of(), Map.of(), ignored -> { }))
                .isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(ControlError.AI_INVALID_PROPOSAL)));
    }

    @Test void retriesEmptyToolStreamsWithNonStreamingConversationAndProposalFallbacks() throws Exception {
        AtomicInteger streamRequests = new AtomicInteger();
        AtomicInteger fallbackRequests = new AtomicInteger();
        String arguments = JsonWriter.write(Map.of("candidateWizard", Map.of("root", Map.of("table", "orders"))));
        Map<String, Object> fallbackToolCall = Map.of("function", Map.of(
                "name", "propose_pipeline", "arguments", arguments));
        Map<String, Object> fallbackMessage = Map.of("content", "I added the requested filter.",
                "tool_calls", List.of(fallbackToolCall));
        List<String> fallbacks = List.of(
                JsonWriter.write(Map.of("choices", List.of(Map.of("message", Map.of("content", "Fallback answer."))))),
                JsonWriter.write(Map.of("choices", List.of(Map.of("message", fallbackMessage)))));
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/chat/completions", exchange -> {
            Map<?, ?> request = (Map<?, ?>) JsonReader.parse(
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            String body;
            if (Boolean.TRUE.equals(request.get("stream"))) {
                streamRequests.incrementAndGet();
                body = "data: " + JsonWriter.write(Map.of("error",
                        Map.of("code", "temporary", "type", "upstream_error"))) + "\n\n";
            } else {
                int fallback = fallbackRequests.getAndIncrement();
                body = fallbacks.get(fallback);
            }
            byte[] response = body.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (var output = exchange.getResponseBody()) { output.write(response); }
        });
        server.start();
        try {
            var settings = new AiProviderSettings("openai-compatible",
                    "http://127.0.0.1:" + server.getAddress().getPort(), "test", "sk-test");
            List<String> conversationalText = new ArrayList<>();
            Optional<Map<String, Object>> conversation = localClient().propose(settings,
                    List.of(Map.of("role", "user", "content", "How do I start?")),
                    Map.of("proposalEnabled", false), conversationalText::add);
            assertThat(conversation).isEmpty();
            assertThat(String.join("", conversationalText)).isEqualTo("Fallback answer.");

            List<String> proposalText = new ArrayList<>();
            Optional<Map<String, Object>> proposal = localClient().propose(settings,
                    List.of(Map.of("role", "user", "content", "Add a filter for active orders")),
                    Map.of("proposalEnabled", true), proposalText::add);
            assertThat(String.join("", proposalText)).isEqualTo("I added the requested filter.");
            assertThat(proposal).hasValueSatisfying(value -> assertThat(value).containsEntry("root", Map.of("table", "orders")));
            assertThat(streamRequests).hasValue(4);
            assertThat(fallbackRequests).hasValue(2);
        } finally { server.stop(0); }
    }

    @Test void rejectsMalformedOrAmbiguousNonStreamingProposalFallbacks() throws Exception {
        Map<String, Object> proposedCall = Map.of("function", Map.of("name", "propose_pipeline", "arguments", "{}"));
        Map<String, Object> otherCall = Map.of("function", Map.of("name", "other_tool", "arguments", "{}"));
        String multipleCalls = JsonWriter.write(Map.of("choices", List.of(Map.of("message", Map.of(
                "content", "Try again.", "tool_calls", List.of(proposedCall, proposedCall))))));
        String wrongCall = JsonWriter.write(Map.of("choices", List.of(Map.of("message", Map.of(
                "content", "Try again.", "tool_calls", List.of(otherCall))))));
        for (String fallback : List.of(
                "{\"choices\":[]}",
                "{\"choices\":[{\"message\":{}}]}",
                multipleCalls,
                wrongCall)) {
            assertFallbackFailure(fallback);
        }
    }

    private static void assertFallbackFailure(String fallback) throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/chat/completions", exchange -> {
            Map<?, ?> request = (Map<?, ?>) JsonReader.parse(
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            String response = Boolean.TRUE.equals(request.get("stream"))
                    ? "data: " + JsonWriter.write(Map.of("error", Map.of("code", "temporary", "type", "upstream_error"))) + "\n\n"
                    : fallback;
            byte[] body = response.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            try (var output = exchange.getResponseBody()) { output.write(body); }
        });
        server.start();
        try {
            var settings = new AiProviderSettings("openai-compatible",
                    "http://127.0.0.1:" + server.getAddress().getPort(), "test", "sk-test");
            Throwable failure = catchThrowable(() -> localClient().propose(settings,
                    List.of(Map.of("role", "user", "content", "Add a filter")),
                    Map.of("proposalEnabled", true), ignored -> { }));
            assertThat(failure).as("fallback response: %s", fallback)
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(ControlError.AI_INVALID_PROPOSAL));
        } finally { server.stop(0); }
    }

    @Test void rejectsUnsupportedToolIndexesAndOversizedStreamingBodies() throws Exception {
        String unsupportedIndex = chunk(Map.of("tool_calls", List.of(Map.of("index", 1,
                "function", Map.of("name", "propose_pipeline", "arguments", "{}")))), "tool_calls")
                + "data: [DONE]\n\n";
        withServer(200, unsupportedIndex, settings -> assertThatThrownBy(() ->
                localClient().propose(settings, List.of(), Map.of(), ignored -> { }))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED)));

        String largeDelta = "a".repeat(300_000);
        String oversized = chunk(Map.of("content", largeDelta), null)
                + chunk(Map.of("content", largeDelta), null) + "data: [DONE]\n\n";
        withServer(200, oversized, settings -> assertThatThrownBy(() ->
                localClient().propose(settings, List.of(), Map.of(), ignored -> { }))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED)));

        String oversizedUtf8Delta = "\u4e2d".repeat(180_000);
        String oversizedUtf8 = chunk(Map.of("content", oversizedUtf8Delta), null) + "data: [DONE]\n\n";
        withServer(200, oversizedUtf8, settings -> assertThatThrownBy(() ->
                localClient().propose(settings, List.of(), Map.of(), ignored -> { }))
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED)));
    }
    @Test void malformedProviderStreamObjectsBecomeCodedProposalFailures() throws Exception {
        for (String malformed : List.of("null", "{\"choices\":[null]}", "{\"choices\":[{\"delta\":null}]")) {
            withServer(200, "data: " + malformed + "\n\ndata: [DONE]\n\n", settings ->
                    assertThatThrownBy(() -> localClient().propose(settings, List.of(), Map.of(), ignored -> { }))
                            .isInstanceOfSatisfying(TapstateException.class,
                                    failure -> assertThat(failure.code()).isEqualTo(ControlError.AI_INVALID_PROPOSAL)));
        }
    }
    @Test void providerErrorFieldsCannotLeakTheCredentialIntoLogs() throws Exception {
        String payload = "data: " + JsonWriter.write(Map.of("error",
                Map.of("code", "sk-test", "type", "invalid_api_key"))) + "\n\n";
        Logger logger = Logger.getLogger(OpenAiCompatibleClient.class.getName());
        List<String> messages = new ArrayList<>();
        java.util.logging.Handler handler = new java.util.logging.Handler() {
            @Override public void publish(LogRecord record) {
                messages.add(record.getMessage());
                Object[] parameters = record.getParameters();
                if (parameters != null) for (Object parameter : parameters) messages.add(String.valueOf(parameter));
            }
            @Override public void flush() { }
            @Override public void close() { }
        };
        logger.addHandler(handler);
        try {
            withServer(200, payload, settings -> assertThatThrownBy(() ->
                    localClient().propose(settings, List.of(), Map.of(), ignored -> { }))
                    .isInstanceOf(TapstateException.class).hasMessageNotContaining("sk-test"));
        } finally {
            logger.removeHandler(handler);
        }
        assertThat(messages).isNotEmpty();
        assertThat(String.join("\n", messages)).doesNotContain("sk-test");
    }

    @Test void boundsTheBodyOfTheNonStreamingFallbackResponse() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        CountDownLatch releaseBody = new CountDownLatch(1);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/chat/completions", exchange -> {
            Map<?, ?> request = (Map<?, ?>) JsonReader.parse(
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            requests.incrementAndGet();
            if (Boolean.TRUE.equals(request.get("stream"))) {
                byte[] body = ("data: " + JsonWriter.write(Map.of("error",
                        Map.of("code", "temporary", "type", "upstream_error"))) + "\n\n")
                        .getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, body.length);
                try (var output = exchange.getResponseBody()) { output.write(body); }
                return;
            }
            exchange.sendResponseHeaders(200, 0);
            try (var output = exchange.getResponseBody()) {
                try {
                    releaseBody.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        server.start();
        try {
            var settings = new AiProviderSettings("openai-compatible",
                    "http://127.0.0.1:" + server.getAddress().getPort(), "test", "sk-test");
            long started = System.nanoTime();
            assertThatThrownBy(() -> new OpenAiCompatibleClient(
                    baseUrl -> URI.create(baseUrl + "/chat/completions"), Duration.ofMillis(300))
                    .propose(settings, List.of(), Map.of(), ignored -> { }))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(ControlError.AI_PROVIDER_FAILED));
            assertThat(Duration.ofNanos(System.nanoTime() - started)).isLessThan(Duration.ofSeconds(2));
            assertThat(requests).hasValue(3);
        } finally {
            releaseBody.countDown();
            server.stop(0);
        }
    }
    private static String chunk(Map<String, Object> delta, String reason) {
        Map<String, Object> choice = new java.util.LinkedHashMap<>(); choice.put("delta", delta);
        if (reason != null) choice.put("finish_reason", reason);
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
