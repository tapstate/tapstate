package io.tapstate.e2e;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.control.core.OpenAiCompatibleClient;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import static org.assertj.core.api.Assertions.assertThat;

/** On-prem users can configure a provider through the authenticated server. */
@RequiresDocker
class OnPremAiProviderSettingsIT {
    private static final Duration REQUEST_TIMEOUT = Duration.ofSeconds(30);

    @Test void providerSettingsAreAuthenticatedAndWriteOnly() throws Exception {
        try (var storage = PreparedNativeStore.create("ai-provider", "ai-provider-password");
                ServerHandle server = Tiers.IN_PROCESS.launch(storage);
                var client = HttpClient.newHttpClient()) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            var uri = server.baseUrl().resolve("/api/ai-provider-settings");
            assertThat(client.send(HttpRequest.newBuilder(uri).timeout(REQUEST_TIMEOUT).GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode())
                    .isEqualTo(401);
            var key = "on-prem-provider-key-sentinel";
            var body = JsonWriter.write(Map.of("providerId", "openai-compatible", "model", "model", "apiKey", key));
            var put = HttpRequest.newBuilder(uri).timeout(REQUEST_TIMEOUT).header("Authorization", "Bearer " + control.credential())
                    .header("Content-Type", "application/json").PUT(HttpRequest.BodyPublishers.ofString(body)).build();
            assertThat(client.send(put, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(204);
            var get = HttpRequest.newBuilder(uri).timeout(REQUEST_TIMEOUT).header("Authorization", "Bearer " + control.credential()).GET().build();
            var response = client.send(get, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).doesNotContain(key, "apiKey");
            assertThat(JsonReader.parse(response.body())).isEqualTo(Map.of(
                    "configured", true, "providerId", "openai-compatible",
                    "baseUrl", "https://api.openai.com/v1", "model", "model"));
            var delete = HttpRequest.newBuilder(uri).timeout(REQUEST_TIMEOUT).header("Authorization", "Bearer " + control.credential()).DELETE().build();
            assertThat(client.send(delete, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(204);
            var deleted = client.send(get, HttpResponse.BodyHandlers.ofString());
            assertThat(deleted.statusCode()).isEqualTo(200);
            assertThat(JsonReader.parse(deleted.body())).isInstanceOfSatisfying(Map.class,
                    metadata -> assertThat(metadata.get("configured")).isEqualTo(false));
        }
    }

    private record ReplyExample(String request, String reply, String explanation) { }

    @Test void forwardsReplyLanguageGuidanceAndKeepsTheProposalReviewable() throws Exception {
        String reportedRequest = "I would like to create an enriched order table";
        var examples = List.of(
                new ReplyExample(reportedRequest, "I will use the selected schema.",
                        "I will copy id from orders to enriched_orders."),
                new ReplyExample(reportedRequest + ". Please reply in German.", "Ich verwende das ausgewählte Schema.",
                        "Ich kopiere id aus orders nach enriched_orders."));
        var wizard = Map.of("root", Map.of("source", "orders_src", "table", "orders", "keys", List.of("id"),
                        "columns", List.of(), "transforms", List.of()),
                "relations", List.of(), "transforms", List.of(), "destinationSourceId", "destination",
                "destinationTable", "enriched_orders", "destinationWriteMode", "upsert");
        var calls = new AtomicInteger();
        var sentRequest = new AtomicReference<Map<?, ?>>();
        var reply = new AtomicReference<ReplyExample>();
        var upstream = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        upstream.createContext("/v1/responses", exchange -> {
            calls.incrementAndGet();
            sentRequest.set((Map<?, ?>) JsonReader.parse(
                    new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8)));
            byte[] stream = providerStream(reply.get(), wizard).getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().set("Content-Type", "text/event-stream");
            exchange.sendResponseHeaders(200, 0);
            try (var response = exchange.getResponseBody()) { response.write(stream); }
        });
        upstream.start();
        try (var storage = PreparedNativeStore.create();
                var configuration = MongoConfigurationDomain.open(
                        new MongoConnectionSettings(storage.configurationUri(), null, Duration.ofSeconds(5)), storage.clusterId());
                var cache = MongoCacheDomain.connect(configuration.bindings().load().orElseThrow().cache(),
                        Duration.ofSeconds(5), "assistant-reply-language");
                var client = HttpClient.newHttpClient()) {
            configuration.artifacts().save(new SourceResource("orders_src", null, "mysql", Map.of(), SourceMode.CDC, null, null, null));
            configuration.artifacts().save(new SourceResource("destination", null, "mongodb", Map.of(), null, null, null, null));
            var orderFields = List.of(new SourceField("id", "integer"));
            cache.schemas(configuration.bindings().cachePreparation().orElseThrow().businessScope(), () -> {})
                    .save(new DiscoveredSourceModel("orders_src", "mysql", 1,
                            new SourceModel(List.of(new SourceTable("orders", orderFields, List.of("id"), List.of())))));

            var endpoint = URI.create("http://127.0.0.1:" + upstream.getAddress().getPort() + "/v1");
            var constructor = OpenAiCompatibleClient.class.getDeclaredConstructor(Function.class);
            constructor.setAccessible(true);
            var provider = constructor.newInstance((Function<String, URI>) ignored -> endpoint);
            try (var server = InProcessServer.startPrepared(storage, List.of(), provider)) {
                var control = new ControlPlane(server.baseUrl());
                storage.loginAndAwaitBusiness(control);
                control.createPipelineDraft("orders", Map.of("pipelineId", "orders", "mode", "wizard", "name", "Orders",
                        "wizard", Map.of("root", Map.of("id", "root", "sourceId", "orders_src", "table", "orders",
                                        "key", List.of("id"), "preTransforms", List.of()),
                                "related", List.of(), "transforms", List.of(), "output", Map.of("kind", "atlas",
                                        "config", Map.of("sourceId", "destination", "table", "orders", "writeMode", "upsert")))));
                var originalDraft = configuration.drafts().get("orders").orElseThrow();
                request(client, server, control, "/api/ai-provider-settings", "PUT",
                        Map.of("providerId", "openai-responses", "model", "test-model", "apiKey", "test-key"), 204);
                for (var example : examples) {
                    reply.set(example);
                    int callsBefore = calls.get();
                    var proposal = requestProposal(client, server, control, example);
                    assertThat(calls).hasValue(callsBefore + 1);
                    assertForwardedRequest(sentRequest.get(), example.request());

                    String review = request(client, server, control, "/api/pipelines/orders/draft:assistant-validate", "POST",
                            Map.of("revision", 1, "candidateWizard", proposal.get("candidateWizard")), 200);
                    var result = (Map<?, ?>) JsonReader.parse(review);
                    assertThat(result.get("valid")).isEqualTo(true);
                    assertThat(((Map<?, ?>) result.get("diff")).get("after")).asString()
                            .contains("orders_src.orders", "enriched_orders");
                    assertThat(configuration.drafts().get("orders")).contains(originalDraft);
                }
            }
        } finally { upstream.stop(0); }
    }

    private static String providerStream(ReplyExample reply, Map<String, Object> wizard) {
        String explanation = "{\"explanation\":" + JsonWriter.write(reply.explanation());
        String candidate = ",\"candidateWizard\":" + JsonWriter.write(wizard) + "}";
        var call = Map.of("type", "function_call", "id", "proposal", "name", "propose_pipeline",
                "status", "completed", "arguments", explanation + candidate);
        var message = Map.of("type", "message", "content", List.of(Map.of("type", "output_text", "text", reply.reply())));
        return event(Map.of("type", "response.reasoning_summary_text.delta", "item_id", "summary",
                        "summary_index", 0, "delta", "Checking the order schema."))
                + event(Map.of("type", "response.output_text.delta", "delta", reply.reply()))
                + event(Map.of("type", "response.output_item.added", "item",
                        Map.of("type", "function_call", "id", "proposal", "name", "propose_pipeline",
                                "status", "in_progress", "arguments", "")))
                + event(Map.of("type", "response.function_call_arguments.delta", "item_id", "proposal", "delta", explanation))
                + event(Map.of("type", "response.function_call_arguments.delta", "item_id", "proposal", "delta", candidate))
                + event(Map.of("type", "response.completed", "response",
                        Map.of("status", "completed", "output", List.of(message, call))));
    }

    private static Map<?, ?> requestProposal(HttpClient client, ServerHandle server, ControlPlane control,
            ReplyExample example) throws Exception {
        String stream = request(client, server, control, "/api/pipelines/orders/draft:assistant", "POST",
                Map.of("revision", 1, "messages", List.of(Map.of("role", "user", "text", example.request()))), 200);
        var events = stream.lines().filter(line -> line.startsWith("data: {"))
                .map(line -> (Map<?, ?>) JsonReader.parse(line.substring("data: ".length()))).toList();
        var visible = events.stream().filter(record -> "text-delta".equals(record.get("type")))
                .map(record -> (String) record.get("delta")).collect(Collectors.joining());
        assertThat(visible).isEqualTo(example.reply() + "\n\n" + example.explanation());
        assertThat(events.stream().map(record -> (String) record.get("type")))
                .contains("finish").doesNotContain("error");
        assertThat(events.stream().filter(record -> "reasoning-start".equals(record.get("type")))).hasSize(1);
        var proposals = events.stream().filter(record -> "data-proposal".equals(record.get("type"))).toList();
        assertThat(proposals).hasSize(1);
        var proposal = (Map<?, ?>) proposals.getFirst().get("data");
        assertThat(proposal.get("baseRevision")).isEqualTo(1L);
        return proposal;
    }

    private static void assertForwardedRequest(Map<?, ?> request, String userRequest) {
        var input = (List<?>) request.get("input");
        assertThat(input).hasSize(2);
        var system = (Map<?, ?>) input.getFirst();
        assertThat(system.get("role")).isEqualTo("system");
        assertThat(system.get("content")).asString()
                .contains("language of the latest user request", "unless the user explicitly requests another reply language",
                        "when referring to them in prose");
        assertThat(input.getLast()).isEqualTo(Map.of("role", "user", "content", userRequest));
    }

    private static String request(HttpClient client, ServerHandle server, ControlPlane control, String path,
            String method, Map<String, Object> body, int status) throws Exception {
        var request = HttpRequest.newBuilder(server.baseUrl().resolve(path)).timeout(REQUEST_TIMEOUT)
                .header("Authorization", "Bearer " + control.credential()).header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.ofString(JsonWriter.write(body))).build();
        var response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).as("%s %s: %s", method, path, response.body()).isEqualTo(status);
        return response.body();
    }

    private static String event(Map<String, Object> event) { return "data: " + JsonWriter.write(event) + "\n\n"; }
}
