package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Cloud Builder provider settings stay authenticated and write-only over the shipped API. */
@RequiresDocker
@RequiresCloudReleaseInputs
class PipelineBuilderAssistantProviderSettingsIT {

    private static final String TEST_KEY = "sk-e2e-provider-key-sentinel";

    @Test
    void aCloudWorkspaceCanSaveProviderSettingsWithoutReadingBackTheKey() throws Exception {
        try (PreparedNativeStore storage = PreparedNativeStore.createCloud();
                AtlasRuntime runtime = new AtlasRuntime(storage);
                ServerHandle server = runtime.launch(Tiers.REAL_PROCESS)) {
            ControlPlane control = runtime.control(server, true);
            HttpClient client = HttpClient.newHttpClient();

            HttpResponse<String> before = providerSettings(client, server, control, "GET", null);
            assertThat(before.statusCode()).isEqualTo(200);
            assertThat(object(before.body())).containsEntry("configured", false).doesNotContainKey("apiKey");

            String requestBody = JsonWriter.write(Map.of(
                    "providerId", "openai-compatible", "model", "e2e-model", "apiKey", TEST_KEY));
            HttpResponse<String> saved = providerSettings(client, server, control, "PUT", requestBody);
            assertThat(saved.statusCode()).isEqualTo(204);
            assertThat(saved.body()).doesNotContain(TEST_KEY);

            HttpResponse<String> after = providerSettings(client, server, control, "GET", null);
            assertThat(after.statusCode()).isEqualTo(200);
            assertThat(after.body()).doesNotContain(TEST_KEY);
            assertThat(object(after.body()))
                    .containsEntry("configured", true)
                    .containsEntry("providerId", "openai-compatible")
                    .containsEntry("model", "e2e-model")
                    .doesNotContainKey("apiKey");
            runtime.assertAuthenticationBoundary();
        }
    }

    @Test
    void aBlankDraftCanValidateAnInputFromADiscoveredSourceWithoutSavingIt() throws Exception {
        try (PreparedNativeStore storage = PreparedNativeStore.createCloud();
                AtlasRuntime runtime = new AtlasRuntime(storage);
                ServerHandle server = runtime.launch(Tiers.REAL_PROCESS);
                var configuration = io.tapstate.adapters.mongostore.MongoConfigurationDomain.open(
                        new io.tapstate.adapters.mongostore.MongoConnectionSettings(storage.configurationUri(), null, java.time.Duration.ofSeconds(5)), storage.clusterId());
                var cache = io.tapstate.adapters.mongostore.MongoCacheDomain.connect(configuration.bindings().load().orElseThrow().cache(),
                        java.time.Duration.ofSeconds(5), "ai-test-models")) {
            ControlPlane control = runtime.control(server, true);
            Map<String, Object> candidate = seedDraft(configuration.artifacts(),
                    cache.schemas(configuration.bindings().cachePreparation().orElseThrow().businessScope(), () -> {}), control);
            HttpRequest request = HttpRequest.newBuilder(server.baseUrl().resolve("/api/pipelines/blank_auto/draft:assistant-validate"))
                    .header("Cookie", control.credential()).header("Origin", server.baseUrl().toString())
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(JsonWriter.write(Map.of("revision", 1, "candidateWizard", candidate))))
                    .build();
            HttpResponse<String> validated = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(validated.statusCode()).isEqualTo(200);
            assertThat(object(validated.body())).containsEntry("valid", true);
            assertThat(configuration.drafts().get("blank_auto").orElseThrow().wizard().root()).isNull();
            runtime.assertAuthenticationBoundary();
        }
    }

    @Test
    void invalidAssistantCandidateCanBeClarifiedWithoutChangingTheDraft() throws Exception {
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        var received = new java.util.concurrent.atomic.AtomicReference<java.util.List<Map<String, String>>>();
        var proposed = new java.util.concurrent.atomic.AtomicReference<Map<String, Object>>();
        var closed = new java.util.concurrent.atomic.AtomicInteger();
        class ControlledProvider implements io.tapstate.control.core.AiProviderClient, AutoCloseable {
            public void close() { closed.incrementAndGet(); }
            public void check(io.tapstate.control.core.AiProviderSettings settings) { }
            public java.util.Optional<Map<String, Object>> propose(io.tapstate.control.core.AiProviderSettings settings,
                    java.util.List<Map<String, String>> messages, Map<String, Object> context,
                    java.util.function.Consumer<String> text) {
                received.set(java.util.List.copyOf(messages));
                text.accept(calls.getAndIncrement() == 0 ? "Which source?" : "Using orders.");
                return java.util.Optional.of(calls.get() == 1 ? Map.of() : proposed.get());
            }
        }
        var provider = new ControlledProvider();
        try (PreparedNativeStore storage = PreparedNativeStore.createCloud();
                AtlasRuntime runtime = new AtlasRuntime(storage);
                ServerHandle server = runtime.launch(Tiers.IN_PROCESS, java.util.List.of(), provider);
                var configuration = io.tapstate.adapters.mongostore.MongoConfigurationDomain.open(
                        new io.tapstate.adapters.mongostore.MongoConnectionSettings(storage.configurationUri(), null, java.time.Duration.ofSeconds(5)), storage.clusterId());
                var cache = io.tapstate.adapters.mongostore.MongoCacheDomain.connect(configuration.bindings().load().orElseThrow().cache(),
                        java.time.Duration.ofSeconds(5), "ai-test-models")) {
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> runtime.launch(Tiers.REAL_PROCESS, java.util.List.of(), provider))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("in-process tier");
            ControlPlane control = runtime.control(server, true);
            Map<String, Object> candidate = seedDraft(configuration.artifacts(),
                    cache.schemas(configuration.bindings().cachePreparation().orElseThrow().businessScope(), () -> {}), control);
            var originalDraft = configuration.drafts().get("blank_auto").orElseThrow();
            proposed.set(candidate);
            HttpClient client = HttpClient.newHttpClient();
            var settingsRequest = HttpRequest.newBuilder(server.baseUrl().resolve("/api/ai-provider-settings"))
                    .header("Cookie", control.credential()).header("Origin", server.baseUrl().toString()).header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(JsonWriter.write(Map.of(
                            "providerId", "openai-compatible", "model", "e2e-model", "apiKey", TEST_KEY)))).build();
            assertThat(client.send(settingsRequest, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(204);
            var first = assistant(client, server, control, java.util.List.of(Map.of("role", "user", "text", "Choose tables")));
            assertThat(first.statusCode()).isEqualTo(200);
            assertThat(first.body()).contains("Which source?", "needs-input", "control.ai-invalid-proposal", "\"finishReason\":\"stop\"")
                    .doesNotContain("data-proposal", "\"type\":\"error\"");
            assertThat(configuration.drafts().get("blank_auto")).contains(originalDraft);
            var second = assistant(client, server, control, java.util.List.of(
                    Map.of("role", "user", "text", "Choose tables"),
                    Map.of("role", "assistant", "text", "Which source?"),
                    Map.of("role", "user", "text", "Use orders_src")));
            assertThat(second.body()).contains("Using orders.", "data-proposal", "\"finishReason\":\"stop\"");
            assertThat(received.get()).contains(Map.of("role", "user", "content", "Use orders_src"));
            assertThat(configuration.drafts().get("blank_auto")).contains(originalDraft);
        }
        assertThat(closed).hasValue(1);
    }

    private static Map<String, Object> seedDraft(io.tapstate.spi.store.ArtifactStore artifacts, io.tapstate.spi.store.SchemaStore schemas, ControlPlane control) {
        artifacts.save(new io.tapstate.core.model.SourceResource("orders_src", null, "mysql",
                Map.of(), io.tapstate.core.model.SourceMode.CDC, null, null, null));
        artifacts.save(new io.tapstate.core.model.SourceResource("destination", null, "mongodb",
                Map.of(), null, null, null, null));
        schemas.save(new io.tapstate.spi.store.DiscoveredSourceModel("orders_src", "mysql", 1,
                new io.tapstate.spi.store.SourceModel(java.util.List.of(new io.tapstate.spi.store.SourceTable("orders",
                    java.util.List.of(new io.tapstate.spi.store.SourceField("id", "integer")),
                    java.util.List.of("id"), java.util.List.of())))));
        Map<String, Object> wizard = new LinkedHashMap<>();
        wizard.put("root", null);
        wizard.put("related", java.util.List.of());
        wizard.put("transforms", java.util.List.of());
        wizard.put("output", Map.of("kind", "atlas", "config", Map.of("sourceId", "destination", "table", "orders", "writeMode", "upsert")));
        control.createPipelineDraft("blank_auto", Map.of("pipelineId", "blank_auto", "mode", "wizard", "name", "Blank", "wizard", wizard));
        return Map.of(
                "root", Map.of("source", "orders_src", "table", "orders", "primaryKey", "id", "keys", java.util.List.of("id"),
                    "columns", java.util.List.of(), "transforms", java.util.List.of()),
                "relations", java.util.List.of(), "transforms", java.util.List.of(),
                "destinationSourceId", "destination", "destinationTable", "orders", "destinationWriteMode", "upsert");
    }

    private static HttpResponse<String> assistant(HttpClient client, ServerHandle server, ControlPlane control,
            java.util.List<Map<String, String>> messages) throws Exception {
        var request = HttpRequest.newBuilder(server.baseUrl().resolve("/api/pipelines/blank_auto/draft:assistant"))
                .header("Cookie", control.credential()).header("Origin", server.baseUrl().toString())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(JsonWriter.write(Map.of("revision", 1, "messages", messages))))
                .build();
        return client.send(request, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> providerSettings(HttpClient client, ServerHandle server,
            ControlPlane control, String method, String body) throws Exception {
        HttpRequest.Builder request = HttpRequest.newBuilder(server.baseUrl().resolve("/api/ai-provider-settings"))
                .header("Cookie", control.credential())
                .header("Origin", server.baseUrl().toString());
        if ("GET".equals(method)) {
            request.GET();
        } else {
            request.header("Content-Type", "application/json")
                    .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8));
        }
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }

    private static Map<String, Object> object(String body) {
        Object value = JsonReader.parse(body);
        assertThat(value).isInstanceOf(Map.class);
        Map<String, Object> result = new LinkedHashMap<>();
        ((Map<?, ?>) value).forEach((key, entry) -> {
            assertThat(key).isInstanceOf(String.class);
            result.put((String) key, entry);
        });
        return result;
    }
}
