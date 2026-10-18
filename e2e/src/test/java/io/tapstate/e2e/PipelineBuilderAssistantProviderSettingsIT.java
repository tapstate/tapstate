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
class PipelineBuilderAssistantProviderSettingsIT {

    private static final String TEST_KEY = "sk-e2e-provider-key-sentinel";

    @Test
    void aCloudWorkspaceCanSaveProviderSettingsWithoutReadingBackTheKey() throws Exception {
        String database = "ts_plan_ai_provider_" + UUID.randomUUID().toString().replace("-", "");
        String storeUri = SharedMongo.replicaSetUrl(database);
        try (AtlasRuntime runtime = new AtlasRuntime(AtlasRuntime.Mode.CLOUD, storeUri);
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
