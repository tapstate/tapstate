package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
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
}
