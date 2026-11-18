package io.tapstate.e2e;

import io.tapstate.core.common.JsonWriter;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

/** On-prem users can configure a provider through the authenticated server. */
@RequiresDocker
class OnPremAiProviderSettingsIT {
    @Test void providerSettingsAreAuthenticatedAndWriteOnly() throws Exception {
        try (var storage = PreparedNativeStore.create("ai-provider", "ai-provider-password");
                ServerHandle server = Tiers.IN_PROCESS.launch(storage);
                var client = HttpClient.newHttpClient()) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            var uri = server.baseUrl().resolve("/api/ai-provider-settings");
            assertThat(client.send(HttpRequest.newBuilder(uri).GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode())
                    .isEqualTo(401);
            var key = "on-prem-provider-key-sentinel";
            var body = JsonWriter.write(Map.of("providerId", "openai-compatible", "model", "model", "apiKey", key));
            var put = HttpRequest.newBuilder(uri).header("Authorization", "Bearer " + control.credential())
                    .header("Content-Type", "application/json").PUT(HttpRequest.BodyPublishers.ofString(body)).build();
            assertThat(client.send(put, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(204);
            var get = HttpRequest.newBuilder(uri).header("Authorization", "Bearer " + control.credential()).GET().build();
            var response = client.send(get, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).isEqualTo(200);
            assertThat(response.body()).contains("model", "configured").doesNotContain(key, "apiKey");
            var delete = HttpRequest.newBuilder(uri).header("Authorization", "Bearer " + control.credential()).DELETE().build();
            assertThat(client.send(delete, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(204);
            assertThat(client.send(get, HttpResponse.BodyHandlers.ofString()).body()).contains("\"configured\":false");
        }
    }
}
