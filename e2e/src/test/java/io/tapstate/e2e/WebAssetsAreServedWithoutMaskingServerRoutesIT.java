package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.testsupport.DockerGate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** The server serves the Web entry point and assets without rewriting a real API 404. */
class WebAssetsAreServedWithoutMaskingServerRoutesIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void servesRootDeepRouteAndAssetWhileKeepingHealthAndApiResponses() throws Exception {
        try (InProcessServer server = InProcessServer.start(SharedMongo.replicaSetUrl("e2e_web_assets"))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("web-assets", "web-assets-password");

            HttpClient client = HttpClient.newHttpClient();
            HttpResponse<String> root = get(client, server.baseUrl(), "/");
            HttpResponse<String> deepRoute = get(client, server.baseUrl(), "/pipelines/orders/edit/api");
            HttpResponse<String> asset = get(client, server.baseUrl(), "/assets/web-packaging.js");
            HttpResponse<String> health = get(client, server.baseUrl(), "/healthz");
            HttpResponse<String> apiMissing = client.send(
                    HttpRequest.newBuilder(server.baseUrl().resolve("/api/pipelines/web-packaging-missing"))
                            .header("Authorization", "Bearer " + control.credential())
                            .GET()
                            .build(),
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));

            assertThat(root.statusCode()).isEqualTo(200);
            assertThat(root.headers().firstValue("content-type").orElse(""))
                    .startsWith("text/html");
            assertThat(root.body()).contains("Tapstate Web packaging fixture");
            assertThat(deepRoute.statusCode()).isEqualTo(200);
            assertThat(deepRoute.body()).isEqualTo(root.body());
            assertThat(asset.statusCode()).isEqualTo(200);
            assertThat(asset.body()).contains("webPackagingFixture");
            assertThat(health.statusCode()).isEqualTo(200);
            assertThat(health.body()).isEqualTo("ok");
            assertThat(apiMissing.statusCode()).isEqualTo(404);
            assertThat(apiMissing.headers().firstValue("content-type").orElse(""))
                    .startsWith("application/json");
            assertThat(apiMissing.body()).doesNotContain("Tapstate Web packaging fixture");
            assertThat(JsonReader.parse(apiMissing.body())).isInstanceOf(Map.class);
        }
    }

    private static HttpResponse<String> get(HttpClient client, URI baseUrl, String path) throws Exception {
        return client.send(
                HttpRequest.newBuilder(baseUrl.resolve(path)).GET().build(),
                HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    }
}
