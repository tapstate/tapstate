package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.bson.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The declarative cases cannot drive the browser sign-in exchange with a controlled issuer. */
@RequiresDocker
@RequiresCloudReleaseInputs
class CloudSignInLinkDiagnosticsIT {
    @Test
    void aConsumedSignInLinkDirectsTheUserToStartAgain() throws Exception {
        try (PreparedNativeStore storage = PreparedNativeStore.createCloud();
                AtlasRuntime runtime = new AtlasRuntime(storage);
                ServerHandle server = runtime.launch(Tiers.REAL_PROCESS);
                HttpClient client = HttpClient.newHttpClient()) {
            String code = runtime.issueExchangeCode(server);
            HttpRequest request = HttpRequest.newBuilder(server.baseUrl().resolve("/auth/exchange?code=" + code))
                    .header("Accept-Language", "en")
                    .timeout(Duration.ofSeconds(10))
                    .GET().build();

            var success = client.send(request, HttpResponse.BodyHandlers.discarding());
            assertThat(success.statusCode()).isEqualTo(302);
            assertThat(success.headers().firstValue("Set-Cookie")).isPresent();

            var rejected = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(rejected.statusCode()).isEqualTo(401);
            assertThat(Document.parse(rejected.body()))
                    .containsEntry("code", "TS0210035")
                    .containsEntry("message", "This sign-in link is no longer valid.")
                    .containsEntry("solution", "Return to the Cloud Console and select Enter for the cluster again.");
            assertThat(rejected.headers().firstValue("Set-Cookie")).isEmpty();
            assertThat(rejected.headers().firstValue("Location")).isEmpty();
            assertThat(rejected.body()).doesNotContain(code);

            String freshCode = runtime.issueExchangeCode(server);
            var retry = client.send(HttpRequest.newBuilder(server.baseUrl().resolve("/auth/exchange?code=" + freshCode))
                    .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.discarding());
            assertThat(retry.statusCode()).isEqualTo(302);
            assertThat(retry.headers().firstValue("Set-Cookie")).isPresent();
        }
    }
}
