package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** The specification vocabulary cannot send HTTP OPTIONS and inspect the Allow header. */
@RequiresDocker
class AnAdmittedApiRouteAdvertisesItsAllowedMethodsIT {
    @Test
    void anAdminCanDiscoverMethodsAfterRuntimeAdmission() throws Exception {
        try (var storage = PreparedNativeStore.create();
                var server = InProcessServer.startPrepared(storage);
                var client = HttpClient.newHttpClient()) {
            var control = new ControlPlane(server.baseUrl());
            storage.loginAndAwaitBusiness(control);
            var request = HttpRequest.newBuilder(server.baseUrl().resolve("/api/cluster/members"))
                    .timeout(Duration.ofSeconds(20))
                    .header("Authorization", "Bearer " + control.mintToken("ADMIN"))
                    .method("OPTIONS", HttpRequest.BodyPublishers.noBody()).build();

            var response = client.send(request, HttpResponse.BodyHandlers.ofString());
            assertThat(response.statusCode()).as(response.body()).isEqualTo(200);
            assertThat(response.headers().firstValue("Allow").orElseThrow().split("\\s*,\\s*"))
                    .containsExactlyInAnyOrder("GET", "HEAD", "OPTIONS");
        }
    }
}
