package io.tapstate.e2e;

import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** An image from the packaged web resources is served anonymously while API reads stay protected. */
@RequiresDocker
class APackagedRootImagesArePublicWithoutAuthenticationIT {
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    @Test
    void aNewRootImageReachesTheStaticHandlerWithoutOpeningTheApi() throws Exception {
        try (PreparedNativeStore storage = PreparedNativeStore.create();
             ServerHandle server = Tiers.IN_PROCESS.launch(storage)) {
            var image = http.send(HttpRequest.newBuilder(server.baseUrl().resolve("/future-brand-image.svg"))
                            .timeout(Duration.ofSeconds(10)).header("Accept", "image/svg+xml").GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(image.statusCode()).isEqualTo(200);
            assertThat(image.headers().firstValue("Content-Type")).contains("image/svg+xml");
            assertThat(image.body()).contains("e2e-public-image");

            var api = http.send(HttpRequest.newBuilder(server.baseUrl().resolve("/api/sources"))
                            .timeout(Duration.ofSeconds(10)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            assertThat(api.statusCode()).isEqualTo(401);
            assertThat(api.body()).contains("control.unauthenticated");
        }
    }
}
