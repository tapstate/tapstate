package io.tapstate.e2e;

import io.tapstate.app.Bootstrap;
import io.tapstate.core.common.JsonReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;

import static org.assertj.core.api.Assertions.assertThat;

/** The specification vocabulary cannot request an unmapped HTTP route before storage setup. */
class AnUnknownApiRouteIsMissingBeforeRuntimeAdmissionIT {
    @Test void missingRoutesAreJson404WhileKnownRuntimeOperationsRemainUnavailable() throws Exception {
        try (var context = new SpringApplicationBuilder(Bootstrap.class).run(
                "--spring.main.web-application-type=servlet", "--server.address=127.0.0.1", "--server.port=0", "--tapstate.store.mongo.uri=",
                "--SDK_STATUS_SENDER_ENABLED=false", "--tapstate.control.auth.jwt-secret=missing-route-test-key");
                var client = HttpClient.newHttpClient()) {
            var base = URI.create("http://127.0.0.1:"
                    + ((WebServerApplicationContext) context).getWebServer().getPort());
            assertThat(get(client, base, "/healthz").statusCode()).isEqualTo(200);
            var known = get(client, base, "/api/cluster/members");
            assertThat(known.statusCode()).as(known.body()).isEqualTo(503);
            assertThat(((Map<?, ?>) JsonReader.parse(known.body())).get("code")).isEqualTo("control.storage-unavailable");

            var missing = get(client, base, "/api/runtime-image-smoke-missing");
            assertThat(missing.statusCode()).as(missing.body()).isEqualTo(404);
            assertThat(missing.headers().firstValue("Content-Type").orElse("")).startsWith("application/json");
            assertThat(JsonReader.parse(missing.body())).isInstanceOf(Map.class);
        }
    }

    private static HttpResponse<String> get(HttpClient client, URI base, String path) throws Exception {
        return client.send(HttpRequest.newBuilder(base.resolve(path)).GET().build(), HttpResponse.BodyHandlers.ofString());
    }
}
