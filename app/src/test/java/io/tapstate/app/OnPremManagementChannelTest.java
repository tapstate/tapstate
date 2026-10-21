package io.tapstate.app;

import io.tapstate.control.core.SetupCapabilityService;
import io.tapstate.core.common.JsonReader;
import java.net.URI;
import java.net.http.*;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import static org.assertj.core.api.Assertions.*;

/** Actual public and loopback connectors; no forwarded header or client-selected port proves management origin. */
class OnPremManagementChannelTest {
    @TempDir Path temporary;

    @Test void onlyTheDedicatedConnectorAndCurrentCapabilityCanUseTheInstallationSurface() throws Exception {
        var files = InstallationFiles.initialize(temporary.resolve("installation"));
        var issued = InstallationCommand.authorize(files, new MongoProperties(), Clock.systemUTC());
        try (var application = new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR",
                "--tapstate.installation.directory=" + files.directory(), "--tapstate.control.auth.jwt-secret=installation-test-signing-key",
                "--tapstate.connectors.seed-dir=" + temporary.resolve("no-seeds"))) {
            int publicPort = ((WebServerApplicationContext)application).getWebServer().getPort();
            String publicUrl = "http://127.0.0.1:" + publicPort;
            String management = files.stage().managementUrl();
            assertThat(management).isNotEqualTo(publicUrl);
            assertThat(application.getBean(StorageRoot.class).phase()).isEqualTo(StorageRoot.Phase.CONFIGURATION_REQUIRED);
            assertThat(application.getBean(StorageRoot.class).runtime().business()).isEmpty();
            try (var http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()) {
                assertThat(post(http, publicUrl + "/api/storage/status", issued, "{}").statusCode()).isIn(401, 403, 503);
                assertThat(post(http, publicUrl + "/installation/first-admin", issued, "{\"username\":\"admin\",\"password\":\"password\"}").statusCode()).isIn(401, 403);
                var status = post(http, management + "/api/storage/status", issued, "{}");
                assertThat(status.statusCode()).withFailMessage(status.body()).isEqualTo(200);
                assertThat(((Map<?,?>)JsonReader.parse(status.body())).get("setupPhase")).isEqualTo("LOCAL_SETUP");
                assertThat(status.body()).doesNotContain(issued.token());
                assertThat(post(http, management + "/auth/bootstrap", issued, "{}").statusCode()).isEqualTo(403);
                assertThat(post(http, management + "/api/sources", issued, "{}").statusCode()).isEqualTo(403);
                assertThat(post(http, management + "/installation/first-admin", issued, "{\"username\":\"admin\",\"password\":\"password\"}").statusCode()).isEqualTo(403);
                var forged = request(management + "/api/storage/status", issued, "{}").header("X-Forwarded-For", "127.0.0.1").build();
                assertThat(http.send(forged, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(403);
                var foreignOrigin = request(management + "/api/storage/status", issued, "{}").header("Origin", "https://unrelated.example").build();
                assertThat(http.send(foreignOrigin, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(403);
                var noCredential = request(management + "/api/storage/status", null, "{}").build();
                assertThat(http.send(noCredential, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(403);
                var next = InstallationCommand.authorize(files, new MongoProperties(), Clock.systemUTC());
                assertThat(next.scope().operationId()).isEqualTo(issued.scope().operationId());
                assertThat(post(http, management + "/api/storage/status", issued, "{}").statusCode()).isEqualTo(403);
                assertThat(post(http, management + "/api/storage/status", next, "{}").statusCode()).isEqualTo(200);
                assertThat(files.configurationUri()).isEmpty();
                assertThat(application.getBeanFactory().containsSingleton("installedConnectorTests")).isFalse();
            }
        }
    }

    private static HttpRequest.Builder request(String url, SetupCapabilityService.Issued issued, String body) {
        var builder = HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(15)).header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));
        if (issued != null) builder.header(OnPremInstallation.CAPABILITY_HEADER, issued.token());
        return builder;
    }
    private static HttpResponse<String> post(HttpClient http, String url, SetupCapabilityService.Issued issued, String body) throws Exception {
        return http.send(request(url, issued, body).build(), HttpResponse.BodyHandlers.ofString());
    }
}
