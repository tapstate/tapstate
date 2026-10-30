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
        var web = java.nio.file.Files.createDirectories(temporary.resolve("web"));
        java.nio.file.Files.createDirectories(web.resolve("assets"));
        java.nio.file.Files.writeString(web.resolve("index.html"), "<!doctype html><title>Storage setup</title><div id=\"root\"></div>");
        java.nio.file.Files.writeString(web.resolve("assets/setup.js"), "document.title = 'Storage setup';");
        var issued = InstallationCommand.authorize(files, new MongoProperties(), Clock.systemUTC());
        var rootLogger = (ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        var originalLevel = rootLogger.getLevel();
        try (var application = new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR",
                "--tapstate.installation.require-credential=true",
                "--spring.web.resources.static-locations=" + web.toUri(),
                "--tapstate.installation.directory=" + files.directory(), "--tapstate.control.auth.jwt-secret=installation-test-signing-key",
                "--tapstate.connectors.seed-dir=" + temporary.resolve("no-seeds"))) {
            int publicPort = ((WebServerApplicationContext)application).getWebServer().getPort();
            String publicUrl = "http://127.0.0.1:" + publicPort;
            String management = files.stage().managementUrl();
            assertThat(management).isNotEqualTo(publicUrl);
            assertThat(application.getBean(StorageRoot.class).phase()).isEqualTo(StorageRoot.Phase.CONFIGURATION_REQUIRED);
            assertThat(application.getBean(StorageRoot.class).runtime().business()).isEmpty();
            try (var http = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build()) {
                var entry = http.send(HttpRequest.newBuilder(URI.create(publicUrl + "/auth/storage-entry")).GET().build(), HttpResponse.BodyHandlers.ofString());
                assertThat(entry.statusCode()).isEqualTo(200);
                assertThat((Map<?,?>)JsonReader.parse(entry.body())).isEqualTo(Map.of("mode", "OP", "entry", "SETUP_REQUIRED", "management", false));
                var managedEntry = http.send(HttpRequest.newBuilder(URI.create(management + "/auth/storage-entry")).GET().build(), HttpResponse.BodyHandlers.ofString());
                assertThat(managedEntry.statusCode()).isEqualTo(200);
                assertThat((Map<?,?>)JsonReader.parse(managedEntry.body())).isEqualTo(Map.of("mode", "OP", "entry", "SETUP_REQUIRED", "management", true));
                var page = http.send(HttpRequest.newBuilder(URI.create(management + "/installation")).header("Accept", "text/html").GET().build(), HttpResponse.BodyHandlers.ofString());
                assertThat(page.statusCode()).withFailMessage(page.body()).isEqualTo(200);
                assertThat(page.body()).contains("id=\"root\"").doesNotContain(issued.token());
                assertThat(http.send(HttpRequest.newBuilder(URI.create(management + "/assets/setup.js")).GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(200);
                assertThat(http.send(HttpRequest.newBuilder(URI.create(management + "/api/sources")).GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(403);
                assertThat(http.send(HttpRequest.newBuilder(URI.create(management + "/auth/storage-entry")).header("Origin", "https://unrelated.example").GET().build(), HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(403);
                assertThat(post(http, publicUrl + "/api/storage/status", issued, "{}").statusCode()).isIn(401, 403, 503);
                assertThat(post(http, publicUrl + "/installation/first-admin", issued, "{\"username\":\"admin\",\"password\":\"password\"}").statusCode()).isIn(401, 403);
                assertPublicReverseProxyCannotGrantManagement(http, publicUrl, issued);
                var status = post(http, management + "/api/storage/status", issued, "{}");
                assertThat(status.statusCode()).withFailMessage(status.body()).isEqualTo(200);
                assertThat(((Map<?,?>)JsonReader.parse(status.body())).get("setupPhase")).isEqualTo("LOCAL_SETUP");
                assertThat(status.body()).doesNotContain(issued.token());
                assertThat(post(http, management + "/auth/bootstrap", issued, "{}").statusCode()).isEqualTo(403);
                assertThat(post(http, management + "/api/sources", issued, "{}").statusCode()).isEqualTo(403);
                assertThat(post(http, management + "/installation/first-admin", issued, "{\"username\":\"admin\",\"password\":\"password\"}").statusCode()).isEqualTo(403);
                for (String header : java.util.List.of("Forwarded", "X-Forwarded-For", "X-Forwarded-Host")) {
                    var forged = request(management + "/api/storage/status", issued, "{}").header(header, "127.0.0.1").build();
                    assertThat(http.send(forged, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(403);
                }
                var crossSite = request(management + "/api/storage/status", issued, "{}").header("Sec-Fetch-Site", "cross-site").build();
                assertThat(http.send(crossSite, HttpResponse.BodyHandlers.ofString()).statusCode()).isEqualTo(403);
                try (var socket = new java.net.Socket("127.0.0.1", URI.create(management).getPort())) {
                    socket.setSoTimeout(15000);
                    String payload = "POST /api/storage/status HTTP/1.1\r\nHost: unrelated.example\r\n"
                            + OnPremInstallation.CAPABILITY_HEADER + ": " + issued.token()
                            + "\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}";
                    socket.getOutputStream().write(payload.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                    var lines = new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream(), java.nio.charset.StandardCharsets.US_ASCII));
                    assertThat(lines.readLine()).contains(" 403 ");
                }
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
        } finally {
            ((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)).setLevel(originalLevel);
        }
        assertThat(((ch.qos.logback.classic.Logger) org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME)).getLevel())
                .isEqualTo(originalLevel);
    }

    private static void assertPublicReverseProxyCannotGrantManagement(HttpClient http, String upstream,
            SetupCapabilityService.Issued issued) throws Exception {
        var forwardedCalls = new java.util.concurrent.atomic.AtomicInteger();
        var proxy = com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1", 0), 0);
        proxy.createContext("/", exchange -> {
            try (exchange) {
                String body = new String(exchange.getRequestBody().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
                var forwarded = request(upstream + exchange.getRequestURI().getPath(), issued, body);
                String suppliedForwarding = exchange.getRequestHeaders().getFirst("X-Forwarded-For");
                if (suppliedForwarding != null) forwarded.header("X-Forwarded-For", suppliedForwarding);
                try {
                    var response = http.send(forwarded.build(), HttpResponse.BodyHandlers.ofByteArray());
                    forwardedCalls.incrementAndGet();
                    exchange.sendResponseHeaders(response.statusCode(), response.body().length);
                    exchange.getResponseBody().write(response.body());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt(); throw new java.io.IOException(interrupted);
                }
            }
        });
        proxy.start();
        try {
            String base = "http://127.0.0.1:" + proxy.getAddress().getPort();
            assertThat(post(http, base + "/api/storage/status", issued, "{}").statusCode()).isIn(401, 403, 503);
            var spoofed = request(base + "/installation/first-admin", issued, "{}").header("X-Forwarded-For", "127.0.0.1").build();
            assertThat(http.send(spoofed, HttpResponse.BodyHandlers.ofString()).statusCode()).isIn(401, 403);
            assertThat(forwardedCalls.get()).isEqualTo(2);
        } finally { proxy.stop(0); }
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
