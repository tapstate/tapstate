package io.tapstate.e2e;

import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.spi.store.ContentHash;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.jar.JarFile;

import static org.assertj.core.api.Assertions.assertThat;

/** The specification vocabulary cannot alter a staged connector JAR after the server starts. */
@RequiresDocker
class AChangedConnectorCacheIsRefusedIT {

    @Test
    void aConnectionTestRefusesAReadableCacheChangedAfterStartup(@TempDir Path directory) throws Exception {
        Path seed = E2eConnectorJar.buildInto(directory);
        byte[] artifact = Files.readAllBytes(seed);
        String hash = ContentHash.of(artifact);
        Path cacheDir = directory.resolve("plugins");
        Path cached = cacheDir.resolve(hash + ".jar");
        Path source = Files.createDirectory(directory.resolve("source"));
        Files.writeString(source.resolve("orders.csv"), "id,amount\n7,70.00\n");
        Map<String, Object> settings = Map.of("uri", source.toString());
        String database = "e2e_cache_integrity_" + UUID.randomUUID().toString().replace("-", "");

        try (ServerHandle server = InProcessServer.start(SharedMongo.replicaSetUrl(database), database + "_operator",
                List.of("--" + ServerHandle.PLUGINS_DIRECTORY_SETTING + "=" + cacheDir))) {
            ControlPlane control = new ControlPlane(server.baseUrl());
            control.bootstrapAndLogin("e2e", "e2e-password");
            control.registerConnector(E2eConnectorJar.CONNECTOR_ID, artifact);
            assertThat(control.testConnection("orders", E2eConnectorJar.CONNECTOR_ID, settings).outcome())
                    .isEqualTo("PASSED");
            assertThat(Files.readAllBytes(cached)).isEqualTo(artifact);

            Files.write(cached, new byte[] {0}, StandardOpenOption.APPEND);

            assertThat(Files.size(cached)).isEqualTo(artifact.length + 1L);
            assertThat(ContentHash.of(Files.readAllBytes(cached))).isNotEqualTo(hash);
            try (JarFile jar = new JarFile(cached.toFile())) {
                var spec = jar.getJarEntry("e2e-file-spec.json");
                assertThat(spec).isNotNull();
                try (var stream = jar.getInputStream(spec)) {
                    assertThat(stream.readAllBytes()).isNotEmpty();
                }
            }
            var request = HttpRequest.newBuilder(server.baseUrl().resolve("/api/connections:test"))
                    .timeout(Duration.ofSeconds(30))
                    .header("Authorization", "Bearer " + control.credential())
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(JsonWriter.write(Map.of(
                            "id", "orders", "connectorId", E2eConnectorJar.CONNECTOR_ID, "settings", settings))))
                    .build();
            try (HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build()) {
                HttpResponse<String> response = http.send(request, HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode()).isEqualTo(500);
                assertThat(JsonReader.parse(response.body())).isEqualTo(Map.of(
                        "code", "connector.load-failed",
                        "params", Map.of("connector", E2eConnectorJar.CONNECTOR_ID),
                        "message", "Connector 'e2e_file' could not be loaded."));
                assertThat(response.body()).doesNotContain(directory.toString(), hash, "cause", "stackTrace");
            }
            assertThat(Files.readAllBytes(seed)).isEqualTo(artifact);
            assertThat(Files.size(cached)).isEqualTo(artifact.length + 1L);
            assertThat(control.healthy()).isTrue();
        }
    }
}
