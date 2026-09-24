package io.tapstate.app;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.control.core.ConnectionTestReport;
import io.tapstate.control.core.SchemaReport;
import io.tapstate.control.core.SourceView;
import io.tapstate.spi.store.StorePort;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/** Exercises a saved Atlas Source through the actual HTTP, Mongo store, and PDK assembly. */
class ManagedAtlasSourceHttpIT {

    @Test
    void atlasUriSourceCanBeCreatedTestedDiscoveredUpdatedAndDeleted(@TempDir Path files) throws IOException {
        String uri = System.getenv("TAPSTATE_ATLAS_TEST_URI");
        assumeTrue(uri != null && !uri.isBlank(), "a controlled Atlas URI is required");
        verifyHttpFlow(files, uri, false);
    }

    @Test
    void atlasStandardSourceCanBeCreatedTestedDiscoveredUpdatedAndDeleted(@TempDir Path files) throws IOException {
        String uri = System.getenv("TAPSTATE_ATLAS_STANDARD_URI");
        assumeTrue(uri != null && !uri.isBlank(), "a resolved-host Atlas URI is required");
        verifyHttpFlow(files, uri, true);
    }

    private static void verifyHttpFlow(Path files, String baseUri, boolean standard) throws IOException {
        String artifact = System.getProperty("tapstate.pdk.it.atlasJar");
        assumeTrue(artifact != null && !artifact.isBlank() && Files.isRegularFile(Path.of(artifact)),
                "the verified Atlas connector jar is required");

        String database = "ts_plan_http_" + UUID.randomUUID().toString().substring(0, 12);
        String atlasUri = inDatabase(baseUri, database);
        Map<String, Object> settings = standard ? standardSettings(atlasUri) : Map.of("isUri", true, "uri", atlasUri);
        Path seed = Files.createDirectories(files.resolve("connectors"));
        Files.copy(Path.of(artifact), seed.resolve("mongodb-atlas-connector.jar"));
        Path plugins = Files.createDirectories(files.resolve("plugins"));

        try (MongoDBContainer metadata = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))) {
            metadata.start();
            try (MongoClient atlas = MongoClients.create(atlasUri);
                 Closeable cleanup = () -> dropTestDatabase(atlasUri, database)) {
                atlas.getDatabase(database).getCollection("probe")
                        .insertOne(new Document("_id", "http-witness").append("value", 1));

                try (ConfigurableApplicationContext context = new SpringApplicationBuilder(AssemblyApp.class)
                        .properties(
                                "tapstate.store.mongo.enabled=true",
                                "tapstate.store.mongo.uri=" + metadata.getReplicaSetUrl("http_metadata"),
                                "tapstate.store.mongo.server-selection-timeout=5s",
                                "tapstate.connectors.seed-dir=" + seed,
                                "tapstate.connectors.plugins-dir=" + plugins)
                        .run("--server.address=127.0.0.1", "--server.port=0",
                                "--logging.level.root=ERROR")) {
                    assertThat(context.getBean(StorePort.class).connectors().list())
                            .extracting(registration -> registration.connectorId())
                            .containsExactly("mongodb-atlas");
                    int port = ((WebServerApplicationContext) context).getWebServer().getPort();
                    RestClient client = RestClient.create("http://127.0.0.1:" + port);
                    String token = login(client);
                    String id = "atlas_http_source";

                    ResponseEntity<Void> created = client.post().uri("/api/sources")
                            .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(sourceDraft(id, settings, "initial"))
                            .retrieve().toBodilessEntity();
                    assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
                    assertThat(created.getHeaders().getETag()).matches("\"[0-9a-f]{64}\"");
                    SourceView savedSource = client.get().uri("/api/sources/" + id)
                            .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                            .retrieve().body(SourceView.class);
                    assertThat(savedSource).isNotNull();
                    assertThat(savedSource.connector()).isEqualTo("mongodb-atlas");
                    if (standard) {
                        assertThat(savedSource.config()).doesNotContainKey("password");
                        assertThat(savedSource.configuredSecrets()).contains("password");
                    }

                    Map<String, Object> connection = Map.of(
                            "id", id,
                            "connectorId", "mongodb-atlas",
                            "settings", savedSource.config());
                    ConnectionTestReport tested = client.post().uri("/api/connections:test")
                            .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                            .contentType(MediaType.APPLICATION_JSON).body(connection)
                            .retrieve().body(ConnectionTestReport.class);
                    assertThat(tested).isNotNull();
                    assertThat(tested.outcome()).isEqualTo(ConnectionTestReport.Outcome.PASSED);
                    assertThat(tested.checks()).isNotEmpty();

                    SchemaReport discovered = client.post().uri("/api/connections:discover-schema")
                            .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                            .contentType(MediaType.APPLICATION_JSON).body(connection)
                            .retrieve().body(SchemaReport.class);
                    assertThat(discovered).isNotNull();
                    assertThat(discovered.tables()).extracting(SchemaReport.Table::name).contains("probe");
                    SchemaReport saved = client.get().uri("/api/sources/" + id + "/schema")
                            .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                            .retrieve().body(SchemaReport.class);
                    assertThat(saved).isNotNull();
                    assertThat(saved.tables()).extracting(SchemaReport.Table::name).contains("probe");

                    if (standard) {
                        Map<String, Object> badSettings = new LinkedHashMap<>(savedSource.config());
                        badSettings.put("password", "invalid-plan-test-password");
                        ConnectionTestReport denied = client.post().uri("/api/connections:test")
                                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(Map.of("id", id, "connectorId", "mongodb-atlas",
                                        "settings", badSettings))
                                .retrieve().body(ConnectionTestReport.class);
                        assertThat(denied).isNotNull();
                        assertThat(denied.outcome()).isEqualTo(ConnectionTestReport.Outcome.FAILED);
                        assertThat(denied.checks()).anyMatch(check ->
                                check.status() == ConnectionTestReport.Check.Status.FAILED
                                        && check.message() != null && !check.message().isBlank());
                        assertThat(String.valueOf(denied).contains("invalid-plan-test-password")).isFalse();
                        assertThat(String.valueOf(denied).contains(settings.get("password").toString()))
                                .isFalse();

                        Map<String, Object> unreachableSettings = new LinkedHashMap<>(savedSource.config());
                        unreachableSettings.put("host", "127.0.0.1:1");
                        unreachableSettings.put("additionalString",
                                "authSource=admin&tls=true&serverSelectionTimeoutMS=1000&connectTimeoutMS=1000");
                        ConnectionTestReport unreachable = client.post().uri("/api/connections:test")
                                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                                .contentType(MediaType.APPLICATION_JSON)
                                .body(Map.of("id", id, "connectorId", "mongodb-atlas",
                                        "settings", unreachableSettings))
                                .retrieve().body(ConnectionTestReport.class);
                        assertThat(unreachable).isNotNull();
                        assertThat(unreachable.outcome()).isEqualTo(ConnectionTestReport.Outcome.FAILED);
                        assertThat(unreachable.checks()).anyMatch(check ->
                                check.status() == ConnectionTestReport.Check.Status.FAILED
                                        && check.message() != null && !check.message().isBlank());
                        assertThat(String.valueOf(unreachable).contains(settings.get("password").toString()))
                                .isFalse();
                    }

                    ResponseEntity<Void> replaced = client.put().uri("/api/sources/" + id)
                            .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                            .header(HttpHeaders.IF_MATCH, created.getHeaders().getETag())
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(sourceDraft(id, settings, "updated"))
                            .retrieve().toBodilessEntity();
                    assertThat(replaced.getStatusCode()).isEqualTo(HttpStatus.OK);
                    assertThat(replaced.getHeaders().getETag()).isNotEqualTo(created.getHeaders().getETag());
                    HttpStatusCode deleted = client.delete().uri("/api/sources/" + id)
                            .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                            .header(HttpHeaders.IF_MATCH, replaced.getHeaders().getETag())
                            .exchange((request, response) -> response.getStatusCode());
                    assertThat(deleted).isEqualTo(HttpStatus.NO_CONTENT);
                }
            }
        }
    }

    private static void dropTestDatabase(String uri, String database) {
        RuntimeException lastFailure = null;
        for (int attempt = 0; attempt < 2; attempt++) {
            try (MongoClient cleanup = MongoClients.create(uri)) {
                cleanup.getDatabase(database).drop();
                assertThat(cleanup.listDatabaseNames().into(new ArrayList<>())).doesNotContain(database);
                return;
            } catch (RuntimeException failure) {
                lastFailure = failure;
            }
        }
        throw lastFailure;
    }

    private static String login(RestClient client) {
        client.post().uri("/auth/bootstrap").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "local-test-password"))
                .retrieve().toBodilessEntity();
        Map<?, ?> login = client.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "local-test-password"))
                .retrieve().body(Map.class);
        return (String) login.get("token");
    }

    private static Map<String, Object> sourceDraft(String id, Map<String, Object> settings, String description) {
        return Map.of(
                "id", id,
                "metadata", Map.of("description", description),
                "connector", "mongodb-atlas",
                "config", settings,
                "mode", "cdc",
                "tables", List.of(Map.of("type", "literal", "name", "probe")),
                "options", Map.of(),
                "experimental", Map.of(),
                "clearSecrets", List.of());
    }

    private static Map<String, Object> standardSettings(String uri) {
        ConnectionString connection = new ConnectionString(uri);
        assertThat(connection.getCredential()).isNotNull();
        assertThat(connection.getCredential().getPassword()).isNotNull();
        int optionStart = uri.indexOf('?');
        return Map.of(
                "isUri", false,
                "host", String.join(",", connection.getHosts()),
                "database", connection.getDatabase(),
                "user", connection.getCredential().getUserName(),
                "password", new String(connection.getCredential().getPassword()),
                "additionalString", optionStart < 0 ? "" : uri.substring(optionStart + 1));
    }

    private static String inDatabase(String uri, String database) {
        int schemeEnd = uri.indexOf("://");
        int slash = schemeEnd < 0 ? -1 : uri.indexOf('/', schemeEnd + 3);
        if (slash < 0) {
            throw new IllegalArgumentException("Atlas test URI must include a database path");
        }
        int options = uri.indexOf('?', slash);
        return uri.substring(0, slash + 1) + database
                + (options < 0 ? "" : uri.substring(options));
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({StoreConfiguration.class, ControlPlaneConfiguration.class})
    static class AssemblyApp {
    }
}
