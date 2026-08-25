package io.tapstate.app;

import io.tapstate.control.core.ConnectorCatalogView;
import io.tapstate.control.core.SourceRepresentation;
import io.tapstate.control.core.SourceProjectionService;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.StorePort;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.AfterEach;
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
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The whole control plane assembled over a real store, end to end through HTTP: the assembly root
 * brings up the store, the authentication ports over it, the control-core services and the
 * authenticated {@code /api} face; a caller then bootstraps the first admin over loopback, signs in
 * for a session token, applies an artifact, and reads it back — each guarded step succeeding, and an
 * unauthenticated request refused. This witnesses {@link ControlPlaneConfiguration} wired against a
 * genuine replica-set. Where Docker is absent this aborts on a developer machine and fails in CI,
 * where a skip would be a green build that ran nothing.
 */
@RequiresDocker
class ControlPlaneAssemblyIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private ConfigurableApplicationContext context;

    @AfterEach
    void stop() {
        if (context != null) {
            context.close();
        }
    }

    @Test
    void assemblesAuthenticatedArtifactAndStructuredSourceFlowsOverTheSingletonStore() {
        int port = start();
        RestClient client = RestClient.create("http://localhost:" + port);

        assertThat(context.getBeansOfType(ConnectorCatalogView.class)).hasSize(1);
        assertThat(context.getBeansOfType(SourceRepresentation.class)).hasSize(1);
        assertThat(context.getBeansOfType(SourceProjectionService.class)).hasSize(1);
        assertThat(context.getBeansOfType(StorePort.class)).hasSize(1);
        assertThat(context.getBeansOfType(ArtifactStore.class)).hasSize(1);

        // Anonymous: the verb surface is guarded from the first request.
        HttpStatusCode anonymous = client.get().uri("/api/artifacts")
                .exchange((request, response) -> response.getStatusCode());
        assertThat(anonymous).isEqualTo(HttpStatus.UNAUTHORIZED);

        // Bootstrap the first admin over loopback (the test client is loopback).
        HttpStatusCode bootstrap = client.post().uri("/auth/bootstrap")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "s3cret"))
                .exchange((request, response) -> response.getStatusCode());
        assertThat(bootstrap).isEqualTo(HttpStatus.NO_CONTENT);

        // Sign in for a session token.
        Map<?, ?> login = client.post().uri("/auth/login")
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "s3cret"))
                .retrieve().body(Map.class);
        String token = (String) login.get("token");
        assertThat(token).isNotNull();

        // Apply an artifact with the session token, then read it back as its canonical form.
        HttpStatusCode applied = client.post().uri("/api/artifacts:apply")
                .header("Authorization", "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", List.of(Map.of("content", SOURCE))))
                .exchange((request, response) -> response.getStatusCode());
        assertThat(applied).isEqualTo(HttpStatus.OK);

        Map<?, ?> got = client.get().uri("/api/artifacts/src_ora")
                .header("Authorization", "Bearer " + token)
                .retrieve().body(Map.class);
        assertThat(got.get("id")).isEqualTo("src_ora");
        assertThat(got.get("canonicalForm")).isEqualTo(offlineCanonical(SOURCE));

        // The node-local logs read face is served by the assembled control plane: a pipeline that has logged
        // nothing yields a benign empty tail with a normal 200, not a 404 like a missing observation.
        Map<?, ?> logs = client.get().uri("/api/pipelines/ghost/logs")
                .header("Authorization", "Bearer " + token)
                .retrieve().body(Map.class);
        assertThat(logs.get("pipelineId")).isEqualTo("ghost");
        assertThat((List<?>) logs.get("lines")).isEmpty();

        ResponseEntity<Map> created = client.post().uri("/api/sources")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .body(sourceDraft("db-primary", true))
                .retrieve().toEntity(Map.class);
        assertThat(created.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(created.getHeaders().getLocation().getPath())
                .isEqualTo("/api/sources/frontend_source");
        assertThat(created.getHeaders().getETag()).matches("\"[0-9a-f]{64}\"");
        assertStructuredSource(created.getBody(), "db-primary");

        StorePort store = context.getBean(StorePort.class);
        assertThat(store.artifacts().get("frontend_source"))
                .containsInstanceOf(SourceResource.class);

        ResponseEntity<Map> read = client.get().uri("/api/sources/frontend_source")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .retrieve().toEntity(Map.class);
        assertThat(read.getHeaders().getETag()).isEqualTo(created.getHeaders().getETag());
        assertStructuredSource(read.getBody(), "db-primary");

        ResponseEntity<Map> replaced = client.put().uri("/api/sources/frontend_source")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .header(HttpHeaders.IF_MATCH, created.getHeaders().getETag())
                .contentType(MediaType.APPLICATION_JSON)
                .body(sourceDraft("db-replica", false))
                .retrieve().toEntity(Map.class);
        assertThat(replaced.getHeaders().getETag())
                .matches("\"[0-9a-f]{64}\"")
                .isNotEqualTo(created.getHeaders().getETag());
        assertStructuredSource(replaced.getBody(), "db-replica");
        SourceResource storedReplacement = (SourceResource) store.artifacts()
                .get("frontend_source").orElseThrow();
        assertThat(storedReplacement.config())
                .containsEntry("host", "db-replica")
                .containsEntry("password", "not-returned");

        HttpStatusCode deleted = client.delete().uri("/api/sources/frontend_source")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .header(HttpHeaders.IF_MATCH, replaced.getHeaders().getETag())
                .exchange((request, response) -> response.getStatusCode());
        assertThat(deleted).isEqualTo(HttpStatus.NO_CONTENT);
        assertThat(store.artifacts().get("frontend_source")).isEmpty();

        HttpStatusCode noYamlSourceEndpoint = client.post().uri("/api/sources:apply")
                .header(HttpHeaders.AUTHORIZATION, "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("content", SOURCE))
                .exchange((request, response) -> response.getStatusCode());
        assertThat(noYamlSourceEndpoint).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private static void assertStructuredSource(Map<?, ?> source, String host) {
        assertThat(source.get("id")).isEqualTo("frontend_source");
        assertThat(source.get("connector")).isEqualTo("mysql");
        assertThat(source.containsKey("canonicalForm")).isFalse();
        assertThat(source.containsKey("kind")).isFalse();
        assertThat(source.containsKey("version")).isFalse();
        assertThat(source.containsKey("clearSecrets")).isFalse();
        Map<?, ?> config = (Map<?, ?>) source.get("config");
        assertThat(config.get("host")).isEqualTo(host);
        assertThat(config.containsKey("password")).isFalse();
        assertThat(source.get("configuredSecrets")).isEqualTo(List.of("password"));
        assertThat(String.valueOf(source.get("contentHash"))).matches("[0-9a-f]{64}");
    }

    private static Map<String, Object> sourceDraft(String host, boolean includePassword) {
        Map<String, Object> config = includePassword
                ? Map.of("host", host, "port", 3306, "database", "orders", "username", "app",
                        "password", "not-returned")
                : Map.of("host", host, "port", 3306, "database", "orders", "username", "app");
        return Map.of(
                "id", "frontend_source",
                "connector", "mysql",
                "config", config,
                "mode", "cdc",
                "tables", List.of(Map.of("type", "literal", "name", "orders")),
                "options", Map.of(),
                "experimental", Map.of(),
                "clearSecrets", List.of());
    }

    @Test
    void connectionTestIsWiredThroughToTheConnectorRegistryOverARealStore() {
        int port = start();
        RestClient client = RestClient.create("http://localhost:" + port);

        // Bootstrap the first admin over loopback and sign in; the admin session covers the write verb.
        client.post().uri("/auth/bootstrap").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "s3cret")).retrieve().toBodilessEntity();
        Map<?, ?> login = client.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "s3cret")).retrieve().body(Map.class);
        String token = (String) login.get("token");

        // The whole connection plane is assembled over the real store: control verb -> runtime probe ->
        // adapter-pdk tester -> provisioner -> connector registry. Testing a connector that was never
        // registered resolves to no artifact and comes back as the coded connector-domain refusal, proving
        // the chain reaches the registry rather than a stub.
        Map<?, ?> body = client.post().uri("/api/connections:test")
                .header("Authorization", "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("id", "conn_x", "connectorId", "mysql", "settings", mysqlConnectionSettings()))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
                    return response.bodyTo(Map.class);
                });
        assertThat(body.get("code")).isEqualTo("connector.not-registered");
        assertThat(((Map<?, ?>) body.get("params")).get("connector")).isEqualTo("mysql");
    }

    @Test
    void schemaDiscoveryIsWiredThroughToTheConnectorRegistryOverARealStore() {
        int port = start();
        RestClient client = RestClient.create("http://localhost:" + port);

        client.post().uri("/auth/bootstrap").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "s3cret")).retrieve().toBodilessEntity();
        Map<?, ?> login = client.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "s3cret")).retrieve().body(Map.class);
        String token = (String) login.get("token");

        // The discovery half is assembled over the same chain: control verb -> runtime discovery probe ->
        // adapter-pdk discoverer -> provisioner -> connector registry. Discovering against a connector that
        // was never registered comes back as the coded connector-domain refusal, proving the chain reaches
        // the registry rather than a stub.
        Map<?, ?> body = client.post().uri("/api/connections:discover-schema")
                .header("Authorization", "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("id", "conn_x", "connectorId", "mysql", "settings", mysqlConnectionSettings()))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.INTERNAL_SERVER_ERROR);
                    return response.bodyTo(Map.class);
                });
        assertThat(body.get("code")).isEqualTo("connector.not-registered");
        assertThat(((Map<?, ?>) body.get("params")).get("connector")).isEqualTo("mysql");

        // The read face renders the never-discovered state as a 404, through the same assembled store.
        HttpStatus schemaStatus = (HttpStatus) client.get().uri("/api/connections/conn_x/schema")
                .header("Authorization", "Bearer " + token)
                .exchange((request, response) -> response.getStatusCode());
        assertThat(schemaStatus).isEqualTo(HttpStatus.NOT_FOUND);
    }

    private static Map<String, Object> mysqlConnectionSettings() {
        return Map.of("host", "127.0.0.1", "port", 3306, "database", "orders", "username", "app");
    }

    @Test
    void aDefectiveSeedArtifactNeverBricksTheBoot(@TempDir Path seedDir) throws IOException {
        // A garbage jar in the swept seed directory: the startup sweep contains the failure as a
        // per-artifact warning and the node still comes up serving.
        Files.write(seedDir.resolve("broken.jar"), new byte[] {0x13, 0x37});

        int port = start("tapstate.connectors.seed-dir=" + seedDir);
        RestClient client = RestClient.create("http://localhost:" + port);

        String health = client.get().uri("/healthz").retrieve().body(String.class);
        assertThat(health).isEqualTo("ok");
    }

    @Test
    void connectorRegisterIsWiredThroughToTheIntrospectorAndStoreOverARealStore() throws IOException {
        int port = start();
        RestClient client = RestClient.create("http://localhost:" + port);

        // Bootstrap the first admin over loopback and sign in; the admin session covers the write verb.
        client.post().uri("/auth/bootstrap").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "s3cret")).retrieve().toBodilessEntity();
        Map<?, ?> login = client.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "s3cret")).retrieve().body(Map.class);
        String token = (String) login.get("token");

        // The register verb is assembled over the same chain as production: control verb -> adapter-pdk
        // registrar -> introspector -> connector registry. A valid jar that carries no connector class is
        // refused with the coded connector-domain error, proving the chain reaches the real introspector and
        // registry rather than a stub, and that a bad upload is a client-attributable 400 (not a bare 500).
        String artifact = Base64.getEncoder().encodeToString(emptyJar());
        Map<?, ?> body = client.post().uri("/api/connectors:register")
                .header("Authorization", "Bearer " + token)
                .contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("artifact", artifact))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
                    return response.bodyTo(Map.class);
                });
        assertThat(body.get("code")).isEqualTo("connector.no-connector-class");
    }

    @Test
    void connectorListIsWiredOverARealStoreAndStartsEmptyBeforeRegistration() {
        int port = start();
        RestClient client = RestClient.create("http://localhost:" + port);

        client.post().uri("/auth/bootstrap").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "s3cret")).retrieve().toBodilessEntity();
        Map<?, ?> login = client.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "s3cret")).retrieve().body(Map.class);
        String token = (String) login.get("token");

        // The connector.list read verb is assembled over the real store and exposed through the authenticated
        // /api face. The authoring list contains only connectors whose specs were registered in this control
        // plane, so a fresh store is empty. The register -> derive -> list happy path needs a real connector
        // jar (a gated real-jar test), while the empty-jar register above proves the write chain reaches the
        // introspector.
        Map<?, ?> body = client.get().uri("/api/connectors")
                .header("Authorization", "Bearer " + token)
                .retrieve().body(Map.class);
        List<?> connectors = (List<?>) body.get("connectors");
        assertThat(connectors).isEmpty();

        // The read verb is guarded like every other: an anonymous request is refused.
        HttpStatusCode anonymous = client.get().uri("/api/connectors")
                .exchange((request, response) -> response.getStatusCode());
        assertThat(anonymous).isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    /** A valid, empty jar (a manifest, no classes): it opens and scans, but carries no connector class. */
    private static byte[] emptyJar() throws IOException {
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (JarOutputStream jar = new JarOutputStream(bytes, manifest)) {
            // no entries: a structurally valid jar the introspector opens but finds no connector class in
        }
        return bytes.toByteArray();
    }

    private int start(String... extraProperties) {
        // Each test method runs against its own database on the shared class container, so the one-time
        // first-admin bootstrap of one method never closes the bootstrap channel for the next.
        String database = "assembly_" + Long.toUnsignedString(System.nanoTime(), 16);
        List<String> properties = new ArrayList<>(List.of(
                "server.port=0",
                "tapstate.store.mongo.enabled=true",
                "tapstate.store.mongo.uri=" + REPLICA_SET.getReplicaSetUrl(database),
                // the container speaks plaintext; TLS is opt-in, so no flag is needed here
                "tapstate.store.mongo.server-selection-timeout=5s"));
        properties.addAll(List.of(extraProperties));
        context = new SpringApplicationBuilder(AssemblyApp.class)
                .properties(properties.toArray(String[]::new))
                .run("--server.port=0");
        return ((WebServerApplicationContext) context).getWebServer().getPort();
    }

    /** The offline canonical contract for a draft: the exact bytes the authoring corpus golden locks. */
    private static String offlineCanonical(String draft) {
        return new CanonicalWriter().write(new DslParser().parse(draft));
    }

    private static final String SOURCE = """
            version: tapstate/v1
            kind: source
            id: src_ora
            connector: oracle
            config: { host: 10.20.0.15 }
            """;

    /**
     * The store bridge and the control plane, assembled without the rest of the process (no Hazelcast
     * member) so the integration stays focused on the store-backed control plane over HTTP.
     */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({StoreConfiguration.class, ControlPlaneConfiguration.class})
    static class AssemblyApp {
    }
}
