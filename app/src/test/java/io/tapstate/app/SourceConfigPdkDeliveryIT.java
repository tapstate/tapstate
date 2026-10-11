package io.tapstate.app;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.adapters.pdk.PdkConnectionTester;
import io.tapstate.adapters.pdk.PdkSchemaDiscoverer;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CloudLoginIdentity;
import io.tapstate.control.core.CloudSessionService;
import io.tapstate.control.core.ClusterIdentityService;
import io.tapstate.control.core.ConnectionTestReport;
import io.tapstate.control.core.SchemaReport;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.SourceView;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.runtime.probe.ConnectionProbe;
import io.tapstate.runtime.probe.PipelinePreviewProbe;
import io.tapstate.runtime.probe.DelegatingConnectionProbe;
import io.tapstate.runtime.probe.DelegatingSchemaDiscoveryProbe;
import io.tapstate.runtime.probe.SchemaDiscoveryProbe;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionStore;
import io.tapstate.spi.store.ConnectionTester;
import io.tapstate.spi.store.SchemaDiscoverer;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.json.JsonMapper;

import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Real locked PDK JARs consume decrypted saved configs against local Mongo, not a Cloud Atlas service. */
@RequiresDocker
class SourceConfigPdkDeliveryIT {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String USER = "pdk-memory-user";
    private static final String SECRET = "pdk-memory-config-password-sentinel";
    private static final String WRONG = "pdk-memory-wrong-password-sentinel";

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @TempDir Path work;

    @ParameterizedTest
    @EnumSource(CloudRuntimeSettings.Mode.class)
    void realPdkProbesAndDiscoveryRecoverStoredCredentialsInBothModesAndAfterRestart(
            CloudRuntimeSettings.Mode mode) {
        String metadata = "pdk_config_" + Long.toUnsignedString(System.nanoTime(), 16);
        String userData = metadata + "_user_data";
        String metadataUri = MONGO.getReplicaSetUrl(metadata);
        String replicaSet;
        try (MongoClient raw = MongoClients.create(metadataUri)) {
            replicaSet = raw.getDatabase("admin").runCommand(new Document("hello", 1)).getString("setName");
            assertThat(replicaSet).isNotBlank();
            raw.getDatabase("admin").runCommand(new Document("createUser", USER + metadata)
                    .append("pwd", SECRET).append("roles", List.of(
                            new Document("role", "readWrite").append("db", userData),
                            new Document("role", "read").append("db", "local"),
                            new Document("role", "clusterMonitor").append("db", "admin"))));
            raw.getDatabase(userData).getCollection("encrypted_probe")
                    .insertOne(new Document("_id", "real-pdk-witness").append("marker", "local-user-data-only"));
        }
        String options = "authSource=admin&replicaSet=" + replicaSet
                + "&directConnection=true&serverSelectionTimeoutMS=2000&connectTimeoutMS=1000";
        String host = MONGO.getHost() + ":" + MONGO.getMappedPort(27017);
        String uri = "mongodb://" + USER + metadata + ":" + SECRET + "@" + host + "/" + userData + "?" + options;
        Map<String, Map<String, Object>> configurations = new LinkedHashMap<>();
        configurations.put("memory_mongodb_uri", Map.of("uri", uri));
        configurations.put("memory_atlas_uri", Map.of("isUri", true, "uri", uri));
        configurations.put("memory_atlas_standard", Map.of("isUri", false, "host", host, "database", userData,
                "user", USER + metadata, "password", SECRET, "additionalString", options));
        Map<String, Document> originals = new LinkedHashMap<>();
        String retainedCookie;
        try (Fixture fixture = start(mode, metadata, metadataUri, null, true)) {
            assertRealPdk(fixture);
            for (var entry : configurations.entrySet()) {
                String id = entry.getKey();
                String connector = connector(id);
                var created = fixture.client.post().uri("/api/sources").contentType(MediaType.APPLICATION_JSON)
                        .body(Map.of("id", id, "connector", connector, "config", entry.getValue()))
                        .exchange((request, response) -> {
                            assertThat(response.getStatusCode().value()).isEqualTo(201);
                            return response.bodyTo(SourceView.class);
                        });
                assertThat(created).isNotNull();
                safe(JSON.writeValueAsString(created));
                originals.put(id, stored(fixture, id));
                probe(fixture, id, connector);
                assertThat(stored(fixture, id)).isEqualTo(originals.get(id));
            }
            SourceView saved = fixture.client.get().uri("/api/sources/memory_atlas_standard")
                    .retrieve().body(SourceView.class);
            Map<String, Object> bad = new LinkedHashMap<>(saved.config());
            bad.put("password", WRONG);
            String failed = fixture.client.post().uri("/api/connections:test").contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("id", saved.id(), "connectorId", "mongodb-atlas", "settings", bad))
                    .retrieve().body(String.class);
            safe(failed);
            assertThat(JSON.readValue(failed, ConnectionTestReport.class).outcome())
                    .as("an explicit wrong credential reaches the real PDK, rather than being silently restored")
                    .isEqualTo(ConnectionTestReport.Outcome.FAILED);
            assertThat(stored(fixture, saved.id())).isEqualTo(originals.get(saved.id()));
            retainedCookie = mode == CloudRuntimeSettings.Mode.CLOUD ? fixture.credential : null;
        }
        try (Fixture restarted = start(mode, metadata, metadataUri, retainedCookie, false)) {
            assertRealPdk(restarted);
            for (var entry : originals.entrySet()) {
                assertThat(stored(restarted, entry.getKey())).isEqualTo(entry.getValue());
                probe(restarted, entry.getKey(), connector(entry.getKey()));
                assertThat(stored(restarted, entry.getKey())).isEqualTo(entry.getValue());
            }
            var database = restarted.raw.getDatabase(metadata);
            safe(SystemCollections.CONNECTION_TEST_RESULTS.on(database).find().into(new ArrayList<>()).toString());
            safe(SystemCollections.SOURCE_SCHEMAS.on(database).find().into(new ArrayList<>()).toString());
            safe(SystemCollections.AUDIT.on(database).find().into(new ArrayList<>()).toString());
        }
    }

    private static String connector(String id) { return id.contains("atlas") ? "mongodb-atlas" : "mongodb"; }

    private static void assertRealPdk(Fixture fixture) {
        assertThat(fixture.context.getBean(ConnectionTester.class)).isInstanceOf(PdkConnectionTester.class);
        assertThat(fixture.context.getBean(SchemaDiscoverer.class)).isInstanceOf(PdkSchemaDiscoverer.class);
        assertThat(fixture.context.getBean(ConnectionProbe.class)).isInstanceOf(DelegatingConnectionProbe.class);
        assertThat(fixture.context.getBean(SchemaDiscoveryProbe.class)).isInstanceOf(DelegatingSchemaDiscoveryProbe.class);
    }

    private static void probe(Fixture fixture, String id, String connector) {
        SourceView view = fixture.client.get().uri("/api/sources/" + id).retrieve().body(SourceView.class);
        assertThat(view).isNotNull();
        safe(JSON.writeValueAsString(view));
        if (!id.endsWith("standard")) assertThat(JSON.writeValueAsString(view)).doesNotContain(USER);
        Map<String, Object> request = Map.of("id", id, "connectorId", connector, "settings", view.config());
        String tested = fixture.client.post().uri("/api/connections:test").contentType(MediaType.APPLICATION_JSON)
                .body(request).retrieve().body(String.class);
        safe(tested);
        ConnectionTestReport report = JSON.readValue(tested, ConnectionTestReport.class);
        assertThat(report.outcome()).as("actual PDK connection checks for %s: %s", id, tested)
                .isEqualTo(ConnectionTestReport.Outcome.PASSED);
        assertThat(report.checks()).isNotEmpty();
        String discovered = fixture.client.post().uri("/api/connections:discover-schema")
                .contentType(MediaType.APPLICATION_JSON).body(request).retrieve().body(String.class);
        safe(discovered);
        SchemaReport schema = JSON.readValue(discovered, SchemaReport.class);
        var table = schema.tables().stream().filter(value -> value.name().equals("encrypted_probe"))
                .findFirst().orElseThrow();
        assertThat(table.fields()).extracting(SchemaReport.Field::name).contains("_id", "marker");
        String storedSchema = fixture.client.get().uri("/api/sources/" + id + "/schema").retrieve().body(String.class);
        safe(storedSchema);
        assertThat(JSON.readValue(storedSchema, SchemaReport.class).tables())
                .extracting(SchemaReport.Table::name).contains("encrypted_probe");
        String generic = fixture.client.get().uri("/api/artifacts/" + id).retrieve().body(String.class);
        safe(generic);
        assertThat(generic).doesNotContain("config:", "tscfg:", USER);
    }

    private static Document stored(Fixture fixture, String id) {
        Document document = SystemCollections.ARTIFACTS.on(fixture.raw.getDatabase(fixture.metadata))
                .find(new Document("_id", id)).first();
        assertThat(document).isNotNull();
        assertThat(document.get("body", Document.class).get("config"))
                .isInstanceOf(String.class).asString().startsWith("tscfg:1:");
        SourceResource internal = (SourceResource) fixture.context.getBean(ArtifactStore.class).get(id).orElseThrow();
        assertThat(document.getString("contentHash")).isEqualTo(CanonicalHash.of(internal));
        assertThat(internal.config().get(id.endsWith("standard") ? "password" : "uri"))
                .asString().contains(SECRET);
        safe(document.toJson());
        assertThat(document.toJson()).doesNotContain(USER);
        return document;
    }

    private static void safe(String value) {
        assertThat(value).doesNotContain(SECRET, WRONG);
    }

    private Fixture start(CloudRuntimeSettings.Mode mode, String metadata, String uri,
            String cookie, boolean firstBoot) {
        boolean cloud = mode == CloudRuntimeSettings.Mode.CLOUD;
        List<String> args = new ArrayList<>(List.of("--server.address=127.0.0.1", "--server.port=0",
                "--logging.level.root=ERROR", "--tapstate.test.pdk-memory=true", "--SDK_STATUS_SENDER_ENABLED=false",
                "--spring.config.location=optional:classpath:/application.properties",
                "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory(),
                "--tapstate.connectors.plugins-dir=" + work.resolve("plugins-" + metadata),
                "--tapstate.store.mongo.operator-state-database=" + metadata + "_operator"));
        if (cloud) args.addAll(List.of("--tapstate.cloud.base-url=https://cloud.example",
                "--tapstate.cloud.token=controlled-outbound-token", "--tapstate.cloud.atlas-uri=" + uri,
                "--tapstate.cloud.cluster-id=shared-source-cluster"));
        else args.add("--tapstate.store.mongo.uri=" + uri);
        ConfigurableApplicationContext context = new SpringApplicationBuilder(Assembly.class)
                .environment(CloudFixtureEnvironment.isolated()).run(args.toArray(String[]::new));
        MongoClient raw = null;
        try {
            raw = MongoClients.create(uri);
            RestClient anonymous = RestClient.builder().baseUrl("http://127.0.0.1:"
                            + ((WebServerApplicationContext) context).getWebServer().getPort())
                    .requestFactory(new JdkClientHttpRequestFactory(HttpClient.newBuilder()
                            .connectTimeout(Duration.ofSeconds(5)).followRedirects(HttpClient.Redirect.NEVER).build()))
                    .build();
            String credential = cookie;
            if (cloud && credential == null) credential = anonymous.get().uri("/auth/exchange?code=controlled-code")
                    .exchange((request, response) -> {
                        assertThat(response.getStatusCode().value()).isEqualTo(302);
                        String header = response.getHeaders().getFirst(HttpHeaders.SET_COOKIE);
                        assertThat(header).contains("Secure", "HttpOnly");
                        return header.substring(0, header.indexOf(';'));
                    });
            if (!cloud) {
                if (firstBoot) anonymous.post().uri("/auth/bootstrap").contentType(MediaType.APPLICATION_JSON)
                        .body(Map.of("username", "admin", "password", "controlled-local-password"))
                        .retrieve().toBodilessEntity();
                credential = JSON.readTree(anonymous.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                        .body(Map.of("username", "admin", "password", "controlled-local-password"))
                        .retrieve().body(String.class)).path("token").asText();
            }
            assertThat(credential).isNotBlank();
            RestClient client = anonymous.mutate().defaultHeader(cloud ? HttpHeaders.COOKIE : HttpHeaders.AUTHORIZATION,
                    cloud ? credential : "Bearer " + credential).build();
            return new Fixture(context, raw, metadata, client, credential);
        } catch (RuntimeException | Error failure) {
            try { context.close(); } catch (RuntimeException | Error cleanup) { failure.addSuppressed(cleanup); }
            if (raw != null) try { raw.close(); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    private record Fixture(ConfigurableApplicationContext context, MongoClient raw, String metadata,
            RestClient client, String credential) implements AutoCloseable {
        @Override public void close() { try { context.close(); } finally { raw.close(); } }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @ConditionalOnProperty(prefix = "tapstate.test", name = "pdk-memory", havingValue = "true")
    @Import({StoreConfiguration.class, ControlPlaneConfiguration.class})
    static class Assembly {
        /** This fixture verifies real PDK config delivery without executing a Pipeline preview. */
        @Bean
        PipelinePreviewProbe unusedPreviewProbe() {
            return request -> {
                throw new AssertionError("this PDK config fixture does not execute a Pipeline preview");
            };
        }

        @Bean
        @Primary
        @ConditionalOnProperty(prefix = "tapstate.cloud", name = "base-url")
        CloudAuthenticationService controlledAuthentication(CloudSessionStore sessions, TokenSecrets secrets,
                Clock clock, ClusterIdentityService clusters) {
            CloudSessionIdentity identity = new CloudSessionIdentity("https://cloud.example", "controlled-org",
                    clusters.identityView().clusterId());
            return new CloudAuthenticationService((code, cluster) -> "controlled-raw-jwt",
                    (jwt, expected, audience) -> Optional.of(new CloudLoginIdentity(identity,
                            "pdk-memory-cloud-user", "pdk-memory-jti", Scope.WRITE, clock.instant().plusSeconds(900))),
                    new CloudSessionService(sessions, identity, secrets, clock));
        }
    }
}
