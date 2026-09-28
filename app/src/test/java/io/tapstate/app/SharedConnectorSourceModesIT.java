package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CloudLoginIdentity;
import io.tapstate.control.core.CloudSessionService;
import io.tapstate.control.core.ClusterIdentityService;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.SourceView;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.core.dsl.DslError;
import io.tapstate.runtime.probe.ConnectionProbe;
import io.tapstate.runtime.probe.SchemaDiscoveryProbe;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionStore;
import io.tapstate.spi.store.ConnectionConfig;
import io.tapstate.spi.store.ConnectionTestItem;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.json.JsonMapper;

import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Real HTTP/authentication/store assembly over controlled probes, not real PDK/Cloud/Atlas/RDS proof. */
@RequiresDocker
class SharedConnectorSourceModesIT {

    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String USER = "controlled-source-user";
    private static final String COOKIE = "__Host-tapstate-cloud-session";

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @TempDir
    Path work;

    @Test
    void onPremRetainsLocalLoginAndTheSharedSourceConnectionContract() {
        verify(false);
    }

    @Test
    void cloudUsesLocalCookiesForTheSameSourceConnectionContract() {
        verify(true);
    }

    private void verify(boolean cloud) {
        String database = "source_modes_" + Long.toUnsignedString(System.nanoTime(), 16);
        String uri = MONGO.getReplicaSetUrl(database);
        List<String> arguments = new ArrayList<>(List.of(
                "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR",
                "--tapstate.test.source-modes=true", "--SDK_STATUS_SENDER_ENABLED=false",
                "--tapstate.store.mongo.operator-state-database=" + database + "_ops",
                "--tapstate.connectors.plugins-dir=" + work.resolve(database).resolve("plugins"),
                "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory()));
        if (cloud) {
            arguments.addAll(List.of("--tapstate.cloud.base-url=https://cloud.example",
                    "--tapstate.cloud.token=controlled-outbound-token", "--tapstate.cloud.atlas-uri=" + uri));
        } else {
            arguments.add("--tapstate.store.mongo.uri=" + uri);
        }

        // Local fixtures must not inherit a developer's Cloud credentials or external Spring files.
        var environment = new StandardEnvironment();
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME);
        environment.getPropertySources().remove(StandardEnvironment.SYSTEM_PROPERTIES_PROPERTY_SOURCE_NAME);
        arguments.add("--spring.config.location=optional:classpath:/application.properties");
        try (var context = new SpringApplicationBuilder(Assembly.class).environment(environment)
                .run(arguments.toArray(String[]::new))) {
            assertThat(context.getBean(CloudRuntimeSettings.class).cloud()).isEqualTo(cloud);
            RestClient client = RestClient.builder().baseUrl("http://127.0.0.1:"
                            + ((WebServerApplicationContext) context).getWebServer().getPort())
                    .requestFactory(new JdkClientHttpRequestFactory(HttpClient.newBuilder()
                            .followRedirects(HttpClient.Redirect.NEVER).build())).build();
            String credential = login(client, cloud);
            var probes = context.getBean(RecordingProbes.class);
            for (Fixture fixture : List.of(
                    new Fixture("atlas_uri", "mongodb-atlas", "uri", true),
                    new Fixture("atlas_standard", "mongodb-atlas", "password", false),
                    new Fixture("rds_source", "aws-rds-mysql", "password", false))) {
                verifySource(client, credential, cloud, fixture, probes, uri, database);
            }

            Fixture rds = new Fixture("role_rds", "aws-rds-mysql", "password", false);
            Map<String, Object> roleSource = new LinkedHashMap<>(draft(false, rds, rds.settings("role-secret"), "role", List.of()));
            roleSource.put("mode", "snapshot");
            roleSource.put("tables", List.of(Map.of("type", "literal", "name", "probe")));
            authorized(client, credential, cloud).post().uri("/api/sources")
                    .contentType(MediaType.APPLICATION_JSON).body(roleSource).retrieve().toBodilessEntity();
            authorized(client, credential, cloud).post().uri("/api/connections:discover-schema")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(connection(rds, source(client, credential, cloud, rds.id).config()))
                    .retrieve().toBodilessEntity();
            Map<String, Object> pipeline = Map.of("id", "forbidden_rds_target", "sources", List.of("role_rds"),
                    "transforms", List.of(), "serve", Map.of("id", "serve", "from", "/.*/",
                            "sync", List.of(Map.of("id", "sink", "source", "role_rds",
                                    "writeMode", "append", "ddl", "apply"))));
            String rejected = authorized(client, credential, cloud).post().uri("/api/pipelines")
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(pipeline)
                    .exchange((request, response) -> {
                        assertThat(response.getStatusCode().is4xxClientError()).isTrue();
                        return response.bodyTo(String.class);
                    });
            assertThat(JSON.readTree(rejected).path("code").asText())
                    .isEqualTo(DslError.UNSUPPORTED_TARGET_CONNECTOR.code());
            assertThat(rejected).doesNotContain("role-secret");
            try (var raw = MongoClients.create(uri)) {
                assertThat(SystemCollections.ARTIFACTS.on(raw.getDatabase(database))
                        .countDocuments(new Document("_id", "forbidden_rds_target"))).isZero();
                if (cloud) assertThat(SystemCollections.USERS.on(raw.getDatabase(database)).countDocuments()).isZero();
            }
            assertThat(context.getBean(RecordingProbes.class).validations.get()).isEqualTo(cloud ? 1 : 0);
        }
    }

    private static void verifySource(RestClient client, String credential, boolean cloud, Fixture fixture,
                                     RecordingProbes probes, String uri, String database) {
        String original = fixture.id + "-original-sentinel";
        String replacement = fixture.id + "-replacement-sentinel";
        Map<String, Object> originalSettings = fixture.settings(original);
        ResponseEntity<String> created = sendSource(client, credential, cloud, fixture, "POST",
                originalSettings, null, "initial", List.of());
        assertThat(created.getStatusCode().value()).isEqualTo(201);
        String etag = created.getHeaders().getETag();
        assertThat(etag).matches("\"[0-9a-f]{64}\"");
        assertThat(created.getBody()).doesNotContain(original);
        SourceView saved = source(client, credential, cloud, fixture.id);
        assertThat(saved.connector()).isEqualTo(fixture.connector);
        assertThat(saved.metadata().cloud()).isEqualTo(cloud ? Boolean.TRUE : null);
        assertThat(saved.metadata().userId()).isEqualTo(cloud ? USER : null);
        assertThat(authorized(client, credential, cloud).get().uri("/api/sources").retrieve().body(String.class))
                .contains(fixture.id).doesNotContain(original, replacement);
        try (var raw = MongoClients.create(uri)) {
            Document body = SystemCollections.ARTIFACTS.on(raw.getDatabase(database))
                    .find(new Document("_id", fixture.id)).first().get("body", Document.class);
            Document metadata = body.get("metadata", Document.class);
            if (cloud) assertThat(metadata).containsEntry("cloud", true).containsEntry("user_id", USER);
            else assertThat(metadata).doesNotContainKeys("cloud", "user_id");
        }

        test(client, credential, cloud, fixture, saved.config(), false);
        assertThat(probes.tested.settings()).containsAllEntriesOf(originalSettings);
        String discovery = authorized(client, credential, cloud).post().uri("/api/connections:discover-schema")
                .contentType(MediaType.APPLICATION_JSON).body(connection(fixture, saved.config()))
                .retrieve().body(String.class);
        assertThat(discovery).contains("probe").doesNotContain(original);
        assertThat(probes.discovered.settings()).containsAllEntriesOf(originalSettings);
        assertThat(authorized(client, credential, cloud).get().uri("/api/sources/" + fixture.id + "/schema")
                .retrieve().body(String.class)).contains("probe").doesNotContain(original);

        probes.failNext = true;
        test(client, credential, cloud, fixture, saved.config(), true);
        assertThat(authorized(client, credential, cloud).get()
                .uri("/api/connections/" + fixture.id + "/test-result").retrieve().body(String.class))
                .contains("FAILED", "28000").doesNotContain(original);

        if (!fixture.uri) {
            // This is the Web preview's explicit-clear request. Omission would restore the old password.
            Map<String, Object> clearedDraft = new LinkedHashMap<>(saved.config());
            clearedDraft.put("password", "");
            test(client, credential, cloud, fixture, clearedDraft, false);
            assertThat(probes.tested.settings()).containsEntry("password", "");
            test(client, credential, cloud, fixture, saved.config(), false);
            assertThat(probes.tested.settings()).containsEntry("password", original);
        }

        ResponseEntity<String> preserved = sendSource(client, credential, cloud, fixture, "PUT",
                saved.config(), etag, "preserved", List.of());
        assertThat(preserved.getBody()).doesNotContain(original);
        assertThat(preserved.getHeaders().getETag()).isNotEqualTo(etag);
        test(client, credential, cloud, fixture, source(client, credential, cloud, fixture.id).config(), false);
        assertThat(probes.tested.settings()).containsAllEntriesOf(originalSettings);
        ResponseEntity<String> replaced = sendSource(client, credential, cloud, fixture, "PUT",
                fixture.settings(replacement), preserved.getHeaders().getETag(), "replaced", List.of());
        assertThat(replaced.getBody()).doesNotContain(original, replacement);
        saved = source(client, credential, cloud, fixture.id);
        test(client, credential, cloud, fixture, saved.config(), false);
        assertThat(probes.tested.settings()).containsAllEntriesOf(fixture.settings(replacement));

        int conflict = authorized(client, credential, cloud).put().uri("/api/sources/" + fixture.id)
                .header(HttpHeaders.IF_MATCH, etag).contentType(MediaType.APPLICATION_JSON)
                .body(draft(cloud, fixture, fixture.settings("stale-sentinel"), "stale", List.of()))
                .exchange((request, response) -> {
                    String body = response.bodyTo(String.class);
                    assertThat(body).contains("source.version-conflict").doesNotContain(original, replacement, "stale-sentinel");
                    return response.getStatusCode().value();
                });
        assertThat(conflict).isEqualTo(412);

        Map<String, Object> cleared = new LinkedHashMap<>(saved.config());
        List<String> clearSecrets = fixture.uri ? List.of() : List.of("password");
        if (fixture.uri) cleared.put("uri", "mongodb+srv://cluster.example/rows");
        ResponseEntity<String> clearedResponse = sendSource(client, credential, cloud, fixture, "PUT",
                cleared, replaced.getHeaders().getETag(), "cleared", clearSecrets);
        assertThat(clearedResponse.getBody()).doesNotContain(original, replacement);
        test(client, credential, cloud, fixture, source(client, credential, cloud, fixture.id).config(), false);
        if (fixture.uri) assertThat(probes.tested.settings()).containsEntry("uri", cleared.get("uri"));
        else assertThat(probes.tested.settings()).doesNotContainKey("password");

        int callsBeforeRefusal = probes.testCalls.get();
        int unauthorized = client.post().uri("/api/connections:test").contentType(MediaType.APPLICATION_JSON)
                .body(connection(fixture, cleared))
                .exchange((request, response) -> response.getStatusCode().value());
        assertThat(unauthorized).isEqualTo(401);
        assertThat(probes.testCalls.get()).isEqualTo(callsBeforeRefusal);
        int deleted = authorized(client, credential, cloud).delete().uri("/api/sources/" + fixture.id)
                .header(HttpHeaders.IF_MATCH, clearedResponse.getHeaders().getETag())
                .exchange((request, response) -> response.getStatusCode().value());
        assertThat(deleted).isEqualTo(204);
        int missing = authorized(client, credential, cloud).get().uri("/api/sources/" + fixture.id)
                .exchange((request, response) -> response.getStatusCode().value());
        assertThat(missing).isEqualTo(404);
    }

    private static SourceView source(RestClient client, String credential, boolean cloud, String id) {
        return authorized(client, credential, cloud).get().uri("/api/sources/" + id).retrieve().body(SourceView.class);
    }

    private static void test(RestClient client, String credential, boolean cloud, Fixture fixture,
                             Map<String, Object> settings, boolean failed) {
        String result = authorized(client, credential, cloud).post().uri("/api/connections:test")
                .contentType(MediaType.APPLICATION_JSON).body(connection(fixture, settings)).retrieve().body(String.class);
        assertThat(result).contains(failed ? "FAILED" : "PASSED")
                .doesNotContain(fixture.id + "-original-sentinel", fixture.id + "-replacement-sentinel");
    }

    private static Map<String, Object> connection(Fixture fixture, Map<String, Object> settings) {
        return Map.of("id", fixture.id, "connectorId", fixture.connector, "settings", settings);
    }

    private static ResponseEntity<String> sendSource(RestClient client, String credential, boolean cloud,
            Fixture fixture, String method, Map<String, Object> config, String etag,
            String description, List<String> clearSecrets) {
        var request = authorized(client, credential, cloud).method(org.springframework.http.HttpMethod.valueOf(method))
                .uri("/api/sources" + (method.equals("PUT") ? "/" + fixture.id : ""))
                .contentType(MediaType.APPLICATION_JSON);
        if (etag != null) request.header(HttpHeaders.IF_MATCH, etag);
        Map<String, Object> body = draft(cloud && method.equals("PUT"), fixture, config, description, clearSecrets);
        return request.body(body).retrieve().toEntity(String.class);
    }

    private static Map<String, Object> draft(boolean attributed, Fixture fixture,
            Map<String, Object> config, String description, List<String> clearSecrets) {
        Map<String, Object> metadata = new LinkedHashMap<>(Map.of("description", description));
        if (attributed) { metadata.put("cloud", true); metadata.put("user_id", USER); }
        return Map.of("id", fixture.id, "connector", fixture.connector, "config", config,
                "metadata", metadata, "clearSecrets", clearSecrets);
    }

    private static RestClient authorized(RestClient client, String credential, boolean cloud) {
        return client.mutate().defaultHeader(cloud ? HttpHeaders.COOKIE : HttpHeaders.AUTHORIZATION,
                cloud ? credential : "Bearer " + credential).build();
    }

    private static String login(RestClient client, boolean cloud) {
        if (cloud) return client.get().uri("/auth/exchange?code=controlled-code")
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode().value()).isEqualTo(302);
                    String header = response.getHeaders().getFirst(HttpHeaders.SET_COOKIE);
                    assertThat(header).startsWith(COOKIE + "=tcs_").contains("Secure", "HttpOnly");
                    return header.substring(0, header.indexOf(';'));
                });
        client.post().uri("/auth/bootstrap").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "controlled-local-password"))
                .retrieve().toBodilessEntity();
        String result = client.post().uri("/auth/login").contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("username", "admin", "password", "controlled-local-password"))
                .retrieve().body(String.class);
        return JSON.readTree(result).path("token").asText();
    }

    private record Fixture(String id, String connector, String secretField, boolean uri) {
        Map<String, Object> settings(String secret) {
            if (uri) return Map.of("isUri", true, "uri", "mongodb+srv://reader:" + secret + "@cluster.example/rows");
            Map<String, Object> settings = new LinkedHashMap<>(Map.of("host", "db.example", "database", "rows",
                    "username", "reader", secretField, secret));
            if (connector.equals("mongodb-atlas")) settings.put("isUri", false);
            else settings.put("port", 3306);
            return settings;
        }
    }

    static final class RecordingProbes {
        volatile ConnectionConfig tested;
        volatile ConnectionConfig discovered;
        volatile boolean failNext;
        final AtomicInteger validations = new AtomicInteger();
        final AtomicInteger testCalls = new AtomicInteger();

        ConnectionTestResult test(ConnectionConfig config) {
            testCalls.incrementAndGet();
            tested = config;
            boolean failed = failNext;
            failNext = false;
            return new ConnectionTestResult(config.id(), config.connectorId(), failed
                    ? ConnectionTestResult.Outcome.FAILED : ConnectionTestResult.Outcome.PASSED,
                    List.of(new ConnectionTestItem("Login", failed ? ConnectionTestItem.Status.FAILED
                            : ConnectionTestItem.Status.PASSED, failed ? "Login denied" : null,
                            null, null, failed ? "28000" : null)), System.currentTimeMillis());
        }

        SourceModel discover(ConnectionConfig config) {
            discovered = config;
            return new SourceModel(List.of(new SourceTable("probe", List.of(new SourceField("id", "bigint")),
                    List.of("id"), List.of())));
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @ConditionalOnProperty(prefix = "tapstate.test", name = "source-modes", havingValue = "true")
    @Import({StoreConfiguration.class, ControlPlaneConfiguration.class})
    static class Assembly {
        @Bean RecordingProbes recordingProbes() { return new RecordingProbes(); }
        @Bean @Primary ConnectionProbe controlledConnectionProbe(RecordingProbes probes) { return probes::test; }
        @Bean @Primary SchemaDiscoveryProbe controlledSchemaDiscoveryProbe(RecordingProbes probes) { return probes::discover; }

        @Bean
        @ConditionalOnProperty(prefix = "tapstate.cloud", name = "base-url")
        CloudAuthenticationService controlledAuthentication(CloudSessionStore sessions, TokenSecrets secrets,
                Clock clock, ClusterIdentityService clusters, RecordingProbes probes) {
            CloudSessionIdentity identity = new CloudSessionIdentity("https://cloud.example", "controlled-org",
                    clusters.identityView().clusterId());
            return new CloudAuthenticationService((code, cluster) -> "controlled-raw-jwt", (jwt, expected) -> {
                probes.validations.incrementAndGet();
                return Optional.of(new CloudLoginIdentity(identity, USER, "source-modes-jti", Scope.WRITE,
                        clock.instant().plusSeconds(900)));
            }, new CloudSessionService(sessions, identity, secrets, clock));
        }
    }
}
