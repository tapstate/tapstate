package io.tapstate.app;

import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CloudLoginIdentity;
import io.tapstate.control.core.CloudSessionCallbackVerifier;
import io.tapstate.control.core.CloudSessionService;
import io.tapstate.control.core.ClusterIdentityService;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.StoredArtifact;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionStore;
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
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.client.JdkClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Assembly/HTTP/raw-store witness over controlled SDK ports, not real Cloud validation or callback proof. */
@RequiresDocker
class ManagedCloudSessionAssemblyIT {

    private static final String COOKIE = "__Host-tapstate-cloud-session";
    private static final String JTI = "managed-assembly-jti";
    private static final AtomicInteger VALIDATIONS = new AtomicInteger();

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @TempDir
    Path work;

    @Test
    void theRealAssemblyAuthenticatesOnlyLocalCookiesAttributesResourcesAndReceivesScopedInvalidation() {
        VALIDATIONS.set(0);
        String database = "managed_auth_" + Long.toUnsignedString(System.nanoTime(), 16);
        String uri = MONGO.getReplicaSetUrl(database);
        try (var context = new SpringApplicationBuilder(Assembly.class).run(
                "--server.address=127.0.0.1", "--server.port=0", "--SDK_STATUS_SENDER_ENABLED=false",
                "--tapstate.test.managed-auth=true",
                "--tapstate.cloud.base-url=https://cloud.example", "--tapstate.cloud.token=controlled-machine-token",
                "--tapstate.cloud.atlas-uri=" + uri,
                "--tapstate.cloud.cluster-id=managed-assembly-cluster",
                "--tapstate.store.mongo.operator-state-database=managed_auth_ops_" + Long.toUnsignedString(System.nanoTime(), 16),
                "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"),
                "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory())) {
            int port = ((WebServerApplicationContext) context).getWebServer().getPort();
            RestClient client = RestClient.builder().baseUrl("http://127.0.0.1:" + port)
                    .requestFactory(new JdkClientHttpRequestFactory(HttpClient.newBuilder()
                            .followRedirects(HttpClient.Redirect.NEVER).build())).build();
            String header = client.get().uri("/auth/exchange?code=controlled-code")
                    .exchange((request, response) -> {
                        assertThat(response.getStatusCode().value()).isEqualTo(302);
                        assertThat(response.getHeaders().getFirst(HttpHeaders.LOCATION)).isEqualTo("/");
                        return response.getHeaders().getFirst(HttpHeaders.SET_COOKIE);
                    });
            assertThat(header).contains("Secure", "HttpOnly", "SameSite=Lax").doesNotContain("controlled-raw-jwt");
            String cookie = header.substring(0, header.indexOf(';'));
            assertThat(cookie).startsWith(COOKIE + "=tcs_");
            String source = """
                    version: tapstate/v1
                    kind: source
                    id: controlled_source
                    connector: mongodb
                    config: { uri: 'mongodb://db.example/target' }
                    """;
            int applied = client.post().uri("/api/artifacts:apply").header(HttpHeaders.COOKIE, cookie)
                    .contentType(MediaType.APPLICATION_JSON)
                    .body(Map.of("drafts", List.of(Map.of("content", source))))
                    .exchange((request, response) -> response.getStatusCode().value());
            assertThat(applied).isEqualTo(200);
            assertThat(client.get().uri("/api/artifacts/controlled_source").header(HttpHeaders.COOKIE, cookie)
                    .retrieve().body(String.class)).contains("user_id: stable-assembly-user");
            assertThat(VALIDATIONS.get()).as("workload requests authenticate locally without another Cloud validation").isEqualTo(1);
            try (var raw = MongoClients.create(uri)) {
                Document stored = SystemCollections.ARTIFACTS.on(raw.getDatabase(database))
                        .find(new Document("_id", "controlled_source")).first();
                assertThat(stored).isNotNull();
                Document metadata = stored.get("body", Document.class).get("metadata", Document.class);
                assertThat(metadata).containsEntry("cloud", true).containsEntry("user_id", "stable-assembly-user");
                Document login = SystemCollections.SESSIONS.on(raw.getDatabase(database))
                        .find(new Document("origin", "cloud")).first();
                assertThat(login).isNotNull().containsEntry("jwtId", JTI).containsEntry("userId", "stable-assembly-user");
                assertThat(login.toJson()).doesNotContain("controlled-raw-jwt", cookie, "controlled-machine-token");
                assertThat(SystemCollections.USERS.on(raw.getDatabase(database)).countDocuments()).isZero();
            }
            int denied = client.post().uri(builder -> builder.path("/auth/invalidate-session")
                            .queryParam("jti", JTI).queryParam("ts", "1780000000000")
                            .queryParam("nonce", "controlled-nonce").queryParam("sign", "invalid").build())
                    .exchange((request, response) -> response.getStatusCode().value());
            assertThat(denied).isEqualTo(401);
            int invalidated = client.post().uri(builder -> builder.path("/auth/invalidate-session")
                            .queryParam("jti", JTI).queryParam("ts", "1780000000000")
                            .queryParam("nonce", "controlled-nonce").queryParam("sign", "controlled").build())
                    .exchange((request, response) -> response.getStatusCode().value());
            assertThat(invalidated).isEqualTo(204);
            int afterRevocation = client.get().uri("/api/artifacts/controlled_source").header(HttpHeaders.COOKIE, cookie)
                    .exchange((request, response) -> response.getStatusCode().value());
            assertThat(afterRevocation).isEqualTo(401);
            assertThat(VALIDATIONS.get()).isEqualTo(1);
        }
    }

    @Test
    void distinctUsersKeepResourceAttributionAndSelectiveRevocationAcrossAContextRestart() {
        String database = "managed_two_users_" + Long.toUnsignedString(System.nanoTime(), 16);
        String uri = MONGO.getReplicaSetUrl(database);
        TwoUserPorts ports = new TwoUserPorts();
        Map<String, String> cookies = new LinkedHashMap<>();
        Map<String, Document> artifacts = new LinkedHashMap<>();
        try (var raw = MongoClients.create(uri)) {
            MongoDatabase stored = raw.getDatabase(database);
            try (var context = startTwoUsers(uri, ports)) {
                RestClient client = client(context);
                for (ControlledLogin login : ports.logins) {
                    String cookie = exchange(client, login);
                    cookies.put(login.jwtId(), cookie);
                    apply(client, origin(context), cookie, source(login), pipeline(login));
                    artifacts.put(login.sourceId(), attributed(context, client, stored, cookie,
                            login.sourceId(), login.userId()));
                    artifacts.put(login.pipelineId(), attributed(context, client, stored, cookie,
                            login.pipelineId(), login.userId()));
                }
                String cookieA = cookies.get(ports.userA.jwtId());
                String cookieB = cookies.get(ports.userB.jwtId());
                SourceResource original = (SourceResource) context.getBean(ArtifactStore.class)
                        .get(ports.userA.sourceId()).orElseThrow();
                // Attribution preserves the creator; it does not introduce a per-user resource ACL.
                apply(client, origin(context), cookieB, """
                        version: tapstate/v1
                        kind: source
                        id: %s
                        connector: mongodb
                        metadata: { description: edited by the second user }
                        """.formatted(ports.userA.sourceId()));
                SourceResource edited = (SourceResource) context.getBean(ArtifactStore.class)
                        .get(ports.userA.sourceId()).orElseThrow();
                assertThat(edited.config()).isEqualTo(original.config());
                assertThat(edited.metadata().description()).isEqualTo("edited by the second user");
                assertThat(CanonicalHash.of(edited)).isNotEqualTo(CanonicalHash.of(original));
                artifacts.put(ports.userA.sourceId(), attributed(context, client, stored, cookieB,
                        ports.userA.sourceId(), ports.userA.userId()));
                assertLoginRows(context, stored, ports, cookies);
                ports.assertOnlyTwoLogins();

                List<Document> beforeBadCallback = loginRows(stored);
                assertThat(callback(client, ports.userA.jwtId(), "invalid")).isEqualTo(401);
                assertThat(loginRows(stored)).isEqualTo(beforeBadCallback);
                assertThat(status(client, cookieA, ports.userA.sourceId())).isEqualTo(200);
                assertThat(status(client, cookieB, ports.userB.sourceId())).isEqualTo(200);

                Document unaffected = loginRow(stored, ports.userB.jwtId());
                assertThat(callback(client, ports.userA.jwtId(), "controlled-two-users")).isEqualTo(204);
                assertThat(callback(client, ports.userA.jwtId(), "controlled-two-users")).isEqualTo(204);
                assertThat(loginRow(stored, ports.userB.jwtId())).isEqualTo(unaffected);
                assertThat(loginRow(stored, ports.userA.jwtId()).getBoolean("revoked")).isTrue();
                assertThat(status(client, cookieA, ports.userA.sourceId())).isEqualTo(401);
                assertThat(status(client, cookieB, ports.userB.sourceId())).isEqualTo(200);
                assertLoginRows(context, stored, ports, cookies);
                ports.assertOnlyTwoLogins();
            }

            try (var restarted = startTwoUsers(uri, ports)) {
                RestClient client = client(restarted);
                String cookieA = cookies.get(ports.userA.jwtId());
                String cookieB = cookies.get(ports.userB.jwtId());
                assertThat(status(client, cookieA, ports.userA.sourceId())).isEqualTo(401);
                assertThat(status(client, cookieB, ports.userB.sourceId())).isEqualTo(200);
                for (ControlledLogin login : ports.logins) {
                    for (String id : List.of(login.sourceId(), login.pipelineId())) {
                        assertThat(attributed(restarted, client, stored, cookieB, id, login.userId()))
                                .isEqualTo(artifacts.get(id));
                    }
                }
                assertLoginRows(restarted, stored, ports, cookies);
                ports.assertOnlyTwoLogins();
                client.post().uri("/auth/logout").header(HttpHeaders.COOKIE, cookieB)
                        .header(HttpHeaders.ORIGIN, origin(restarted))
                        .exchange((request, response) -> {
                            assertThat(response.getStatusCode().value()).isEqualTo(204);
                            assertThat(response.getHeaders().getFirst(HttpHeaders.SET_COOKIE))
                                    .contains(COOKIE + "=", "Max-Age=0");
                            return null;
                        });
                assertThat(status(client, cookieB, ports.userB.sourceId())).isEqualTo(401);
                assertThat(status(client, cookieA, ports.userA.sourceId())).isEqualTo(401);
                assertThat(loginRow(stored, ports.userB.jwtId()).getBoolean("revoked")).isTrue();
                assertLoginRows(restarted, stored, ports, cookies);
                ports.assertOnlyTwoLogins();
            }
        }
    }

    private ConfigurableApplicationContext startTwoUsers(String uri, TwoUserPorts ports) {
        return new SpringApplicationBuilder(TwoUserAssembly.class).environment(CloudFixtureEnvironment.isolated())
                .initializers(context -> context.getBeanFactory().registerSingleton("twoUserPorts", ports))
                .run("--server.address=127.0.0.1", "--server.port=0", "--SDK_STATUS_SENDER_ENABLED=false",
                        "--spring.config.location=optional:classpath:/application.properties",
                        "--tapstate.test.managed-auth-two-users=true",
                        "--tapstate.cloud.base-url=https://cloud.example",
                        "--tapstate.cloud.token=" + TwoUserPorts.STATIC_TOKEN,
                        "--tapstate.cloud.atlas-uri=" + uri,
                        "--tapstate.cloud.cluster-id=" + TwoUserPorts.CLUSTER,
                        "--tapstate.connectors.plugins-dir=" + work.resolve("two-user-plugins"),
                        "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory());
    }

    private static String origin(ConfigurableApplicationContext context) {
        return "http://127.0.0.1:" + ((WebServerApplicationContext) context).getWebServer().getPort();
    }

    private static RestClient client(ConfigurableApplicationContext context) {
        var factory = new JdkClientHttpRequestFactory(HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(5)).followRedirects(HttpClient.Redirect.NEVER).build());
        factory.setReadTimeout(Duration.ofSeconds(10));
        return RestClient.builder().baseUrl(origin(context)).requestFactory(factory).build();
    }

    private static String exchange(RestClient client, ControlledLogin login) {
        String header = client.get().uri("/auth/exchange?code=" + login.code())
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode().value()).isEqualTo(302);
                    assertThat(response.getHeaders().getFirst(HttpHeaders.LOCATION)).isEqualTo("/");
                    assertThat(response.getHeaders().get(HttpHeaders.SET_COOKIE)).hasSize(1);
                    assertThat(response.bodyTo(String.class)).isNullOrEmpty();
                    return response.getHeaders().getFirst(HttpHeaders.SET_COOKIE);
                });
        assertThat(header).isNotNull().doesNotContain(login.jwt(), login.code());
        String[] parts = header.split(";\\s*");
        assertThat(Arrays.copyOfRange(parts, 1, parts.length))
                .containsExactlyInAnyOrder("Path=/", "Secure", "HttpOnly", "SameSite=Lax");
        assertThat(parts[0]).startsWith(COOKIE + "=tcs_");
        return parts[0];
    }

    private static void apply(RestClient client, String origin, String cookie, String... drafts) {
        client.post().uri("/api/artifacts:apply").header(HttpHeaders.COOKIE, cookie)
                .header(HttpHeaders.ORIGIN, origin).contentType(MediaType.APPLICATION_JSON)
                .body(Map.of("drafts", Arrays.stream(drafts).map(content -> Map.of("content", content)).toList()))
                .exchange((request, response) -> {
                    assertThat(response.getStatusCode().value()).isEqualTo(200);
                    return null;
                });
    }

    private static String source(ControlledLogin login) {
        return """
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config: { uri: 'mongodb://db.example/target' }
                mode: snapshot
                tables: [orders]
                """.formatted(login.sourceId());
    }

    private static String pipeline(ControlledLogin login) {
        return """
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: %s
                transforms:
                  - id: selected
                    type: filter
                    from: [orders]
                    expr: "true"
                view:
                  id: orders_view
                  from: selected
                  primary_key: id
                  storage:
                    warm:
                      collection: two_user_orders_%s
                """.formatted(login.pipelineId(), login.sourceId(), login.suffix());
    }

    private static Document attributed(ConfigurableApplicationContext context, RestClient client,
            MongoDatabase stored, String cookie, String id, String userId) {
        Resource resource = context.getBean(ArtifactStore.class).get(id).orElseThrow();
        assertThat(resource.metadata().cloud()).isTrue();
        assertThat(resource.metadata().userId()).isEqualTo(userId);
        String hash = CanonicalHash.of(resource);
        Document raw = SystemCollections.ARTIFACTS.on(stored).find(new Document("_id", id)).first();
        assertThat(raw).isNotNull().containsEntry("contentHash", hash);
        Document metadata = raw.get("body", Document.class).get("metadata", Document.class);
        assertThat(metadata).containsEntry("cloud", true).containsEntry("user_id", userId);
        StoredArtifact view = client.get().uri("/api/artifacts/" + id).header(HttpHeaders.COOKIE, cookie)
                .retrieve().body(StoredArtifact.class);
        assertThat(view).isNotNull();
        assertThat(view.id()).isEqualTo(id);
        assertThat(view.kind()).isEqualTo(resource.kind());
        assertThat(view.contentHash()).isEqualTo(hash);
        Resource presented = new DslParser().parse(view.canonicalForm());
        Map<String, Object> expected = new LinkedHashMap<>(new CanonicalWriter().tree(resource));
        Map<String, Object> actual = new LinkedHashMap<>(new CanonicalWriter().tree(presented));
        if (resource instanceof SourceResource) {
            assertThat(view.canonicalForm()).doesNotContain("config:");
            expected.remove("config");
            actual.remove("config");
        }
        assertThat(actual).isEqualTo(expected);
        return raw;
    }

    private static int status(RestClient client, String cookie, String id) {
        return client.get().uri("/api/artifacts/" + id).header(HttpHeaders.COOKIE, cookie)
                .exchange((request, response) -> response.getStatusCode().value());
    }

    private static int callback(RestClient client, String jwtId, String signature) {
        return client.post().uri(builder -> builder.path("/auth/invalidate-session")
                        .queryParam("jti", jwtId).queryParam("ts", "1780000000000")
                        .queryParam("nonce", "two-user-nonce").queryParam("sign", signature).build())
                .exchange((request, response) -> response.getStatusCode().value());
    }

    private static List<Document> loginRows(MongoDatabase stored) {
        return SystemCollections.SESSIONS.on(stored).find(new Document("origin", "cloud"))
                .sort(new Document("jwtId", 1)).into(new ArrayList<>());
    }

    private static Document loginRow(MongoDatabase stored, String jwtId) {
        Document row = SystemCollections.SESSIONS.on(stored)
                .find(new Document("origin", "cloud").append("jwtId", jwtId)).first();
        assertThat(row).isNotNull();
        return row;
    }

    private static void assertLoginRows(ConfigurableApplicationContext context, MongoDatabase stored,
            TwoUserPorts ports, Map<String, String> cookies) {
        assertThat(loginRows(stored)).hasSize(2);
        TokenSecrets secrets = context.getBean(TokenSecrets.class);
        for (ControlledLogin login : ports.logins) {
            Document row = loginRow(stored, login.jwtId());
            assertThat(row).containsEntry("userId", login.userId()).containsEntry("clusterId", TwoUserPorts.CLUSTER);
            assertThat(row.keySet()).containsExactlyInAnyOrder("_id", "origin", "issuer", "organizationId",
                    "clusterId", "jwtId", "secretHash", "userId", "scope", "revoked", "createdAt",
                    "lastUsedAt", "idleExpiresAt");
            String cookie = cookies.get(login.jwtId());
            String secret = cookie.substring(cookie.lastIndexOf('.') + 1);
            assertThat(row.getString("secretHash")).isEqualTo(secrets.hash(secret));
            for (ControlledLogin other : ports.logins) {
                String otherCookie = cookies.get(other.jwtId());
                assertThat(row.toJson()).doesNotContain(other.jwt(), other.code(), otherCookie,
                        otherCookie.substring(otherCookie.lastIndexOf('.') + 1), TwoUserPorts.STATIC_TOKEN);
            }
        }
        assertThat(SystemCollections.USERS.on(stored).countDocuments()).isZero();
    }

    private record ControlledLogin(String code, String jwt, String userId, String jwtId, String suffix) {
        String sourceId() { return "two_user_source_" + suffix; }
        String pipelineId() { return "two_user_pipeline_" + suffix; }
    }

    private static final class TwoUserPorts {
        static final String CLUSTER = "managed-two-user-cluster";
        static final String STATIC_TOKEN = "controlled-two-user-machine-token";
        final ControlledLogin userA = new ControlledLogin("two-user-code-a", "controlled-two-user-jwt-a",
                "stable-two-user-a", "two-user-jti-a", "a");
        final ControlledLogin userB = new ControlledLogin("two-user-code-b", "controlled-two-user-jwt-b",
                "stable-two-user-b", "two-user-jti-b", "b");
        final List<ControlledLogin> logins = List.of(userA, userB);
        final AtomicInteger exchanges = new AtomicInteger();
        final AtomicInteger validations = new AtomicInteger();

        void assertOnlyTwoLogins() {
            assertThat(exchanges.get()).isEqualTo(2);
            assertThat(validations.get()).isEqualTo(2);
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @ConditionalOnProperty(prefix = "tapstate.test", name = "managed-auth-two-users", havingValue = "true")
    @Import({StoreConfiguration.class, ControlPlaneConfiguration.class})
    static class TwoUserAssembly {
        @Bean
        @Primary
        CloudAuthenticationService controlledTwoUserAuthentication(CloudSessionStore sessions,
                TokenSecrets secrets, Clock clock, CloudRuntimeSettings settings, TwoUserPorts ports) {
            CloudSessionIdentity identity = new CloudSessionIdentity(settings.baseUrl().toString(), "controlled-org",
                    settings.clusterId());
            CloudSessionService local = new CloudSessionService(sessions, identity, secrets, clock);
            return new CloudAuthenticationService((code, cluster) -> {
                assertThat(cluster).isEqualTo(TwoUserPorts.CLUSTER);
                ports.exchanges.incrementAndGet();
                return ports.logins.stream().filter(login -> login.code().equals(code)).findFirst()
                        .map(ControlledLogin::jwt).orElseThrow(CloudAuthenticationService::unavailable);
            }, (jwt, expected, audience) -> {
                ports.validations.incrementAndGet();
                if (!identity.equals(expected)) return Optional.empty();
                return ports.logins.stream().filter(login -> login.jwt().equals(jwt)).findFirst()
                        .map(login -> new CloudLoginIdentity(identity, login.userId(), login.jwtId(),
                                Scope.WRITE, clock.instant().plusSeconds(900)));
            }, local);
        }

        @Bean
        @Primary
        CloudSessionCallbackVerifier controlledTwoUserCallbackVerifier(TwoUserPorts ports) {
            return (issuer, org, cluster, method, timestamp, nonce, data, signature) ->
                    issuer.equals("https://cloud.example") && org.equals("controlled-org")
                            && cluster.equals(TwoUserPorts.CLUSTER) && method.equals("POST")
                            && timestamp.equals("1780000000000") && nonce.equals("two-user-nonce")
                            && ports.logins.stream().anyMatch(login -> login.jwtId().equals(data))
                            && signature.equals("controlled-two-users");
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @ConditionalOnProperty(prefix = "tapstate.test", name = "managed-auth", havingValue = "true")
    @Import({StoreConfiguration.class, ControlPlaneConfiguration.class})
    static class Assembly {
        @Bean
        @Primary
        CloudAuthenticationService controlledAuthentication(
                CloudSessionStore sessions, TokenSecrets secrets, Clock clock, ClusterIdentityService clusters) {
            CloudSessionIdentity identity = new CloudSessionIdentity("https://cloud.example", "controlled-org",
                    clusters.identityView().clusterId());
            CloudSessionService local = new CloudSessionService(sessions, identity, secrets, clock);
            return new CloudAuthenticationService((code, cluster) -> "controlled-raw-jwt", (jwt, expected, audience) -> {
                VALIDATIONS.incrementAndGet();
                return Optional.of(new CloudLoginIdentity(identity, "stable-assembly-user", JTI,
                        Scope.WRITE, clock.instant().plusSeconds(900)));
            }, local);
        }

        @Bean
        @Primary
        CloudSessionCallbackVerifier controlledCallbackVerifier() {
            return (issuer, org, cluster, method, timestamp, nonce, data, signature) ->
                    method.equals("POST") && timestamp.equals("1780000000000")
                            && nonce.equals("controlled-nonce") && data.equals(JTI)
                            && signature.equals("controlled");
        }
    }
}
