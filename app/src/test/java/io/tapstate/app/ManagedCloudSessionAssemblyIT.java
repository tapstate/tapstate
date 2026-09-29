package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CloudLoginIdentity;
import io.tapstate.control.core.CloudSessionCallbackVerifier;
import io.tapstate.control.core.CloudSessionService;
import io.tapstate.control.core.ClusterIdentityService;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenSecrets;
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
            return new CloudAuthenticationService((code, cluster) -> "controlled-raw-jwt", (jwt, expected) -> {
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
