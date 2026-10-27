package io.tapstate.control.restapi;

import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CloudClusterEgressAddress;
import io.tapstate.control.core.CloudClusterEgressAddressProvider;
import io.tapstate.control.core.CloudSessionService;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.spi.store.CloudSessionContext;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionRecord;
import io.tapstate.spi.store.CloudSessionStore;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.client.RestClient;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual HTTP serialization over a controlled session context, without a live Cloud state provider. */
class ClusterContextApiTest {
    private static final Instant NOW = Instant.parse("2026-10-09T00:00:00Z");
    private static final String JTI = "context-jti";
    private static final String SECRET = "context-cookie-secret-sentinel";
    private static final String COOKIE = "tcs_" + Base64.getUrlEncoder().withoutPadding()
            .encodeToString(JTI.getBytes(StandardCharsets.UTF_8)) + "." + SECRET;
    private static final CloudSessionContext DISPLAY = new CloudSessionContext(
            "context-org", "context-cluster", "Workspace organization", "Workspace cluster", "workspace-region",
            "198.51.100.1", "DYNAMIC", "user@example.test");
    private static final CloudClusterEgressAddress CURRENT_ADDRESS = new CloudClusterEgressAddress(
            DISPLAY.organizationId(), DISPLAY.clusterId(), "203.0.113.42", "STABLE");
    private static final CloudSessionIdentity IDENTITY = new CloudSessionIdentity(
            "https://context.example.test", DISPLAY.organizationId(), DISPLAY.clusterId());

    @ParameterizedTest
    @CsvSource({"false,true", "true,true", "false,false", "true,false"})
    void contextReturnsTheCurrentAddressAndKeepsOptionalKeysExplicit(boolean omitNulls, boolean includeEgress) {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestApp.class).run(
                "--server.address=127.0.0.1", "--server.port=0",
                "--tapstate.test.include-egress=" + includeEgress,
                "--spring.jackson.default-property-inclusion=" + (omitNulls ? "non_null" : "always"))) {
            int port = ((WebServerApplicationContext) context).getWebServer().getPort();
            Map<?, ?> rawBody = RestClient.create("http://127.0.0.1:" + port).get().uri("/api/cluster/context")
                    .header(HttpHeaders.COOKIE, CloudSessionCookies.NAME + "=" + COOKIE)
                    .exchange((request, response) -> {
                        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                        assertThat(response.getHeaders().getCacheControl()).isEqualTo("no-store");
                        return response.bodyTo(Map.class);
                    });
            Map<String, Object> body = new HashMap<>();
            rawBody.forEach((key, value) -> body.put((String) key, value));

            assertThat(body.get("organizationId")).isEqualTo(DISPLAY.organizationId());
            assertThat(body.get("clusterId")).isEqualTo(DISPLAY.clusterId());
            assertThat(body.get("organizationName")).isEqualTo(DISPLAY.organizationName());
            assertThat(body.get("clusterName")).isEqualTo(DISPLAY.clusterName());
            assertThat(body.get("region")).isEqualTo(DISPLAY.region());
            assertThat(body).containsEntry("userEmail", includeEgress ? "user@example.test" : null);
            if (includeEgress) {
                assertThat(body).containsEntry("ec2PublicIp", CURRENT_ADDRESS.ec2PublicIp());
                assertThat(body).containsEntry("egressAddressStatus", CURRENT_ADDRESS.egressAddressStatus());
            } else {
                assertThat(body).containsEntry("ec2PublicIp", null);
                assertThat(body).containsEntry("egressAddressStatus", null);
            }
            assertThat(body.keySet().stream().map(Object::toString).toList()).containsExactlyInAnyOrder("organizationId", "organizationName", "clusterId",
                    "clusterName", "region", "clusterState", "ec2PublicIp", "egressAddressStatus", "userEmail");
            // The workspace client accepts a null or string state and rejects an absent property.
            assertThat(body).as("workspace context contains its nullable state")
                    .containsKey("clusterState")
                    .containsEntry("clusterState", null);
            assertThat(body.toString()).doesNotContain(COOKIE, "password", "jwt", "jti", "secret-sentinel");
            for (String cookie : new String[] {"", COOKIE + "-wrong-secret"}) {
                String errorBody = RestClient.create("http://127.0.0.1:" + port).get().uri("/api/cluster/context")
                        .header(HttpHeaders.COOKIE, CloudSessionCookies.NAME + "=" + cookie)
                        .exchange((request, response) -> {
                            assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);
                            return response.bodyTo(String.class);
                        });
                assertThat(errorBody).doesNotContain(COOKIE, SECRET, JTI, "hash-" + SECRET);
            }
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({RestApiConfiguration.class, RestApiSecurityConfiguration.class, ApiExceptionHandler.class})
    @ComponentScan(basePackageClasses = ControlHttpFace.class, useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX,
                    pattern = "io\\.tapstate\\.control\\.restapi\\.ClusterContextController"))
    static class TestApp {
        @Bean
        AuthenticationMode mode() {
            return AuthenticationMode.CLOUD;
        }

        @Bean
        OperationRegistry registry() {
            return ControlOperations.registry();
        }

        @Bean
        CloudAuthenticationService authentication(
                @Value("${tapstate.test.include-egress:true}") boolean includeEgress) {
            TokenSecrets secrets = new TokenSecrets() {
                @Override public GeneratedSecret generate() { throw new IllegalStateException("context read must not create a session"); }
                @Override public String hash(String raw) { return "hash-" + raw; }
                @Override public boolean matches(String raw, String hash) { return hash(raw).equals(hash); }
            };
            CloudSessionContext sessionContext = includeEgress ? DISPLAY : new CloudSessionContext(
                    DISPLAY.organizationId(), DISPLAY.clusterId(), DISPLAY.organizationName(), DISPLAY.clusterName(),
                    DISPLAY.region());
            CloudSessionService sessions = new CloudSessionService(
                    new ContextStore(sessionContext), IDENTITY, secrets, Clock.fixed(NOW, ZoneOffset.UTC));
            CloudClusterEgressAddressProvider addressProvider = includeEgress
                    ? (organizationId, clusterId) -> Optional.of(CURRENT_ADDRESS)
                    : (organizationId, clusterId) -> Optional.empty();
            return new CloudAuthenticationService((code, cluster) -> {
                throw new IllegalStateException("context read must not call Cloud");
            }, (jwt, identity, audience) -> {
                throw new IllegalStateException("context read must not verify a JWT");
            }, sessions, io.tapstate.control.core.CloudAuthenticationObserver.NONE, addressProvider);
        }

        @Bean
        CredentialAuthenticator credentials(CloudAuthenticationService authentication) {
            return new CredentialAuthenticator(authentication::authenticate);
        }
    }

    private static final class ContextStore implements CloudSessionStore {
        private final CloudSessionRecord record;

        private ContextStore(CloudSessionContext display) {
            record = new CloudSessionRecord(IDENTITY, JTI, "hash-" + SECRET,
                    "context-user", Scope.READ.name(), false, NOW, NOW, NOW.plusSeconds(1800), display);
        }

        @Override public boolean create(CloudSessionRecord created) { throw new IllegalStateException("unexpected session creation"); }
        @Override public Optional<CloudSessionRecord> find(CloudSessionIdentity identity, String jwtId) {
            return IDENTITY.equals(identity) && JTI.equals(jwtId) ? Optional.of(record) : Optional.empty();
        }
        @Override public Optional<CloudSessionRecord> authenticate(CloudSessionIdentity identity, String jwtId,
                String hash, Instant now, Instant expiry) {
            return IDENTITY.equals(identity) && JTI.equals(jwtId) && record.secretHash().equals(hash)
                    && record.idleExpiresAt().isAfter(now) ? Optional.of(record) : Optional.empty();
        }
        @Override public boolean logout(CloudSessionIdentity identity, String jwtId, String hash, Instant now) {
            throw new IllegalStateException("context read must not log out");
        }
        @Override public void invalidate(CloudSessionIdentity identity, String jwtId, Instant now) {
            throw new IllegalStateException("context read must not invalidate a session");
        }
    }
}
