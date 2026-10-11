package io.tapstate.control.restapi;

import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.CloudAuthenticationService;
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
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
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
            "context-org", "context-cluster", "Workspace organization", "Workspace cluster", "workspace-region");
    private static final CloudSessionIdentity IDENTITY = new CloudSessionIdentity(
            "https://context.example.test", DISPLAY.organizationId(), DISPLAY.clusterId());

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unknownClusterStateIsExplicitForTheWorkspaceConsumerEvenWhenNullsAreOmitted(boolean omitNulls) {
        try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestApp.class).run(
                "--server.address=127.0.0.1", "--server.port=0",
                "--spring.jackson.default-property-inclusion=" + (omitNulls ? "non_null" : "always"))) {
            int port = ((WebServerApplicationContext) context).getWebServer().getPort();
            Map<?, ?> body = RestClient.create("http://127.0.0.1:" + port).get().uri("/api/cluster/context")
                    .header(HttpHeaders.COOKIE, CloudSessionCookies.NAME + "=" + COOKIE)
                    .exchange((request, response) -> {
                        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
                        assertThat(response.getHeaders().getCacheControl()).isEqualTo("no-store");
                        return response.bodyTo(Map.class);
                    });

            assertThat(body.get("organizationId")).isEqualTo(DISPLAY.organizationId());
            assertThat(body.get("clusterId")).isEqualTo(DISPLAY.clusterId());
            assertThat(body.get("organizationName")).isEqualTo(DISPLAY.organizationName());
            assertThat(body.get("clusterName")).isEqualTo(DISPLAY.clusterName());
            assertThat(body.get("region")).isEqualTo(DISPLAY.region());
            // The workspace client accepts a null or string state and rejects an absent property.
            assertThat(body.containsKey("clusterState")).as("workspace context contains its nullable state").isTrue();
            assertThat(body.get("clusterState")).isNull();
            assertThat(body.toString()).doesNotContain(COOKIE, "password", "jwt", "jti", "secret-sentinel");
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
        CloudAuthenticationService authentication() {
            TokenSecrets secrets = new TokenSecrets() {
                @Override public GeneratedSecret generate() { throw new IllegalStateException("context read must not create a session"); }
                @Override public String hash(String raw) { return "hash-" + raw; }
                @Override public boolean matches(String raw, String hash) { return hash(raw).equals(hash); }
            };
            CloudSessionService sessions = new CloudSessionService(
                    new ContextStore(), IDENTITY, secrets, Clock.fixed(NOW, ZoneOffset.UTC));
            return new CloudAuthenticationService((code, cluster) -> {
                throw new IllegalStateException("context read must not exchange a code");
            }, (jwt, identity, audience) -> {
                throw new IllegalStateException("context read must not verify a JWT");
            }, sessions);
        }

        @Bean
        CredentialAuthenticator credentials(CloudAuthenticationService authentication) {
            return new CredentialAuthenticator(authentication::authenticate);
        }
    }

    private static final class ContextStore implements CloudSessionStore {
        private final CloudSessionRecord record = new CloudSessionRecord(IDENTITY, JTI, "hash-" + SECRET,
                "context-user", Scope.READ.name(), false, NOW, NOW, NOW.plusSeconds(1800), DISPLAY);

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
