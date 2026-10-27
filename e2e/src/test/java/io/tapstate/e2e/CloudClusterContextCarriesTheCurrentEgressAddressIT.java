package io.tapstate.e2e;

import io.tapstate.app.Bootstrap;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CloudClusterEgressAddress;
import io.tapstate.control.core.CloudClusterEgressAddressProvider;
import io.tapstate.control.core.CloudCodeExchangeResult;
import io.tapstate.control.core.CloudCodeExchanger;
import io.tapstate.control.core.CloudJwtValidator;
import io.tapstate.control.core.CloudLoginIdentity;
import io.tapstate.control.core.CloudSessionService;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.core.common.JsonReader;
import io.tapstate.spi.store.CloudSessionContext;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionStore;
import io.tapstate.testsupport.RequiresDocker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** A Cloud login carries its current egress address through the real application HTTP surface. */
@RequiresDocker
class CloudClusterContextCarriesTheCurrentEgressAddressIT {

    private static final String CLOUD_URL = "https://cloud.example.test";
    private static final String ORGANIZATION = "context-org";
    private static final String CLUSTER = "context-cluster";
    private static final String JWT = "controlled-context-jwt-sentinel";
    private static final String JWT_WITHOUT_ADDRESS = "controlled-context-no-address-jwt-sentinel";
    private static final String JTI = "context-jti";
    private static final String JTI_WITHOUT_ADDRESS = "context-no-address-jti";
    private static final CloudSessionIdentity DEPLOYMENT =
            new CloudSessionIdentity(CLOUD_URL, ORGANIZATION, CLUSTER);
    private static final CloudSessionContext LOGIN_CONTEXT = new CloudSessionContext(
            ORGANIZATION, CLUSTER, "Workspace organization", "Workspace cluster", "workspace-region",
            "198.51.100.1", "DYNAMIC", "user@example.test");
    private static final CloudClusterEgressAddress CURRENT_ADDRESS = new CloudClusterEgressAddress(
            ORGANIZATION, CLUSTER, "203.0.113.42", "STABLE");
    private static final CloudSessionContext LOGIN_CONTEXT_WITHOUT_ADDRESS = new CloudSessionContext(
            ORGANIZATION, CLUSTER, "Workspace organization", "Workspace cluster", "workspace-region");

    @Test
    void authenticatedClusterContextReturnsTheCurrentAddressAndRepresentsUnavailableAddressExplicitly()
            throws Exception {
        String database = "cloud_egress_context_" + Long.toUnsignedString(System.nanoTime(), 16);
        String storeUri = SharedMongo.replicaSetUrl(database);
        Path stagingDirectory = ServerHandle.privateStagingDirectory();
        try {
            try (var context = new SpringApplicationBuilder(Bootstrap.class, ControlledCloudAuthentication.class).run(
                    "--server.address=127.0.0.1", "--server.port=0",
                    "--tapstate.store.mongo.enabled=true",
                    "--tapstate.store.mongo.uri=" + storeUri,
                    "--tapstate.store.mongo.operator-state-database=" + database + "_operator",
                    "--tapstate.cloud.base-url=" + CLOUD_URL,
                    "--tapstate.cloud.token=controlled-machine-token",
                    "--tapstate.cloud.atlas-uri=" + storeUri,
                    "--tapstate.cloud.cluster-id=" + CLUSTER,
                    "--tapstate.connectors.seed-dir=" + AtlasRuntime.seedDirectory(),
                    "--tapstate.connectors.plugins-dir=" + stagingDirectory,
                    "--tapstate.connectors.also-accept-ids=" + E2eConnectorJar.CONNECTOR_ID,
                    "--SDK_STATUS_SENDER_ENABLED=false")) {
                URI base = URI.create("http://127.0.0.1:"
                        + ((WebServerApplicationContext) context).getWebServer().getPort());
                HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.NEVER).build();

                HttpResponse<String> login = client.send(HttpRequest.newBuilder(
                                base.resolve("/auth/exchange?code=controlled-code"))
                        .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString());
                assertThat(login.statusCode()).isEqualTo(302);
                assertThat(login.headers().firstValue("Location")).hasValue("/");
                String setCookie = login.headers().firstValue("Set-Cookie").orElseThrow();
                assertThat(setCookie).startsWith("__Host-tapstate-cloud-session=tcs_")
                        .contains("HttpOnly", "Secure", "SameSite=Lax")
                        .doesNotContain(JWT);
                String cookie = setCookie.substring(0, setCookie.indexOf(';'));

                HttpResponse<String> response = client.send(HttpRequest.newBuilder(base.resolve("/api/cluster/context"))
                        .timeout(Duration.ofSeconds(10)).header("Cookie", cookie).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(response.statusCode()).isEqualTo(200);
                assertThat(response.headers().firstValue("Cache-Control")).contains("no-store");
                assertThat(JsonReader.parse(response.body())).isInstanceOf(Map.class);
                Map<?, ?> parsedBody = (Map<?, ?>) JsonReader.parse(response.body());
                Map<String, Object> body = new HashMap<>();
                parsedBody.forEach((key, value) -> body.put((String) key, value));
                assertThat(body)
                        .containsEntry("organizationId", ORGANIZATION)
                        .containsEntry("clusterId", CLUSTER)
                        .containsEntry("organizationName", "Workspace organization")
                        .containsEntry("clusterName", "Workspace cluster")
                        .containsEntry("region", "workspace-region")
                        .containsEntry("userEmail", "user@example.test")
                        .containsEntry("ec2PublicIp", CURRENT_ADDRESS.ec2PublicIp())
                        .containsEntry("egressAddressStatus", CURRENT_ADDRESS.egressAddressStatus())
                        .containsKey("clusterState")
                        .containsEntry("clusterState", null);
                assertThat(response.body()).doesNotContain(JWT, cookie, "controlled-machine-token");

                HttpResponse<String> noAddressLogin = client.send(HttpRequest.newBuilder(
                                base.resolve("/auth/exchange?code=controlled-code-without-egress"))
                        .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString());
                assertThat(noAddressLogin.statusCode()).isEqualTo(302);
                String noAddressSetCookie = noAddressLogin.headers().firstValue("Set-Cookie").orElseThrow();
                String noAddressCookie = noAddressSetCookie.substring(0, noAddressSetCookie.indexOf(';'));
                HttpResponse<String> noAddressResponse = client.send(
                        HttpRequest.newBuilder(base.resolve("/api/cluster/context"))
                                .timeout(Duration.ofSeconds(10)).header("Cookie", noAddressCookie).GET().build(),
                        HttpResponse.BodyHandlers.ofString());
                assertThat(noAddressResponse.statusCode()).isEqualTo(200);
                Map<?, ?> noAddressParsed = (Map<?, ?>) JsonReader.parse(noAddressResponse.body());
                Map<String, Object> noAddressBody = new HashMap<>();
                noAddressParsed.forEach((key, value) -> noAddressBody.put((String) key, value));
                assertThat(noAddressBody)
                        .containsEntry("ec2PublicIp", null)
                        .containsEntry("egressAddressStatus", null)
                        .containsEntry("userEmail", null);
                assertThat(noAddressResponse.body())
                        .doesNotContain(JWT_WITHOUT_ADDRESS, noAddressCookie, "controlled-machine-token");

                HttpResponse<String> anonymous = client.send(HttpRequest.newBuilder(base.resolve("/api/cluster/context"))
                        .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString());
                assertThat(anonymous.statusCode()).isEqualTo(401);
                assertThat(anonymous.body()).doesNotContain(
                        LOGIN_CONTEXT.ec2PublicIp(), "user@example.test", JWT, JWT_WITHOUT_ADDRESS);
            }
        } finally {
            ServerHandle.discardStagingDirectory(stagingDirectory);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ControlledCloudAuthentication {
        @Bean
        @Primary
        CloudAuthenticationService controlledAuthentication(CloudSessionStore sessions, TokenSecrets secrets,
                Clock clock) {
            CloudCodeExchanger exchanger = new CloudCodeExchanger() {
                @Override
                public String exchange(String exchangeCode, String clusterId) {
                    return JWT;
                }

                @Override
                public CloudCodeExchangeResult exchangeWithContext(String exchangeCode, String clusterId) {
                    assertThat(clusterId).isEqualTo(CLUSTER);
                    return switch (exchangeCode) {
                        case "controlled-code" -> new CloudCodeExchangeResult(JWT, LOGIN_CONTEXT);
                        case "controlled-code-without-egress" ->
                                new CloudCodeExchangeResult(JWT_WITHOUT_ADDRESS, LOGIN_CONTEXT_WITHOUT_ADDRESS);
                        default -> throw new AssertionError("unexpected exchange code");
                    };
                }

            };
            CloudJwtValidator validator = (jwt, deployment, audience) -> {
                if (!DEPLOYMENT.equals(deployment)) return Optional.empty();
                if (JWT.equals(jwt)) {
                    return Optional.of(new CloudLoginIdentity(DEPLOYMENT, "context-user", ORGANIZATION, CLUSTER,
                            JTI, Scope.WRITE, Instant.now(clock).plusSeconds(300)));
                }
                if (JWT_WITHOUT_ADDRESS.equals(jwt)) {
                    return Optional.of(new CloudLoginIdentity(DEPLOYMENT, "context-user", ORGANIZATION, CLUSTER,
                            JTI_WITHOUT_ADDRESS, Scope.WRITE, Instant.now(clock).plusSeconds(300)));
                }
                return Optional.empty();
            };
            AtomicInteger addressReads = new AtomicInteger();
            CloudClusterEgressAddressProvider addressProvider = (organizationId, clusterId) ->
                    addressReads.getAndIncrement() == 0 ? Optional.of(CURRENT_ADDRESS) : Optional.empty();
            return new CloudAuthenticationService(exchanger, validator,
                    new CloudSessionService(sessions, DEPLOYMENT, secrets, clock),
                    io.tapstate.control.core.CloudAuthenticationObserver.NONE, addressProvider);
        }
    }
}
