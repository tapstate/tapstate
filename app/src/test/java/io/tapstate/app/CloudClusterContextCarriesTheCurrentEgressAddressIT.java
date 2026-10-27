package io.tapstate.app;

import io.tapstate.control.core.CloudClusterEgressAddress;
import io.tapstate.core.common.JsonReader;
import io.tapstate.spi.store.CloudSessionContext;
import io.tapstate.testsupport.RequiresDocker;
import io.tapstate.cloud.sdk.client.ClusterEgressAddress;
import io.tapstate.control.core.CloudRuntimeStatus;
import io.tapstate.spi.store.StorageBinding;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
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
    private static final CloudSessionContext LOGIN_CONTEXT = new CloudSessionContext(
            ORGANIZATION, CLUSTER, "Workspace organization", "Workspace cluster", "workspace-region",
            "198.51.100.1", "DYNAMIC");
    private static final CloudClusterEgressAddress CURRENT_ADDRESS = new CloudClusterEgressAddress(
            ORGANIZATION, CLUSTER, "203.0.113.42", "STABLE");
    private static final CloudSessionContext LOGIN_CONTEXT_WITHOUT_ADDRESS = new CloudSessionContext(
            ORGANIZATION, CLUSTER, "Workspace organization", "Workspace cluster", "workspace-region");

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    @TempDir Path stagingDirectory;

    @Test
    void authenticatedClusterContextReturnsTheCurrentAddressAndRepresentsUnavailableAddressExplicitly()
            throws Exception {
        var storage = NativeAppStorage.create(MONGO, "cloud_egress_context", StorageBinding.Mode.CLOUD, CLUSTER);
        String storeUri = storage.configurationUri();
        try (var context = new SpringApplicationBuilder(Bootstrap.class, ControlledCloudAuthentication.class)
                .environment(CloudFixtureEnvironment.isolated()).run(
                    "--server.address=127.0.0.1", "--server.port=0", "--tapstate.hz.member-port=0",
                    "--tapstate.hz.jet.cooperative-thread-count=2",
                    "--tapstate.store.mongo.uri=" + storeUri,
                    "--tapstate.cloud.base-url=" + CLOUD_URL,
                    "--tapstate.cloud.token=controlled-machine-token",
                    "--tapstate.cloud.atlas-uri=" + storeUri,
                    "--tapstate.cloud.cluster-id=" + CLUSTER,
                    "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory(),
                    "--tapstate.connectors.plugins-dir=" + stagingDirectory,
                    "--SDK_STATUS_SENDER_ENABLED=false")) {
                assertThat(NativeAppStorage.business(context).isRunning()).isTrue();
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
                        .containsEntry("ec2PublicIp", CURRENT_ADDRESS.ec2PublicIp())
                        .containsEntry("egressAddressStatus", CURRENT_ADDRESS.egressAddressStatus())
                        .containsKey("clusterState")
                        .containsEntry("clusterState", null);
                assertThat(response.body()).doesNotContain(JWT, cookie, "controlled-machine-token");

                // The production provider retains recent observations; advance beyond its stale budget.
                context.getBean(ContextClock.class).advance(CachedCloudClusterEgressAddressProvider.MAX_STALE_AGE.plusSeconds(1));
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
                        .containsEntry("egressAddressStatus", null);
                assertThat(noAddressResponse.body())
                        .doesNotContain(JWT_WITHOUT_ADDRESS, noAddressCookie, "controlled-machine-token");

                HttpResponse<String> anonymous = client.send(HttpRequest.newBuilder(base.resolve("/api/cluster/context"))
                        .timeout(Duration.ofSeconds(10)).GET().build(), HttpResponse.BodyHandlers.ofString());
                assertThat(anonymous.statusCode()).isEqualTo(401);
                assertThat(anonymous.body()).doesNotContain(
                        LOGIN_CONTEXT.ec2PublicIp(), JWT, JWT_WITHOUT_ADDRESS);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class ControlledCloudAuthentication {
        @Bean
        @Primary
        CloudSdkBridge controlledAuthentication(CloudRuntimeSettings settings, ContextClock clock) {
            AtomicInteger addressReads = new AtomicInteger();
            return new CloudSdkBridge(settings, new CloudSdkBridge.SdkRuntime() {
                @Override public CloudSdkBridge.ExchangeResult exchange(String code) {
                    var selected = switch (code) {
                        case "controlled-code" -> LOGIN_CONTEXT;
                        case "controlled-code-without-egress" -> LOGIN_CONTEXT_WITHOUT_ADDRESS;
                        default -> throw new AssertionError("unexpected exchange code");
                    };
                    return new CloudSdkBridge.ExchangeResult(selected == LOGIN_CONTEXT ? JWT : JWT_WITHOUT_ADDRESS,
                            ORGANIZATION, CLUSTER, selected.organizationName(), selected.clusterName(), selected.region(),
                            selected.ec2PublicIp(), selected.egressAddressStatus());
                }
                @Override public CloudSdkBridge.VerifiedClaims verify(String jwt) {
                    String jti = JWT.equals(jwt) ? JTI : JWT_WITHOUT_ADDRESS.equals(jwt) ? JTI_WITHOUT_ADDRESS : null;
                    if (jti == null) throw new CloudSdkBridge.ProviderFailure();
                    return new CloudSdkBridge.VerifiedClaims("context-user", ORGANIZATION, CLUSTER, jti,
                            clock.instant().plusSeconds(300), CLOUD_URL, "127.0.0.1", List.of("workload:write"));
                }
                @Override public ClusterEgressAddress currentEgressAddress() {
                    if (addressReads.getAndIncrement() == 0)
                        return new ClusterEgressAddress(ORGANIZATION, CLUSTER, CURRENT_ADDRESS.ec2PublicIp(),
                                CURRENT_ADDRESS.egressAddressStatus());
                    throw new CloudSdkBridge.ProviderFailure();
                }
                @Override public boolean verifyCallback(String method, String timestamp, String nonce, String data, String signature) {
                    throw new AssertionError("no callback in this scenario");
                }
                @Override public void report(String clusterId, String nonce, CloudRuntimeStatus status, String token) {
                    throw new AssertionError("status reporting is disabled in this scenario");
                }
            });
        }

        @Bean @Primary ContextClock contextClock() { return new ContextClock(); }
    }

    static final class ContextClock extends Clock {
        private final AtomicReference<Instant> now = new AtomicReference<>(Instant.now());
        void advance(Duration duration) { now.updateAndGet(value -> value.plus(duration)); }
        @Override public Instant instant() { return now.get(); }
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return Clock.fixed(instant(), zone); }
    }
}
