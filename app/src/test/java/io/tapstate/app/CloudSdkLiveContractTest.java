package io.tapstate.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import io.jsonwebtoken.Jwts;
import io.tapstate.control.core.CloudRuntimeStatus;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CloudSessionIdentity;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Runs the real Cloud SDK against the current C1, JWKS, callback, and C2 wire shapes. */
class CloudSdkLiveContractTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String CLUSTER = "cluster-one";
    private static final String REQUEST_AUDIENCE = "cluster.dev.cloud.tapstate.com";
    private static final String TOKEN = "shared-static-token-sentinel";
    private static final String KID = "test-key-one";

    @Test
    void jwtVerificationRejectionsKeepAnInternalClassificationWithoutLoggingTheJwt() throws Exception {
        KeyPair keyPair = rsaKeyPair();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/api/jwks.json", exchange -> respond(exchange, jwks(keyPair)));
        server.start();
        Logger logger = (Logger) LoggerFactory.getLogger(CloudSdkBridge.class);
        ListAppender<ILoggingEvent> captured = new ListAppender<>();
        captured.start();
        logger.addAppender(captured);
        try {
            String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
            CloudProperties properties = new CloudProperties();
            properties.setBaseUrl(baseUrl);
            properties.setToken(TOKEN);
            properties.setAtlasUri("mongodb://user:secret@atlas.example/cluster_meta");
            properties.setClusterId(CLUSTER);
            String rawJwt = jwt(keyPair, "http://wrong-issuer.example");
            CloudSdkBridge bridge = new CloudSdkBridge(CloudRuntimeSettings.resolve(properties));
            MockHttpServletRequest request = new MockHttpServletRequest("GET", "/auth/exchange");
            request.setQueryString("code=request-code-secret-sentinel");
            MockHttpServletResponse response = new MockHttpServletResponse();
            new CloudHttpDiagnosticsFilter().doFilter(request, response, (incoming, outgoing) -> {
                assertThat(bridge.validate(rawJwt, new CloudSessionIdentity(
                        baseUrl, CloudSdkBridge.DEPLOYMENT_ORGANIZATION, CLUSTER), REQUEST_AUDIENCE)).isEmpty();
                response.setStatus(401);
            });
            assertThat(captured.list).extracting(ILoggingEvent::getFormattedMessage)
                    .anySatisfy(message -> assertThat(message)
                            .contains("stage=jwt-verification", "reason=bad-issuer")
                            .contains(response.getHeader("X-Request-ID"))
                            .doesNotContain(rawJwt, TOKEN, "atlas.example"));
            assertThat(captured.list).allSatisfy(event -> assertThat(event.getThrowableProxy()).isNull());
        } finally {
            logger.detachAppender(captured);
            captured.stop();
            server.stop(0);
        }
    }

    @Test
    void exchangeFailuresLogTheirStatusAndCodeWithoutProviderPayloads() throws Exception {
        AtomicReference<String> failureCode = new AtomicReference<>("exchange.code-expired");
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/api/auth/exchange", exchange -> respond(exchange, 401, Map.of(
                "code", failureCode.get(), "msg", "provider-message-secret-sentinel",
                "data", Map.of("jwt", "provider-jwt-secret-sentinel"))));
        server.start();
        Logger logger = (Logger) LoggerFactory.getLogger(CloudSdkBridge.class);
        ListAppender<ILoggingEvent> captured = new ListAppender<>();
        captured.start();
        logger.addAppender(captured);
        try {
            CloudProperties properties = new CloudProperties();
            properties.setBaseUrl("http://127.0.0.1:" + server.getAddress().getPort());
            properties.setToken(TOKEN);
            properties.setAtlasUri("mongodb://user:secret@atlas.example/cluster_meta");
            properties.setClusterId(CLUSTER);
            CloudSdkBridge bridge = new CloudSdkBridge(CloudRuntimeSettings.resolve(properties));
            for (String suppliedCode : List.of("exchange.code-expired", "provider-code-secret-sentinel\nforged-log")) {
                failureCode.set(suppliedCode);
                assertThatThrownBy(() -> bridge.exchange("request-code-secret-sentinel", CLUSTER))
                        .isInstanceOf(TapstateException.class).hasNoCause();
            }
            assertThat(captured.list).extracting(ILoggingEvent::getFormattedMessage)
                    .containsExactly(
                            "Cloud authentication rejected [request_id=none, stage=code-exchange, http=401, reason=exchange.code-expired]",
                            "Cloud authentication rejected [request_id=none, stage=code-exchange, http=401, reason=unclassified]");
            for (ILoggingEvent event : captured.list) {
                assertThat(event.getFormattedMessage()).doesNotContain(
                        TOKEN, "request-code-secret-sentinel", "provider-message-secret-sentinel",
                        "provider-jwt-secret-sentinel", "provider-code-secret-sentinel", "atlas.example");
                assertThat(event.getThrowableProxy()).isNull();
            }
        } finally {
            logger.detachAppender(captured);
            captured.stop();
            server.stop(0);
        }
    }

    @Test
    void oneStaticTokenDrivesCodeExchangeAndStatusWhileTheSdkVerifiesJwtAndCallback() throws Exception {
        KeyPair keyPair = rsaKeyPair();
        AtomicReference<String> exchangeSecret = new AtomicReference<>();
        AtomicReference<JsonNode> exchangeBody = new AtomicReference<>();
        AtomicReference<String> statusAuthorization = new AtomicReference<>();
        AtomicReference<JsonNode> statusBody = new AtomicReference<>();
        AtomicInteger exchanges = new AtomicInteger();
        AtomicReference<String> requestedAudience = new AtomicReference<>(REQUEST_AUDIENCE);

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
        server.createContext("/v1/api/auth/exchange", exchange -> {
            exchanges.incrementAndGet();
            exchangeSecret.set(exchange.getRequestHeaders().getFirst("X-Cluster-Identity-Secret"));
            exchangeBody.set(JSON.readTree(exchange.getRequestBody()));
            respond(exchange, Map.of(
                    "opId", "exchange-one", "code", "ok", "msg", "ok",
                    "data", Map.of(
                            "jwt", jwt(keyPair, baseUrl, requestedAudience.get()),
                            "expiresAt", "2030-01-01T00:00:00Z",
                            "jti", "jwt-one",
                            "userEmail", "user@example.test",
                            "orgId", "org-one",
                            "clusterId", CLUSTER)));
        });
        server.createContext("/v1/api/jwks.json", exchange -> respond(exchange, jwks(keyPair)));
        server.createContext("/v1/api/clusters/" + CLUSTER + "/status-report", exchange -> {
            statusAuthorization.set(exchange.getRequestHeaders().getFirst("Authorization"));
            statusBody.set(JSON.readTree(exchange.getRequestBody()));
            respond(exchange, Map.of(
                    "opId", "status-one", "code", "ok", "msg", "ok",
                    "data", Map.of("status", "accepted")));
        });
        server.start();
        try {
            CloudProperties properties = new CloudProperties();
            properties.setBaseUrl(baseUrl);
            properties.setToken(TOKEN);
            properties.setAtlasUri("mongodb://user:secret@atlas.example/cluster_meta");
            properties.setClusterId(CLUSTER);
            CloudSdkBridge bridge = new CloudSdkBridge(CloudRuntimeSettings.resolve(properties));
            CloudSessionIdentity deployment = new CloudSessionIdentity(
                    baseUrl, CloudSdkBridge.DEPLOYMENT_ORGANIZATION, CLUSTER);

            int expectedExchanges = 0;
            for (String audience : List.of(REQUEST_AUDIENCE, "cluster.cloud.tapstate.com", "data.customer.example")) {
                requestedAudience.set(audience);
                String code = "one-time-code-" + ++expectedExchanges;
                String exchanged = bridge.exchange(code, CLUSTER);
                assertThat(bridge.validate(exchanged, deployment, audience)).hasValueSatisfying(login -> {
                    assertThat(login.userId()).isEqualTo("stable-user-one");
                    assertThat(login.jwtId()).isEqualTo("jwt-one");
                });
                assertThat(exchangeSecret.get()).isEqualTo(TOKEN);
                assertThat(exchangeBody.get()).isEqualTo(JSON.valueToTree(Map.of(
                        "exchangeCode", code, "clusterId", CLUSTER)));
                assertThat(bridge.validate(exchanged, deployment, "different.customer.example")).isEmpty();
                assertThat(bridge.validate(jwt(keyPair, baseUrl, CLUSTER + ".api.tapstate.io"), deployment, audience))
                        .isEmpty();
                assertThat(bridge.validate(jwt(keyPair, baseUrl, audience, "other-cluster"), deployment, audience))
                        .isEmpty();
                assertThat(exchanges.get()).as("JWT validation must not retry the one-time exchange")
                        .isEqualTo(expectedExchanges);
            }
            assertThat(bridge.validate(jwt(keyPair, baseUrl, REQUEST_AUDIENCE, CLUSTER,
                    Instant.now().minusSeconds(60)), deployment, REQUEST_AUDIENCE)).isEmpty();
            assertThat(exchanges.get()).isEqualTo(3);

            String timestamp = "1780000000000";
            String nonce = "callback-nonce";
            String signature = sign(keyPair, "POST|" + timestamp + "|" + nonce + "|jwt-one");
            assertThat(bridge.verify(baseUrl, CloudSdkBridge.DEPLOYMENT_ORGANIZATION, CLUSTER,
                    "POST", timestamp, nonce, "jwt-one", signature)).isTrue();

            bridge.send(CLUSTER, "status-nonce",
                    new CloudRuntimeStatus("0.5.0", 1234, 2, Instant.parse("2026-09-29T08:00:00Z")));
            assertThat(statusAuthorization.get()).isEqualTo("Bearer " + TOKEN);
            assertThat(statusBody.get().path("nonce").asText()).isEqualTo("status-nonce");
            assertThat(statusBody.get().path("runtimeVersion").asText()).isEqualTo("0.5.0");
            assertThat(statusBody.get().path("uptimeMs").asLong()).isEqualTo(1234);
            assertThat(statusBody.get().path("activePipelines").asInt()).isEqualTo(2);
            assertThat(statusBody.get().toString())
                    .doesNotContain("atlas.example", "secret", "one-time-code", "stable-user-one");
        } finally {
            server.stop(0);
        }
    }

    private static KeyPair rsaKeyPair() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        return generator.generateKeyPair();
    }

    private static String jwt(KeyPair keyPair, String issuer) {
        return jwt(keyPair, issuer, REQUEST_AUDIENCE);
    }

    private static String jwt(KeyPair keyPair, String issuer, String audience) {
        return jwt(keyPair, issuer, audience, CLUSTER);
    }

    private static String jwt(KeyPair keyPair, String issuer, String audience, String clusterId) {
        return jwt(keyPair, issuer, audience, clusterId, Instant.parse("2030-01-01T00:00:00Z"));
    }

    private static String jwt(KeyPair keyPair, String issuer, String audience, String clusterId, Instant expiresAt) {
        Instant issued = Instant.now();
        return Jwts.builder()
                .subject("user@example.test")
                .issuer(issuer)
                .audience().add(audience).and()
                .issuedAt(Date.from(issued))
                .expiration(Date.from(expiresAt))
                .id("jwt-one")
                .claim("user_id", "stable-user-one")
                .claim("org_id", "org-one")
                .claim("cluster_id", clusterId)
                .claim("scope", List.of("workload:read", "workload:write"))
                .header().keyId(KID).and()
                .signWith((RSAPrivateKey) keyPair.getPrivate())
                .compact();
    }

    private static Map<String, Object> jwks(KeyPair keyPair) {
        RSAPublicKey key = (RSAPublicKey) keyPair.getPublic();
        return Map.of("keys", List.of(Map.of(
                "kty", "RSA",
                "kid", KID,
                "use", "sig",
                "alg", "RS256",
                "n", base64(unsigned(key.getModulus().toByteArray())),
                "e", base64(unsigned(key.getPublicExponent().toByteArray())))));
    }

    private static String sign(KeyPair keyPair, String canonical) throws Exception {
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(keyPair.getPrivate());
        signature.update(canonical.getBytes(StandardCharsets.UTF_8));
        return base64(signature.sign());
    }

    private static byte[] unsigned(byte[] value) {
        return value.length > 1 && value[0] == 0 ? java.util.Arrays.copyOfRange(value, 1, value.length) : value;
    }

    private static String base64(byte[] value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value);
    }

    private static void respond(HttpExchange exchange, Map<String, ?> body) throws IOException {
        respond(exchange, 200, body);
    }

    private static void respond(HttpExchange exchange, int status, Map<String, ?> body) throws IOException {
        byte[] bytes = JSON.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
