package io.tapstate.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.jsonwebtoken.Jwts;
import io.tapstate.control.core.CloudRuntimeStatus;
import io.tapstate.spi.store.CloudSessionIdentity;
import org.junit.jupiter.api.Test;

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
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Runs the real Cloud SDK against the current C1, JWKS, callback, and C2 wire shapes. */
class CloudSdkLiveContractTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String CLUSTER = "cluster-one";
    private static final String TOKEN = "shared-static-token-sentinel";
    private static final String KID = "test-key-one";

    @Test
    void oneStaticTokenDrivesCodeExchangeAndStatusWhileTheSdkVerifiesJwtAndCallback() throws Exception {
        KeyPair keyPair = rsaKeyPair();
        AtomicReference<String> exchangeSecret = new AtomicReference<>();
        AtomicReference<JsonNode> exchangeBody = new AtomicReference<>();
        AtomicReference<String> statusAuthorization = new AtomicReference<>();
        AtomicReference<JsonNode> statusBody = new AtomicReference<>();

        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        String baseUrl = "http://127.0.0.1:" + server.getAddress().getPort();
        String jwt = jwt(keyPair, baseUrl);
        server.createContext("/v1/api/auth/exchange", exchange -> {
            exchangeSecret.set(exchange.getRequestHeaders().getFirst("X-Cluster-Identity-Secret"));
            exchangeBody.set(JSON.readTree(exchange.getRequestBody()));
            respond(exchange, Map.of(
                    "opId", "exchange-one", "code", "ok", "msg", "ok",
                    "data", Map.of(
                            "jwt", jwt,
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

            String exchanged = bridge.exchange("one-time-code", CLUSTER);
            var login = bridge.validate(exchanged, deployment).orElseThrow();
            assertThat(login.userId()).isEqualTo("stable-user-one");
            assertThat(login.jwtId()).isEqualTo("jwt-one");
            assertThat(exchangeSecret.get()).isEqualTo(TOKEN);
            assertThat(exchangeBody.get().path("exchangeCode").asText()).isEqualTo("one-time-code");
            assertThat(exchangeBody.get().path("clusterId").asText()).isEqualTo(CLUSTER);

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
        Instant issued = Instant.now();
        return Jwts.builder()
                .subject("user@example.test")
                .issuer(issuer)
                .audience().add(CLUSTER + ".api.tapstate.io").and()
                .issuedAt(Date.from(issued))
                .expiration(Date.from(Instant.parse("2030-01-01T00:00:00Z")))
                .id("jwt-one")
                .claim("user_id", "stable-user-one")
                .claim("org_id", "org-one")
                .claim("cluster_id", CLUSTER)
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
        byte[] bytes = JSON.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
}
