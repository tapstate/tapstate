package io.tapstate.app;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.jsonwebtoken.Jwts;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

/** Local provider boundary; the application still uses the installed SDK and real JWT verification. */
final class CloudConsumerWireFixture implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String KEY_ID = "consumer-key";
    private final HttpServer server;
    private final KeyPair keys;
    private final String cluster;
    final List<Request> exchanges = new CopyOnWriteArrayList<>();
    final List<Request> reports = new CopyOnWriteArrayList<>();
    final List<String> unexpected = new CopyOnWriteArrayList<>();

    CloudConsumerWireFixture(String cluster) throws Exception {
        this.cluster = cluster;
        var generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        keys = generator.generateKeyPair();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/api/auth/exchange", exchange -> {
            JsonNode body = JSON.readTree(exchange.getRequestBody());
            exchanges.add(new Request(exchange.getRequestMethod(), exchange.getRequestHeaders().getFirst("X-Cluster-Identity-Secret"), body));
            String code = body.path("exchangeCode").asText();
            respond(exchange, Map.of("opId", "exchange", "code", "ok", "msg", "ok", "data", Map.of(
                    "jwt", jwt(code), "expiresAt", Instant.now().plusSeconds(900).toString(), "jti", code,
                    "userEmail", "user@example.test", "orgId", "consumer-org", "clusterId", cluster,
                    "organizationName", "Consumer Organization", "clusterName", "Consumer Cluster", "region", "us-east-1")));
        });
        server.createContext("/v1/api/jwks.json", exchange -> {
            var publicKey = (RSAPublicKey) keys.getPublic();
            respond(exchange, Map.of("keys", List.of(Map.of("kty", "RSA", "kid", KEY_ID, "use", "sig", "alg", "RS256",
                    "n", base64(unsigned(publicKey.getModulus().toByteArray())),
                    "e", base64(unsigned(publicKey.getPublicExponent().toByteArray()))))));
        });
        server.createContext("/v1/api/clusters/" + cluster + "/status-report", exchange -> {
            reports.add(new Request(exchange.getRequestMethod(), exchange.getRequestHeaders().getFirst("Authorization"),
                    JSON.readTree(exchange.getRequestBody())));
            respond(exchange, Map.of("opId", "status", "code", "ok", "msg", "ok", "data", Map.of("status", "accepted")));
        });
        server.createContext("/", exchange -> {
            unexpected.add(exchange.getRequestURI().getPath());
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        server.start();
    }

    String baseUrl() { return "http://127.0.0.1:" + server.getAddress().getPort(); }

    String invalidationPath(String jwtId) throws Exception {
        String timestamp = Long.toString(Instant.now().toEpochMilli());
        String nonce = java.util.UUID.randomUUID().toString();
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(keys.getPrivate());
        signature.update(("POST|" + timestamp + "|" + nonce + "|" + jwtId).getBytes(StandardCharsets.UTF_8));
        return "/auth/invalidate-session?jti=" + jwtId + "&ts=" + timestamp + "&nonce=" + nonce
                + "&sign=" + base64(signature.sign());
    }

    private String jwt(String jwtId) {
        return Jwts.builder().subject("user@example.test").issuer(baseUrl()).audience().add("127.0.0.1").and()
                .issuedAt(Date.from(Instant.now())).expiration(Date.from(Instant.now().plusSeconds(900))).id(jwtId)
                .claim("user_id", "consumer-user").claim("org_id", "consumer-org").claim("cluster_id", cluster)
                .claim("scope", List.of("workload:read", "workload:write"))
                .header().keyId(KEY_ID).and().signWith((RSAPrivateKey) keys.getPrivate()).compact();
    }

    private static byte[] unsigned(byte[] bytes) {
        return bytes.length > 1 && bytes[0] == 0 ? Arrays.copyOfRange(bytes, 1, bytes.length) : bytes;
    }
    private static String base64(byte[] bytes) { return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes); }
    private static void respond(HttpExchange exchange, Map<String, ?> body) throws IOException {
        byte[] bytes = JSON.writeValueAsBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }
    @Override public void close() { server.stop(0); }
    record Request(String method, String authorization, JsonNode body) { }
}
