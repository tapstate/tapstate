package io.tapstate.app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.jsonwebtoken.Jwts;
import io.tapstate.control.core.AuditGate;
import io.tapstate.control.core.AuthenticationMode;
import io.tapstate.control.core.CloudAuthenticationService;
import io.tapstate.control.core.CloudSessionCallbackVerifier;
import io.tapstate.control.core.CloudSessionService;
import io.tapstate.control.core.ConnectionTestResultQueryService;
import io.tapstate.control.core.ConnectionTestService;
import io.tapstate.control.core.ControlOperations;
import io.tapstate.control.core.CredentialAuthenticator;
import io.tapstate.control.core.CurrentUserQueryService;
import io.tapstate.control.core.GeneratedSecret;
import io.tapstate.control.core.OperationRegistry;
import io.tapstate.control.core.SchemaDiscoveryService;
import io.tapstate.control.core.SchemaQueryService;
import io.tapstate.control.core.TokenAdminService;
import io.tapstate.control.core.TokenSecrets;
import io.tapstate.control.core.TokenService;
import io.tapstate.control.restapi.ControlHttpFace;
import io.tapstate.control.restapi.RestApiConfiguration;
import io.tapstate.spi.store.AuditRecord;
import io.tapstate.spi.store.CloudSessionIdentity;
import io.tapstate.spi.store.CloudSessionRecord;
import io.tapstate.spi.store.CloudSessionStore;
import io.tapstate.spi.store.ConnectionTestResult;
import io.tapstate.spi.store.ConnectionTestResultStore;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.TokenRecord;
import io.tapstate.spi.store.TokenStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Real SDK and HTTP authorization over isolated memory ports; no browser, database, or PDK proof. */
class ClusterServerAuthorizationApiTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String CLUSTER = "authorization-cluster";
    private static final String ORGANIZATION = "authorization-org";
    private static final String TOKEN = "authorization-static-secret-sentinel";
    private static final String KID = "authorization-key";
    private static final String COOKIE_NAME = "__Host-tapstate-cloud-session";
    private static final Map<String, Object> CONNECTION = Map.of(
            "id", "authorization-probe", "connectorId", "mongodb", "settings", Map.of("host", "unused"));

    @Test
    void verifiedCloudCookiesEnforceGradesAndRejectedLoginProofsHaveNoSessionSideEffects() throws Exception {
        try (Provider provider = new Provider(); HttpClient client = HttpClient.newBuilder()
                .followRedirects(HttpClient.Redirect.NEVER).connectTimeout(Duration.ofSeconds(5)).build()) {
            State state = new State(provider);
            try (ConfigurableApplicationContext context = new SpringApplicationBuilder(TestApp.class)
                    .initializers(app -> app.getBeanFactory().registerSingleton("authorizationState", state))
                    .run("--server.address=127.0.0.1", "--server.port=0",
                            "--tapstate.test.cluster-authorization-api=true")) {
                String origin = "http://127.0.0.1:" + ((WebServerApplicationContext) context).getWebServer().getPort();
                String read = login(client, origin, "read");
                String write = login(client, origin, "write");
                assertThat(state.sessions.snapshot()).hasSize(2);
                assertThat(state.secrets.generated.get()).isEqualTo(2);
                assertThat(provider.jwksRequests.get()).isPositive();

                Map<String, CloudSessionRecord> before = state.sessions.snapshot();
                assertThat(state.bridge.validate(provider.jwt("wrong-org"), new CloudSessionIdentity(
                        provider.baseUrl(), CloudSdkBridge.DEPLOYMENT_ORGANIZATION, CLUSTER), "127.0.0.1"))
                        .hasValueSatisfying(login -> assertThat(login.organizationId()).isEqualTo(ORGANIZATION));
                for (String code : List.of("tampered", "wrong-org")) {
                    var rejected = request(client, origin, "GET", "/auth/exchange?code=" + code,
                            null, null, Map.of());
                    assertError(rejected, 401, "control.unauthenticated");
                    for (String jwt : provider.issuedJwts) {
                        assertThat(rejected.body().contains(jwt)).as("login refusal contains no issued JWT").isFalse();
                    }
                    assertThat(state.sessions.snapshot()).isEqualTo(before);
                    assertThat(state.sessions.creates.get()).isEqualTo(2);
                    assertThat(state.secrets.generated.get()).isEqualTo(2);
                }
                assertThat(provider.exchanges).hasSize(4);
                assertThat(provider.exchanges).extracting(exchange -> exchange.body().get("exchangeCode"))
                        .containsExactly("read", "write", "tampered", "wrong-org");
                for (Exchange exchange : provider.exchanges) {
                    assertThat(exchange.body()).containsOnlyKeys("exchangeCode", "clusterId")
                            .containsEntry("clusterId", CLUSTER);
                    assertThat(exchange.identitySecret()).isEqualTo(TOKEN);
                }

                var readMe = request(client, origin, "GET", "/api/auth/me", read, null, Map.of());
                assertThat(readMe.statusCode()).isEqualTo(200);
                assertThat(body(readMe)).isEqualTo(Map.of("mode", "cloud", "principal", "user-read",
                        "scopes", List.of("read")));
                var writeMe = request(client, origin, "GET", "/api/auth/me", write, null, Map.of());
                assertThat(writeMe.statusCode()).isEqualTo(200);
                assertThat(body(writeMe)).isEqualTo(Map.of("mode", "cloud", "principal", "user-write",
                        "scopes", List.of("read", "write")));

                var deniedWrite = request(client, origin, "POST", "/api/connections:test", read,
                        CONNECTION, Map.of());
                assertError(deniedWrite, 403, "control.forbidden");
                assertThat(body(deniedWrite).get("params"))
                        .isEqualTo(Map.of("op", "connection.test", "required", "write"));
                assertThat(state.probes.get()).isZero();
                assertThat(state.results).isEmpty();
                assertThat(state.audit).isEmpty();

                var permittedWrite = request(client, origin, "POST", "/api/connections:test", write,
                        CONNECTION, Map.of());
                assertThat(permittedWrite.statusCode()).isEqualTo(200);
                assertThat(body(permittedWrite)).containsEntry("outcome", "PASSED");
                assertThat(state.probes.get()).isEqualTo(1);
                assertThat(state.results).containsOnlyKeys("authorization-probe");
                assertThat(state.audit).singleElement().satisfies(record -> {
                    assertThat(record.principal()).isEqualTo("user-write");
                    assertThat(record.operationId()).isEqualTo("connection.test");
                    assertThat(record.resourceId()).isEqualTo("authorization-probe");
                });

                List<AuditRecord> auditBefore = List.copyOf(state.audit);
                Map<String, ConnectionTestResult> resultsBefore = Map.copyOf(state.results);
                var deniedAdmin = request(client, origin, "POST", "/api/tokens", write,
                        Map.of("scope", "read"), Map.of());
                assertError(deniedAdmin, 403, "control.forbidden");
                assertThat(body(deniedAdmin).get("params"))
                        .isEqualTo(Map.of("op", "token.create", "required", "admin"));
                assertThat(state.tokens).isEmpty();
                assertThat(state.secrets.generated.get()).isEqualTo(2);
                assertThat(state.audit).isEqualTo(auditBefore);

                Map<String, String> forged = Map.of(
                        "X-Global-Control-Plane-Signature", "forged-signature-sentinel",
                        "X-Cloud-Signature", "forged-signature-sentinel");
                assertError(request(client, origin, "GET", "/api/auth/me", null, null, forged),
                        401, "control.unauthenticated");
                assertError(request(client, origin, "POST", "/api/connections:test", null, CONNECTION, forged),
                        401, "control.unauthenticated");
                Map<String, CloudSessionRecord> callbackBefore = state.sessions.snapshot();
                assertError(request(client, origin, "POST", "/auth/invalidate-session?jti=jti-write&ts="
                                + Instant.now().toEpochMilli() + "&nonce=authorization-nonce&sign=forged",
                        null, null, forged), 401, "control.unauthenticated");
                assertThat(state.sessions.snapshot()).isEqualTo(callbackBefore);
                assertThat(state.sessions.revocations.get()).isZero();
                assertThat(state.probes.get()).isEqualTo(1);
                assertThat(state.results).isEqualTo(resultsBefore);
                assertThat(state.audit).isEqualTo(auditBefore);
                assertThat(request(client, origin, "GET", "/api/auth/me", write, null, Map.of()).statusCode())
                        .isEqualTo(200);
                assertThat(provider.exchanges).hasSize(4);
            }
        }
    }

    private static String login(HttpClient client, String origin, String code) throws Exception {
        var response = request(client, origin, "GET", "/auth/exchange?code=" + code, null, null, Map.of());
        assertThat(response.statusCode()).isEqualTo(302);
        assertThat(response.headers().firstValue("Location")).hasValue("/");
        List<String> cookies = response.headers().allValues("Set-Cookie");
        assertThat(cookies).hasSize(1);
        String cookie = cookies.getFirst();
        assertThat(cookie.startsWith(COOKIE_NAME + "=tcs_")).isTrue();
        assertThat(cookie).contains("HttpOnly", "Secure", "Path=/", "SameSite=Lax");
        return cookie.substring(0, cookie.indexOf(';'));
    }

    private static HttpResponse<String> request(HttpClient client, String origin, String method, String path,
            String cookie, Object body, Map<String, String> headers) throws Exception {
        var request = HttpRequest.newBuilder(URI.create(origin + path)).timeout(Duration.ofSeconds(10));
        if (cookie != null) request.header("Cookie", cookie);
        if (!"GET".equals(method)) request.header("Origin", origin);
        headers.forEach(request::header);
        request.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                : HttpRequest.BodyPublishers.ofString(JSON.writeValueAsString(body)));
        if (body != null) request.header("Content-Type", "application/json");
        return client.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    private static Map<String, Object> body(HttpResponse<String> response) throws IOException {
        return JSON.readValue(response.body(), new com.fasterxml.jackson.core.type.TypeReference<>() { });
    }

    private static void assertError(HttpResponse<String> response, int status, String code) throws IOException {
        assertThat(response.statusCode()).isEqualTo(status);
        assertThat(body(response).get("code")).isEqualTo(code);
        assertThat(response.headers().allValues("Set-Cookie")).isEmpty();
        assertThat(response.body().contains(TOKEN)).isFalse();
        assertThat(response.body().contains("secret-sentinel")).isFalse();
        assertThat(response.body().contains("forged-signature-sentinel")).isFalse();
    }

    @SpringBootConfiguration
    @ConditionalOnProperty(name = "tapstate.test.cluster-authorization-api", havingValue = "true")
    @EnableAutoConfiguration
    @Import(RestApiConfiguration.class)
    @ComponentScan(basePackageClasses = ControlHttpFace.class, useDefaultFilters = false,
            includeFilters = @ComponentScan.Filter(type = FilterType.REGEX, pattern =
                    "io\\.tapstate\\.control\\.restapi\\.(RestApiSecurityConfiguration|ApiExceptionHandler|"
                            + "CloudAuthController|CurrentUserController|ConnectionController|TokenController)"))
    static class TestApp {
        @Bean AuthenticationMode mode() { return AuthenticationMode.CLOUD; }
        @Bean OperationRegistry operations() { return ControlOperations.registry(); }
        @Bean CurrentUserQueryService currentUser() { return new CurrentUserQueryService(AuthenticationMode.CLOUD); }
        @Bean CloudAuthenticationService authentication(State state) {
            return new CloudAuthenticationService(state.bridge, state.bridge, new CloudSessionService(state.sessions,
                    new CloudSessionIdentity(state.provider.baseUrl(), CloudSdkBridge.DEPLOYMENT_ORGANIZATION, CLUSTER),
                    state.secrets, Clock.systemUTC()));
        }
        @Bean CloudSessionCallbackVerifier callbacks(State state) { return state.bridge; }
        @Bean CredentialAuthenticator credentials(CloudAuthenticationService authentication) {
            return new CredentialAuthenticator(authentication::authenticate);
        }
        @Bean AuditGate audit(State state) { return new AuditGate(state.audit::add, Clock.systemUTC()); }
        @Bean ConnectionTestResultStore results(State state) {
            return new ConnectionTestResultStore() {
                @Override public void save(ConnectionTestResult result) { state.results.put(result.connectionId(), result); }
                @Override public Optional<ConnectionTestResult> find(String id) { return Optional.ofNullable(state.results.get(id)); }
            };
        }
        @Bean ConnectionTestService connectionTests(State state, ConnectionTestResultStore results, AuditGate audit) {
            return new ConnectionTestService(config -> {
                state.probes.incrementAndGet();
                return new ConnectionTestResult(config.id(), config.connectorId(), ConnectionTestResult.Outcome.PASSED,
                        List.of(), System.currentTimeMillis());
            }, results, audit);
        }
        @Bean ConnectionTestResultQueryService connectionResults(ConnectionTestResultStore results) {
            return new ConnectionTestResultQueryService(results);
        }
        @Bean SchemaStore schemas() {
            return new SchemaStore() {
                @Override public void save(DiscoveredSourceModel model) { throw new AssertionError("unexpected schema write"); }
                @Override public Optional<DiscoveredSourceModel> get(String id) { return Optional.empty(); }
            };
        }
        @Bean SchemaDiscoveryService schemaDiscovery(SchemaStore schemas, AuditGate audit) {
            return new SchemaDiscoveryService(config -> { throw new AssertionError("unexpected discovery"); },
                    schemas, audit, Clock.systemUTC());
        }
        @Bean SchemaQueryService schemaQuery(SchemaStore schemas) { return new SchemaQueryService(schemas); }
        @Bean TokenAdminService tokens(State state, AuditGate audit) {
            TokenStore store = new TokenStore() {
                @Override public void save(TokenRecord record) { state.tokens.put(record.tokenId(), record); }
                @Override public Optional<TokenRecord> find(String id) { return Optional.ofNullable(state.tokens.get(id)); }
                @Override public void revoke(String id) { throw new AssertionError("unexpected token revocation"); }
                @Override public List<TokenRecord> list() { return List.copyOf(state.tokens.values()); }
            };
            return new TokenAdminService(new TokenService(store, state.secrets, Clock.systemUTC()), audit);
        }
    }

    private static final class State {
        final Provider provider;
        final CloudSdkBridge bridge;
        final Sessions sessions = new Sessions();
        final CountingSecrets secrets = new CountingSecrets();
        final AtomicInteger probes = new AtomicInteger();
        final List<AuditRecord> audit = new CopyOnWriteArrayList<>();
        final Map<String, ConnectionTestResult> results = new java.util.concurrent.ConcurrentHashMap<>();
        final Map<String, TokenRecord> tokens = new java.util.concurrent.ConcurrentHashMap<>();
        State(Provider provider) {
            this.provider = provider;
            CloudProperties properties = new CloudProperties();
            properties.setBaseUrl(provider.baseUrl());
            properties.setToken(TOKEN);
            properties.setClusterId(CLUSTER);
            properties.setAtlasUri("mongodb://unused:unused@atlas.example/authorization_metadata");
            bridge = new CloudSdkBridge(CloudRuntimeSettings.resolve(properties));
        }
    }

    private static final class CountingSecrets implements TokenSecrets {
        final AtomicInteger generated = new AtomicInteger();
        final TokenSecrets delegate = new RandomTokenSecrets();
        @Override public GeneratedSecret generate() { generated.incrementAndGet(); return delegate.generate(); }
        @Override public String hash(String raw) { return delegate.hash(raw); }
        @Override public boolean matches(String raw, String hash) { return delegate.matches(raw, hash); }
    }

    private static final class Sessions implements CloudSessionStore {
        final Map<String, CloudSessionRecord> rows = new LinkedHashMap<>();
        final AtomicInteger creates = new AtomicInteger();
        final AtomicInteger revocations = new AtomicInteger();
        synchronized Map<String, CloudSessionRecord> snapshot() { return Map.copyOf(rows); }
        @Override public synchronized boolean create(CloudSessionRecord record) {
            creates.incrementAndGet();
            return rows.putIfAbsent(record.jwtId(), record) == null;
        }
        @Override public synchronized Optional<CloudSessionRecord> find(CloudSessionIdentity identity, String jti) {
            return Optional.ofNullable(rows.get(jti)).filter(row -> identity.equals(row.identity()));
        }
        @Override public synchronized Optional<CloudSessionRecord> authenticate(CloudSessionIdentity identity,
                String jti, String hash, Instant now, Instant expiry) {
            Optional<CloudSessionRecord> found = find(identity, jti).filter(row -> !row.revoked()
                    && row.secretHash().equals(hash) && row.idleExpiresAt().isAfter(now));
            return found.map(row -> {
                var touched = new CloudSessionRecord(identity, jti, hash, row.userId(), row.scope(), false,
                        row.createdAt(), now, expiry, row.clusterContext());
                rows.put(jti, touched);
                return touched;
            });
        }
        @Override public boolean logout(CloudSessionIdentity identity, String jti, String hash, Instant now) {
            throw new AssertionError("unexpected logout");
        }
        @Override public void invalidate(CloudSessionIdentity identity, String jti, Instant now) {
            revocations.incrementAndGet();
            throw new AssertionError("unverified callback reached revocation");
        }
    }

    private record Exchange(Map<String, Object> body, String identitySecret) { }

    private static final class Provider implements AutoCloseable {
        final HttpServer server;
        final KeyPair key;
        final List<Exchange> exchanges = new CopyOnWriteArrayList<>();
        final List<String> issuedJwts = new CopyOnWriteArrayList<>();
        final AtomicInteger jwksRequests = new AtomicInteger();
        Provider() throws Exception {
            KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
            generator.initialize(2048);
            key = generator.generateKeyPair();
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/v1/api/jwks.json", exchange -> {
                jwksRequests.incrementAndGet();
                RSAPublicKey publicKey = (RSAPublicKey) key.getPublic();
                respond(exchange, Map.of("keys", List.of(Map.of("kty", "RSA", "kid", KID, "use", "sig",
                        "alg", "RS256", "n", base64(unsigned(publicKey.getModulus().toByteArray())),
                        "e", base64(unsigned(publicKey.getPublicExponent().toByteArray()))))));
            });
            server.createContext("/v1/api/auth/exchange", exchange -> {
                Map<String, Object> request = JSON.readValue(exchange.getRequestBody(),
                        new com.fasterxml.jackson.core.type.TypeReference<>() { });
                exchanges.add(new Exchange(request, exchange.getRequestHeaders().getFirst("X-Cluster-Identity-Secret")));
                String code = (String) request.get("exchangeCode");
                String jwt = jwt(code);
                if ("tampered".equals(code)) {
                    String[] parts = jwt.split("\\.");
                    Map<String, Object> claims = JSON.readValue(Base64.getUrlDecoder().decode(parts[1]),
                            new com.fasterxml.jackson.core.type.TypeReference<>() { });
                    claims.put("user_id", "tampered-user");
                    jwt = parts[0] + "." + base64(JSON.writeValueAsBytes(claims)) + "." + parts[2];
                }
                issuedJwts.add(jwt);
                respond(exchange, Map.of("opId", "authorization-exchange", "code", "ok", "msg", "ok", "data",
                        Map.of("jwt", jwt, "expiresAt", Instant.now().plusSeconds(300).toString(), "jti", "jti-" + code,
                                "userEmail", "user@example.test", "orgId", "wrong-org".equals(code) ? "other-org" : ORGANIZATION,
                                "clusterId", CLUSTER, "organizationName", "Authorization organization",
                                "clusterName", "Authorization cluster", "region", "test-region")));
            });
            server.start();
        }
        String baseUrl() { return "http://127.0.0.1:" + server.getAddress().getPort(); }
        String jwt(String code) {
            return Jwts.builder().subject("user@example.test").issuer(baseUrl()).audience().add("127.0.0.1").and()
                    .issuedAt(Date.from(Instant.now())).expiration(Date.from(Instant.now().plusSeconds(300)))
                    .id("jti-" + code).claim("user_id", "user-" + code).claim("org_id", ORGANIZATION)
                    .claim("cluster_id", CLUSTER).claim("scope", "read".equals(code)
                            ? List.of("workload:read") : List.of("workload:read", "workload:write"))
                    .header().keyId(KID).and().signWith((RSAPrivateKey) key.getPrivate()).compact();
        }
        @Override public void close() { server.stop(0); }
    }

    private static byte[] unsigned(byte[] value) {
        return value.length > 1 && value[0] == 0 ? java.util.Arrays.copyOfRange(value, 1, value.length) : value;
    }

    private static String base64(byte[] value) { return Base64.getUrlEncoder().withoutPadding().encodeToString(value); }

    private static void respond(HttpExchange exchange, Map<String, Object> response) throws IOException {
        byte[] bytes = JSON.writeValueAsBytes(response);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(200, bytes.length);
        try (var output = exchange.getResponseBody()) { output.write(bytes); }
    }
}
