package io.tapstate.e2e;

import com.mongodb.ConnectionString;
import com.mongodb.client.MongoClients;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.common.JsonWriter;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.security.interfaces.RSAPublicKey;
import java.time.Instant;
import java.time.Duration;
import io.tapstate.core.lifecycle.PipelineState;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Real runtime modes and SDK verification with a controlled provider, not Cloud provisioning. */
final class AtlasRuntime implements AutoCloseable {

    enum Mode { ON_PREM, CLOUD }

    private static final String USER = "atlas-data-user";
    private static final String CLUSTER = "atlas-data-cluster";
    private static final String TOKEN = "controlled-atlas-sdk-token";
    private static final String KID = "atlas-data-key";
    private final Mode mode;
    private final String storeUri;
    private final String metadata;
    private final HttpServer cloud;
    private final KeyPair keys;
    private final String issuer;
    private final Map<String, String> codes = new ConcurrentHashMap<>();
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicInteger exchanges = new AtomicInteger();
    private String cookie;

    AtlasRuntime(Mode mode, String storeUri) throws IOException, GeneralSecurityException {
        this.mode = mode;
        this.storeUri = storeUri;
        metadata = new ConnectionString(storeUri).getDatabase();
        if (metadata == null || !metadata.startsWith("ts_plan_")) {
            throw new IllegalArgumentException("this fixture requires its own isolated metadata database");
        }
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        keys = generator.generateKeyPair();
        cloud = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        issuer = "http://127.0.0.1:" + cloud.getAddress().getPort();
        cloud.createContext("/v1/api/auth/exchange", this::exchange);
        cloud.createContext("/v1/api/jwks.json", exchange -> {
            requests.incrementAndGet();
            RSAPublicKey key = (RSAPublicKey) keys.getPublic();
            respond(exchange, 200, Map.of("keys", List.of(Map.of(
                    "kty", "RSA", "kid", KID, "use", "sig", "alg", "RS256",
                    "n", base64(unsigned(key.getModulus().toByteArray())),
                    "e", base64(unsigned(key.getPublicExponent().toByteArray()))))));
        });
        cloud.createContext("/", exchange -> {
            requests.incrementAndGet();
            respond(exchange, 404, Map.of("code", "fixture.unexpected-route"));
        });
        cloud.start();
    }

    ServerHandle launch(Tiers tier) {
        List<String> arguments = new ArrayList<>(List.of(
                "--spring.config.location=optional:classpath:/application.properties",
                "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                "--logging.level.root=ERROR", "--logging.level.io.tapstate.app.Bootstrap=INFO",
                "--SDK_STATUS_SENDER_ENABLED=false"));
        if (mode == Mode.CLOUD) {
            arguments.addAll(List.of("--tapstate.cloud.base-url=" + issuer,
                    "--tapstate.cloud.token=" + TOKEN, "--tapstate.cloud.atlas-uri=" + storeUri,
                    "--tapstate.cloud.cluster-id=" + CLUSTER,
                    "--tapstate.connectors.seed-dir=" + seedDirectory()));
        }
        ServerHandle server = tier == Tiers.IN_PROCESS
                ? InProcessServer.start(storeUri, metadata + "_operator", arguments)
                : RealProcessServer.start(storeUri, metadata + "_operator", arguments);
        try {
            if (server instanceof RealProcessServer process) process.awaitReady();
            return new ServerHandle() {
                @Override public java.net.URI baseUrl() { return server.baseUrl(); }
                @Override public void close() {
                    server.close();
                    if (server instanceof RealProcessServer process) {
                        assertThat(process.isAlive()).as("the replaced JVM has actually exited").isFalse();
                    }
                }
            };
        } catch (RuntimeException | Error failure) {
            server.close();
            throw failure;
        }
    }

    ControlPlane control(ServerHandle server, boolean firstBoot) {
        ControlPlane control = new ControlPlane(server.baseUrl());
        if (mode == Mode.ON_PREM) {
            if (firstBoot) control.bootstrapAndLogin("e2e", "e2e-password");
            else control.login("e2e", "e2e-password");
        } else if (cookie == null) {
            String code = UUID.randomUUID().toString();
            codes.put(code, server.baseUrl().getHost());
            cookie = control.exchangeCloudCode(code);
        } else {
            control.useCloudSessionCookie(cookie);
        }
        return control;
    }

    void register(ControlPlane control, String... connectors) {
        if (mode == Mode.ON_PREM) {
            for (String connector : connectors) control.registerConnector(connector, ConnectorJars.bytesFor(connector));
        }
    }

    void assertAuthenticationBoundary() {
        assertThat(exchanges.get()).as("a Cloud restart reuses its local session without another exchange")
                .isEqualTo(mode == Mode.CLOUD ? 1 : 0);
        if (mode == Mode.ON_PREM) {
            assertThat(requests.get()).as("on-prem does not contact the Cloud provider").isZero();
        }
    }

    static void assertResumedRun(ControlPlane control, String pipeline, String table, long seededRows, Duration bound) {
        assertThat(control.state(pipeline)).contains(PipelineState.RUNNING);
        // ACKs are stored immediately; the live job's counters arrive on a separate metrics collection.
        long count = Await.answered("sampled record count from the resumed job", bound,
                () -> control.recordCount(pipeline).filter(sample -> sample > 0));
        assertThat(count).as("resumed job record count").isStrictlyBetween(0L, seededRows);
        assertThat(control.snapshotRowsRead(pipeline)).as("resumed job snapshot reads").containsEntry(table, 0L);
    }

    private static Path seedDirectory() {
        String value = System.getenv("TAPSTATE_TEST_CLOUD_RELEASE_DIR");
        if (value == null || value.isBlank()) throw new AssertionError("the locked Cloud release inputs are required");
        Path release = Path.of(value).toAbsolutePath().normalize();
        assertThat(release.resolve("release/connectors.lock.json")).isRegularFile();
        assertThat(release.resolve("connectors")).isDirectory();
        return release.resolve("connectors");
    }

    private void exchange(HttpExchange exchange) throws IOException {
        requests.incrementAndGet();
        exchanges.incrementAndGet();
        Object parsed = JsonReader.parse(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
        if (!(parsed instanceof Map<?, ?> body) || !"POST".equals(exchange.getRequestMethod())
                || !TOKEN.equals(exchange.getRequestHeaders().getFirst("X-Cluster-Identity-Secret"))
                || !CLUSTER.equals(body.get("clusterId"))) {
            respond(exchange, 401, Map.of("code", "fixture.invalid-service-identity"));
            return;
        }
        String audience = body.get("exchangeCode") instanceof String code ? codes.remove(code) : null;
        if (audience == null) {
            respond(exchange, 401, Map.of("code", "exchange.code-consumed"));
            return;
        }
        String jti = UUID.randomUUID().toString();
        Instant expires = Instant.now().plusSeconds(900);
        respond(exchange, 200, Map.of("opId", "atlas-exchange", "code", "ok", "msg", "ok", "data", Map.of(
                "jwt", jwt(audience, jti, expires), "expiresAt", expires.toString(), "jti", jti,
                "userEmail", "atlas-user@example.test", "orgId", "atlas-data-org", "clusterId", CLUSTER)));
    }

    private String jwt(String audience, String jti, Instant expires) {
        String header = base64(JsonWriter.write(Map.of("alg", "RS256", "kid", KID)).getBytes(StandardCharsets.UTF_8));
        String payload = base64(JsonWriter.write(Map.of(
                "iss", issuer, "sub", "atlas-user@example.test", "aud", List.of(audience),
                "iat", Instant.now().getEpochSecond(), "exp", expires.getEpochSecond(), "jti", jti,
                "user_id", USER, "org_id", "atlas-data-org", "cluster_id", CLUSTER,
                "scope", List.of("workload:read", "workload:write"))).getBytes(StandardCharsets.UTF_8));
        String unsigned = header + "." + payload;
        try {
            Signature signature = Signature.getInstance("SHA256withRSA");
            signature.initSign(keys.getPrivate());
            signature.update(unsigned.getBytes(StandardCharsets.US_ASCII));
            return unsigned + "." + base64(signature.sign());
        } catch (GeneralSecurityException failure) {
            throw new AssertionError("the controlled provider could not sign its JWT", failure);
        }
    }

    private static byte[] unsigned(byte[] value) {
        return value.length > 1 && value[0] == 0 ? Arrays.copyOfRange(value, 1, value.length) : value;
    }

    private static String base64(byte[] value) { return Base64.getUrlEncoder().withoutPadding().encodeToString(value); }

    private static void respond(HttpExchange exchange, int status, Map<String, ?> body) throws IOException {
        byte[] bytes = JsonWriter.write(body).getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (var output = exchange.getResponseBody()) { output.write(bytes); }
    }

    @Override public void close() {
        cloud.stop(0);
        // Only fixture-owned local databases; the actual Atlas source/target are cleaned by their witness.
        try (var raw = MongoClients.create(storeUri)) {
            RuntimeException failed = null;
            List<String> databases = mode == Mode.CLOUD
                    ? List.of(metadata, metadata + "_operator", metadata + "_views")
                    : List.of(metadata, metadata + "_operator");
            for (String database : databases) {
                try { raw.getDatabase(database).drop(); }
                catch (RuntimeException failure) {
                    if (failed == null) failed = failure;
                    else failed.addSuppressed(failure);
                }
            }
            if (failed != null) throw failed;
        }
    }
}
