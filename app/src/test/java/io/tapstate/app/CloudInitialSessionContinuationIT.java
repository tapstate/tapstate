package io.tapstate.app;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.*;
import io.tapstate.control.core.*;
import io.tapstate.control.restapi.*;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.*;
import io.tapstate.testsupport.RequiresDocker;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.bson.Document;
import org.junit.jupiter.api.*;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.*;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestClient;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.*;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import static org.assertj.core.api.Assertions.*;

/** Original-cookie continuity over real HTTP and local MongoDB, with controlled SDK and business ports. */
@RequiresDocker
class CloudInitialSessionContinuationIT {
    private static final String PASSWORD = "local-reproduction-only-password";
    private static final String CLUSTER = "session-transition-reproduction";
    private static final String RS = "session-repro-rs";
    private static final Trust TLS = Trust.create();
    @Container static final GenericContainer<?> MONGO = new GenericContainer<>(DockerImageName.parse("mongo:7.0"))
            .withExposedPorts(27017)
            .withEnv("MONGO_INITDB_ROOT_USERNAME", "root").withEnv("MONGO_INITDB_ROOT_PASSWORD", PASSWORD)
            .withCopyToContainer(Transferable.of(Base64.getEncoder().encode(new byte[64]), 0400), "/data/configdb/member.key")
            .withCopyToContainer(Transferable.of(TLS.server(), 0400), "/data/configdb/server.pem")
            .withCopyToContainer(Transferable.of(TLS.ca(), 0444), "/data/configdb/ca.pem")
            .withCommand("--replSet", RS, "--keyFile", "/data/configdb/member.key", "--bind_ip_all",
                    "--tlsMode", "allowTLS", "--tlsCertificateKeyFile", "/data/configdb/server.pem",
                    "--tlsCAFile", "/data/configdb/ca.pem", "--tlsAllowConnectionsWithoutCertificates")
            .waitingFor(new WaitAllStrategy()
                    .withStrategy(Wait.forLogMessage(".*MongoDB init process complete; ready for start up.*", 1))
                    .withStrategy(Wait.forLogMessage("(?i).*waiting for connections.*", 2))
                    .withStrategy(Wait.forListeningPort())).withStartupTimeout(Duration.ofSeconds(90));

    @BeforeAll static void initiate() throws Exception {
        var result = MONGO.execInContainer("mongosh", "--quiet", "--username", "root", "--password", PASSWORD,
                "--authenticationDatabase", "admin", "--eval",
                "rs.initiate({_id:'" + RS + "',members:[{_id:0,host:'localhost:27017'}]})");
        assertThat(result.getExitCode()).isZero();
        try (var client = MongoClients.create(uri("admin", false))) {
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(30)).until(() -> Boolean.TRUE.equals(
                    client.getDatabase("admin").runCommand(new Document("hello", 1)).getBoolean("isWritablePrimary")));
        }
    }

    @RepeatedTest(3)
    void theOriginalCookieContinuesThroughInitialAuthenticationPublication(RepetitionInfo repetition) throws Exception {
        String prefix = "repro_" + UUID.randomUUID().toString().replace("-", "");
        var inputs = new Inputs(prefix);
        try (var context = new SpringApplicationBuilder(HttpAssembly.class)
                .environment(CloudFixtureEnvironment.isolated())
                .initializers(app -> app.getBeanFactory().registerSingleton("inputs", inputs))
                .run("--server.address=127.0.0.1", "--server.port=0", "--SDK_STATUS_SENDER_ENABLED=false",
                        "--logging.level.org.mongodb.driver=WARN", "--logging.level.org.springframework=WARN")) {
            StorageRoot root = context.getBean(StorageRoot.class);
            int port = ((WebServerApplicationContext) context).getWebServer().getPort();
            RestClient client = RestClient.builder().baseUrl("http://127.0.0.1:" + port)
                    .requestFactory(new org.springframework.http.client.JdkClientHttpRequestFactory(java.net.http.HttpClient.newBuilder()
                            .followRedirects(java.net.http.HttpClient.Redirect.NEVER).build())).build();
            String oldCookie = enter(client, "temporary");
            var initial = request(client, oldCookie, "/api/storage/status", Map.of(), "before-save", repetition);
            assertThat(initial.status()).isEqualTo(200);
            assertThat(initial.body().get("setupPhase")).isEqualTo("CONFIG_PREPARED");
            Map<String,Object> candidate = Map.of("cache", connection(inputs.cache), "master", connection(inputs.master));
            var scope = root.runtime().requireConfiguration().setupStates().load().scope();
            var test = request(client, oldCookie, "/api/storage/test", Map.of("operationId", scope.operationId(),
                    "generation", scope.generation(), "candidate", candidate, "layer", "CACHE"), "test-cache", repetition);
            assertThat(test.status()).isEqualTo(200);
            var commit = request(client, oldCookie, "/api/storage/setup/commit", Map.of("operationId", scope.operationId(),
                    "generation", scope.generation(), "candidate", candidate, "testStamps", Map.of("CACHE", test.body().get("testStamp"))),
                    "commit-before-activation", repetition);
            assertThat(commit.status()).isEqualTo(200);
            assertThat(commit.body().get("setupPhase")).isEqualTo("STORAGE_BOUND");
            assertThat(request(client, oldCookie, "/api/storage/status", Map.of(), "old-cookie-before-switch", repetition).status()).isEqualTo(200);

            inputs.activate.countDown();
            assertThat(inputs.businessEntered.await(20, TimeUnit.SECONDS)).as("real CACHE/auth publication must finish").isTrue();
            assertThat(root.phase()).isEqualTo(StorageRoot.Phase.AUTH_READY);
            assertThat(root.runtime().requireConfiguration().setupStates().load().phase()).isEqualTo(StorageSetupStateStore.Phase.COMPLETE);
            for (int probe = 0; probe < 3; probe++) {
                var continued = request(client, oldCookie, "/api/storage/status", Map.of(), "original-cookie-after-switch-" + probe, repetition);
                assertThat(continued.status()).isEqualTo(200);
                assertThat(continued.body().get("operationId")).isEqualTo(scope.operationId());
                assertThat(continued.body().get("businessPhase")).isEqualTo("AUTH_READY");
            }
            assertThat(inputs.sdk.verifications).hasValue(1);
            try (var raw = MongoClients.create(uri("admin", false))) {
                var sessions = SystemCollections.CLOUD_SESSIONS.on(raw.getDatabase(inputs.cache));
                assertThat(sessions.countDocuments()).isOne();
                assertThat(sessions.find(new Document("jwtId", "temporary")).first()).isNotNull();
            }
            inputs.ready.countDown();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(10)).until(() -> root.phase() == StorageRoot.Phase.BUSINESS_READY);
            var ready = request(client, oldCookie, "/api/storage/status", Map.of(), "original-cookie-ready", repetition);
            assertThat(ready.status()).isEqualTo(200);
            assertThat(ready.body().get("businessPhase")).isEqualTo("READY");
            assertThat(inputs.sdk.verifications).hasValue(1);
            client.post().uri("/auth/logout").header(HttpHeaders.COOKIE, oldCookie)
                    .exchange((request, response) -> {
                        assertThat(response.getStatusCode().value()).isEqualTo(204);
                        return null;
                    });
            var loggedOut = request(client, oldCookie, "/api/storage/status", Map.of(), "original-cookie-after-logout", repetition);
            assertThat(loggedOut.status()).isEqualTo(401);
            assertThat(loggedOut.body().get("code")).isEqualTo("control.unauthenticated");
        } finally {
            inputs.activate.countDown(); inputs.ready.countDown();
            try (var raw = MongoClients.create(uri("admin", false))) {
                for (String db : List.of(inputs.config, inputs.cache, inputs.master)) raw.getDatabase(db).drop();
            }
        }
    }

    private static String enter(RestClient client, String code) {
        return client.get().uri("/auth/exchange?code=" + code).exchange((request, response) -> {
            assertThat(response.getStatusCode().value()).isEqualTo(302);
            String header = response.getHeaders().getFirst(HttpHeaders.SET_COOKIE);
            assertThat(header).contains("Secure", "HttpOnly");
            return header.substring(0, header.indexOf(';'));
        });
    }
    private record Reply(int status, Map<?,?> body) {}
    private static Reply request(RestClient client, String cookie, String path, Map<String,Object> body,
            String step, RepetitionInfo repetition) {
        long start = System.nanoTime();
        Reply result = client.post().uri(path).header(HttpHeaders.COOKIE, cookie).contentType(MediaType.APPLICATION_JSON)
                .body(body).exchange((request, response) -> new Reply(response.getStatusCode().value(), response.bodyTo(Map.class)));
        var event = new LinkedHashMap<String,Object>();
        event.put("run", repetition.getCurrentRepetition()); event.put("step", step); event.put("path", path);
        event.put("status", result.status()); event.put("elapsedMillis", TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        for (String key : List.of("code", "setupPhase", "businessPhase", "operationId"))
            if (result.body().containsKey(key)) event.put(key, result.body().get(key));
        String line = JsonWriter.write(event); System.out.println("REPRO " + line);
        String evidence = System.getProperty("repro.evidence");
        if (evidence != null) try { Files.writeString(Path.of(evidence), line + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND); }
        catch (java.io.IOException failed) { throw new java.io.UncheckedIOException(failed); }
        return result;
    }
    private static Map<String,Object> connection(String db) {
        return Map.of("kind", "ATLAS", "uri", uri(db, true), "database", db, "caPem", new String(TLS.ca(), StandardCharsets.UTF_8));
    }
    private static String uri(String db, boolean tls) {
        return "mongodb://root:" + PASSWORD + "@127.0.0.1:" + MONGO.getMappedPort(27017) + "/" + db
                + "?authSource=admin&directConnection=true&replicaSet=" + RS + (tls ? "&tls=true" : "");
    }
    static final class Inputs {
        final String config, cache, master;
        final CountDownLatch activate = new CountDownLatch(1), businessEntered = new CountDownLatch(1), ready = new CountDownLatch(1);
        final Provider sdk = new Provider();
        Inputs(String prefix) { config = prefix + "_config"; cache = prefix + "_cache"; master = prefix + "_views"; }
    }
    @SpringBootConfiguration @EnableAutoConfiguration @Import(StorageHttpFace.class)
    static class HttpAssembly {
        @Bean Clock clock() { return Clock.systemUTC(); }
        @Bean AuthenticationMode authenticationMode() { return AuthenticationMode.CLOUD; }
        @Bean OperationRegistry operations() { return ControlOperations.registry(); }
        @Bean(destroyMethod = "close") StorageRoot root(Inputs inputs, Clock clock) {
            var mongo = new MongoProperties(); mongo.setUri(uri(inputs.config, false));
            var properties = new CloudProperties(); properties.setBaseUrl("https://cloud.example.test");
            properties.setClusterId(CLUSTER); properties.setToken("local-sdk-only"); properties.setAtlasUri(mongo.getUri());
            var cloud = CloudRuntimeSettings.resolve(properties);
            return new StorageRoot(mongo, cloud, new CloudSdkBridge(cloud, inputs.sdk), BootId.fresh(),
                    new MetricsHistoryProperties(), new RandomTokenSecrets(), clock, new byte[32]);
        }
        @Bean CredentialAuthenticator credentials(StorageRoot root) { return CredentialAuthenticator.scoped(root::authenticateCredentials); }
        @Bean ManagedAuthenticationContext.Provider managed(StorageRoot root) { return root::managedAuthentication; }
        @Bean StorageControlCaller callers(StorageRoot root) {
            return StorageControlCallers.ordinary((principal, original) -> StorageControlAccess.ordinary(root.mode(),
                    () -> root.runtime().requireConfiguration().setupStates().load().scope(), principal, original));
        }
        @Bean StorageSetupService setup(StorageRoot root, Inputs inputs, Clock clock) {
            var config = root.runtime().requireConfiguration();
            var target = new StorageSetupService.DefaultTarget() {
                @Override public StorageSetupService.ConnectorIdentity identity(StorageBinding.Connection master) {
                    return new StorageSetupService.ConnectorIdentity("a".repeat(64), "b".repeat(64));
                }
                @Override public SourceResource definition(StorageBinding.Connection master, String id) {
                    return new SourceResource(id, null, "mongodb-atlas", Map.of("isUri", true, "uri", master.uri(),
                            "tlsCaPem", master.caPem()), null, null, null, null);
                }
                @Override public void test(StorageBinding.Connection master, SourceResource source, StorageSetupService.ConnectorIdentity id) {
                    throw new AssertionError("default Source testing is outside initial authentication");
                }
            };
            return new StorageSetupService(root.mode(), config.bindings(), config.setupStates(), new MongoStorageConnectionProbe(Duration.ofSeconds(5)),
                    target, root::configurationEndpoint, new AuditGate(config.audit(), clock), clock, root::requireActive,
                    committed -> root.handoffActivation(committed, () -> {
                        await(inputs.activate);
                        root.continueCloudSetup(committed, target, ignored -> new StorageRuntime.Business() {
                            @Override public void start() { inputs.businessEntered.countDown(); await(inputs.ready); }
                            @Override public void close() {}
                        });
                    }), root::coordinateSetup, (expected, tested, active) -> {}, root.initializationProgress());
        }
        @Bean StorageControlDispatcher dispatcher(OperationRegistry operations, StorageSetupService setup, StorageRoot root) {
            return StorageControlDispatcher.staged(operations, () -> setup, () -> null, () -> null,
                    (caller, active) -> root.snapshotProjection(caller));
        }
    }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(20, TimeUnit.SECONDS)) throw new AssertionError("controlled phase gate timed out"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }
    private static final class Provider implements CloudSdkBridge.SdkRuntime {
        final java.util.concurrent.atomic.AtomicInteger verifications = new java.util.concurrent.atomic.AtomicInteger();
        final String operation = UUID.randomUUID().toString();
        @Override public CloudSdkBridge.ExchangeResult exchange(String code) {
            return new CloudSdkBridge.ExchangeResult(code, "organization", CLUSTER, "Local", "Reproduction", "local");
        }
        @Override public CloudSdkBridge.VerifiedClaims verify(String jwt) {
            verifications.incrementAndGet();
            return new CloudSdkBridge.VerifiedClaims("user", "organization", CLUSTER, jwt, Instant.now().plusSeconds(600),
                    "https://cloud.example.test", "127.0.0.1", List.of("workload:write"));
        }
        @Override public io.tapstate.cloud.sdk.client.ClusterEgressAddress currentEgressAddress() {
            return new io.tapstate.cloud.sdk.client.ClusterEgressAddress("organization", CLUSTER, "127.0.0.1", "STABLE");
        }
        @Override public boolean verifyCallback(String method, String timestamp, String nonce, String data, String signature) { return false; }
        @Override public void report(String cluster, String nonce, CloudRuntimeStatus status, String token) { throw new AssertionError(); }
        @Override public CloudSdkBridge.ConfigurationInitialization claimConfiguration(String boot, String hash) {
            return new CloudSdkBridge.ConfigurationInitialization(CLUSTER, operation, "CLAIMED", hash, true);
        }
        @Override public CloudSdkBridge.ConfigurationInitialization completeConfiguration(String operation, String hash) {
            return new CloudSdkBridge.ConfigurationInitialization(CLUSTER, operation, "COMPLETE", hash, false);
        }
    }
    private record Trust(byte[] ca, byte[] server) {
        static Trust create() {
            try {
                Path directory = Files.createTempDirectory("cloud-session-repro-tls-");
                Path store = directory.resolve("server.p12"), log = directory.resolve("keytool.log");
                try {
                    var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                            "-genkeypair", "-alias", "server", "-keyalg", "RSA", "-keysize", "2048", "-storetype", "PKCS12",
                            "-keystore", store.toString(), "-storepass", "fixture-only-password", "-dname", "CN=localhost",
                            "-ext", "SAN=dns:localhost,ip:127.0.0.1", "-ext", "BC=ca:true", "-validity", "2", "-noprompt")
                            .redirectErrorStream(true).redirectOutput(log.toFile()).start();
                    assertThat(process.waitFor(30, TimeUnit.SECONDS)).isTrue(); assertThat(process.exitValue()).isZero();
                    var keys = java.security.KeyStore.getInstance("PKCS12");
                    try (var input = Files.newInputStream(store)) { keys.load(input, "fixture-only-password".toCharArray()); }
                    String ca = pem("CERTIFICATE", keys.getCertificate("server").getEncoded());
                    String key = pem("PRIVATE KEY", keys.getKey("server", "fixture-only-password".toCharArray()).getEncoded());
                    return new Trust(ca.getBytes(StandardCharsets.UTF_8), (key + ca).getBytes(StandardCharsets.UTF_8));
                } finally { Files.deleteIfExists(store); Files.deleteIfExists(log); Files.deleteIfExists(directory); }
            } catch (Exception failed) { throw new IllegalStateException("cannot generate local test trust", failed); }
        }
        static String pem(String type, byte[] bytes) {
            return "-----BEGIN " + type + "-----\n" + Base64.getMimeEncoder(64, new byte[]{'\n'}).encodeToString(bytes)
                    + "\n-----END " + type + "-----\n";
        }
    }
}
