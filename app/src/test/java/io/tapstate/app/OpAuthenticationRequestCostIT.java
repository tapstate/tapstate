package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoHttpDiagnosticsScope;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.JsonReader;
import io.tapstate.core.model.SourceResource;
import io.tapstate.control.core.Scope;
import io.tapstate.control.core.TokenService;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** Exercises the actual OP HTTP admission path against healthy, deliberately delayed MongoDB. */
@RequiresDocker
class OpAuthenticationRequestCostIT {
    private static final Map<String, CompletableFuture<Map<String, Object>>> RECORDINGS = new ConcurrentHashMap<>();
    @TempDir Path directory;

    @Test
    void aHealthySourceCreationSurvivesSlowAuthenticationReads() throws Exception {
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withLabel("io.tapstate.task", "issue-1060-op-auth-local")
                .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1")
                .withCreateContainerCmdModifier(command -> command.getHostConfig()
                        .withMemory(512L * 1024 * 1024).withNanoCPUs(1_000_000_000L))) {
            mongo.start();
            var storage = NativeAppStorage.create(mongo, "op_request_cost", StorageBinding.Mode.OP,
                    "op-auth-" + UUID.randomUUID());
            Path seeds = Files.createDirectory(directory.resolve("seeds"));
            Files.copy(CloudConnectorTestInputs.seedDirectory().resolve("mongodb-connector.jar"),
                    seeds.resolve("mongodb-connector.jar"));
            try (var application = new SpringApplicationBuilder(Bootstrap.class, Measurements.class)
                    .environment(CloudFixtureEnvironment.isolated()).run(
                            "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR",
                            "--tapstate.store.mongo.uri=" + storage.configurationUri(),
                            "--tapstate.control.auth.jwt-secret=local-authentication-cost-fixture-signing-key",
                            "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2",
                            "--tapstate.connectors.seed-dir=" + seeds,
                            "--tapstate.connectors.plugins-dir=" + directory.resolve("plugins"));
                 var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
                 var raw = MongoClients.create(mongo.getReplicaSetUrl())) {
                NativeAppStorage.awaitBusiness(application);
                String origin = "http://127.0.0.1:" + ((WebServerApplicationContext) application).getWebServer().getPort();
                var login = request(http, origin, "POST", "/auth/login", null,
                        Map.of("username", NativeAppStorage.USERNAME, "password", NativeAppStorage.PASSWORD,
                                "createSession", false));
                assertThat(login.status()).isEqualTo(200);
                String token = (String) login.body().get("token");
                var sessionLogin = request(http, origin, "POST", "/auth/login", null,
                        Map.of("username", NativeAppStorage.USERNAME, "password", NativeAppStorage.PASSWORD,
                                "createSession", true));
                assertThat(sessionLogin.status()).isEqualTo(200);
                var control = request(http, origin, "POST", "/api/sources", token, source("fast_source", storage.masterUri()));
                assertThat(control.status()).isEqualTo(201);
                assertThat(authCommands(control)).isEqualTo(20);
                var configuration = application.getBean(StorageRoot.class).runtime().requireConfiguration();
                var tokens = new TokenService(configuration.tokens(), application.getBean(io.tapstate.control.core.TokenSecrets.class),
                        java.time.Clock.systemUTC());
                String machine = tokens.create(Scope.ADMIN);
                var machineCreated = request(http, origin, "POST", "/api/sources", machine,
                        source("machine_source", storage.masterUri()));
                assertThat(machineCreated.status()).isEqualTo(201);
                assertThat(authCommands(machineCreated)).isEqualTo(21);
                tokens.revoke(machine.substring("cyxt_".length(), machine.indexOf('.')));
                var revoked = request(http, origin, "POST", "/api/sources", machine,
                        source("revoked_source", storage.masterUri()));
                assertThat(revoked.status()).isEqualTo(401);
                assertThat(raw.getDatabase(storage.configurationDatabase()).getCollection("sources")
                        .countDocuments(new Document("_id", "revoked_source"))).isZero();

                var admin = raw.getDatabase("admin");
                String namespace = storage.cacheDatabase() + "." + SystemCollections.CACHE_IDENTITY.collectionName();
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "alwaysOn")
                        .append("data", new Document("failCommands", List.of("find"))
                                .append("namespace", namespace).append("blockConnection", true).append("blockTimeMS", 3500)));
                var outcomes = new ArrayList<Response>();
                try {
                    var delayedLogin = request(http, origin, "POST", "/auth/login", null,
                            Map.of("username", NativeAppStorage.USERNAME, "password", NativeAppStorage.PASSWORD, "createSession", false));
                    assertThat(delayedLogin.status()).isEqualTo(200);
                    for (int attempt = 1; attempt <= 3; attempt++) {
                        outcomes.add(request(http, origin, "POST", "/api/sources", token,
                                source("slow_source_" + attempt, storage.masterUri())));
                    }
                } finally {
                    admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                }
                for (int attempt = 1; attempt <= outcomes.size(); attempt++) {
                    var outcome = outcomes.get(attempt - 1);
                    assertThat(outcome.status()).isIn(201, 503);
                    long saved = raw.getDatabase(storage.configurationDatabase()).getCollection("sources")
                            .countDocuments(new Document("_id", "slow_source_" + attempt));
                    if (outcome.status() == 503) {
                        assertThat(outcome.body().get("code")).isEqualTo("control.storage-unavailable");
                        assertThat(saved).isZero();
                        assertThat(outcome.diagnostics().get("complete")).isEqualTo(true);
                        assertThat(identityFinds(outcome)).isEqualTo(3);
                        assertThat(authMillis(outcome)).isGreaterThanOrEqualTo(9000);
                        assertThat(hasBusinessWrite(outcome)).isFalse();
                    } else {
                        assertThat(saved).isOne();
                    }
                }
                var after = request(http, origin, "POST", "/api/sources", token,
                        source("recovered_source", storage.masterUri()));
                assertThat(after.status()).isEqualTo(201);
                System.out.println("OP_AUTH_LOCAL_RESULTS " + JsonWriter.write(Map.of(
                        "controlStatus", control.status(), "recoveredStatus", after.status(),
                        "delayedStatuses", outcomes.stream().map(Response::status).toList(),
                        "delayedAuthCommands", outcomes.stream().map(OpAuthenticationRequestCostIT::authCommands).toList())));
                assertThat(outcomes).extracting(Response::status)
                        .as("a healthy storage endpoint must remain usable under the bounded authentication delay")
                        .containsOnly(201);
                assertThat(authCommands(control)).as("bounded OP admission command cost").isEqualTo(20);
                var smallPage = request(http, origin, "GET", "/api/sources?limit=1", token, null);
                assertThat(smallPage.status()).isEqualTo(200);
                assertThat((List<?>) smallPage.body().get("items")).hasSize(1);
                assertThat(authCommands(smallPage)).isEqualTo(20);
                for (int index = 0; index < 25; index++) {
                    configuration.artifacts().save(new SourceResource("unrelated_" + index, null, "mongodb",
                            Map.of("isUri", true, "uri", storage.masterUri()), null, null, null, null));
                }
                for (int limit : List.of(1, 8)) {
                    var page = request(http, origin, "GET", "/api/sources?limit=" + limit, token, null);
                    assertThat(page.status()).isEqualTo(200);
                    assertThat((List<?>) page.body().get("items")).hasSize(limit);
                    assertThat(authCommands(page)).isEqualTo(20);
                }
                SystemCollections.OP_SESSIONS.on(raw.getDatabase(storage.cacheDatabase())).drop();
                var missingStructure = request(http, origin, "POST", "/api/sources", token,
                        source("missing_structure_source", storage.masterUri()));
                assertThat(missingStructure.status()).isEqualTo(503);
                assertThat(missingStructure.body().get("code")).isEqualTo("control.storage-unavailable");
                assertThat(hasBusinessWrite(missingStructure)).isFalse();
                assertThat(raw.getDatabase(storage.cacheDatabase()).listCollectionNames())
                        .doesNotContain(SystemCollections.OP_SESSIONS.collectionName());
                assertThat(raw.getDatabase(storage.configurationDatabase()).getCollection("sources")
                        .countDocuments(new Document("_id", "missing_structure_source"))).isZero();
            }
        }
    }

    private static Map<String, Object> source(String id, String uri) {
        return Map.of("id", id, "connector", "mongodb", "config", Map.of("isUri", true, "uri", uri), "mode", "snapshot");
    }

    @SuppressWarnings("unchecked")
    private static long authCommands(Response response) {
        var phases = (Map<String, Object>) response.diagnostics().get("mongo_by_phase");
        var authentication = (Map<String, Object>) phases.get("auth_verify");
        return authentication == null ? 0 : ((Number) authentication.get("started")).longValue();
    }

    @SuppressWarnings("unchecked")
    private static long identityFinds(Response response) {
        var groups = (List<Map<String, Object>>) response.diagnostics().get("mongo_groups");
        return groups.stream().filter(group -> "find".equals(group.get("command"))
                && "cache_identity".equals(group.get("collection")))
                .mapToLong(group -> ((Number) group.get("started")).longValue()).sum();
    }

    @SuppressWarnings("unchecked")
    private static double authMillis(Response response) {
        var phases = (Map<String, Map<String, Object>>) response.diagnostics().get("phases");
        return ((Number) phases.get("auth_verify").get("inclusive_ms")).doubleValue();
    }

    @SuppressWarnings("unchecked")
    private static boolean hasBusinessWrite(Response response) {
        var groups = (List<Map<String, Object>>) response.diagnostics().get("mongo_groups");
        return groups.stream().anyMatch(group -> List.of("insert", "update", "delete", "findAndModify")
                .contains(group.get("command")));
    }

    private record Response(int status, Map<?, ?> body, Map<String, Object> diagnostics) { }

    @SuppressWarnings("unchecked")
    private static Response request(HttpClient http, String origin, String method, String path,
            String token, Map<String, Object> body) throws Exception {
        String trace = UUID.randomUUID().toString();
        var recorded = new CompletableFuture<Map<String, Object>>();
        RECORDINGS.put(trace, recorded);
        try {
            var builder = HttpRequest.newBuilder(URI.create(origin + path)).timeout(Duration.ofSeconds(45))
                    .header("Content-Type", "application/json").header("Origin", origin).header("X-Op-Auth-Trace", trace);
            if (token != null) builder.header("Authorization", "Bearer " + token);
            long started = System.nanoTime();
            var response = http.send(builder.method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(JsonWriter.write(body))).build(),
                    HttpResponse.BodyHandlers.ofString());
            double elapsedMs = (System.nanoTime() - started) / 1_000_000.0;
            var diagnostics = recorded.get(5, TimeUnit.SECONDS);
            var event = new java.util.LinkedHashMap<String, Object>();
            event.put("path", path); event.put("status", response.statusCode());
            event.put("elapsedMs", elapsedMs);
            event.put("diagnostics", diagnostics);
            var decoded = (Map<?, ?>) JsonReader.parse(response.body());
            if (decoded.containsKey("code")) event.put("code", decoded.get("code"));
            System.out.println("OP_AUTH_HTTP " + JsonWriter.write(event));
            return new Response(response.statusCode(), decoded, diagnostics);
        } finally {
            RECORDINGS.remove(trace);
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class Measurements {
        @Bean
        org.springframework.boot.web.servlet.FilterRegistrationBean<org.springframework.web.filter.OncePerRequestFilter> opAuthMeasurements() {
            var filter = new org.springframework.web.filter.OncePerRequestFilter() {
                @Override protected void doFilterInternal(jakarta.servlet.http.HttpServletRequest request,
                        jakarta.servlet.http.HttpServletResponse response, jakarta.servlet.FilterChain chain)
                        throws java.io.IOException, jakarta.servlet.ServletException {
                    var recorded = RECORDINGS.get(request.getHeader("X-Op-Auth-Trace"));
                    if (recorded == null) { chain.doFilter(request, response); return; }
                    try (var scope = MongoHttpDiagnosticsScope.open()) {
                        try { chain.doFilter(request, response); }
                        finally { recorded.complete(scope.snapshot()); }
                    }
                }
            };
            var registration = new org.springframework.boot.web.servlet.FilterRegistrationBean<org.springframework.web.filter.OncePerRequestFilter>(filter);
            registration.setOrder(org.springframework.core.Ordered.HIGHEST_PRECEDENCE);
            return registration;
        }
    }
}
