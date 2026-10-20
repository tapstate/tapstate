package io.tapstate.app;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoSecurityException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import io.tapstate.adapters.mongostore.MigrationError;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Metadata;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.core.event.Op;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.*;
import io.tapstate.core.model.*;
import io.tapstate.spi.store.ConnectorRegistration;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SrsMeta;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Enforced Mongo authorization, not an unauthenticated simulation of database routing. */
@RequiresDocker
class ManagedMongoStoreIsolationIT {
    enum Missing { OPERATOR, VIEWS }
    private static final String PASSWORD = "controlled-mongo-permissions-password";
    private static final String REPLICA_SET = "permissions-rs";
    @Container
    private static final GenericContainer<?> MONGO = mongo();
    @TempDir Path work;

    private static GenericContainer<?> mongo() {
        byte[] key = new byte[64];
        new java.security.SecureRandom().nextBytes(key);
        return new GenericContainer<>(DockerImageName.parse("mongo:7.0"))
                .withExposedPorts(27017)
                .withEnv("MONGO_INITDB_ROOT_USERNAME", "root")
                .withEnv("MONGO_INITDB_ROOT_PASSWORD", PASSWORD)
                .withCopyToContainer(Transferable.of(Base64.getEncoder().encode(key), 0400), "/data/configdb/member.key")
                .withCommand("--replSet", REPLICA_SET, "--keyFile", "/data/configdb/member.key", "--bind_ip_all")
                // The image opens a temporary unauthenticated server while creating its administrator.
                .waitingFor(new WaitAllStrategy()
                        .withStrategy(Wait.forLogMessage(".*MongoDB init process complete; ready for start up.*", 1))
                        .withStrategy(Wait.forLogMessage("(?i).*waiting for connections.*", 2))
                        .withStrategy(Wait.forListeningPort())).withStartupTimeout(Duration.ofSeconds(90));
    }

    @BeforeAll
    static void initiateAuthenticatedReplicaSet() throws Exception {
        var initiated = MONGO.execInContainer("mongosh", "--quiet", "--username", "root", "--password", PASSWORD,
                "--authenticationDatabase", "admin", "--eval", "rs.initiate({_id:'" + REPLICA_SET
                        + "',members:[{_id:0,host:'localhost:27017'}]})");
        assertThat(initiated.getExitCode()).withFailMessage("authenticated replica initialization failed: %s %s; container: %s",
                initiated.getStdout().replace(PASSWORD, "<redacted>"),
                initiated.getStderr().replace(PASSWORD, "<redacted>"), MONGO.getLogs().replace(PASSWORD, "<redacted>")).isZero();
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            long until = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < until) {
                if (Boolean.TRUE.equals(admin.getDatabase("admin").runCommand(new Document("hello", 1))
                        .getBoolean("isWritablePrimary"))) return;
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        }
        throw new AssertionError("the authenticated replica set did not elect a primary");
    }

    @Test
    void storageProbeRequiresTransactionalCrudAndIndexPrivilegesWithoutKeepingProbeNamespaces() {
        String database = "setup_probe_" + UUID.randomUUID().toString().replace("-", "");
        String writer = "writer_" + database;
        String reader = "reader_" + database;
        String noIndex = "noindex_" + database;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, writer, List.of(database));
            admin.getDatabase("admin").runCommand(new Document("createUser", reader).append("pwd", PASSWORD)
                    .append("roles", List.of(new Document("role", "read").append("db", database))));
            admin.getDatabase(database).runCommand(new Document("createRole", "crud_without_indexes")
                    .append("roles", List.of()).append("privileges", List.of(new Document("resource",
                            new Document("db", database).append("collection", ""))
                            .append("actions", List.of("find", "insert", "update", "remove", "createCollection", "listCollections")))));
            admin.getDatabase("admin").runCommand(new Document("createUser", noIndex).append("pwd", PASSWORD)
                    .append("roles", List.of(new Document("role", "crud_without_indexes").append("db", database))));
            for (String user : List.of(writer, reader, noIndex)) {
                var candidate = new io.tapstate.spi.store.StorageBinding.Connection(
                        io.tapstate.spi.store.StorageBinding.Connection.Kind.MONGODB, uri(user, database), database, null);
                try (var connection = io.tapstate.adapters.mongostore.MongoConnection.forCache(candidate, Duration.ofSeconds(5), null)) {
                    connection.verifyConnectivity();
                    var probe = new io.tapstate.adapters.mongostore.MongoStorageConnectionProbe(Duration.ofSeconds(5));
                    var normalized = probe.normalize(candidate);
                    if (user.equals(writer)) probe.test(normalized);
                    else assertThatThrownBy(() -> probe.test(normalized))
                            .isInstanceOfSatisfying(io.tapstate.core.common.TapstateException.class,
                                    failure -> assertThat(failure.code()).isEqualTo(
                                            io.tapstate.adapters.mongostore.StoreError.DATABASE_ACCESS_FAILED));
                }
                assertThat(admin.getDatabase(database).listCollectionNames().into(new ArrayList<>())).isEmpty();
            }
        }
    }

    @Test
    void unboundCloudRootNeedsOnlyConfigAndDoesNotBuildRuntimeOrConnectorTestsForStatus() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configDb = "unbound_config_" + suffix, configUser = "unbound_" + suffix;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, configUser, List.of(configDb));
            var settings = new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(configUser, configDb), null, Duration.ofSeconds(5));
            String cluster = "unbound-" + suffix;
            try (var ignored = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(settings, cluster, "installation", "setup", 1)) { }
            try (var application = new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                    "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR", "--SDK_STATUS_SENDER_ENABLED=false",
                    "--tapstate.cloud.base-url=https://cloud.example", "--tapstate.cloud.token=fixture-runtime-token",
                    "--tapstate.cloud.cluster-id=" + cluster, "--tapstate.cloud.atlas-uri=" + uri(configUser, configDb),
                    "--tapstate.control.auth.jwt-secret=unbound-test-key", "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds"))) {
                var root = application.getBean(StorageRoot.class);
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.SETUP_REQUIRED);
                assertThat(root.runtime().current()).isEmpty(); assertThat(root.runtime().business()).isEmpty();
                assertThat(root.authentication().managed().service().identity().clusterId()).isEqualTo(cluster);
                assertThat(application.getBeansOfType(com.hazelcast.core.HazelcastInstance.class)).isEmpty();
                assertThat(application.getBeansOfType(io.tapstate.spi.store.UserStore.class)).isEmpty();
                assertThat(application.getBean(io.tapstate.control.core.CloudRuntimeStatusProvider.class).snapshot().activePipelines()).isZero();
                var original = root.runtime().requireConfiguration().setupStates().load().scope();
                assertThat(application.getBean(io.tapstate.control.core.StorageSetupService.class).status(original, () -> {}).bindingVersion()).isZero();
                assertThat(application.getBeanFactory().containsSingleton("installedConnectorTests")).isFalse();
                try (MongoClient limited = MongoClients.create(uri(configUser, configDb))) {
                    assertDenied(limited, configDb + "_operator"); assertDenied(limited, configDb + "_views");
                }
                assertThat(admin.getDatabase(configDb).listCollectionNames().into(new ArrayList<>()))
                        .doesNotContain("cloud_sessions", "workload_claims", "system_meta");
            }
        }
    }

    /** Low-level fixture preparation isolates production-root loading; it does not certify the installer or a registered target JAR. */
    @Test
    void wholeCacheLossUsesNormalOpLoginAndDoesNotRecreateBusinessState() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configDb = "loss_config_" + suffix, cacheDb = "loss_cache_" + suffix, masterDb = "loss_master_" + suffix;
        String user = "loss_" + suffix;
        var original = new io.tapstate.spi.store.StorageSetupScope("loss-" + suffix, "installation", "setup", 1);
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(configDb, cacheDb, masterDb));
            var cacheConnection = new io.tapstate.spi.store.StorageBinding.Connection(io.tapstate.spi.store.StorageBinding.Connection.Kind.MONGODB,
                    uri(user, cacheDb), cacheDb, null);
            try (var configuration = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(
                    new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(user, configDb), null, Duration.ofSeconds(5)),
                    original.clusterId(), original.installationId(), original.operationId(), 1)) {
                var target = new SourceResource("views", null, "mongodb", Map.of("uri", uri(user, masterDb)), null, null, null, null);
                var initial = configuration.bindings().commitWithFirstCache(original, io.tapstate.spi.store.StorageBinding.Mode.OP, cacheConnection, target);
                try (var cached = io.tapstate.adapters.mongostore.MongoCacheDomain.connect(cacheConnection, Duration.ofSeconds(5), "loss-fixture")) {
                    cached.prepareFirstAuth(initial.firstCreation().orElseThrow()); cached.prepareInitialBusiness(initial.preparation(), () -> {});
                }
                configuration.bindings().complete(original);
                configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(),
                        new io.tapstate.spi.store.User("admin", new BCryptPasswordHasher().hash("sentinel-login-password"), "admin"));
            }
            var config = admin.getDatabase(configDb);
            String keyId = config.getCollection("cluster_keys").find().first().getString("keyId");
            Document binding = config.getCollection("storage_binding").find().first();
            Document firstAdmin = config.getCollection("op_bootstrap").find().first();
            io.tapstate.control.restapi.LoginResponse old;
            try (var first = nativeRoot(uri(user, configDb))) { old = ordinaryLogin(first, true); }
            admin.getDatabase(cacheDb).drop();
            try (var restarted = nativeRoot(uri(user, configDb))) {
                var fresh = ordinaryLogin(restarted, true);
                var root = restarted.getBean(StorageRoot.class);
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.AUTH_READY);
                assertThat(root.runtime().business()).isEmpty();
                var current = root.runtime().current().orElseThrow().preparation().authScope();
                assertThat(current.authGeneration()).isEqualTo(2);
                var client = rootClient(restarted);
                assertThatThrownBy(() -> client.post().uri("/api/storage/status").header("Authorization", "Bearer " + old.token())
                        .body(Map.of()).retrieve().body(String.class)).isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                failure -> assertThat(failure.getStatusCode().value()).isEqualTo(401));
                assertThatThrownBy(() -> client.post().uri("/auth/session").header("Authorization", "TapstateSession " + old.sessionToken())
                        .retrieve().body(String.class)).isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                failure -> assertThat(failure.getStatusCode().value()).isEqualTo(401));
                String status = client.post().uri("/api/storage/status").header("Authorization", "Bearer " + fresh.token())
                        .body(Map.of()).retrieve().body(String.class);
                assertThat(((Map<?,?>)io.tapstate.core.common.JsonReader.parse(status)).get("businessPhase")).isEqualTo("AWAITING_REBUILD_CONFIRMATION");
                assertThat(admin.getDatabase(cacheDb).listCollectionNames().into(new ArrayList<>()))
                        .containsExactlyInAnyOrder("cache_identity", "schema_migrations", "op_sessions");
                assertThat(config.getCollection("cluster_keys").find().first().getString("keyId")).isEqualTo(keyId);
                assertThat(binding.equals(config.getCollection("storage_binding").find().first())).isTrue();
                assertThat(firstAdmin.equals(config.getCollection("op_bootstrap").find().first())).isTrue();
            }
        }
    }

    /** Real production-root routing and Mongo persistence; the SDK boundary remains a local fixture. */
    @Test
    void wholeCacheLossUsesOriginalCloudExchangeAndDurableRevocationWithoutBusinessState() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configDb = "cloud_config_" + suffix, cacheDb = "cloud_cache_" + suffix, masterDb = "cloud_master_" + suffix;
        String user = "cloud_" + suffix;
        var original = new io.tapstate.spi.store.StorageSetupScope("cloud-" + suffix, "installation", "setup", 1);
        var sdk = new RecoverySdk(original.clusterId());
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(configDb, cacheDb, masterDb));
            var cache = new io.tapstate.spi.store.StorageBinding.Connection(io.tapstate.spi.store.StorageBinding.Connection.Kind.MONGODB,
                    uri(user, cacheDb), cacheDb, null);
            try (var configuration = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(
                    new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(user, configDb), null, Duration.ofSeconds(5)),
                    original.clusterId(), original.installationId(), original.operationId(), 1)) {
                var target = new SourceResource("atlas-store", null, "mongodb", Map.of("uri", uri(user, masterDb)), null, null, null, null);
                var initial = configuration.bindings().commitWithFirstCache(original, io.tapstate.spi.store.StorageBinding.Mode.CLOUD, cache, target);
                try (var cached = io.tapstate.adapters.mongostore.MongoCacheDomain.connect(cache, Duration.ofSeconds(5), "cloud-fixture")) {
                    cached.prepareFirstAuth(initial.firstCreation().orElseThrow());
                }
                configuration.bindings().complete(original);
            }
            var configuration = admin.getDatabase(configDb);
            Document binding = configuration.getCollection("storage_binding").find().first();
            Document keys = configuration.getCollection("cluster_keys").find().first();
            String old;
            try (var first = cloudRecoveryRoot(uri(user, configDb), sdk)) { old = exchangeCookie(first, "old"); }
            admin.getDatabase(cacheDb).drop();
            try (var restarted = cloudRecoveryRoot(uri(user, configDb), sdk)) {
                var root = restarted.getBean(StorageRoot.class);
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.AUTH_READY);
                assertThat(root.runtime().current().orElseThrow().preparation().authScope().authGeneration()).isEqualTo(2);
                assertCloudStatus(restarted, old, 401);
                assertThatThrownBy(() -> exchangeCookie(restarted, "old"))
                        .isInstanceOf(org.springframework.web.client.RestClientResponseException.class);
                String timestamp = Long.toString(Instant.now().getEpochSecond());
                rootClient(restarted).post().uri("/auth/invalidate-session?jti=blocked&ts=" + timestamp + "&nonce=fixture-nonce&sign=fixture-signature")
                        .retrieve().toBodilessEntity();
                assertThat(admin.getDatabase(cacheDb).getCollection("cloud_revocations").countDocuments()).isOne();
            }
            // The blocked code has never been consumed. Only the durable callback survives this restart.
            try (var restarted = cloudRecoveryRoot(uri(user, configDb), sdk)) {
                var root = restarted.getBean(StorageRoot.class);
                assertThat(root.runtime().current().orElseThrow().preparation().authScope().authGeneration()).isEqualTo(2);
                assertThatThrownBy(() -> exchangeCookie(restarted, "blocked"))
                        .isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                failure -> assertThat(failure.getStatusCode().value()).isEqualTo(401));
                String fresh = exchangeCookie(restarted, "fresh");
                assertCloudStatus(restarted, fresh, 200);
                assertThat(root.runtime().business()).isEmpty();
                assertThat(restarted.getBeansOfType(com.hazelcast.core.HazelcastInstance.class)).isEmpty();
                assertThat(restarted.getBeansOfType(io.tapstate.spi.store.UserStore.class)).isEmpty();
                assertThat(admin.getDatabase(cacheDb).listCollectionNames().into(new ArrayList<>()))
                        .containsExactlyInAnyOrder("cache_identity", "schema_migrations", "cloud_sessions", "cloud_revocations");
                assertThat(binding.equals(configuration.getCollection("storage_binding").find().first())).isTrue();
                var currentKey = configuration.getCollection("cluster_keys").find().first();
                var materialBefore = new Document(keys); materialBefore.remove("sourceWriteFence");
                var materialAfter = new Document(currentKey); materialAfter.remove("sourceWriteFence");
                assertThat(materialBefore.equals(materialAfter)).isTrue();
                assertThat(configuration.getCollection("users").countDocuments()).isZero();
            }
        }
    }

    private ConfigurableApplicationContext cloudRecoveryRoot(String configurationUri, RecoverySdk sdk) {
        return new SpringApplicationBuilder(Bootstrap.class, RecoverySdkConfiguration.class)
                .environment(CloudFixtureEnvironment.isolated())
                .initializers(context -> context.getBeanFactory().registerSingleton("fixtureRecoverySdk", sdk)).run(
                        "--server.address=127.0.0.1", "--server.port=0", "--logging.level.root=ERROR", "--SDK_STATUS_SENDER_ENABLED=false",
                        "--tapstate.cloud.base-url=https://cloud.example", "--tapstate.cloud.token=fixture-runtime-token",
                        "--tapstate.cloud.cluster-id=" + sdk.cluster, "--tapstate.cloud.atlas-uri=" + configurationUri,
                        "--tapstate.control.auth.jwt-secret=cloud-loss-stable-test-key",
                        "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds"));
    }
    private static String exchangeCookie(ConfigurableApplicationContext application, String code) {
        int port = ((org.springframework.boot.web.server.context.WebServerApplicationContext)application).getWebServer().getPort();
        var client = org.springframework.web.client.RestClient.builder().baseUrl("http://127.0.0.1:" + port)
                .requestFactory(new org.springframework.http.client.JdkClientHttpRequestFactory(java.net.http.HttpClient.newBuilder()
                        .followRedirects(java.net.http.HttpClient.Redirect.NEVER).build())).build();
        var response = client.get().uri("/auth/exchange?code=" + code).retrieve().toBodilessEntity();
        assertThat(response.getStatusCode().value()).isEqualTo(302);
        String cookie = response.getHeaders().getFirst("Set-Cookie");
        assertThat(cookie).contains("Secure", "HttpOnly");
        return cookie.split(";", 2)[0];
    }
    private static void assertCloudStatus(ConfigurableApplicationContext application, String cookie, int expected) {
        int port = ((org.springframework.boot.web.server.context.WebServerApplicationContext)application).getWebServer().getPort();
        rootClient(application).post().uri("/api/storage/status").header("Cookie", cookie)
                .header("Origin", "http://127.0.0.1:" + port).body(Map.of()).exchange((request, response) -> {
                    assertThat(response.getStatusCode().value()).isEqualTo(expected);
                    if (expected == 200) assertThat(response.bodyTo(Map.class).get("businessPhase")).isEqualTo("AWAITING_REBUILD_CONFIRMATION");
                    return null;
                });
    }
    @org.springframework.boot.test.context.TestConfiguration(proxyBeanMethods = false)
    static class RecoverySdkConfiguration {
        @org.springframework.context.annotation.Bean
        @org.springframework.context.annotation.Primary
        CloudSdkBridge recoveryBridge(CloudRuntimeSettings settings, RecoverySdk sdk) { return new CloudSdkBridge(settings, sdk); }
    }
    static final class RecoverySdk implements CloudSdkBridge.SdkRuntime {
        final String cluster;
        final java.util.Set<String> consumed = java.util.concurrent.ConcurrentHashMap.newKeySet();
        RecoverySdk(String cluster) { this.cluster = cluster; }
        @Override public CloudSdkBridge.ExchangeResult exchange(String code) {
            if (!List.of("old", "blocked", "fresh").contains(code) || !consumed.add(code)) throw new CloudSdkBridge.ProviderFailure();
            return new CloudSdkBridge.ExchangeResult(code, "organization", cluster, "Fixture", "Cluster", "local");
        }
        @Override public CloudSdkBridge.VerifiedClaims verify(String jwt) {
            return new CloudSdkBridge.VerifiedClaims("cloud-user", "organization", cluster, jwt, Instant.now().plusSeconds(600),
                    "https://cloud.example", "127.0.0.1", List.of("workload:write"));
        }
        @Override public boolean verifyCallback(String method, String timestamp, String nonce, String data, String signature) {
            return method.equals("POST") && nonce.equals("fixture-nonce") && signature.equals("fixture-signature") && data.equals("blocked");
        }
        @Override public void report(String cluster, String nonce, io.tapstate.control.core.CloudRuntimeStatus status, String token) {
            throw new AssertionError("the fixture disables status reporting");
        }
    }

    /** Native ports seed exact historical receipts; HTTP/production DI perform the actual mixed-source edit. */
    @Test void originalPositionApiCommitsSharedAndDirectEditsTogetherAndAllowsASecondEdit() {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configDb = "position_cfg_" + suffix, cacheDb = "position_cache_" + suffix, masterDb = "position_master_" + suffix;
        String user = "position_" + suffix;
        var setup = new StorageSetupScope("position-" + suffix, "installation", "setup", 1);
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(configDb, cacheDb, masterDb));
            var cacheConnection = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, uri(user, cacheDb), cacheDb, null);
            try (var configuration = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(
                    new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(user, configDb), null, Duration.ofSeconds(5)),
                    setup.clusterId(), setup.installationId(), setup.operationId(), 1)) {
                var target = new SourceResource("views", null, "mongodb", Map.of("uri", uri(user, masterDb)), null, null, null, null);
                var committed = configuration.bindings().commitWithFirstCache(setup, StorageBinding.Mode.OP, cacheConnection, target);
                try (var cached = io.tapstate.adapters.mongostore.MongoCacheDomain.connect(cacheConnection, Duration.ofSeconds(5), "position-fixture")) {
                    cached.prepareFirstAuth(committed.firstCreation().orElseThrow()); cached.prepareInitialBusiness(committed.preparation(), () -> {});
                }
                configuration.bindings().complete(setup);
                configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(),
                        new User("admin", new BCryptPasswordHasher().hash("sentinel-login-password"), "admin"));
            }
            try (var application = nativeRoot(uri(user, configDb))) {
                var root = application.getBean(StorageRoot.class); assertThat(root.phase()).isEqualTo(StorageRoot.Phase.BUSINESS_READY);
                var bound = root.runtime().current().orElseThrow(); var cache = bound.cache(); var business = bound.preparation().businessScope();
                var configuration = root.runtime().requireConfiguration();
                var child = (ConfigurableApplicationContext)org.springframework.test.util.ReflectionTestUtils.getField(root.runtime().business().orElseThrow(), "context");
                var sources = new java.util.LinkedHashMap<String,SourceResource>();
                var table = new SourceTable("orders", List.of(new SourceField("id", "bigint", io.tapstate.core.common.TapstateType.INT64)), List.of("id"), List.of());
                for (String id : List.of("shared_source", "direct_source")) {
                    var source = new SourceResource(id, null, "mongodb", Map.of("uri", uri(user, masterDb)), SourceMode.SNAPSHOT,
                            List.of(TableRef.literal("orders")), null, null);
                    configuration.artifacts().create(source); sources.put(id, source);
                    cache.schemas(business, () -> {}).save(new DiscoveredSourceModel(id, "mongodb", 1, new SourceModel(List.of(table))));
                }
                var pipeline = new PipelineResource("position_flow", null, List.of(SourceRef.spec("shared_source", true), SourceRef.spec("direct_source", false)), null, null, null, null, null);
                configuration.artifacts().create(pipeline);
                var scope = new PipelineIntentScope(business.clusterId(), business.bindingVersion(), business.cacheIncarnation(), pipeline.id(), 1);
                var inputs = SourceModelSnapshot.capture(pipeline.id(), List.copyOf(sources.values()), cache.schemas(business, () -> {}))
                        .preparationInputs(scope, pipeline, pipeline, sources);
                child.getBean(PipelineFirstStatePreparer.class).prepare(scope, inputs);
                var ownership = child.getBean(PipelineActuationOwnership.class);
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15)).until(() -> ownership.permit(scope).granted());
                var permit = ownership.permit(scope); assertThat(permit.granted()).isTrue();
                var execution = ownership.beginExecution(scope, WorkloadClaimFence.from(permit.claim()));
                assertThat(execution.allowed()).isTrue(); var pipelineFence = execution.claimFence();
                var node = child.getBean(NodeSessionLease.class).currentFence().orElseThrow();
                var captures = child.getBean(CaptureOwnership.class);
                var srs = cache.srs(business, () -> {});
                String sharedId = null; DirectChannelContext directContext = null; DirectBatch pending = null;
                for (String source : sources.keySet()) {
                    var issued = (CaptureCreationResult.Issued)configuration.capturePreparations().issueFirstCreate(new CapturePreparationRequest(scope, pipelineFence, node, source, inputs));
                    var prepared = cache.capturePreparations(business, () -> {}).createFirst(issued.creation()).preparation();
                    var capture = captures.acquire(new io.tapstate.runtime.srs.CaptureId(prepared.captureId()));
                    assertThat(capture.acquired()).isTrue();
                    try {
                        if (prepared.mode() == CapturePreparation.Mode.SHARED_SRS) {
                            sharedId = prepared.chainId();
                            srs.chains().requestTables(sharedId, java.util.Set.of("orders"), capture.fence());
                            srs.chains().openEpoch(sharedId, 0, capture.fence()).orElseThrow();
                            srs.chains().publishServingTables(sharedId, 1, java.util.Set.of("orders"), capture.fence());
                            var key = new SrsModelKey(sharedId, "orders", business.cacheIncarnation(), 1, 0);
                            srs.models().publishInitial(key, io.tapstate.runtime.srs.SrsRingbuffer.ringName(sharedId, "orders"), table, capture.fence());
                            srs.chains().checkpoint(sharedId, 0, new SrsCaptureCheckpoint(new ChainPosition(SourceOrder.snapshotRow(1), "old-shared"),
                                    Map.of("orders", new SrsTableCheckpoint(key, -1))), capture.fence()).orElseThrow();
                            var consumer = new SrsConsumerContext(new SrsConsumerKey(sharedId, pipeline.id(), source), 1);
                            srs.consumers().attach(consumer, 0, java.util.Set.of("orders"), pipelineFence).orElseThrow();
                            srs.consumers().beginWriterRun(consumer, 1, "old-writer", Map.of("orders", List.of("writer")), pipelineFence);
                        } else {
                            directContext = inputs.directChannels().getFirst().context();
                            var direct = cache.directChannels(business, () -> {}); var before = direct.load(directContext);
                            long epoch = direct.beginCapture(directContext, before.revision(), 0, capture.fence(), pipelineFence).orElseThrow();
                            direct.confirmAnchor(directContext, epoch, "old-direct", capture.fence(), pipelineFence);
                            assertThat(direct.pinSnapshot(directContext, direct.load(directContext).revision(), epoch, "old-direct", pipelineFence)).isTrue();
                            direct.beginWriterRun(directContext, epoch, "old-writer", Map.of("orders", List.of("writer")), pipelineFence);
                            pending = new DirectBatch(directContext, epoch, 1, new ChainPosition(new SourceOrder(epoch, 0), "pending"), Map.of("orders", 0L));
                            direct.recordBatch(pending, 0, capture.fence(), pipelineFence);
                        }
                    } finally { captures.release(capture.claim()); }
                }
                var login = ordinaryLogin(application, false); var client = rootClient(application);
                var reading = client.get().uri("/api/pipelines/position_flow/position").header("Authorization", "Bearer " + login.token())
                        .retrieve().body(io.tapstate.control.core.PipelinePosition.class);
                assertThat(reading.chains()).extracting(value -> value.resumeFrom().token()).containsExactly("old-shared", "old-direct");
                for (String token : List.of("first", "second")) {
                    var request = Map.of("chains", List.of(Map.of("chainId", sharedId, "resumeFrom", Map.of("token", token + "-shared")),
                            Map.of("chainId", directContext.channelId(), "resumeFrom", Map.of("token", token + "-direct"))));
                    var result = client.put().uri("/api/pipelines/position_flow/position").header("Authorization", "Bearer " + login.token())
                            .body(request).retrieve().body(io.tapstate.control.core.PipelinePosition.class);
                    assertThat(result.chains()).extracting(value -> value.resumeFrom().token()).containsExactly(token + "-shared", token + "-direct");
                    var projected = child.getBean(StorePort.class).meta();
                    assertThat(projected.durableSourceRead(sharedId)).isEmpty();
                    assertThat(projected.durableSourceRead(directContext.channelId())).isEmpty();
                    assertThat(List.of(projected.read(sharedId).orElseThrow(), projected.read(directContext.channelId()).orElseThrow()))
                            .extracting(SrsMeta::sourceReadDurable).containsExactly(false, false);
                }
                assertThat(srs.chains().load(sharedId).orElseThrow().checkpoint().position().token()).isEqualTo("old-shared");
                var direct = cache.directChannels(business, () -> {}); var changed = direct.load(directContext);
                assertThat(changed.cursor().sinkAcked().token()).isEqualTo("old-direct"); assertThat(changed.writerRun()).isNull();
                assertThat(direct.pending(directContext, 1)).containsExactly(pending);
                var oldConsumer = new SrsConsumerContext(new SrsConsumerKey(sharedId, pipeline.id(), "shared_source"), 1);
                assertThatThrownBy(() -> srs.consumers().beginWriterRun(oldConsumer, 1, "old-writer", Map.of("orders", List.of("writer")), pipelineFence))
                        .isInstanceOf(TapstateException.class);
                var nextPermit = ownership.permit(scope);
                var nextExecution = ownership.beginExecution(scope, WorkloadClaimFence.from(nextPermit.claim()));
                var nextCapture = captures.acquire(new io.tapstate.runtime.srs.CaptureId(directContext.captureId()));
                assertThat(nextCapture.acquired()).isTrue();
                try {
                    long epoch = direct.beginCapture(directContext, changed.revision(), 1, nextCapture.fence(), nextExecution.claimFence()).orElseThrow();
                    var opened = cache.directChannels(business, () -> {}).load(directContext);
                    assertThat(opened.resumeToken()).isEqualTo("second-direct");
                    assertThat(opened.cursor().sinkAcked().token()).isEqualTo("old-direct");
                    assertThat(opened.retained().pendingCount()).isOne();
                    assertThat(direct.beginWriterRun(directContext, epoch, "next-writer", Map.of("orders", List.of("writer")), nextExecution.claimFence())).isTrue();
                    assertThat(direct.advanceWriter(directContext, epoch, "next-writer", "orders", "writer",
                            new WriterProgress(SourceOrder.snapshotRow(1),new ChainPosition(SourceOrder.snapshotRow(1),"old-direct")),nextExecution.claimFence())).isTrue();
                    var loaded = direct.load(directContext);
                    assertThat(loaded.cursor().snapshotCompletedTables()).containsExactly("orders");
                    assertThat(loaded.cursor().sinkAcked().token()).isEqualTo("old-direct");
                    assertThat(loaded.resumeOverride().token()).isEqualTo("second-direct");
                    var fixedDirect = directContext;
                    assertThatThrownBy(() -> direct.advanceWriter(fixedDirect,epoch,"next-writer","orders","writer",
                            new WriterProgress(new SourceOrder(epoch,0),new ChainPosition(new SourceOrder(epoch,0),"unstarted-cdc")),nextExecution.claimFence()))
                            .isInstanceOfSatisfying(TapstateException.class,failure -> assertThat(failure.code()).isEqualTo(IoError.DIRECT_CHANNEL_UNAVAILABLE));
                    assertThat(direct.load(directContext)).isEqualTo(loaded);
                    assertThat(direct.confirmAnchor(directContext, epoch, "second-direct", nextCapture.fence(), nextExecution.claimFence())).isTrue();
                    assertThat(direct.load(directContext).resumeOverride()).isNull();
                    assertThat(direct.load(directContext).cursor().sinkAcked().order()).isEqualTo(SourceOrder.snapshotRow(epoch));
                    assertThat(admin.getDatabase(cacheDb).getCollection("direct_batches").countDocuments()).isZero();
                } finally { captures.release(nextCapture.claim()); }
                // A required second-source fact disappears after preflight/audit. The first source must stay unchanged.
                var sharedCollection = admin.getDatabase(cacheDb).getCollection("srs_chains");
                var cursorCollection = admin.getDatabase(cacheDb).getCollection("srs_consumer_offsets");
                var channelCollection = admin.getDatabase(cacheDb).getCollection("direct_channel_offsets");
                var sharedBefore = sharedCollection.find(new Document("_id", sharedId)).first();
                var cursorBefore = cursorCollection.find().first(); var channelBefore = channelCollection.find().first();
                var service = child.getBean(io.tapstate.control.core.PipelinePositionService.class);
                var writer = child.getBean(io.tapstate.control.core.PipelinePositionService.WriteBack.class);
                org.springframework.test.util.ReflectionTestUtils.setField(service, "nativeWriteBack",
                        (io.tapstate.control.core.PipelinePositionService.WriteBack)(id, before, moves) -> {
                            var command = writer.prepare(id, before, moves);
                            return () -> { channelCollection.deleteOne(new Document("_id", channelBefore.get("_id"))); command.run(); };
                        });
                try {
                    var refused = Map.of("chains", List.of(Map.of("chainId", sharedId, "resumeFrom", Map.of("token", "never-shared")),
                            Map.of("chainId", directContext.channelId(), "resumeFrom", Map.of("token", "never-direct"))));
                    assertThatThrownBy(() -> client.put().uri("/api/pipelines/position_flow/position").header("Authorization", "Bearer " + login.token())
                            .body(refused).retrieve().toBodilessEntity()).isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                    failure -> assertThat(failure.getResponseBodyAsString()).contains("io.direct-channel-unavailable"));
                    assertThat(sharedCollection.find(new Document("_id", sharedId)).first()).isEqualTo(sharedBefore);
                    assertThat(cursorCollection.find().first()).isEqualTo(cursorBefore);
                    assertThat(channelCollection.countDocuments()).isZero();
                } finally {
                    channelCollection.insertOne(channelBefore);
                    org.springframework.test.util.ReflectionTestUtils.setField(service, "nativeWriteBack", writer);
                }
            }
        }
    }

    private ConfigurableApplicationContext nativeRoot(String configurationUri) {
        return new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                "--server.address=127.0.0.1", "--server.port=0", "--tapstate.hz.member-port=0", "--logging.level.root=ERROR",
                "--tapstate.hz.jet.cooperative-thread-count=2", "--SDK_STATUS_SENDER_ENABLED=false",
                "--tapstate.store.mongo.uri=" + configurationUri, "--tapstate.control.auth.jwt-secret=loss-restart-stable-test-key",
                "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds"), "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"));
    }
    private static org.springframework.web.client.RestClient rootClient(ConfigurableApplicationContext application) {
        int port = ((org.springframework.boot.web.server.context.WebServerApplicationContext)application).getWebServer().getPort();
        return org.springframework.web.client.RestClient.create("http://127.0.0.1:" + port);
    }
    private static io.tapstate.control.restapi.LoginResponse ordinaryLogin(ConfigurableApplicationContext application, boolean session) {
        return rootClient(application).post().uri("/auth/login")
                .body(Map.of("username", "admin", "password", "sentinel-login-password", "createSession", session))
                .retrieve().body(io.tapstate.control.restapi.LoginResponse.class);
    }

    @ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void nativeProductionRootLoadsConfigAndAuthBeforeAdmittingItsCacheRuntime(boolean businessPrepared) {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String configDb = "root_config_" + suffix, cacheDb = "root_cache_" + suffix, masterDb = "root_master_" + suffix;
        String configUser = "config_" + suffix, cacheUser = "cache_" + suffix, masterUser = "master_" + suffix;
        var original = new io.tapstate.spi.store.StorageSetupScope("root-" + suffix, "installation", "setup-" + suffix, 1);
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, configUser, List.of(configDb)); createUser(admin, cacheUser, List.of(cacheDb)); createUser(admin, masterUser, List.of(masterDb));
            var settings = new io.tapstate.adapters.mongostore.MongoConnectionSettings(uri(configUser, configDb), null, Duration.ofSeconds(5));
            var cache = new io.tapstate.spi.store.StorageBinding.Connection(io.tapstate.spi.store.StorageBinding.Connection.Kind.MONGODB,
                    uri(cacheUser, cacheDb), cacheDb, null);
            try (var configuration = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(settings,
                    original.clusterId(), original.installationId(), original.operationId(), 1)) {
                var target = new SourceResource("views", null, "mongodb", Map.of("isUri", true, "uri", uri(masterUser, masterDb)), null, null, null, null);
                var published = configuration.bindings().commitWithFirstCache(original, io.tapstate.spi.store.StorageBinding.Mode.OP, cache, target);
                var preparation = configuration.bindings().cachePreparation().orElseThrow();
                try (var cached = io.tapstate.adapters.mongostore.MongoCacheDomain.connect(cache, Duration.ofSeconds(5), "fixture")) {
                    cached.prepareFirstAuth(published.firstCreation().orElseThrow());
                    if (businessPrepared) cached.prepareInitialBusiness(preparation, () -> {});
                }
                configuration.bindings().complete(original);
                configuration.firstAdmin().create(configuration.firstAdmin().load().orElseThrow().scope(),
                        new io.tapstate.spi.store.User("admin", new BCryptPasswordHasher().hash("sentinel-login-password"), "admin"));
            }
            try (var application = new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated()).run(
                    "--server.address=127.0.0.1", "--server.port=0", "--tapstate.hz.member-port=0",
                    "--tapstate.hz.jet.cooperative-thread-count=2", "--logging.level.root=ERROR", "--SDK_STATUS_SENDER_ENABLED=false",
                    "--tapstate.store.mongo.uri=" + uri(configUser, configDb), "--tapstate.control.auth.jwt-secret=native-root-test-key",
                    "--tapstate.connectors.seed-dir=" + work.resolve("no-seeds"), "--tapstate.connectors.plugins-dir=" + work.resolve("plugins"))) {
                var root = application.getBean(StorageRoot.class);
                assertThat(root.phase()).isEqualTo(businessPrepared ? StorageRoot.Phase.BUSINESS_READY : StorageRoot.Phase.AUTH_READY);
                if (businessPrepared) org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(15)).until(root.runtime()::businessStarted);
                else assertThat(root.runtime().business()).isEmpty();
                int port = ((org.springframework.boot.web.server.context.WebServerApplicationContext)application).getWebServer().getPort();
                var client = org.springframework.web.client.RestClient.create("http://127.0.0.1:" + port);
                var login = client.post().uri("/auth/login").body(Map.of("username", "admin", "password", "sentinel-login-password"))
                        .retrieve().body(io.tapstate.control.restapi.LoginResponse.class);
                assertThat(login).isNotNull();
                String snapshot = client.post().uri("/api/storage/status").header("Authorization", "Bearer " + login.token())
                        .body(Map.of()).retrieve().body(String.class);
                assertThat(((Map<?,?>)io.tapstate.core.common.JsonReader.parse(snapshot)).get("businessPhase"))
                        .isEqualTo(businessPrepared ? "READY" : "AUTH_READY");
                assertThat(snapshot).contains("COMPLETE").doesNotContain(PASSWORD, "sentinel-login-password", "mongodb://");
                if (!businessPrepared) {
                    assertThat(admin.getDatabase(cacheDb).listCollectionNames().into(new ArrayList<>()))
                            .doesNotContain("workload_claims", "pipeline_state", "srs_chain_state");
                    assertThatThrownBy(() -> client.get().uri("/api/sources").header("Authorization", "Bearer " + login.token()).retrieve().body(String.class))
                            .isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                    failure -> assertThat(failure.getStatusCode().value()).isEqualTo(503));
                    return;
                }
                String sources = client.get().uri("/api/sources").header("Authorization", "Bearer " + login.token()).retrieve().body(String.class);
                assertThat(sources).contains("views").doesNotContain(PASSWORD, "sentinel-login-password");
                try (MongoClient limited = MongoClients.create(uri(configUser, configDb))) {
                    assertDenied(limited, cacheDb); assertDenied(limited, masterDb);
                }
                assertThat(admin.getDatabase(cacheDb).getCollection("workload_claims").countDocuments()).isEqualTo(1);
                assertThat(admin.getDatabase(configDb).listCollectionNames().into(new ArrayList<>())).doesNotContain(
                        "workload_claims", "system_meta", "operator_state", "op_sessions", "srs_meta");
                assertThat(admin.getDatabase(cacheDb).listCollectionNames().into(new ArrayList<>())).doesNotContain("users", "cluster_keys", "system_meta", "operator_state", "srs_meta");
                var current = root.runtime().current().orElseThrow();
                var modelSource = new SourceResource("model_source", null, "mongodb", Map.of("uri", uri(masterUser, masterDb)),
                        io.tapstate.core.model.SourceMode.SNAPSHOT, List.of(io.tapstate.core.model.TableRef.literal("orders")), null, null);
                root.runtime().requireConfiguration().artifacts().create(modelSource);
                root.runtime().requireConfiguration().artifacts().create(new io.tapstate.core.model.PipelineResource("model_flow", null,
                        List.of(io.tapstate.core.model.SourceRef.spec("model_source", false)), null, null, null, null, null));
                current.cache().schemas(current.preparation().businessScope(), () -> {}).save(new io.tapstate.spi.store.DiscoveredSourceModel(
                        "model_source", "mongodb", 1L, new io.tapstate.spi.store.SourceModel(List.of(new io.tapstate.spi.store.SourceTable("orders",
                        List.of(new io.tapstate.spi.store.SourceField("id", "bigint", io.tapstate.core.common.TapstateType.INT64)), List.of("id"), List.of())))));
                client.post().uri("/api/pipelines/model_flow:accept-derived-schema").header("Authorization", "Bearer " + login.token())
                        .retrieve().toBodilessEntity();
                var modelScope = new io.tapstate.spi.store.PipelineIntentScope(original.clusterId(), 1,
                        current.preparation().authScope().cacheIncarnation(), "model_flow", 1);
                assertThat(current.cache().unpreparedPipelineDerivedSchemas(modelScope, null, () -> {}).latest("model_flow", "model_source.orders").orElseThrow().schema())
                        .containsEntry("id", "INT64 NULL");
                root.runtime().requireConfiguration().artifacts().create(new SourceResource("pending_model", null, "mongodb",
                        Map.of("uri", uri(masterUser, masterDb)), io.tapstate.core.model.SourceMode.SNAPSHOT, null, null, null));
                root.runtime().requireConfiguration().artifacts().create(new io.tapstate.core.model.PipelineResource("partial_model_flow", null,
                        List.of(io.tapstate.core.model.SourceRef.spec("pending_model", false), io.tapstate.core.model.SourceRef.spec("model_source", false)),
                        null, null, null, null, null));
                var child = (ConfigurableApplicationContext)org.springframework.test.util.ReflectionTestUtils.getField(root.runtime().business().orElseThrow(), "context");
                child.getBean(io.tapstate.control.core.SchemaDerivation.class).derive("partial_model_flow");
                var partialScope = new io.tapstate.spi.store.PipelineIntentScope(original.clusterId(), 1,
                        current.preparation().authScope().cacheIncarnation(), "partial_model_flow", 1);
                assertThat(current.cache().unpreparedPipelineDerivedSchemas(partialScope, null, () -> {}).latest("partial_model_flow", "model_source.orders"))
                        .isPresent();
                assertThat(root.runtime().requireConfiguration().preparations().read("model_flow").orElseThrow().phase())
                        .isEqualTo(io.tapstate.spi.store.PipelinePreparation.Phase.UNASSIGNED);
                assertThat(root.runtime().requireConfiguration().preparations().read("partial_model_flow").orElseThrow().phase())
                        .isEqualTo(io.tapstate.spi.store.PipelinePreparation.Phase.UNASSIGNED);
                assertThat(admin.getDatabase(cacheDb).getCollection("workload_claims").countDocuments()).isEqualTo(1);
                var originalModelFlow = root.runtime().requireConfiguration().artifacts().get("model_flow").orElseThrow();
                client.delete().uri("/api/artifacts/model_flow").header("Authorization", "Bearer " + login.token())
                        .header("If-Match", "\"" + io.tapstate.core.model.canonical.CanonicalHash.of(originalModelFlow) + "\"")
                        .retrieve().toBodilessEntity();
                assertThat(root.runtime().requireConfiguration().artifacts().get("model_flow")).isEmpty();
                assertThat(root.runtime().requireConfiguration().pipelineStates().read("model_flow").orElseThrow().phase())
                        .isEqualTo(io.tapstate.spi.store.PipelineStateMapping.Phase.RETIRED);
                var configuration = root.runtime().requireConfiguration();
                configuration.artifacts().create(originalModelFlow);
                var reusedScope = new io.tapstate.spi.store.PipelineIntentScope(original.clusterId(), 1,
                        current.preparation().authScope().cacheIncarnation(), "model_flow", 2);
                var inputs = SourceModelSnapshot.capture("model_flow", List.of(modelSource),
                        current.cache().schemas(current.preparation().businessScope(), () -> {}))
                        .preparationInputs(reusedScope, (io.tapstate.core.model.PipelineResource)originalModelFlow,
                                (io.tapstate.core.model.PipelineResource)originalModelFlow, Map.of(modelSource.id(), modelSource));
                child.getBean(PipelineFirstStatePreparer.class).prepare(reusedScope, inputs);
                var driving=child.getBean(PipelineActuationOwnership.class);
                var permitted=driving.permit(reusedScope);
                assertThat(permitted.granted()).isTrue();
                var execution=driving.beginExecution(reusedScope,io.tapstate.spi.store.WorkloadClaimFence.from(permitted.claim()));
                assertThat(execution.allowed()).isTrue();
                var completedDag=new com.hazelcast.jet.core.DAG();
                completedDag.newVertex("done",com.hazelcast.jet.core.ProcessorMetaSupplier.forceTotalParallelismOne(
                        com.hazelcast.jet.core.ProcessorSupplier.of((com.hazelcast.function.SupplierEx<com.hazelcast.jet.core.Processor>) FinishedPipelineProcessor::new)));
                var retainedRun=child.getBean(io.tapstate.runtime.engine.Engine.class).submitRun("model_flow",completedDag,Map.of(),
                        io.tapstate.runtime.engine.nest.NestSettings.defaults(),new io.tapstate.runtime.engine.OperatorRunManifest("model_flow",execution.fence(),List.of()));
                assertThat(retainedRun.awaitTerminal(Duration.ofSeconds(15))).isTrue();
                String role = "retire_until_commit_" + suffix;
                admin.getDatabase("admin").runCommand(new Document("createRole", role).append("privileges", List.of(
                        new Document("resource", new Document("db", configDb).append("collection", ""))
                                .append("actions", List.of("find", "insert", "update", "createIndex", "listIndexes", "listCollections"))))
                        .append("roles", List.of()));
                admin.getDatabase("admin").runCommand(new Document("updateUser", configUser).append("roles", List.of(new Document("role", role).append("db", "admin"))));
                try {
                    assertThatThrownBy(() -> client.delete().uri("/api/artifacts/model_flow").header("Authorization", "Bearer " + login.token())
                            .header("If-Match", "\"" + io.tapstate.core.model.canonical.CanonicalHash.of(originalModelFlow) + "\"")
                            .retrieve().toBodilessEntity()).isInstanceOf(org.springframework.web.client.HttpServerErrorException.class);
                    assertThat(configuration.pipelineStates().read("model_flow").orElseThrow().phase())
                            .isEqualTo(io.tapstate.spi.store.PipelineStateMapping.Phase.RETIRING);
                    assertThat(configuration.artifacts().get("model_flow")).isPresent();
                    assertThat(admin.getDatabase(cacheDb).getCollection("workload_claims").find(new Document("pipelineScope.pipelineId", "model_flow"))
                            .first()).containsKey("pipelineRetirement");
                } finally {
                    admin.getDatabase("admin").runCommand(new Document("updateUser", configUser)
                            .append("roles", List.of(new Document("role", "readWrite").append("db", configDb))));
                }
                client.delete().uri("/api/artifacts/model_flow").header("Authorization", "Bearer " + login.token())
                        .header("If-Match", "\"" + io.tapstate.core.model.canonical.CanonicalHash.of(originalModelFlow) + "\"")
                        .retrieve().toBodilessEntity();
                assertThat(configuration.artifacts().get("model_flow")).isEmpty();
                assertThat(configuration.pipelineStates().read("model_flow").orElseThrow().phase())
                        .isEqualTo(io.tapstate.spi.store.PipelineStateMapping.Phase.RETIRED);
                configuration.artifacts().create(originalModelFlow);
                assertThat(configuration.pipelineStates().read("model_flow").orElseThrow().stateGeneration()).isEqualTo(3);
                assertThat(child.getBean(io.tapstate.runtime.engine.Engine.class).captureRun("model_flow").orElseThrow().jobId()).isEqualTo(retainedRun.jobId());
                client.delete().uri("/api/artifacts/model_flow").header("Authorization", "Bearer " + login.token())
                        .header("If-Match", "\"" + io.tapstate.core.model.canonical.CanonicalHash.of(originalModelFlow) + "\"")
                        .retrieve().toBodilessEntity();
                assertThat(configuration.artifacts().get("model_flow")).isEmpty();
                configuration.artifacts().create(originalModelFlow);
                var beforeFull = new io.tapstate.spi.store.PipelineIntentScope(original.clusterId(), 1,
                        current.preparation().authScope().cacheIncarnation(), "model_flow", 4);
                var fullInputs = SourceModelSnapshot.capture("model_flow", List.of(modelSource),
                        current.cache().schemas(current.preparation().businessScope(), () -> {}))
                        .preparationInputs(beforeFull, (io.tapstate.core.model.PipelineResource)originalModelFlow,
                                (io.tapstate.core.model.PipelineResource)originalModelFlow, Map.of(modelSource.id(), modelSource));
                child.getBean(PipelineFirstStatePreparer.class).prepare(beforeFull, fullInputs);
                // The fixture seeds the accepted stop intent; the production driver performs the actual replacement.
                child.getBean(io.tapstate.spi.store.StorePort.class).desired().save(new io.tapstate.core.lifecycle.DesiredState("model_flow",
                        io.tapstate.core.lifecycle.PipelineState.STOPPED, io.tapstate.core.model.canonical.CanonicalHash.of(originalModelFlow), true));
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(20)).until(() -> {
                    child.getBean(ConvergenceDriver.class).reconcile();
                    var mapping = configuration.pipelineStates().read("model_flow").orElseThrow();
                    return mapping.phase() == io.tapstate.spi.store.PipelineStateMapping.Phase.ACTIVE && mapping.stateGeneration() == 5;
                });
                var serverFailure = new AtomicReference<Throwable>();
                var deletion = child.getBean(io.tapstate.control.core.ArtifactMutationService.PipelineDeletion.class);
                org.springframework.test.util.ReflectionTestUtils.setField(child.getBean(io.tapstate.control.core.ArtifactMutationService.class), "pipelineDeletion",
                        (io.tapstate.control.core.ArtifactMutationService.PipelineDeletion)(definition, expected) -> {
                            try {
                                var command = deletion.prepare(definition, expected);
                                return () -> { try { command.run(); } catch (RuntimeException | Error failure) { serverFailure.set(failure); throw failure; } };
                            } catch (RuntimeException | Error failure) { serverFailure.set(failure); throw failure; }
                        });
                // Publication can precede the previous synchronous pass's finally block; only its busy fence is retried.
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(45)).untilAsserted(() -> {
                    try {
                        client.delete().uri("/api/artifacts/model_flow").header("Authorization", "Bearer " + login.token())
                                .header("If-Match", "\"" + io.tapstate.core.model.canonical.CanonicalHash.of(originalModelFlow) + "\"")
                                .retrieve().toBodilessEntity();
                    } catch (org.springframework.web.client.RestClientResponseException busy) {
                        var body = (Map<?,?>)io.tapstate.core.common.JsonReader.parse(busy.getResponseBodyAsString());
                        if (busy.getStatusCode().value() == 500 && "io.workload-claim-fenced".equals(body.get("code")))
                            throw new AssertionError("the native control is not yet available",serverFailure.get());
                        throw busy;
                    }
                });
                assertThat(configuration.artifacts().get("model_flow")).isEmpty();
                var businessOwner = (NativeBusinessContext)root.runtime().business().orElseThrow();
                var businessContext = (org.springframework.context.ConfigurableApplicationContext)org.springframework.test.util.ReflectionTestUtils.getField(businessOwner, "context");
                var originalMember = businessContext.getBean(com.hazelcast.core.HazelcastInstance.class);
                var originalNode = businessContext.getBean(NodeSessionLease.class).currentFence().orElseThrow();
                businessOwner.close();
                assertThat(originalMember.getLifecycleService().isRunning()).isFalse();
                assertThat(current.cache().workloadClaims(current.preparation().businessScope()).read(originalNode.key()).orElseThrow().leased()).isFalse();
                assertThat(root.runtime().businessStarted()).isFalse();
                String degraded = client.post().uri("/api/storage/status").header("Authorization", "Bearer " + login.token())
                        .body(Map.of()).retrieve().body(String.class);
                assertThat(((Map<?,?>)io.tapstate.core.common.JsonReader.parse(degraded)).get("businessPhase"))
                        .isEqualTo("AUTH_READY");
                admin.getDatabase(cacheDb).getCollection("op_sessions").drop();
                assertThatThrownBy(() -> client.post().uri("/auth/login")
                        .body(Map.of("username", "admin", "password", "sentinel-login-password", "createSession", false))
                        .retrieve().body(String.class))
                        .isInstanceOfSatisfying(org.springframework.web.client.RestClientResponseException.class,
                                failure -> assertThat(failure.getStatusCode().value()).isEqualTo(503));
            }
        }
    }

    private static final class FinishedPipelineProcessor extends com.hazelcast.jet.core.AbstractProcessor {
        @Override public boolean complete() { return true; }
    }

    /** Native control-service composition; the target fixture does not certify a registered connector or TLS. */
    @Test
    void setupAndFirstAdministratorServicesKeepNativeAuthorityAcrossAuditFailureAndReopen() {
        String configDb = "control_config_" + UUID.randomUUID().toString().replace("-", "");
        String cacheDb = configDb + "_cache";
        String masterDb = configDb + "_master";
        String user = "owner_" + configDb;
        var clock = java.time.Clock.systemUTC();
        var allowedAudit = new java.util.concurrent.atomic.AtomicBoolean(true);
        var targetTests = new AtomicInteger();
        var probe = new io.tapstate.adapters.mongostore.MongoStorageConnectionProbe(Duration.ofSeconds(5));
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(configDb, cacheDb, masterDb));
            var config = new io.tapstate.spi.store.StorageBinding.Connection(
                    io.tapstate.spi.store.StorageBinding.Connection.Kind.MONGODB, uri(user, configDb), configDb, null);
            var cache = new io.tapstate.spi.store.StorageBinding.Connection(config.kind(), uri(user, cacheDb), cacheDb, null);
            var master = new io.tapstate.spi.store.StorageBinding.Connection(config.kind(), uri(user, masterDb), masterDb, null);
            var settings = new io.tapstate.adapters.mongostore.MongoConnectionSettings(config.uri(), null, Duration.ofSeconds(5));
            var original = new io.tapstate.spi.store.StorageSetupScope("control-cluster", "installation", "original-setup", 1);
            var candidate = new io.tapstate.control.core.StorageSetupService.Candidate(config, cache, master);
            var target = new io.tapstate.control.core.StorageSetupService.DefaultTarget() {
                @Override public io.tapstate.control.core.StorageSetupService.ConnectorIdentity identity(
                        io.tapstate.spi.store.StorageBinding.Connection connection) {
                    return new io.tapstate.control.core.StorageSetupService.ConnectorIdentity("a".repeat(64), "b".repeat(64));
                }
                @Override public SourceResource definition(io.tapstate.spi.store.StorageBinding.Connection connection, String id) {
                    return new SourceResource(id, null, "mongodb", Map.of("uri", connection.uri()), null, null, null, null);
                }
                @Override public void test(io.tapstate.spi.store.StorageBinding.Connection connection, SourceResource source,
                        io.tapstate.control.core.StorageSetupService.ConnectorIdentity identity) {
                    assertThat(source.config().get("uri")).isEqualTo(probe.normalize(master).uri());
                    targetTests.incrementAndGet();
                }
            };
            try (var domain = io.tapstate.adapters.mongostore.MongoConfigurationBootstrap.prepare(
                    settings, original.clusterId(), original.installationId(), original.operationId(), original.generation())) {
                var audit = new io.tapstate.control.core.AuditGate(record -> {
                    if (!allowedAudit.get()) throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "audit-unavailable"), null);
                    domain.audit().record(record);
                }, clock);
                var authority = new io.tapstate.control.core.SetupCapabilityService(domain.setupStates(), clock, () -> {}, () -> {}, () -> {});
                var issued = authority.issue(original, io.tapstate.spi.store.StorageSetupStateStore.Purpose.SETUP);
                var permission = authority.authorize(issued.token(), issued.scope(), issued.purpose());
                var service = new io.tapstate.control.core.StorageSetupService(io.tapstate.spi.store.StorageBinding.Mode.OP,
                        domain.bindings(), domain.setupStates(), probe, target, () -> java.util.Optional.of(config), audit, clock, () -> {});
                var stamps = new java.util.EnumMap<io.tapstate.control.core.StorageSetupService.Layer, String>(
                        io.tapstate.control.core.StorageSetupService.Layer.class);
                for (var layer : io.tapstate.control.core.StorageSetupService.Layer.values())
                    stamps.put(layer, service.test(issued.scope(), permission, candidate, layer).testStamp());
                assertThat(targetTests).hasValue(1);
                allowedAudit.set(false);
                assertThatThrownBy(() -> service.commit("owner", issued.scope(), permission, candidate, stamps))
                        .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code())
                                .isEqualTo(io.tapstate.control.core.ControlError.AUDIT_BLOCKED));
                assertThat(domain.bindings().load()).isEmpty();
                assertThat(domain.artifacts().list()).isEmpty();
                allowedAudit.set(true);
                var bound = service.commit("owner", issued.scope(), permission, candidate, stamps);
                assertThat(bound.setupPhase()).isEqualTo(io.tapstate.spi.store.StorageSetupStateStore.Phase.STORAGE_BOUND);
                var first = service.pending(issued.scope(), permission).orElseThrow().firstCreation().orElseThrow();
                var originalPreparation = domain.bindings().cachePreparation().orElseThrow();
                try (var reopened = io.tapstate.adapters.mongostore.MongoConfigurationDomain.open(settings, original.clusterId())) {
                    var nextAuthority = new io.tapstate.control.core.SetupCapabilityService(reopened.setupStates(), clock, () -> {}, () -> {}, () -> {});
                    var next = nextAuthority.issue(issued.scope(), issued.purpose());
                    var nextPermission = nextAuthority.authorize(next.token(), next.scope(), next.purpose());
                    assertThatThrownBy(permission::run).isInstanceOf(TapstateException.class);
                    var resumed = new io.tapstate.control.core.StorageSetupService(io.tapstate.spi.store.StorageBinding.Mode.OP,
                            reopened.bindings(), reopened.setupStates(), probe, target, () -> java.util.Optional.of(config), audit, clock, () -> {});
                    assertThat(resumed.commit("owner", next.scope(), nextPermission, candidate, Map.of()).bindingVersion()).isEqualTo(1);
                    assertThat(resumed.pending(next.scope(), nextPermission).orElseThrow().firstCreation()).isEmpty();
                    assertThat(reopened.bindings().cachePreparation()).contains(originalPreparation);
                    assertThat(reopened.artifacts().list()).hasSize(1);
                    first.requireActive();
                    reopened.bindings().complete(next.scope());
                    assertThatThrownBy(nextPermission::run).isInstanceOf(TapstateException.class);
                    var adminIssued = nextAuthority.issue(next.scope(), io.tapstate.spi.store.StorageSetupStateStore.Purpose.FIRST_ADMIN);
                    var adminPermission = nextAuthority.authorize(adminIssued.token(), adminIssued.scope(), adminIssued.purpose());
                    var passwords = new BCryptPasswordHasher();
                    var administrators = new io.tapstate.control.core.FirstAdministratorService(reopened.firstAdmin(), passwords, audit);
                    allowedAudit.set(false);
                    assertThatThrownBy(() -> administrators.create(adminPermission, "first-admin", "sentinel-password"))
                            .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code())
                                    .isEqualTo(io.tapstate.control.core.ControlError.AUDIT_BLOCKED));
                    assertThat(reopened.users().isEmpty()).isTrue();
                    allowedAudit.set(true);
                    assertThat(administrators.create(adminPermission, "first-admin", "sentinel-password").phase())
                            .isEqualTo(io.tapstate.spi.store.FirstAdminStore.Phase.COMPLETE);
                    assertThat(passwords.matches("sentinel-password", reopened.users().find("first-admin").orElseThrow().passwordHash())).isTrue();
                    assertThatThrownBy(adminPermission::run).isInstanceOf(TapstateException.class);
                    assertThatThrownBy(() -> administrators.create(adminPermission, "another", "another-password"))
                            .isInstanceOf(TapstateException.class);
                    assertThat(reopened.users().find("another")).isEmpty();
                }
                assertThat(admin.getDatabase(cacheDb).listCollectionNames().into(new ArrayList<>())).isEmpty();
                assertThat(admin.getDatabase(masterDb).listCollectionNames().into(new ArrayList<>())).isEmpty();
                assertThat(SystemCollections.AUDIT.on(admin.getDatabase(configDb)).find().into(new ArrayList<>()).toString())
                        .doesNotContain(PASSWORD, "sentinel-password", "mongodb://");
            } finally {
                for (String database : List.of(configDb, cacheDb, masterDb)) admin.getDatabase(database).drop();
                admin.getDatabase("admin").runCommand(new Document("dropUser", user));
            }
        }
    }

    @Test
    void twoClusterUsersCanUseOnlyTheirOwnThreeDatabasesAndRecoverAfterRestart() throws Exception {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String a = "isolation_a_" + suffix;
        String b = "isolation_b_" + suffix;
        String userA = "user_a_" + suffix;
        String userB = "user_b_" + suffix;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, userA, List.of(a, a + "_operator", a + "_views"));
            createUser(admin, userB, List.of(b, b + "_operator", b + "_views"));
            try (MongoClient ownA = MongoClients.create(uri(userA, a));
                 MongoClient ownB = MongoClients.create(uri(userB, b));
                 MongoClient anonymous = MongoClients.create("mongodb://" + MONGO.getHost() + ":" + MONGO.getMappedPort(27017)
                         + "/" + a + "?directConnection=true")) {
                assertDenied(anonymous, a);
                for (String other : List.of(b, b + "_operator", b + "_views")) assertDenied(ownA, other);
                for (String other : List.of(a, a + "_operator", a + "_views")) assertDenied(ownB, other);

                for (String metadata : List.of(a, b)) {
                    String user = metadata.equals(a) ? userA : userB;
                    StorageProof proof;
                    try (ConfigurableApplicationContext first = start(uri(user, metadata), metadata)) {
                        StorePort store = first.getBean(StorePort.class);
                        proof = writeStorageProof(store, metadata);
                        assertStorageProof(admin.getDatabase(metadata), store, proof);
                        store.keyedState().save("permissions-proof", "same-key", metadata.getBytes(StandardCharsets.UTF_8));
                        assertViewsNotSeeded(admin.getDatabase(metadata), store);
                        try (MongoClient view = MongoClients.create(uri(user, metadata + "_views"))) {
                            view.getDatabase(metadata + "_views").getCollection("proof")
                                    .insertOne(new Document("_id", "same-key").append("owner", metadata));
                        }
                        assertNoProbes(admin, metadata);
                    }
                    try (ConfigurableApplicationContext restarted = start(uri(user, metadata), metadata)) {
                        assertStorageProof(admin.getDatabase(metadata), restarted.getBean(StorePort.class), proof);
                        assertViewsNotSeeded(admin.getDatabase(metadata), restarted.getBean(StorePort.class));
                        assertThat(restarted.getBean(StorePort.class).keyedState().load("permissions-proof", "same-key"))
                                .hasValueSatisfying(value -> assertThat(value).isEqualTo(metadata.getBytes(StandardCharsets.UTF_8)));
                        assertThat(admin.getDatabase(metadata + "_views").getCollection("proof")
                                .find(new Document("_id", "same-key")).first().getString("owner")).isEqualTo(metadata);
                        assertNoProbes(admin, metadata);
                    }
                }
            }
        }
    }

    private record StorageProof(CheckpointDoc checkpoint, SrsMeta meta, List<SrsLogRecord> log,
            RateSample sample, List<ConnectorRegistration> connectors) { }

    private static StorageProof writeStorageProof(StorePort store, String cluster) {
        Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        store.state().create("permissions-proof", "{\"cluster\":\"" + cluster + "\"}", now);
        store.meta().create("permissions-chain", "7d");
        long epoch = store.meta().openEpoch("permissions-chain");
        ChainPosition position = new ChainPosition(new SourceOrder(epoch, 3), cluster + "/position-3");
        var claim = store.workloadClaims().acquire(
                new WorkloadClaimKey(cluster, WorkloadClaimType.CAPTURE, "permissions-capture"),
                new WorkloadOwner("permissions-node", "permissions-boot"), 1, Duration.ofMinutes(5)).claim();
        WorkloadClaimFence fence = WorkloadClaimFence.from(claim);
        List<SrsLogRecord> log = List.of(
                new SrsLogRecord(cluster + "/position-1", Op.INSERT, 1, null,
                        Map.of("id", 1, "owner", cluster), 0, fence),
                new SrsLogRecord(cluster + "/position-2", Op.UPDATE, 2,
                        Map.of("id", 1, "owner", cluster), Map.of("id", 1, "owner", cluster, "changed", true), 0, fence),
                new SrsLogRecord(cluster + "/position-3", Op.DELETE, 3,
                        Map.of("id", 1, "owner", cluster, "changed", true), null, 0, fence));
        store.srsLog().storeAll("srs.permissions", 1, log);
        store.meta().advanceSourceReadOffset("permissions-chain", position);
        assertThat(store.workloadClaims().release(claim)).isTrue();
        assertThatThrownBy(() -> store.srsLog().storeAll("srs.permissions", 4, List.of(
                new SrsLogRecord("released-owner-position", Op.INSERT, 4, null, Map.of("id", 2), 0, fence))))
                .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        assertThat(store.srsLog().load("srs.permissions", 4)).isEmpty();
        RateSample sample = new RateSample("permissions-proof", now, Map.of("records.out", 3L), Map.of(), now.minusSeconds(60));
        store.rateHistory().append(sample);
        SrsMeta meta = store.meta().read("permissions-chain").orElseThrow();
        assertThat(meta.sourceRead()).isEqualTo(position);
        assertThat(meta.epoch()).isEqualTo(epoch).isEqualTo(1);
        assertThat(meta.retention()).isEqualTo("7d");
        assertThat(meta.consumerOffsets()).isEmpty();
        assertThat(meta.schemaHistory()).isEmpty();
        assertThat(meta.sourceReadAt()).isNotNull().isAfterOrEqualTo(now.minusSeconds(2))
                .isBeforeOrEqualTo(Instant.now().plusSeconds(2));
        return new StorageProof(CheckpointDoc.initial("permissions-proof", "{\"cluster\":\"" + cluster + "\"}", now),
                meta, log, sample, store.connectors().list());
    }

    private static void assertStorageProof(MongoDatabase database, StorePort store, StorageProof proof) throws Exception {
        assertThat(store.state().read("permissions-proof")).contains(proof.checkpoint());
        assertThat(store.meta().read("permissions-chain")).contains(proof.meta());
        assertThat(store.srsLog().largestSequence("srs.permissions")).isEqualTo(3);
        for (int index = 0; index < proof.log().size(); index++) {
            assertThat(store.srsLog().load("srs.permissions", index + 1)).contains(proof.log().get(index));
        }
        assertThat(store.srsLog().load("srs.permissions", 4)).isEmpty();
        assertThat(store.rateHistory().readPage("permissions-proof", proof.sample().observedAt().minusSeconds(1),
                proof.sample().observedAt().plusSeconds(1), null, 1).entries())
                .singleElement().satisfies(entry -> assertThat(entry.sample()).isEqualTo(proof.sample()));
        assertThat(store.connectors().list()).containsExactlyInAnyOrderElementsOf(proof.connectors());
        assertThat(proof.connectors()).extracting(ConnectorRegistration::connectorId).containsExactlyInAnyOrder(
                "mysql", "mongodb", "postgres", "oracle", "sqlserver", "mongodb-atlas", "aws-rds-mysql", "db2");
        for (ConnectorRegistration registration : proof.connectors()) {
            byte[] bytes = store.connectors().artifact(registration.contentHash()).orElseThrow();
            Path seed = CloudConnectorTestInputs.seedDirectory().resolve(registration.connectorId() + "-connector.jar");
            assertThat(bytes.length).isEqualTo(Files.size(seed));
            assertThat(HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes)))
                    .isEqualTo(registration.contentHash());
            MessageDigest expected = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(seed)) {
                byte[] buffer = new byte[8192];
                for (int length; (length = input.read(buffer)) != -1;) expected.update(buffer, 0, length);
            }
            assertThat(HexFormat.of().formatHex(expected.digest())).isEqualTo(registration.contentHash());
        }
        assertThat(SystemCollections.PIPELINE_STATE.on(database).countDocuments(new Document("_id", "permissions-proof"))).isEqualTo(1);
        assertThat(SystemCollections.SRS_META.on(database).countDocuments(new Document("_id", "permissions-chain"))).isEqualTo(1);
        // Change rows share the collection with a private high-water mark; the latter is not an event.
        var logs = SystemCollections.SRS_LOG.on(database);
        assertThat(logs.countDocuments(new Document("_id.ring", "srs.permissions"))).isEqualTo(3);
        assertThat(logs.find(new Document("_id", "srs-log-bounds:srs.permissions")).first())
                .containsEntry("largestSequence", 3L).containsEntry("trimmedThrough", -1L);
        Document history = SystemCollections.PIPELINE_RATE_HISTORY.on(database)
                .find(new Document("pipelineId", "permissions-proof")).first();
        assertThat(history.getDate("observedAt").toInstant()).isEqualTo(proof.sample().observedAt());
        assertThat(database.getCollection("connector_artifacts.files").countDocuments()).isEqualTo(8);
        assertThat(database.getCollection("connector_artifacts.chunks").countDocuments()).isGreaterThan(8);
        assertThat(historyIndex(database).get("expireAfterSeconds", Number.class).longValue()).isEqualTo(Duration.ofDays(15).toSeconds());
    }

    private ConfigurableApplicationContext start(String metadataUri, String clusterId) {
        return start(metadataUri, clusterId, new AtomicInteger());
    }

    @Test
    void changingHistoryRetentionNeedsOnlyTheHistoryCollectionsAlterPrivilege() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String metadata = "isolation_ttl_" + suffix;
        String user = "user_ttl_" + suffix;
        long originalSeconds = Duration.ofDays(15).toSeconds();
        long changedSeconds = Duration.ofDays(7).toSeconds();
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(metadata, metadata + "_operator", metadata + "_views"));
            MongoDatabase database = admin.getDatabase(metadata);
            Instant observedAt = Instant.now().minusSeconds(60).truncatedTo(ChronoUnit.MILLIS);
            RateSample sample = new RateSample("retention-proof", observedAt,
                    Map.of("records.out", 3L), Map.of(), observedAt.minusSeconds(60));
            try (ConfigurableApplicationContext first = start(uri(user, metadata), metadata)) {
                first.getBean(StorePort.class).rateHistory().append(sample);
            }
            Document originalIndex = historyIndex(database);
            Document originalCollection = historyCollection(database);
            Document originalSample = SystemCollections.PIPELINE_RATE_HISTORY.on(database).find().first();
            Document originalSource = SystemCollections.ARTIFACTS.on(database).find(new Document("_id", "views")).first();
            assertThat(originalIndex.get("expireAfterSeconds", Number.class).longValue()).isEqualTo(originalSeconds);
            try (ConfigurableApplicationContext unchanged = start(uri(user, metadata), metadata)) {
                assertThat(unchanged.getBean(StorePort.class).rateHistory()
                        .readPage(sample.pipelineId(), observedAt.minusSeconds(1), observedAt.plusSeconds(1), null, 1)
                        .entries()).singleElement().satisfies(entry -> assertThat(entry.sample()).isEqualTo(sample));
            }
            try (MongoClient limited = MongoClients.create(uri(user, metadata))) {
                assertThatThrownBy(() -> limited.getDatabase(metadata).runCommand(historyExpiry(changedSeconds)))
                        .isInstanceOfSatisfying(MongoCommandException.class, error -> assertThat(error.getErrorCode()).isEqualTo(13));
            }
            AtomicInteger ready = new AtomicInteger();
            Throwable failure = catchThrowable(() -> {
                try (var unexpected = start(uri(user, metadata), metadata, ready,
                        "--tapstate.metrics.history.retention=7d")) {
                    throw new AssertionError("Cloud startup accepted an unchanged TTL under a changed retention");
                }
            });
            assertThat(ready.get()).isZero();
            assertThat(coded(failure)).isInstanceOfSatisfying(TapstateException.class, error -> {
                assertThat(error.code()).isEqualTo(IoError.STORE_UNAVAILABLE);
                assertThat(error.args()).containsExactlyEntriesOf(Map.of("detail", "MongoCommandException code=13"));
                assertThat(error.getCause()).isNull();
            });
            assertThat(historyIndex(database)).isEqualTo(originalIndex);
            assertThat(historyCollection(database)).isEqualTo(originalCollection);
            assertThat(SystemCollections.PIPELINE_RATE_HISTORY.on(database).find().first()).isEqualTo(originalSample);
            assertThat(SystemCollections.ARTIFACTS.on(database).find(new Document("_id", "views")).first()).isEqualTo(originalSource);

            String role = "alter_history_" + suffix;
            database.runCommand(new Document("createRole", role).append("roles", List.of()).append("privileges", List.of(
                    new Document("resource", new Document("db", metadata)
                            .append("collection", SystemCollections.PIPELINE_RATE_HISTORY.collectionName()))
                            .append("actions", List.of("collMod")))));
            admin.getDatabase("admin").runCommand(new Document("grantRolesToUser", user).append("roles", List.of(
                    new Document("role", role).append("db", metadata))));
            database.getCollection("unrelated_history").createIndex(new Document("observedAt", 1),
                    new IndexOptions().name("observedAt_idx").expireAfter(originalSeconds, TimeUnit.SECONDS));
            try (MongoClient limited = MongoClients.create(uri(user, metadata))) {
                assertThatThrownBy(() -> limited.getDatabase(metadata).runCommand(
                        historyExpiry(changedSeconds).append("collMod", "unrelated_history")))
                        .isInstanceOfSatisfying(MongoCommandException.class, error -> assertThat(error.getErrorCode()).isEqualTo(13));
            }
            AtomicInteger recoveredReady = new AtomicInteger();
            try (ConfigurableApplicationContext recovered = start(uri(user, metadata), metadata, recoveredReady,
                    "--tapstate.metrics.history.retention=7d")) {
                assertThat(recoveredReady.get()).isEqualTo(1);
                assertThat(recovered.getBean(StorePort.class).rateHistory().retention()).isEqualTo(Duration.ofDays(7));
                assertThat(recovered.getBean(StorePort.class).rateHistory()
                        .readPage(sample.pipelineId(), observedAt.minusSeconds(1), observedAt.plusSeconds(1), null, 1)
                        .entries()).singleElement().satisfies(entry -> assertThat(entry.sample()).isEqualTo(sample));
            }
            Document expectedIndex = new Document(originalIndex).append("expireAfterSeconds", changedSeconds);
            Document actualIndex = historyIndex(database);
            assertThat(actualIndex.get("expireAfterSeconds", Number.class).longValue()).isEqualTo(changedSeconds);
            // The server may store the altered expiry as an int rather than the original long.
            assertThat(new Document(actualIndex).append("expireAfterSeconds", changedSeconds)).isEqualTo(expectedIndex);
            assertThat(historyCollection(database)).isEqualTo(originalCollection);
            assertThat(SystemCollections.PIPELINE_RATE_HISTORY.on(database).find().first()).isEqualTo(originalSample);
            assertThat(SystemCollections.ARTIFACTS.on(database).find(new Document("_id", "views")).first()).isEqualTo(originalSource);
            assertNoProbes(admin, metadata);
        }
    }

    @Test
    void missingMigrationIndexPrivilegeIsCodedWithoutTheDriverMessageOrCause() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String metadata = "isolation_migration_" + suffix;
        String user = "user_migration_" + suffix;
        String role = "without_create_index_" + suffix;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            Document builtin = admin.getDatabase("admin").runCommand(new Document("rolesInfo",
                    new Document("role", "readWrite").append("db", metadata)).append("showPrivileges", true))
                    .getList("roles", Document.class).getFirst();
            List<Document> privileges = builtin.getList("privileges", Document.class).stream()
                    .map(privilege -> new Document(privilege).append("actions", privilege.getList("actions", String.class)
                            .stream().filter(action -> !"createIndex".equals(action)).toList())).toList();
            admin.getDatabase(metadata).runCommand(new Document("createRole", role)
                    .append("roles", List.of()).append("privileges", privileges));
            admin.getDatabase("admin").runCommand(new Document("createUser", user).append("pwd", PASSWORD).append("roles", List.of(
                    new Document("role", role).append("db", metadata),
                    new Document("role", "readWrite").append("db", metadata + "_operator"),
                    new Document("role", "readWrite").append("db", metadata + "_views"))));
            try (MongoClient limited = MongoClients.create(uri(user, metadata))) {
                assertThatThrownBy(() -> limited.getDatabase(metadata).getCollection("index_denied")
                        .createIndex(new Document("value", 1)))
                        .isInstanceOfSatisfying(MongoCommandException.class, error -> assertThat(error.getErrorCode()).isEqualTo(13));
            }
            AtomicInteger ready = new AtomicInteger();
            Throwable failure = catchThrowable(() -> {
                try (var unexpected = start(uri(user, metadata), metadata, ready)) {
                    throw new AssertionError("Cloud startup accepted a failed index migration");
                }
            });
            assertThat(ready.get()).isZero();
            assertThat(coded(failure)).isInstanceOfSatisfying(TapstateException.class, error -> {
                assertThat(error.code()).isEqualTo(MigrationError.CHANGESET_FAILED);
                assertThat(error.args()).containsExactlyEntriesOf(Map.of(
                        "changeset", "V1BaselineIndexes", "cause", "MongoCommandException code=13"));
                assertThat(error.getCause()).isNull();
            });
            Document schema = SystemCollections.SYSTEM_META.on(admin.getDatabase(metadata)).find(new Document("_id", "schema")).first();
            assertThat(schema).doesNotContainKey("installedVersion");
            assertThat(schema.get("lock", Document.class)).doesNotContainKeys("owner", "since", "heartbeat");
            assertNoProbes(admin, metadata);

            admin.getDatabase(metadata).runCommand(new Document("updateRole", role)
                    .append("privileges", builtin.getList("privileges", Document.class)));
            AtomicInteger recoveredReady = new AtomicInteger();
            try (ConfigurableApplicationContext recovered = start(uri(user, metadata), metadata, recoveredReady)) {
                assertThat(recoveredReady.get()).isEqualTo(1);
                assertViewsNotSeeded(admin.getDatabase(metadata), recovered.getBean(StorePort.class));
            }
            assertThat(SystemCollections.SYSTEM_META.on(admin.getDatabase(metadata)).find(new Document("_id", "schema"))
                    .first().getInteger("installedVersion"))
                    .isEqualTo(io.tapstate.adapters.mongostore.migration.MigrationRunner.SUPPORTED_VERSION);
            assertNoProbes(admin, metadata);
        }
    }

    private static Throwable coded(Throwable failure) {
        Throwable current = failure;
        for (int depth = 0; depth < 16 && current != null && !(current instanceof TapstateException); depth++) current = current.getCause();
        return current;
    }

    private static Document historyExpiry(long seconds) {
        return new Document("collMod", SystemCollections.PIPELINE_RATE_HISTORY.collectionName())
                .append("index", new Document("name", "observedAt_idx").append("expireAfterSeconds", seconds));
    }

    private static Document historyIndex(MongoDatabase database) {
        return SystemCollections.PIPELINE_RATE_HISTORY.on(database).listIndexes().into(new ArrayList<>()).stream()
                .filter(index -> "observedAt_idx".equals(index.getString("name"))).findFirst().orElseThrow();
    }

    private static Document historyCollection(MongoDatabase database) {
        return database.listCollections().filter(new Document("name", SystemCollections.PIPELINE_RATE_HISTORY.collectionName())).first();
    }

    @Test
    void changingCloudMetadataCredentialsDoesNotCreateOrRenewAViewsSource() {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String metadata = "isolation_renewed_" + suffix;
        String user = "user_renewed_" + suffix;
        String renewedPassword = "controlled-renewed-mongo-password";
        SourceResource authored = new SourceResource("views",
                new Metadata(Map.of(), "authored", true, "verified-user"), "mongodb",
                Map.of("isUri", true, "uri", "mongodb://source-user:source-secret@user.example/owned"),
                null, null, null, null);
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(metadata, metadata + "_operator", metadata + "_views"));
            try (ConfigurableApplicationContext first = start(uri(user, metadata), metadata)) {
                StorePort store = first.getBean(StorePort.class);
                store.keyedState().save("permissions-proof", "renewed-key", new byte[] {7});
                assertViewsNotSeeded(admin.getDatabase(metadata), store);
                store.artifacts().create(authored);
                try (MongoClient materialization = MongoClients.create(uri(user, metadata + "_views"))) {
                    materialization.getDatabase(metadata + "_views").getCollection("proof")
                            .insertOne(new Document("_id", "before-renewal").append("owner", metadata));
                }
            }
            Document originalSource = SystemCollections.ARTIFACTS.on(admin.getDatabase(metadata))
                    .find(new Document("_id", "views")).first();
            assertThat(originalSource.get("body", Document.class).get("config")).isInstanceOf(String.class);
            admin.getDatabase("admin").runCommand(new Document("updateUser", user).append("pwd", renewedPassword));
            try (MongoClient revoked = MongoClients.create(uri(user, metadata))) {
                assertThatThrownBy(() -> revoked.getDatabase(metadata).getCollection("proof").countDocuments())
                        .as("the server no longer accepts the original password")
                        .isInstanceOf(MongoSecurityException.class);
            }
            try (ConfigurableApplicationContext restarted = start(uri(user, metadata, renewedPassword), metadata)) {
                StorePort store = restarted.getBean(StorePort.class);
                assertThat(store.keyedState().load("permissions-proof", "renewed-key"))
                        .hasValueSatisfying(value -> assertThat(value).isEqualTo(new byte[] {7}));
                assertThat(store.artifacts().get("views")).contains(authored);
                assertThat(SystemCollections.ARTIFACTS.on(admin.getDatabase(metadata))
                        .find(new Document("_id", "views")).first()).isEqualTo(originalSource);
                try (MongoClient materialization = MongoClients.create(uri(user, metadata + "_views", renewedPassword))) {
                    materialization.getDatabase(metadata + "_views").getCollection("proof")
                            .insertOne(new Document("_id", "after-renewal").append("owner", metadata));
                }
                assertThat(admin.getDatabase(metadata + "_views").getCollection("proof").countDocuments()).isEqualTo(2);
                Object rawConfig = admin.getDatabase(metadata).getCollection(SystemCollections.ARTIFACTS.collectionName())
                        .find(new Document("_id", "views")).first().get("body", Document.class).get("config");
                assertThat(rawConfig).isInstanceOf(String.class);
                assertThat((String) rawConfig).doesNotContain(PASSWORD, renewedPassword);
                assertNoProbes(admin, metadata);
            }
        }
    }

    private static void assertViewsNotSeeded(MongoDatabase metadata, StorePort store) {
        assertThat(store.artifacts().get(ViewTargetResolver.STATE_STORE_SOURCE_ID)).isEmpty();
        assertThat(SystemCollections.ARTIFACTS.on(metadata)
                .countDocuments(new Document("_id", ViewTargetResolver.STATE_STORE_SOURCE_ID))).isZero();
    }

    @ParameterizedTest
    @EnumSource(Missing.class)
    void readableButUnwritableDerivedDatabaseCannotReportReady(Missing readOnly) {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String metadata = "isolation_readonly_" + suffix;
        String denied = metadata + (readOnly == Missing.OPERATOR ? "_operator" : "_views");
        String other = metadata + (readOnly == Missing.OPERATOR ? "_views" : "_operator");
        String user = "user_readonly_" + suffix;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            admin.getDatabase("admin").runCommand(new Document("createUser", user).append("pwd", PASSWORD).append("roles", List.of(
                    new Document("role", "readWrite").append("db", metadata),
                    new Document("role", "readWrite").append("db", other),
                    new Document("role", "read").append("db", denied))));
            try (MongoClient limited = MongoClients.create(uri(user, metadata))) {
                assertThat(limited.getDatabase(denied).getCollection("proof").countDocuments()).isZero();
                assertThatThrownBy(() -> limited.getDatabase(denied).getCollection("proof")
                        .insertOne(new Document("_id", "denied-write")))
                        .isInstanceOfSatisfying(MongoCommandException.class, error -> assertThat(error.getErrorCode()).isEqualTo(13));
            }
            AtomicInteger ready = new AtomicInteger();
            Throwable failure = catchThrowable(() -> {
                try (var unexpected = start(uri(user, metadata), metadata, ready)) {
                    throw new AssertionError("Cloud startup accepted a read-only deployment database");
                }
            });
            assertThat(ready.get()).isZero();
            Throwable coded = failure;
            for (int depth = 0; depth < 16 && coded != null && !(coded instanceof TapstateException); depth++) coded = coded.getCause();
            assertThat(coded).isInstanceOfSatisfying(TapstateException.class, error -> {
                assertThat(error.code()).isEqualTo(StoreError.DATABASE_ACCESS_FAILED);
                assertThat(error.args()).containsExactlyEntriesOf(java.util.Map.of("database", denied));
                assertThat(error.getCause()).isNull();
            });
            assertNoProbes(admin, metadata);
        }
    }

    private static void assertNoProbes(MongoClient admin, String metadata) {
        for (String database : List.of(metadata, metadata + "_operator", metadata + "_views")) {
            assertThat(admin.getDatabase(database).listCollectionNames().into(new ArrayList<>()))
                    .noneMatch(name -> name.startsWith("tapstate_access_probe_"));
        }
    }

    @ParameterizedTest
    @EnumSource(Missing.class)
    void missingDerivedDatabaseRightsCannotReportReady(Missing missing) {
        String suffix = UUID.randomUUID().toString().substring(0, 8);
        String metadata = "isolation_missing_" + suffix;
        String denied = metadata + (missing == Missing.OPERATOR ? "_operator" : "_views");
        String allowed = metadata + (missing == Missing.OPERATOR ? "_views" : "_operator");
        String user = "user_missing_" + suffix;
        try (MongoClient admin = MongoClients.create(uri("root", "admin"))) {
            createUser(admin, user, List.of(metadata, allowed));
            try (MongoClient limited = MongoClients.create(uri(user, metadata))) { assertDenied(limited, denied); }
            AtomicInteger ready = new AtomicInteger();
            AtomicReference<ConfigurableApplicationContext> started = new AtomicReference<>();
            Throwable failure = catchThrowable(() -> started.set(start(uri(user, metadata), metadata, ready)));
            if (started.get() != null) started.get().close();
            assertThat(failure).as("Cloud startup refuses the missing %s grant before reporting ready", missing).isNotNull();
            assertThat(ready.get()).as("no ApplicationReadyEvent was published").isZero();
            Throwable coded = failure;
            for (int depth = 0; depth < 16 && !(coded instanceof TapstateException) && coded != null; depth++) {
                coded = coded.getCause();
            }
            assertThat(coded).as("a storage authorization failure is diagnosable with a canonical code")
                    .isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code()).isEqualTo(StoreError.DATABASE_ACCESS_FAILED);
                        assertThat(error.args()).containsExactlyEntriesOf(java.util.Map.of("database", denied));
                        assertThat(error.getCause()).isNull();
                    });
            for (String database : List.of(metadata, allowed, denied)) {
                assertThat(admin.getDatabase(database).listCollectionNames().into(new ArrayList<>()))
                        .noneMatch(name -> name.startsWith("tapstate_access_probe_"));
            }
        }
    }

    private ConfigurableApplicationContext start(String metadataUri, String clusterId, AtomicInteger ready, String... overrides) {
        List<String> args = new ArrayList<>(List.of("--server.address=127.0.0.1", "--server.port=0",
                "--tapstate.hz.member-port=0", "--tapstate.hz.jet.cooperative-thread-count=2", "--logging.level.root=ERROR",
                "--SDK_STATUS_SENDER_ENABLED=false", "--tapstate.store.mongo.server-selection-timeout=2s",
                "--tapstate.connectors.plugins-dir=" + work.resolve(clusterId),
                "--tapstate.connectors.seed-dir=" + CloudConnectorTestInputs.seedDirectory(),
                "--tapstate.cloud.base-url=https://cloud.example.invalid", "--tapstate.cloud.token=controlled-sdk-token",
                "--tapstate.cloud.cluster-id=" + clusterId, "--tapstate.cloud.atlas-uri=" + metadataUri));
        args.addAll(List.of(overrides));
        return new SpringApplicationBuilder(Bootstrap.class).environment(CloudFixtureEnvironment.isolated())
                .listeners((ApplicationListener<ApplicationReadyEvent>) event -> ready.incrementAndGet())
                .run(args.toArray(String[]::new));
    }

    private static void createUser(MongoClient admin, String user, List<String> databases) {
        admin.getDatabase("admin").runCommand(new Document("createUser", user).append("pwd", PASSWORD)
                .append("roles", databases.stream().map(database -> new Document("role", "readWrite").append("db", database)).toList()));
    }

    private static void assertDenied(MongoClient client, String database) {
        assertThatThrownBy(() -> client.getDatabase(database).getCollection("proof").countDocuments())
                .isInstanceOfSatisfying(MongoCommandException.class, failure -> assertThat(failure.getErrorCode()).isEqualTo(13));
    }

    private static String uri(String user, String database) {
        return uri(user, database, PASSWORD);
    }

    private static String uri(String user, String database, String password) {
        return "mongodb://" + user + ":" + password + "@" + MONGO.getHost() + ":" + MONGO.getMappedPort(27017)
                + "/" + database + "?replicaSet=" + REPLICA_SET + "&directConnection=true&authSource=admin";
    }
}
