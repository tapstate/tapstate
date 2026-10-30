package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoConfigurationBootstrap;
import io.tapstate.adapters.mongostore.MongoConfigurationDomain;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.control.core.ControlError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.testsupport.RequiresDocker;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

/** Real scoped session reads retain their deadline and final authority checks with slow healthy IO. */
@RequiresDocker
class StorageAuthenticationLatencyIT {
    enum Change { NONE, AUTH_GENERATION, REVOCATION, EPOCH }
    enum Damage { MISSING_REVOCATIONS, TTL_INDEX }

    @ParameterizedTest
    @EnumSource(Damage.class)
    void damagedAuthenticationStructuresCannotPublishEitherAValidOrMissingSession(Damage damage) {
        withCloud(Change.NONE, fixture -> {
            if (damage == Damage.MISSING_REVOCATIONS) {
                SystemCollections.CLOUD_REVOCATIONS.on(fixture.raw.getDatabase(fixture.cacheDatabase)).drop();
            } else {
                SystemCollections.CLOUD_SESSIONS.on(fixture.raw.getDatabase(fixture.cacheDatabase)).createIndex(
                        new Document("unexpectedExpiry", 1), new com.mongodb.client.model.IndexOptions()
                                .expireAfter(1L, java.util.concurrent.TimeUnit.SECONDS));
            }
            for (String cookie : List.of(fixture.cookie, "tcs_anRp.not-a-real-secret")) {
                assertThatThrownBy(() -> fixture.authentication.credentials().authenticateContext(cookie))
                        .isInstanceOfSatisfying(TapstateException.class,
                                failure -> assertThat(failure.code()).isEqualTo(ControlError.STORAGE_UNAVAILABLE));
            }
        });
    }

    @Test
    void aReadOnlyStorageSnapshotDoesNotRepeatAuthorizationForEachConfigurationRead() {
        withCloud(Change.NONE, fixture -> {
            try (var root = new StorageRoot(new MongoProperties(), fixture.cloud, fixture.bridge, BootId.fresh(),
                    new MetricsHistoryProperties(), new RandomTokenSecrets(), Clock.systemUTC(), new byte[32])) {
                root.loadExisting(ignored -> { throw StorageRuntime.unavailable(); });
                assertThat(root.phase()).isEqualTo(StorageRoot.Phase.AUTH_READY);
                var proof = root.authentication().credentials().authenticateContext(fixture.cookie).orElseThrow();
                var setup = root.runtime().requireConfiguration().setupStates().load().scope();
                var caller = io.tapstate.control.core.StorageControlAccess.ordinary(StorageBinding.Mode.CLOUD,
                        setup, fixture.scope, io.tapstate.control.core.TapstatePrincipal.cloudSession(proof.token()),
                        "organization", proof.requireActive());
                var admin = fixture.raw.getDatabase("admin");
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "alwaysOn")
                        .append("data", new Document("failCommands", List.of("find"))
                                .append("namespace", fixture.cacheDatabase + "." + SystemCollections.CACHE_IDENTITY.collectionName())
                                .append("blockConnection", true).append("blockTimeMS", 600)));
                try {
                    org.junit.jupiter.api.Assertions.assertTimeout(Duration.ofSeconds(8), () -> {
                        var snapshot = root.snapshot(caller, proof.requireActive());
                        assertThat(snapshot.clusterId()).isEqualTo(fixture.scope.clusterId());
                        assertThat(snapshot.businessPhase()).isEqualTo(io.tapstate.control.core.StorageControlDispatcher.BusinessPhase.AUTH_READY);
                    });
                } finally {
                    admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                }
            }
        });
    }

    @Test
    void healthyAuthenticationDoesNotRepeatTheIndexCensusAroundItsFencedSessionRead() {
        withCloud(Change.NONE, fixture -> {
            var admin = fixture.raw.getDatabase("admin");
            admin.runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", "alwaysOn").append("data", new Document("failCommands", List.of("listIndexes"))
                            .append("blockConnection", true).append("blockTimeMS", 2600)));
            try {
                var proof = fixture.authentication.credentials().authenticateContext(fixture.cookie).orElseThrow();
                assertThat(proof.storageScope()).contains(fixture.scope);
                assertThat(proof.organizationId()).contains("organization");
            } finally {
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
        });
    }

    @Test
    void healthyCloudAuthenticationDoesNotRepeatAuthorityBetweenItsSessionReads() {
        withCloud(Change.NONE, fixture -> {
            var admin = fixture.raw.getDatabase("admin");
            String namespace = fixture.cacheDatabase + "." + SystemCollections.CACHE_IDENTITY.collectionName();
            admin.runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", "alwaysOn").append("data", new Document("failCommands", List.of("find"))
                            .append("namespace", namespace).append("blockConnection", true).append("blockTimeMS", 2300)));
            try {
                var proof = fixture.authentication.credentials().authenticateContext(fixture.cookie).orElseThrow();
                assertThat(proof.organizationId()).contains("organization");
            } finally {
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
        });
    }

    @Test
    void healthyCloudRevalidationDoesNotRepeatAdjacentAuthorityChecks() {
        withCloud(Change.NONE, fixture -> {
            var proof = fixture.authentication.credentials().authenticateContext(fixture.cookie).orElseThrow();
            var admin = fixture.raw.getDatabase("admin");
            String namespace = fixture.cacheDatabase + "." + SystemCollections.CACHE_IDENTITY.collectionName();
            admin.runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", "alwaysOn").append("data", new Document("failCommands", List.of("find"))
                            .append("namespace", namespace).append("blockConnection", true).append("blockTimeMS", 2300)));
            try {
                proof.requireActive().run();
            } finally {
                admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
            }
            assertThat(fixture.identity().getLong("authGeneration")).isEqualTo(fixture.scope.authGeneration());
        });
    }

    @ParameterizedTest
    @EnumSource(value = Change.class, names = {"AUTH_GENERATION", "REVOCATION", "EPOCH"})
    void aReadOnlySnapshotCannotPublishAfterItsOriginalAuthorityChanges(Change change) {
        withCloud(Change.NONE, fixture -> {
            try (var root = new StorageRoot(new MongoProperties(), fixture.cloud, fixture.bridge, BootId.fresh(),
                    new MetricsHistoryProperties(), new RandomTokenSecrets(), Clock.systemUTC(), new byte[32])) {
                root.loadExisting(ignored -> { throw StorageRuntime.unavailable(); });
                var proof = root.authentication().credentials().authenticateContext(fixture.cookie).orElseThrow();
                var setup = root.runtime().requireConfiguration().setupStates().load().scope();
                var caller = io.tapstate.control.core.StorageControlAccess.ordinary(StorageBinding.Mode.CLOUD,
                        setup, fixture.scope, io.tapstate.control.core.TapstatePrincipal.cloudSession(proof.token()),
                        "organization", proof.requireActive());
                var checked = new AtomicBoolean();
                Runnable active = () -> {
                    if (checked.getAndSet(true)) {
                        switch (change) {
                            case AUTH_GENERATION -> SystemCollections.CACHE_IDENTITY.on(fixture.raw.getDatabase(fixture.cacheDatabase))
                                    .updateOne(new Document("_id", fixture.identity().get("_id")),
                                            new Document("$set", new Document("authGeneration", fixture.scope.authGeneration() + 1)));
                            case REVOCATION -> root.managedAuthentication().service().invalidateJwt("jti");
                            case EPOCH -> root.authentication().invalidateFormal();
                            case NONE -> throw new AssertionError("a change is required");
                        }
                    }
                    proof.requireActive().run();
                };
                assertThatThrownBy(() -> root.snapshot(caller, active)).isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(change == Change.AUTH_GENERATION
                                ? ControlError.STORAGE_UNAVAILABLE : ControlError.UNAUTHENTICATED));
                assertThat(checked).isTrue();
            }
        });
    }

    @Test
    void healthyCloudSessionReadsRetainTheirDeadlineWithSlowRoundTrips() {
        withCloud(Change.NONE, fixture -> {
            var admin = fixture.raw.getDatabase("admin");
            String namespace = fixture.cacheDatabase + "." + SystemCollections.CACHE_IDENTITY.collectionName();
            long before = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand")
                    .append("mode", "alwaysOn").append("data", new Document("failCommands", List.of("find"))
                            .append("namespace", namespace).append("blockConnection", true).append("blockTimeMS", 1300)))
                    .get("count")).longValue();
            try {
                var proof = fixture.authentication.credentials().authenticateContext(fixture.cookie).orElseThrow();
                assertThat(proof.storageScope()).contains(fixture.scope);
                assertThat(proof.organizationId()).contains("organization");
                // Revalidation is another bounded operation, not a reuse of the first request's deadline.
                proof.requireActive().run();
            } finally {
                long after = ((Number) admin.runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", "off")).get("count")).longValue();
                assertThat(after - before).isGreaterThanOrEqualTo(2);
            }
            assertThat(fixture.identity().getLong("authGeneration")).isEqualTo(fixture.scope.authGeneration());
        });
    }

    @ParameterizedTest
    @EnumSource(value = Change.class, names = {"AUTH_GENERATION", "EPOCH"})
    void anIssuedProofCannotRevalidateAfterItsAuthorityChanges(Change change) {
        withCloud(change, fixture -> {
            var proof = fixture.authentication.credentials().authenticateContext(fixture.cookie).orElseThrow();
            fixture.arm();
            assertThatThrownBy(() -> proof.requireActive().run())
                    .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(
                            change == Change.AUTH_GENERATION ? ControlError.STORAGE_UNAVAILABLE : ControlError.UNAUTHENTICATED));
            assertThat(fixture.changed).isTrue();
        });
    }

    @Test
    void anIssuedProofCannotRevalidateAfterItsSessionIsRevoked() {
        withCloud(Change.NONE, fixture -> {
            var proof = fixture.authentication.credentials().authenticateContext(fixture.cookie).orElseThrow();
            fixture.authentication.managed().service().invalidateJwt("jti");
            assertThat(SystemCollections.CLOUD_REVOCATIONS.on(fixture.raw.getDatabase(fixture.cacheDatabase))
                    .countDocuments()).isEqualTo(1);
            assertThatThrownBy(() -> proof.requireActive().run())
                    .isInstanceOfSatisfying(TapstateException.class, failure ->
                            assertThat(failure.code()).isEqualTo(ControlError.UNAUTHENTICATED));
        });
    }

    @ParameterizedTest
    @EnumSource(value = Change.class, names = {"AUTH_GENERATION", "REVOCATION", "EPOCH"})
    void aChangeAfterTheRealSessionReadCannotAuthorizeTheOriginalCredential(Change change) {
        withCloud(change, fixture -> {
            fixture.arm();
            if (change == Change.REVOCATION) {
                assertThat(fixture.authentication.credentials().authenticateContext(fixture.cookie)).isEmpty();
                assertThat(SystemCollections.CLOUD_REVOCATIONS.on(fixture.raw.getDatabase(fixture.cacheDatabase))
                        .countDocuments()).isEqualTo(1);
            } else {
                assertThatThrownBy(() -> fixture.authentication.credentials().authenticateContext(fixture.cookie))
                        .isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(
                                change == Change.AUTH_GENERATION ? ControlError.STORAGE_UNAVAILABLE : ControlError.UNAUTHENTICATED));
            }
            assertThat(fixture.changed).isTrue();
            if (change == Change.AUTH_GENERATION)
                assertThat(fixture.identity().getLong("authGeneration")).isEqualTo(fixture.scope.authGeneration() + 1);
        });
    }

    private static void withCloud(Change change, Consumer<Fixture> scenario) {
        String cluster = "cloud-auth-" + UUID.randomUUID();
        try (var mongo = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
                .withCommand("--replSet", "docker-rs", "--bind_ip_all", "--setParameter", "enableTestCommands=1")) {
            mongo.start();
            String cacheDatabase = "authentication_cache";
            var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                    mongo.getReplicaSetUrl(cacheDatabase), cacheDatabase, null);
            try (MongoClient raw = MongoClients.create(mongo.getReplicaSetUrl());
                 MongoConfigurationDomain configuration = MongoConfigurationBootstrap.prepare(
                         new MongoConnectionSettings(mongo.getReplicaSetUrl("configuration"), null, Duration.ofSeconds(5)),
                         cluster, "installation", "setup", 1)) {
                var setup = new StorageSetupScope(cluster, "installation", "setup", 1);
                var target = new SourceResource(StorageBinding.defaultSourceId(StorageBinding.Mode.CLOUD), null, "mongodb",
                        Map.of("isUri", true, "uri", mongo.getReplicaSetUrl("master")), null, null, null, null);
                var initial = configuration.bindings().commitWithFirstCache(setup, StorageBinding.Mode.CLOUD, binding, target);
                try (var cache = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "authentication-latency-preparation")) {
                    cache.prepareFirstAuth(initial.firstCreation().orElseThrow());
                    cache.prepareInitialBusiness(initial.preparation(), () -> {});
                }
                configuration.bindings().complete(setup);
                var now = new AtomicReference<>(Instant.now());
                var clock = new Clock() {
                    @Override public ZoneId getZone() { return ZoneOffset.UTC; }
                    @Override public Clock withZone(ZoneId zone) { return Clock.fixed(instant(), zone); }
                    @Override public Instant instant() { return now.get(); }
                };
                var properties = new CloudProperties();
                properties.setBaseUrl("https://cloud.example"); properties.setClusterId(cluster);
                properties.setAtlasUri(mongo.getReplicaSetUrl("configuration")); properties.setToken("fixture-runtime-token");
                var cloud = CloudRuntimeSettings.resolve(properties);
                var bridge = new CloudSdkBridge(cloud, new Provider(cluster, clock));
                var fixtureReference = new AtomicReference<Fixture>();
                try (var runtime = new StorageRuntime(configuration,
                        selected -> MongoCacheDomain.connect(selected.cache(), Duration.ofSeconds(5), "authentication-latency"),
                        () -> { var fixture = fixtureReference.get(); if (fixture != null) fixture.afterRead(); });
                     var authentication = new StorageAuthentication(configuration, cloud,
                             bridge, new RandomTokenSecrets(), clock, new byte[32])) {
                    var bound = runtime.loadBound().orElseThrow(); authentication.install(bound);
                    var fixture = new Fixture(raw, cacheDatabase, authentication, initial.preparation().authScope(), change, now, cloud, bridge);
                    fixtureReference.set(fixture);
                    fixture.cookie = authentication.managed().service().exchangeCode("fresh", "cluster.example").token();
                    scenario.accept(fixture);
                }
            }
        }
    }

    private static final class Fixture {
        final MongoClient raw;
        final String cacheDatabase;
        final StorageAuthentication authentication;
        final CacheAuthScope scope;
        final Change change;
        final CloudRuntimeSettings cloud;
        final CloudSdkBridge bridge;
        final AtomicReference<Instant> now;
        final AtomicBoolean changed = new AtomicBoolean();
        String cookie;
        boolean armed;
        long previousUse;
        Fixture(MongoClient raw, String cacheDatabase, StorageAuthentication authentication,
                CacheAuthScope scope, Change change, AtomicReference<Instant> now, CloudRuntimeSettings cloud, CloudSdkBridge bridge) {
            this.raw = raw; this.cacheDatabase = cacheDatabase; this.authentication = authentication;
            this.scope = scope; this.change = change; this.now = now;
            this.cloud = cloud; this.bridge = bridge;
        }
        Document identity() { return SystemCollections.CACHE_IDENTITY.on(raw.getDatabase(cacheDatabase)).find().first(); }
        Document session() { return SystemCollections.CLOUD_SESSIONS.on(raw.getDatabase(cacheDatabase)).find().first(); }
        void arm() {
            previousUse = session().getLong("lastUsedAt");
            now.updateAndGet(value -> value.plusSeconds(1)); armed = true;
        }
        void afterRead() {
            if (!armed || changed.get()) return;
            var used = session();
            if (used.getLong("lastUsedAt") <= previousUse || !changed.compareAndSet(false, true)) return;
            // The lifecycle callback runs after the real session transaction has committed its idle update.
            switch (change) {
                case AUTH_GENERATION -> SystemCollections.CACHE_IDENTITY.on(raw.getDatabase(cacheDatabase)).updateOne(
                        new Document("_id", identity().get("_id")),
                        new Document("$set", new Document("authGeneration", scope.authGeneration() + 1)));
                case REVOCATION -> {
                    var denial = new Document("_id", used.get("_id")).append("formatVersion", 1);
                    for (String field : List.of("issuer", "organizationId", "clusterId", "jwtId")) denial.put(field, used.get(field));
                    denial.put("revokedAt", now.get().toEpochMilli());
                    SystemCollections.CLOUD_REVOCATIONS.on(raw.getDatabase(cacheDatabase)).insertOne(denial);
                }
                case EPOCH -> authentication.invalidateFormal();
                case NONE -> throw new AssertionError("no change was requested");
            }
        }
    }

    private record Provider(String cluster, Clock clock) implements CloudSdkBridge.SdkRuntime {
        @Override public CloudSdkBridge.ExchangeResult exchange(String code) {
            return new CloudSdkBridge.ExchangeResult("fixture-jwt", "organization", cluster, "Org", "Cluster", "local");
        }
        @Override public CloudSdkBridge.VerifiedClaims verify(String jwt) {
            return new CloudSdkBridge.VerifiedClaims("user", "organization", cluster, "jti", clock.instant().plusSeconds(600),
                    "https://cloud.example", "cluster.example", List.of("workload:write"));
        }
        @Override public io.tapstate.cloud.sdk.client.ClusterEgressAddress currentEgressAddress() {
            return new io.tapstate.cloud.sdk.client.ClusterEgressAddress("organization", cluster, null, "PROVISIONING");
        }
        @Override public boolean verifyCallback(String method, String time, String nonce, String data, String signature) { return false; }
        @Override public void report(String cluster, String nonce, io.tapstate.control.core.CloudRuntimeStatus status, String token) {
            throw new AssertionError("no reporting in this authentication witness");
        }
    }
}
