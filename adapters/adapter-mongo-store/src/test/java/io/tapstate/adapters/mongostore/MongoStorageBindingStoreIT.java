package io.tapstate.adapters.mongostore;

import com.mongodb.MongoWriteException;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.SourceMode;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RequiredNoteOwner;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.StorageSetupScope;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real configuration transactions, independent clients and load-only restart witnesses. */
@RequiresDocker
class MongoStorageBindingStoreIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final StorageSetupScope SCOPE = new StorageSetupScope("cluster-a", "installation-a", "setup-a", 1);
    private static final StorageBinding.Connection CACHE = new StorageBinding.Connection(
            StorageBinding.Connection.Kind.MONGODB, "mongodb://cache-user:cache-secret@cache.invalid/cache?tls=true",
            "cache", "CA-material-sentinel");

    @Test
    void committedFirstCacheInvocationKeepsItsOperationOwnerWithoutSubmissionChecks() {
        withPrepared(f -> {
            var owner = new java.util.concurrent.atomic.AtomicBoolean(true);
            var checks = new AtomicInteger();
            var cache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                    MONGO.getReplicaSetUrl("late_cache_" + UUID.randomUUID().toString().replace("-", "")),
                    "cache_" + UUID.randomUUID().toString().replace("-", ""), null);
            Runnable lifetime = () -> { if (!owner.get()) throw new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null); };
            Runnable submission = () -> {
                checks.incrementAndGet();
                if (SystemCollections.STORAGE_BINDING.on(f.database).find().first() != null)
                    throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
            };
            var committed = f.domain.bindings().guardedBy(lifetime).commitWithFirstCache(
                    SCOPE, StorageBinding.Mode.OP, cache, source("views", "secret"), submission);
            int originalChecks = checks.get();
            assertThat(originalChecks).isGreaterThanOrEqualTo(2);
            var invocation = committed.firstCreation().orElseThrow();
            f.domain.setupStates().rotate(SCOPE, io.tapstate.spi.store.StorageSetupStateStore.Purpose.SETUP);
            invocation.requireActive();
            try (var connected = MongoCacheDomain.connect(cache, Duration.ofSeconds(5), null)) {
                assertThat(connected.prepareFirstAuth(invocation)).isEqualTo(committed.preparation().authScope());
                connected.loadAuthScope(committed.preparation().authScope());
            }
            assertThat(checks.get()).isEqualTo(originalChecks);
            owner.set(false);
            assertThatThrownBy(invocation::requireActive).isInstanceOf(TapstateException.class);
        });
    }

    @Test
    void losingSubmissionAuthorizationAfterTheWriteRollsBackTheWholeBinding() {
        withPrepared(f -> {
            var checks = new AtomicInteger();
            assertThatThrownBy(() -> f.domain.bindings().commitWithFirstCache(SCOPE, StorageBinding.Mode.OP, CACHE,
                    source("views", "secret"), () -> {
                        if (checks.incrementAndGet() == 2) throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
                    })).isInstanceOf(TapstateException.class);
            assertThat(f.domain.bindings().load()).isEmpty();
            assertThat(SystemCollections.SOURCES.on(f.database).find().first()).isNull();
            assertThat(SystemCollections.STORAGE_RECOVERY.on(f.database).find().first()).isNull();
            assertThat(f.revision()).isZero();
        });
    }

    @Test
    void setupGenerationReissueRevokesOldWritesAndPreservesTheOriginalBinding() {
        withPrepared(f -> {
            var original = f.domain.setupStates().load();
            assertThat(original.scope()).isEqualTo(SCOPE);
            assertThat(original.phase()).isEqualTo(io.tapstate.spi.store.StorageSetupStateStore.Phase.CONFIG_PREPARED);
            var master = source("views", "secret");
            f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, master);
            var bound = f.domain.bindings().load().orElseThrow();
            var next = f.domain.setupStates().rotate(SCOPE, io.tapstate.spi.store.StorageSetupStateStore.Purpose.SETUP);
            assertThat(next.scope().generation()).isEqualTo(2);
            assertThatThrownBy(() -> f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, master))
                    .isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> f.domain.bindings().complete(SCOPE)).isInstanceOf(TapstateException.class);
            assertThat(f.domain.bindings().commit(next.scope(), StorageBinding.Mode.OP, CACHE, master)).isEqualTo(bound);
            f.domain.bindings().complete(next.scope());
            assertThatThrownBy(() -> f.domain.setupStates().rotate(next.scope(), io.tapstate.spi.store.StorageSetupStateStore.Purpose.SETUP))
                    .isInstanceOf(TapstateException.class);
            var first = f.domain.setupStates().rotate(next.scope(), io.tapstate.spi.store.StorageSetupStateStore.Purpose.FIRST_ADMIN);
            assertThat(first.scope().generation()).isEqualTo(3);
            assertThat(f.domain.firstAdmin().load().orElseThrow().scope()).isEqualTo(first.scope());
            assertThatThrownBy(() -> f.domain.firstAdmin().create(next.scope(), new io.tapstate.spi.store.User("old", "hash", "admin")))
                    .isInstanceOf(TapstateException.class);
            f.domain.firstAdmin().create(first.scope(), new io.tapstate.spi.store.User("admin", "hash", "admin"));
            assertThatThrownBy(() -> f.domain.setupStates().rotate(first.scope(), io.tapstate.spi.store.StorageSetupStateStore.Purpose.FIRST_ADMIN))
                    .isInstanceOf(TapstateException.class);
            assertThat(f.domain.bindings().load()).contains(bound);
        });
    }

    @Test
    void independentSetupIssuersCompeteOnTheSameConfigurationGeneration() throws Exception {
        withPrepared(f -> {
            try (var other = MongoConfigurationDomain.open(f.settings, SCOPE.clusterId()); var threads = Executors.newFixedThreadPool(2)) {
                var start = new CountDownLatch(1);
                var results = new java.util.ArrayList<java.util.concurrent.Future<Boolean>>();
                for (var states : List.of(f.domain.setupStates(), other.setupStates())) results.add(threads.submit(() -> {
                    start.await();
                    try { states.rotate(SCOPE, io.tapstate.spi.store.StorageSetupStateStore.Purpose.SETUP); return true; }
                    catch (TapstateException refused) { return false; }
                }));
                start.countDown();
                int winners = 0;
                for (var result : results) try { if (result.get(10, TimeUnit.SECONDS)) winners++; }
                catch (Exception failure) { throw new AssertionError(failure); }
                assertThat(winners).isEqualTo(1);
                assertThat(f.domain.setupStates().load().scope().generation()).isEqualTo(2);
                assertThat(f.revision()).isEqualTo(1);
            }
        });
    }

    @Test
    void bindingAllocatesOneOriginalCacheIdentityAcrossReplayCompletionAndRestart() {
        for (var mode : StorageBinding.Mode.values()) {
            withPrepared(f -> {
                assertThat(f.domain.bindings().cachePreparation()).isEmpty();
                var master = source(StorageBinding.defaultSourceId(mode), "secret");
                f.domain.bindings().commit(SCOPE, mode, CACHE, master);
                var original = f.domain.bindings().cachePreparation().orElseThrow();
                assertThat(original.operationId()).isEqualTo(SCOPE.operationId());
                assertThat(original.authScope().clusterId()).isEqualTo(SCOPE.clusterId());
                assertThat(original.authScope().mode()).isEqualTo(mode);
                assertThat(original.authScope().bindingVersion()).isEqualTo(1);
                assertThat(original.authScope().authGeneration()).isEqualTo(1);
                assertThat(original.businessScope().cacheIncarnation()).isEqualTo(original.authScope().cacheIncarnation());
                assertThat(UUID.fromString(original.authScope().cacheIncarnation()).toString())
                        .isEqualTo(original.authScope().cacheIncarnation());
                Document stored = SystemCollections.STORAGE_RECOVERY.on(f.database).find().first();
                assertThat(stored).containsEntry("phase", "INITIAL_PREPARATION").containsEntry("revision", 0L);
                f.domain.bindings().commit(SCOPE, mode, CACHE, master);
                f.domain.bindings().complete(SCOPE);
                try (var second = MongoConfigurationDomain.open(f.settings, SCOPE.clusterId())) {
                    assertThat(second.bindings().cachePreparation()).contains(original);
                    assertThat(second.bindings().commit(SCOPE, mode, CACHE, master)).isEqualTo(f.domain.bindings().load().orElseThrow());
                }
                assertThat(SystemCollections.STORAGE_RECOVERY.on(f.database).countDocuments()).isEqualTo(1);
                assertThat(SystemCollections.STORAGE_RECOVERY.on(f.database).find().first()).isEqualTo(stored);
                assertThat(f.revision()).isEqualTo(2);
                assertThat(original.toString()).doesNotContain("cache-secret", "master-secret", "CA-material-sentinel");
            });
        }
    }

    @Test
    void missingDamagedOrUnrelatedRecoveryFactsRefuseWithoutAllocatingAnotherIdentity() {
        withPrepared(f -> {
            f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "secret"));
            Document original = SystemCollections.STORAGE_RECOVERY.on(f.database).find().first();
            var faults = new java.util.ArrayList<Document>();
            for (var field : List.of("formatVersion", "layoutId", "clusterId", "bindingVersion", "recoveryId", "phase", "revision", "authGeneration", "cacheIncarnation")) {
                var missing = new Document(original); missing.remove(field); faults.add(missing);
            }
            faults.add(new Document(original).append("formatVersion", 2));
            faults.add(new Document(original).append("bindingVersion", 2L));
            faults.add(new Document(original).append("clusterId", "another-cluster"));
            faults.add(new Document(original).append("recoveryId", "another-operation"));
            faults.add(new Document(original).append("authGeneration", 2L));
            faults.add(new Document(original).append("cacheIncarnation", "not-a-uuid"));
            faults.add(new Document(original).append("revision", 0));
            faults.add(new Document(original).append("_id", "another-row"));
            faults.add(null);
            for (Document fault : faults) {
                SystemCollections.STORAGE_RECOVERY.on(f.database).deleteMany(new Document());
                if (fault != null) SystemCollections.STORAGE_RECOVERY.on(f.database).insertOne(fault);
                assertThatThrownBy(() -> f.domain.bindings().cachePreparation()).satisfies(MongoStorageBindingStoreIT::unreadable);
                assertThatThrownBy(() -> f.domain.bindings().load()).satisfies(MongoStorageBindingStoreIT::unreadable);
                assertThatThrownBy(() -> f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "secret")))
                        .satisfies(MongoStorageBindingStoreIT::unreadable);
                assertThatThrownBy(() -> MongoConfigurationDomain.open(f.settings, SCOPE.clusterId())).satisfies(MongoStorageBindingStoreIT::unreadable);
                assertThat(SystemCollections.STORAGE_RECOVERY.on(f.database).countDocuments()).isEqualTo(fault == null ? 0 : 1);
                if (fault != null) assertThat(SystemCollections.STORAGE_RECOVERY.on(f.database).find().first()).isEqualTo(fault);
                assertThat(f.revision()).isEqualTo(1);
            }
            SystemCollections.STORAGE_RECOVERY.on(f.database).insertOne(original);
            SystemCollections.STORAGE_RECOVERY.on(f.database).insertOne(new Document("_id", "unknown"));
            assertThatThrownBy(() -> f.domain.bindings().cachePreparation()).satisfies(MongoStorageBindingStoreIT::unreadable);
        });
    }

    @Test
    void unboundConfigurationDoesNotAdoptAnOrphanRecoveryRecord() {
        withPrepared(f -> {
            Document orphan = new Document("_id", "cluster").append("cacheIncarnation", UUID.randomUUID().toString());
            SystemCollections.STORAGE_RECOVERY.on(f.database).insertOne(orphan);
            assertThatThrownBy(() -> f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "secret")))
                    .satisfies(MongoStorageBindingStoreIT::unreadable);
            assertThat(SystemCollections.STORAGE_RECOVERY.on(f.database).find().first()).isEqualTo(orphan);
            assertThat(f.revision()).isZero();
            assertThat(SystemCollections.STORAGE_BINDING.on(f.database).countDocuments()).isZero();
            assertThat(SystemCollections.SOURCES.on(f.database).countDocuments()).isZero();
        });
    }

    @Test
    void originalConfigurationScopePreparesOnlyAuthenticationOnItsSeparateCacheClient() {
        withPrepared(f -> {
            String cacheName = f.database.getName() + "_cache";
            var actualCache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                    MONGO.getReplicaSetUrl(cacheName), cacheName, null);
            try {
                f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, actualCache, source("views", "secret"));
                var preparation = f.domain.bindings().cachePreparation().orElseThrow();
                try (var cache = MongoCacheDomain.connect(actualCache, Duration.ofSeconds(5), "cache-preparation-test")) {
                    assertThatThrownBy(() -> cache.loadAuthScope(preparation.authScope())).isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(IoError.CACHE_AUTH_FENCED));
                    cache.prepareAuth(preparation.authScope(), preparation.operationId(), () -> {});
                    assertThat(cache.loadAuthScope(preparation.authScope())).isEqualTo(preparation.authScope());
                }
                try (var config = MongoConfigurationDomain.open(f.settings, SCOPE.clusterId());
                     var cache = MongoCacheDomain.connect(config.bindings().load().orElseThrow().cache(), Duration.ofSeconds(5), "cache-reopen-test")) {
                    assertThat(config.bindings().cachePreparation()).contains(preparation);
                    assertThat(cache.loadAuthScope(preparation.authScope())).isEqualTo(preparation.authScope());
                }
                var cacheDatabase = f.admin.getDatabase(cacheName);
                assertThat(SystemCollections.CACHE_IDENTITY.on(cacheDatabase).find().first())
                        .containsEntry("businessPhase", "ABSENT").containsEntry("cacheIncarnation", preparation.authScope().cacheIncarnation());
                assertThat(cacheDatabase.listCollectionNames().into(new java.util.ArrayList<>()))
                        .containsExactlyInAnyOrder("cache_identity", "schema_migrations", "op_sessions");
                assertThat(SystemCollections.STORAGE_RECOVERY.on(f.database).countDocuments()).isEqualTo(1);
            } finally { f.admin.getDatabase(cacheName).drop(); }
        });
    }

    @Test
    void eachReusableDefinitionKindReservesTheDefaultSourceIdBeforeBinding() {
        for (String kind : List.of("transform", "view", "serve")) {
            withPrepared(f -> {
                var occupied = definition(kind, "views");
                f.domain.artifacts().create(occupied);
                assertThatThrownBy(() -> f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "secret")))
                        .satisfies(MongoStorageBindingStoreIT::refused);
                assertThat(f.domain.artifacts().get("views")).contains(occupied);
                assertThat(f.domain.bindings().load()).isEmpty();
                assertThat(f.revision()).isEqualTo(1);
                assertThat(SystemCollections.SOURCES.on(f.database).countDocuments()).isZero();
                assertThat(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(f.database).find().first()).containsEntry("bindingVersion", 0L);
            });
        }
    }

    @Test
    void definitionsCannotReplaceABoundDefaultSourceAndDuplicateAliasesMakeLoadingRefuse() {
        withPrepared(f -> {
            var master = source("views", "secret");
            f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, master);
            for (String kind : List.of("transform", "view", "serve")) {
                var replacement = definition(kind, "views");
                assertThat(f.domain.artifacts().create(replacement)).isEqualTo(io.tapstate.spi.store.ArtifactMutation.ALREADY_EXISTS);
                assertThatThrownBy(() -> f.domain.artifacts().replace("views", CanonicalHash.of(master), replacement))
                        .satisfies(MongoStorageBindingStoreIT::protectedSource);
                assertThat(f.revision()).isEqualTo(1);
                SystemCollections table = switch (kind) {
                    case "transform" -> SystemCollections.TRANSFORM_DEFINITIONS;
                    case "view" -> SystemCollections.VIEW_DEFINITIONS;
                    case "serve" -> SystemCollections.SERVE_DEFINITIONS;
                    default -> throw new AssertionError("kind");
                };
                table.on(f.database).insertOne(MongoArtifactStore.toDocument(replacement));
                assertThatThrownBy(() -> f.domain.bindings().load()).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
                assertThat(table.on(f.database).countDocuments()).isEqualTo(1);
                table.on(f.database).deleteMany(new Document());
            }
            assertThat(f.domain.bindings().load()).isPresent();
            assertThat(f.domain.artifacts().get("views")).contains(master);
        });
    }

    @Test
    void publishesAllBindingFactsAndReopensEncryptedInputsWithoutACacheClient() {
        withPrepared(f -> {
            assertThat(f.domain.bindings().load()).isEmpty();
            StorageBinding bound = f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "master-secret"));
            assertThat(bound.bindingVersion()).isEqualTo(1);
            assertThat(bound.cache()).isEqualTo(CACHE);
            assertThat(f.domain.bindings().load()).contains(bound);
            assertThat(bound.toString()).doesNotContain("cache-secret", "cache-user", "CA-material-sentinel");
            assertThat(CACHE.toString()).doesNotContain("cache-secret", "CA-material-sentinel");
            assertThat(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(f.database).find().first())
                    .containsEntry("bindingVersion", 1L);
            assertThat(SystemCollections.STORAGE_SETUP.on(f.database).find().first())
                    .containsEntry("phase", MongoStorageBindingStore.BOUND).containsEntry("defaultSourceId", "views");
            assertThat(SystemCollections.SOURCES.on(f.database).find().first())
                    .containsEntry("systemRole", MongoStorageBindingStore.DEFAULT_ROLE);
            assertThat(SystemCollections.STORAGE_BINDING.on(f.database).find().first().toJson())
                    .doesNotContain("cache-secret", "cache-user", "cache.invalid", "CA-material-sentinel", "master-secret");
            assertThat(SystemCollections.SOURCES.on(f.database).find().first().toJson()).doesNotContain("master-secret");
            assertThat(f.revision()).isEqualTo(1);
            var global = f.domain.requiredNotes().open(new RequiredNoteOwner(1, "pdk.global-state",
                    RequiredNoteOwner.Kind.GLOBAL, "cluster-a", 0));
            assertThat(global.generation()).isEqualTo(1);
            Document originalKey = SystemCollections.CLUSTER_KEYS.on(f.database).find().first();
            try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster-a")) {
                assertThat(reopened.bindings().load()).contains(bound);
                assertThat(reopened.artifacts().get("views")).contains(source("views", "master-secret"));
            }
            assertThat(SystemCollections.CLUSTER_KEYS.on(f.database).find().first()).isEqualTo(originalKey);
        });
    }

    @Test
    void replayRequiresTheSameOperationGenerationModeAndOriginalContents() {
        withPrepared(f -> {
            var original = f.domain.bindings().commit(SCOPE, StorageBinding.Mode.CLOUD, CACHE, source("atlas-store", "one"));
            assertThat(f.domain.bindings().commit(SCOPE, StorageBinding.Mode.CLOUD, CACHE, source("atlas-store", "one")))
                    .isEqualTo(original);
            for (StorageSetupScope changed : List.of(new StorageSetupScope("cluster-b", "installation-a", "setup-a", 1),
                    new StorageSetupScope("cluster-a", "installation-b", "setup-a", 1),
                    new StorageSetupScope("cluster-a", "installation-a", "setup-b", 1),
                    new StorageSetupScope("cluster-a", "installation-a", "setup-a", 2))) {
                assertThatThrownBy(() -> f.domain.bindings().commit(changed, StorageBinding.Mode.CLOUD,
                        CACHE, source("atlas-store", "one"))).satisfies(MongoStorageBindingStoreIT::refused);
            }
            assertThatThrownBy(() -> f.domain.bindings().commit(SCOPE, StorageBinding.Mode.CLOUD,
                    CACHE, source("atlas-store", "changed"))).satisfies(MongoStorageBindingStoreIT::refused);
            var otherCache = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                    "mongodb://other.invalid/cache", "cache", null);
            assertThatThrownBy(() -> f.domain.bindings().commit(SCOPE, StorageBinding.Mode.CLOUD,
                    otherCache, source("atlas-store", "one"))).satisfies(MongoStorageBindingStoreIT::refused);
            assertThatThrownBy(() -> f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP,
                    CACHE, source("views", "one"))).satisfies(MongoStorageBindingStoreIT::refused);
            assertThat(f.revision()).isEqualTo(1);
            assertThat(f.domain.bindings().load()).contains(original);
        });
    }

    @Test
    void separateClientsRacingDifferentCandidatesPublishExactlyOneWholeBinding() {
        withPrepared(f -> {
            try (var second = MongoConfigurationDomain.open(f.settings, "cluster-a");
                    var executor = Executors.newFixedThreadPool(2)) {
                CountDownLatch ready = new CountDownLatch(2);
                CountDownLatch start = new CountDownLatch(1);
                var one = executor.submit(() -> attempt(f.domain, "one", ready, start));
                var two = executor.submit(() -> attempt(second, "two", ready, start));
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                start.countDown();
                assertThat(List.of(one.get(20, TimeUnit.SECONDS), two.get(20, TimeUnit.SECONDS)))
                        .containsExactlyInAnyOrder("committed", "refused");
                assertThat(f.revision()).isEqualTo(1);
                assertThat(SystemCollections.STORAGE_BINDING.on(f.database).countDocuments()).isEqualTo(1);
                assertThat(SystemCollections.SOURCES.on(f.database).countDocuments()).isEqualTo(1);
                assertThat(SystemCollections.PDK_NOTE_SCOPES.on(f.database).countDocuments()).isEqualTo(1);
                assertThat(f.domain.bindings().load()).isEqualTo(second.bindings().load());
                assertThat(f.domain.bindings().cachePreparation()).isEqualTo(second.bindings().cachePreparation());
                assertThat(SystemCollections.STORAGE_RECOVERY.on(f.database).countDocuments()).isEqualTo(1);
            }
        });
    }

    @Test
    void aRevokedSetupGenerationCannotPublishAnyCandidateFact() {
        withPrepared(f -> {
            SystemCollections.STORAGE_SETUP.on(f.database).updateOne(new Document("_id", "cluster"),
                    new Document("$set", new Document("generation", 2L)));
            assertThatThrownBy(() -> f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "one")))
                    .satisfies(MongoStorageBindingStoreIT::refused);
            assertUnbound(f);
        });
    }

    @Test
    void guardInvalidationAfterTheBodyAbortsItsWritesAndRevision() {
        withPrepared(f -> {
            AtomicInteger checks = new AtomicInteger();
            var guarded = f.domain.bindings().guardedBy(() -> {
                if (checks.incrementAndGet() == 3) throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
            });
            assertThatThrownBy(() -> guarded.commit(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "one")))
                    .satisfies(MongoStorageBindingStoreIT::refused);
            assertThat(checks.get()).isEqualTo(3);
            assertUnbound(f);
        });
    }

    @Test
    void aLateNamespaceInsertFaultRollsBackBindingSourceVersionAndPhase() {
        withPrepared(f -> {
            SystemCollections.PDK_NOTE_SCOPES.on(f.database).insertOne(new Document("_id",
                    new Document("bindingVersion", 1L).append("namespace", "pdk.global-state")));
            assertThatThrownBy(() -> f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "one")))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
            assertThat(f.revision()).isZero();
            assertThat(SystemCollections.STORAGE_BINDING.on(f.database).countDocuments()).isZero();
            assertThat(SystemCollections.SOURCES.on(f.database).countDocuments()).isZero();
            assertThat(SystemCollections.STORAGE_SETUP.on(f.database).find().first()).containsEntry("phase", MongoConfigurationBootstrap.PREPARED);
            assertThat(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(f.database).find().first()).containsEntry("bindingVersion", 0L);
        });
    }

    @Test
    void ordinaryAuthoringCannotMoveOrRemoveTheDefaultOrPartiallyWriteItsBatch() {
        withPrepared(f -> {
            SourceResource original = source("views", "one");
            f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, original);
            assertThatThrownBy(() -> f.domain.artifacts().replace("views", CanonicalHash.of(original), source("views", "changed")))
                    .satisfies(MongoStorageBindingStoreIT::protectedSource);
            assertThatThrownBy(() -> f.domain.artifacts().delete("views", CanonicalHash.of(original)))
                    .satisfies(MongoStorageBindingStoreIT::protectedSource);
            assertThatThrownBy(() -> f.domain.artifacts().saveAll(List.of(source("ordinary", "two"), source("views", "changed"))))
                    .satisfies(MongoStorageBindingStoreIT::protectedSource);
            assertThat(f.domain.artifacts().get("ordinary")).isEmpty();
            assertThat(f.domain.artifacts().get("views")).contains(original);
            assertThat(f.revision()).isEqualTo(1);
        });
    }

    @Test
    void anOrdinaryIdCollisionIsRefusedRatherThanAdoptedAsManaged() {
        withPrepared(f -> {
            SourceResource ordinary = source("views", "ordinary");
            f.domain.artifacts().save(ordinary);
            assertThatThrownBy(() -> f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "managed")))
                    .satisfies(MongoStorageBindingStoreIT::refused);
            assertThat(f.revision()).isEqualTo(1);
            assertThat(f.domain.artifacts().get("views")).contains(ordinary);
            assertThat(SystemCollections.STORAGE_BINDING.on(f.database).countDocuments()).isZero();
            assertThat(SystemCollections.SOURCES.on(f.database).find().first()).doesNotContainKey("systemRole");
        });
    }

    @Test
    void theDeclaredPartialUniqueIndexAllowsOrdinarySourcesButOnlyOneSystemDefault() {
        withPrepared(f -> {
            f.domain.artifacts().saveAll(List.of(source("one", "one"), source("two", "two")));
            f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "master"));
            assertThatThrownBy(() -> SystemCollections.SOURCES.on(f.database).insertOne(new Document("_id", "forged")
                    .append("systemRole", MongoStorageBindingStore.DEFAULT_ROLE))).isInstanceOf(MongoWriteException.class);
            assertThat(SystemCollections.SOURCES.on(f.database).countDocuments()).isEqualTo(3);
            Document index = IndexEnsure.existing(SystemCollections.SOURCES.on(f.database), SystemCollections.SOURCES.indexes().getFirst());
            assertThat(index).containsEntry("unique", true)
                    .containsEntry("partialFilterExpression", new Document("systemRole", MongoStorageBindingStore.DEFAULT_ROLE));
        });
    }

    @Test
    void aNormalRestartRefusesMissingOrTamperedBindingFactsWithoutRepair() {
        for (String field : List.of("binding", "source", "setup", "global", "global-fence", "index", "cache", "key")) {
            withPrepared(f -> {
                f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "master"));
                switch (field) {
                    case "binding" -> SystemCollections.STORAGE_BINDING.on(f.database).deleteMany(new Document());
                    case "source" -> SystemCollections.SOURCES.on(f.database).deleteMany(new Document());
                    case "setup" -> SystemCollections.STORAGE_SETUP.on(f.database).deleteMany(new Document());
                    case "global" -> SystemCollections.PDK_NOTE_SCOPES.on(f.database).deleteMany(new Document());
                    case "global-fence" -> SystemCollections.PDK_NOTE_SCOPES.on(f.database).updateOne(new Document(),
                            new Document("$unset", new Document("writeFence", "")));
                    case "index" -> SystemCollections.SOURCES.on(f.database).dropIndex("system_default_target");
                    case "cache" -> SystemCollections.STORAGE_BINDING.on(f.database).updateOne(new Document(),
                            new Document("$set", new Document("cache", "broken-envelope")));
                    case "key" -> SystemCollections.CLUSTER_KEYS.on(f.database).deleteMany(new Document());
                    default -> throw new AssertionError(field);
                }
                assertThatThrownBy(() -> MongoConfigurationDomain.open(f.settings, "cluster-a"))
                        .isInstanceOf(TapstateException.class);
                assertThat(f.revision()).isEqualTo(1);
                assertThat(SystemCollections.STORAGE_SETUP.on(f.database).find().first() == null ||
                        MongoStorageBindingStore.BOUND.equals(SystemCollections.STORAGE_SETUP.on(f.database).find().first().getString("phase"))).isTrue();
            });
        }
    }

    @Test
    void aCaptureSourceCannotBecomeTheSystemDefault() {
        withPrepared(f -> {
            SourceResource capture = new SourceResource("views", null, "mongodb", Map.of(), SourceMode.CDC,
                    List.of(), null, null);
            assertThatThrownBy(() -> f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, capture))
                    .satisfies(MongoStorageBindingStoreIT::refused);
            assertUnbound(f);
        });
    }

    @Test
    void aPreparedDomainRefusesMissingOrMalformedSetupAuthority() {
        for (String field : List.of("missing", "installationId", "phase", "generation")) {
            withPrepared(f -> {
                switch (field) {
                    case "missing" -> SystemCollections.STORAGE_SETUP.on(f.database).deleteMany(new Document());
                    case "installationId" -> SystemCollections.STORAGE_SETUP.on(f.database).updateOne(new Document(),
                            new Document("$unset", new Document("installationId", "")));
                    case "phase" -> SystemCollections.STORAGE_SETUP.on(f.database).updateOne(new Document(),
                            new Document("$set", new Document("phase", "COMPLETE")));
                    case "generation" -> SystemCollections.STORAGE_SETUP.on(f.database).updateOne(new Document(),
                            new Document("$set", new Document("generation", 0L)));
                    default -> throw new AssertionError(field);
                }
                assertThatThrownBy(() -> f.domain.bindings().load()).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> MongoConfigurationDomain.open(f.settings, "cluster-a")).isInstanceOf(TapstateException.class);
                assertThat(f.revision()).isZero();
            });
        }
    }

    @Test
    void aBoundDomainRefusesMissingMistypedOrRegressedSetupGeneration() {
        for (Object generation : List.of("missing", "2", 0L, 1L)) {
            withPrepared(f -> {
                SystemCollections.STORAGE_SETUP.on(f.database).updateOne(new Document(),
                        new Document("$set", new Document("generation", 2L)));
                var scope = new StorageSetupScope("cluster-a", "installation-a", "setup-a", 2);
                f.domain.bindings().commit(scope, StorageBinding.Mode.OP, CACHE, source("views", "master"));
                Document change = generation.equals("missing")
                        ? new Document("$unset", new Document("generation", ""))
                        : new Document("$set", new Document("generation", generation));
                SystemCollections.STORAGE_SETUP.on(f.database).updateOne(new Document(), change);
                assertThatThrownBy(() -> f.domain.bindings().load()).isInstanceOf(TapstateException.class);
                assertThatThrownBy(() -> MongoConfigurationDomain.open(f.settings, "cluster-a")).isInstanceOf(TapstateException.class);
                assertThat(f.revision()).isEqualTo(1);
            });
        }
    }

    @Test
    void aLaterCapabilityGenerationStillLoadsTheOriginalBinding() {
        withPrepared(f -> {
            var original = f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "master"));
            SystemCollections.STORAGE_SETUP.on(f.database).updateOne(new Document(),
                    new Document("$set", new Document("generation", 2L)));
            assertThat(f.domain.bindings().load()).contains(original);
            try (var reopened = MongoConfigurationDomain.open(f.settings, "cluster-a")) {
                assertThat(reopened.bindings().load()).contains(original);
            }
        });
    }

    @Test
    void aPreparedIndexCrashGapRequiresTheOriginalControlledSetupToResume() {
        withPrepared(f -> {
            SystemCollections.SOURCES.on(f.database).dropIndex("system_default_target");
            assertThatThrownBy(() -> f.domain.bindings().load()).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> MongoConfigurationDomain.open(f.settings, "cluster-a")).isInstanceOf(TapstateException.class);
            assertThat(IndexEnsure.existing(SystemCollections.SOURCES.on(f.database), SystemCollections.SOURCES.indexes().getFirst())).isNull();
            try (var resumed = MongoConfigurationBootstrap.prepare(f.settings, "cluster-a", "installation-a", "setup-a", 1)) {
                assertThat(resumed.bindings().load()).isEmpty();
            }
            assertThat(f.revision()).isZero();
        });
    }

    @Test
    void aRealTransactionRetryRechecksBothComposedGuardsBeforeWriting() {
        withPrepared(f -> {
            AtomicInteger startedUpdates = new AtomicInteger();
            AtomicInteger failedUpdates = new AtomicInteger();
            CommandListener witness = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update")) startedUpdates.incrementAndGet();
                }
                @Override public void commandFailed(CommandFailedEvent event) {
                    if (event.getCommandName().equals("update")) failedUpdates.incrementAndGet();
                }
            };
            try (MongoClient observed = MongoClients.create(MongoClientSettings.builder()
                    .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(witness).build())) {
                String material = SystemCollections.CLUSTER_KEYS.on(f.database).find().first().getString("material");
                var ciphers = SourceConfigCipherProvider.fixed(new SourceConfigCipher(java.util.Base64.getUrlDecoder().decode(material)));
                var store = new MongoStorageBindingStore(observed, observed.getDatabase(f.database.getName()), ciphers);
                AtomicInteger original = new AtomicInteger();
                AtomicInteger additional = new AtomicInteger();
                var guarded = store.guardedBy(() -> {
                    if (original.incrementAndGet() > 2) throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
                }).guardedBy(additional::incrementAndGet);
                f.admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", new Document("times", 1))
                        .append("data", new Document("failCommands", List.of("update")).append("errorCode", 112)
                                .append("errorLabels", List.of("TransientTransactionError"))));
                try {
                    assertThatThrownBy(() -> guarded.commit(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "master")))
                            .satisfies(MongoStorageBindingStoreIT::refused);
                } finally {
                    f.admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off"));
                }
                assertThat(failedUpdates.get()).as("the first callback reached a real injected update failure").isEqualTo(1);
                assertThat(startedUpdates.get()).as("the retried callback was rejected before a second protected update").isEqualTo(1);
                assertThat(original.get()).isEqualTo(3);
                assertThat(additional.get()).isEqualTo(2);
                assertUnbound(f);
            }
        });
    }

    @Test
    void aRetainedPipelineIdCannotBeClaimedByTheSystemDefaultSource() {
        withPrepared(f -> {
            var pipeline = new DslParser().parse("version: tapstate/v1\nkind: pipeline\nid: views\nsource: []\n");
            f.domain.artifacts().create(pipeline);
            SystemCollections.PIPELINES.on(f.database).deleteMany(new Document());
            assertThatThrownBy(() -> f.domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "master")))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(StoreError.PIPELINE_RETIREMENT_REQUIRED));
            assertThat(SystemCollections.SOURCES.on(f.database).countDocuments()).isZero();
            assertThat(SystemCollections.STORAGE_BINDING.on(f.database).countDocuments()).isZero();
            assertThat(f.revision()).isEqualTo(1);
        });
    }

    @Test
    void onlyTheConfirmedFirstBindingWinnerReceivesAnInitialCacheInvocation() {
        withPrepared(f -> {
            var first = f.domain.bindings().commitWithFirstCache(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "master"));
            assertThat(first.firstCreation()).isPresent();
            var replay = f.domain.bindings().commitWithFirstCache(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "master"));
            assertThat(replay.firstCreation()).isEmpty(); assertThat(replay.preparation()).isEqualTo(first.preparation());
            try (var reopened = MongoConfigurationDomain.open(f.settings, SCOPE.clusterId())) {
                assertThat(reopened.bindings().commitWithFirstCache(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "master")).firstCreation()).isEmpty();
            }
            var invocation = first.firstCreation().orElseThrow();
            assertThat(MongoStorageBindingStore.consumeFirst(invocation, MongoStorageBindingStore.targetFingerprint(CACHE))).isSameAs(invocation);
            assertThatThrownBy(() -> MongoStorageBindingStore.consumeFirst(invocation, MongoStorageBindingStore.targetFingerprint(CACHE))).satisfies(MongoStorageBindingStoreIT::refused);
            assertThat(f.revision()).isOne();
        });
    }

    @Test
    void twoIndependentConfigurationClientsIssueExactlyOneInitialCacheInvocation() {
        withPrepared(f -> {
            try (var other = MongoConfigurationDomain.open(f.settings, SCOPE.clusterId()); var executor = Executors.newFixedThreadPool(2)) {
                var ready = new CountDownLatch(2); var start = new CountDownLatch(1);
                var a = executor.submit(() -> { ready.countDown(); if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start barrier");
                    return f.domain.bindings().commitWithFirstCache(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "master")); });
                var b = executor.submit(() -> { ready.countDown(); if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start barrier");
                    return other.bindings().commitWithFirstCache(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "master")); });
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
                var results = List.of(a.get(20, TimeUnit.SECONDS), b.get(20, TimeUnit.SECONDS));
                assertThat(results.stream().filter(result -> result.firstCreation().isPresent()).count()).isOne();
                assertThat(results.getFirst().preparation()).isEqualTo(results.getLast().preparation()); assertThat(f.revision()).isOne();
            }
        });
    }

    @Test
    void arbitraryCapabilitiesCannotRunCallbacksAndAWrongTargetConsumesTheNativeInvocation() {
        withPrepared(f -> {
            var first = f.domain.bindings().commitWithFirstCache(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "master"));
            io.tapstate.spi.store.FirstCacheCreation forged = new io.tapstate.spi.store.FirstCacheCreation() {
                @Override public io.tapstate.spi.store.CachePreparation preparation() { throw new AssertionError("forged preparation called"); }
                @Override public void consume() { throw new AssertionError("forged consume called"); }
                @Override public void requireActive() { throw new AssertionError("forged guard called"); }
            };
            assertThatThrownBy(() -> MongoStorageBindingStore.consumeFirst(forged, MongoStorageBindingStore.targetFingerprint(CACHE))).satisfies(MongoStorageBindingStoreIT::refused);
            var invocation = first.firstCreation().orElseThrow();
            var wrong = new StorageBinding.Connection(CACHE.kind(), CACHE.uri(), "other-cache", CACHE.caPem());
            assertThatThrownBy(() -> MongoStorageBindingStore.consumeFirst(invocation, MongoStorageBindingStore.targetFingerprint(wrong))).satisfies(MongoStorageBindingStoreIT::refused);
            assertThatThrownBy(() -> MongoStorageBindingStore.consumeFirst(invocation, MongoStorageBindingStore.targetFingerprint(CACHE))).satisfies(MongoStorageBindingStoreIT::refused);
        });
    }

    @Test
    void firstCreationSeparatesItsPureCacheGuardFromOriginalConfigurationAuthority() {
        withPrepared(f -> {
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(), f.database.getName(), "find", "storage_binding")) {
                var live = new java.util.concurrent.atomic.AtomicBoolean(true); var checks = new AtomicInteger();
                var material = SystemCollections.CLUSTER_KEYS.on(f.database).find(new Document("_id", "cluster")).first().getString("material");
                byte[] bytes = java.util.Base64.getUrlDecoder().decode(material);
                SourceConfigCipher cipher;
                try { cipher = new SourceConfigCipher(bytes); } finally { java.util.Arrays.fill(bytes, (byte) 0); }
                var nativeBindings = new MongoStorageBindingStore(witness.client, witness.database, SourceConfigCipherProvider.fixed(cipher));
                var guarded = nativeBindings.guardedBy(() -> {
                    checks.incrementAndGet(); if (!live.get()) throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
                });
                var grant = guarded.commitWithFirstCache(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "master")).firstCreation().orElseThrow();
                MongoStorageBindingStore.consumeFirst(grant, MongoStorageBindingStore.targetFingerprint(CACHE));
                int reads = witness.confirmedCommands(); int before = checks.get();
                MongoStorageBindingStore.requireFirstLocal(grant);
                assertThat(checks.get()).isGreaterThan(before); assertThat(witness.confirmedCommands()).isEqualTo(reads);
                grant.requireActive(); assertThat(witness.confirmedCommands()).isGreaterThan(reads);
                live.set(false); assertThatThrownBy(() -> MongoStorageBindingStore.requireFirstLocal(grant)).satisfies(MongoStorageBindingStoreIT::refused);
            }
        });
    }

    @Test
    void aLostInvocationBeforeAnyCacheWriteCannotBeReissuedAndAuthRecoveryDoesNotCreateBusiness() {
        withPrepared(f -> {
            String name = f.database.getName() + "_cache";
            var endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(), name, null);
            var active = new java.util.concurrent.atomic.AtomicBoolean(true);
            var guarded = f.domain.bindings().guardedBy(() -> { if (!active.get()) throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null); });
            var first = guarded.commitWithFirstCache(SCOPE, StorageBinding.Mode.OP, endpoint, source("views", "master"));
            try (var cache = MongoCacheDomain.connect(endpoint, Duration.ofSeconds(10), "birth-it")) {
                active.set(false);
                assertThatThrownBy(() -> cache.prepareFirstAuth(first.firstCreation().orElseThrow())).satisfies(MongoStorageBindingStoreIT::refused);
                active.set(true);
                assertThatThrownBy(() -> cache.prepareFirstAuth(first.firstCreation().orElseThrow())).satisfies(MongoStorageBindingStoreIT::refused);
                assertThat(f.admin.getDatabase(name).listCollectionNames().into(new java.util.ArrayList<>())).isEmpty();
                assertThat(guarded.commitWithFirstCache(SCOPE, StorageBinding.Mode.OP, endpoint, source("views", "master")).firstCreation()).isEmpty();
                assertThatThrownBy(() -> cache.resumeInitialAuth(first.preparation(), () -> {})).isInstanceOf(TapstateException.class);
                assertThat(f.admin.getDatabase(name).listCollectionNames().into(new java.util.ArrayList<>())).isEmpty();
                cache.prepareAuth(first.preparation().authScope(), SCOPE.operationId(), () -> {});
                assertThat(cache.loadAuthScope(first.preparation().authScope())).isEqualTo(first.preparation().authScope());
                assertThatThrownBy(() -> cache.prepareInitialBusiness(first.preparation(), () -> {})).satisfies(MongoStorageBindingStoreIT::refused);
                assertThat(SystemCollections.CACHE_IDENTITY.on(f.admin.getDatabase(name)).find().first()).containsEntry("businessPhase", "ABSENT");
            } finally { f.admin.getDatabase(name).drop(); }
        });
    }

    @Test
    void aRetainedInitialBusinessBirthCanResumeAfterTheOriginalInvocationWasConsumed() {
        withPrepared(f -> {
            String name = f.database.getName() + "_cache";
            var endpoint = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(), name, null);
            var armed = new java.util.concurrent.atomic.AtomicBoolean();
            var guarded = f.domain.bindings().guardedBy(() -> {
                if (armed.get() && SystemCollections.CACHE_IDENTITY.on(f.admin.getDatabase(name)).countDocuments() != 0)
                    throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
            });
            var first = guarded.commitWithFirstCache(SCOPE, StorageBinding.Mode.OP, endpoint, source("views", "master")); armed.set(true);
            try (var cache = MongoCacheDomain.connect(endpoint, Duration.ofSeconds(10), "birth-it")) {
                assertThatThrownBy(() -> cache.prepareFirstAuth(first.firstCreation().orElseThrow())).satisfies(MongoStorageBindingStoreIT::refused);
                assertThat(SystemCollections.CACHE_IDENTITY.on(f.admin.getDatabase(name)).find().first()).containsEntry("authPhase", "PREPARING")
                        .containsEntry("businessPhase", "PREPARING").containsEntry("businessPreparationId", SCOPE.operationId());
            }
            try (var restarted = MongoCacheDomain.connect(endpoint, Duration.ofSeconds(10), "birth-restarted")) {
                restarted.resumeInitialAuth(first.preparation(), () -> {});
                assertThat(restarted.prepareInitialBusiness(first.preparation(), () -> {})).isEqualTo(first.preparation().businessScope());
            } finally { f.admin.getDatabase(name).drop(); }
        });
    }

    @Test
    void losingDeliveryAfterConfigurationCommitDoesNotMintACapabilityOnReplay() {
        withPrepared(f -> {
            var guarded = f.domain.bindings().guardedBy(() -> {
                if (SystemCollections.STORAGE_BINDING.on(f.database).countDocuments() != 0)
                    throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
            });
            assertThatThrownBy(() -> guarded.commitWithFirstCache(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "master")))
                    .satisfies(MongoStorageBindingStoreIT::refused);
            assertThat(f.domain.bindings().load()).isPresent();
            var replay = f.domain.bindings().commitWithFirstCache(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", "master"));
            assertThat(replay.firstCreation()).isEmpty(); assertThat(f.revision()).isOne();
        });
    }

    private static String attempt(MongoConfigurationDomain domain, String secret, CountDownLatch ready, CountDownLatch start) throws Exception {
        ready.countDown();
        if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start not released");
        try {
            domain.bindings().commit(SCOPE, StorageBinding.Mode.OP, CACHE, source("views", secret));
            return "committed";
        } catch (TapstateException failure) {
            refused(failure);
            return "refused";
        }
    }

    private static io.tapstate.core.model.Resource definition(String kind, String id) {
        String body = switch (kind) {
            case "transform" -> "type: filter\nexpr: \"true\"\n";
            case "view" -> "primary_key: id\n";
            case "serve" -> "";
            default -> throw new IllegalArgumentException("definition kind required");
        };
        return new DslParser().parse("version: tapstate/v1\nkind: " + kind + "\nid: " + id + "\n" + body);
    }

    private static SourceResource source(String id, String secret) {
        return (SourceResource) new DslParser().parse("""
                version: tapstate/v1
                id: %s
                kind: source
                connector: mongodb
                config:
                  uri: mongodb://master-user:%s@master.invalid/views?tls=true
                  ca: master-ca-material
                """.formatted(id, secret));
    }
    private static void assertUnbound(Fixture f) {
        assertThat(f.revision()).isZero();
        assertThat(SystemCollections.STORAGE_RECOVERY.on(f.database).countDocuments()).isZero();
        assertThat(f.domain.bindings().cachePreparation()).isEmpty();
        assertThat(f.domain.bindings().load()).isEmpty();
        assertThat(SystemCollections.SOURCES.on(f.database).countDocuments()).isZero();
        assertThat(SystemCollections.PDK_NOTE_SCOPES.on(f.database).countDocuments()).isZero();
        assertThat(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(f.database).find().first()).containsEntry("bindingVersion", 0L);
        assertThat(SystemCollections.STORAGE_SETUP.on(f.database).find().first()).containsEntry("phase", MongoConfigurationBootstrap.PREPARED);
    }
    private static void unreadable(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
    private static void refused(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(StoreError.INITIALIZATION_REFUSED));
    }
    private static void protectedSource(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(StoreError.DEFAULT_SOURCE_PROTECTED));
    }
    private static void withPrepared(Scenario scenario) {
        String name = "storage_binding_" + UUID.randomUUID().toString().replace("-", "");
        try (MongoClient admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = admin.getDatabase(name);
            var settings = new MongoConnectionSettings(MONGO.getReplicaSetUrl(name), null, Duration.ofSeconds(5));
            try (var domain = MongoConfigurationBootstrap.prepare(settings, SCOPE.clusterId(), SCOPE.installationId(), SCOPE.operationId(), SCOPE.generation())) {
                scenario.run(new Fixture(admin, database, settings, domain));
            } finally { database.drop(); }
        } catch (Exception failure) { throw new AssertionError(failure); }
    }
    private record Fixture(MongoClient admin, com.mongodb.client.MongoDatabase database,
            MongoConnectionSettings settings, MongoConfigurationDomain domain) {
        long revision() { return SystemCollections.CONFIGURATION_VERSION.on(database).find().first().getLong("revision"); }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
