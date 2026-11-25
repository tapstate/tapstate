package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Cache-only identity publication without configuration IO, business assembly or legacy migrations. */
@RequiresDocker
class MongoCacheDomainIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final CacheAuthScope OP = new CacheAuthScope("cluster-a", 1, StorageBinding.Mode.OP, "incarnation-a", 1);
    private static final Runnable ACTIVE = () -> {};

    @Test
    void connectivityAndOrdinaryLoadNeverInitializeAnEmptyCache() {
        withCache((root, database, binding) -> {
            assertThat(database.listCollectionNames().into(new ArrayList<>())).isEmpty();
            assertThatThrownBy(() -> root.loadAuthScope(OP)).satisfies(MongoCacheDomainIT::fenced);
            assertThat(database.listCollectionNames().into(new ArrayList<>())).isEmpty();
        });
    }

    @Test
    void eachModePreparesOnlyItsMinimalAuthenticationResourcesAndReopensTheSameScope() {
        for (var mode : StorageBinding.Mode.values()) {
            withCache((root, database, binding) -> {
                var scope = new CacheAuthScope(OP.clusterId(), 1, mode, OP.cacheIncarnation(), 1);
                assertThat(root.prepareAuth(scope, "setup-a", ACTIVE)).isEqualTo(scope);
                var names = database.listCollectionNames().into(new ArrayList<>());
                assertThat(names).containsExactlyInAnyOrderElementsOf(mode == StorageBinding.Mode.OP
                        ? List.of("cache_identity", "schema_migrations", "op_sessions")
                        : List.of("cache_identity", "schema_migrations", "cloud_sessions", "cloud_revocations"));
                assertThat(SystemCollections.CACHE_IDENTITY.on(database).find().first()).containsEntry("businessPhase", "ABSENT");
                assertThat(SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).find().first())
                        .containsEntry("installedVersion", 1).containsEntry("phase", "READY").containsEntry("epoch", 1L);
                try (var reopened = connect(binding)) { assertThat(reopened.loadAuthScope(scope)).isEqualTo(scope); }
                Document before = SystemCollections.CACHE_IDENTITY.on(database).find().first();
                root.prepareAuth(scope, "setup-a", ACTIVE);
                assertThat(SystemCollections.CACHE_IDENTITY.on(database).find().first()).isEqualTo(before);
            });
        }
    }

    @Test
    void anUnknownOccupiedDatabaseIsNeverClaimedAsANewCache() {
        withCache((root, database, binding) -> {
            SystemCollections.SYSTEM_META.on(database).insertOne(new Document("_id", "legacy"));
            assertThatThrownBy(() -> root.prepareAuth(OP, "setup-a", ACTIVE)).satisfies(MongoCacheDomainIT::refused);
            assertThat(database.listCollectionNames().into(new ArrayList<>())).containsExactly("system_meta");
        });
    }

    @Test
    void aCrashedPreparationCanOnlyResumeItsOriginalScopeAndOperation() {
        withCache((root, database, binding) -> {
            var checks = new AtomicInteger();
            assertThatThrownBy(() -> root.prepareAuth(OP, "setup-a", () -> {
                if (checks.incrementAndGet() == 4) throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
            })).satisfies(MongoCacheDomainIT::refused);
            assertThat(SystemCollections.CACHE_IDENTITY.on(database).find().first()).containsEntry("authPhase", "PREPARING");
            assertThatThrownBy(() -> root.loadAuthScope(OP)).satisfies(MongoCacheDomainIT::fenced);
            assertThatThrownBy(() -> root.prepareAuth(OP, "different-operation", ACTIVE)).satisfies(MongoCacheDomainIT::refused);
            root.prepareAuth(OP, "setup-a", ACTIVE);
            assertThat(root.loadAuthScope(OP)).isEqualTo(OP);
        });
    }

    @Test
    void concurrentClientsPublishOneIdentityAndOneAuthenticationSchema() {
        withCache((root, database, binding) -> {
            try (var other = connect(binding); var executor = Executors.newFixedThreadPool(2)) {
                var ready = new CountDownLatch(2); var start = new CountDownLatch(1);
                var a = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue(); return root.prepareAuth(OP, "setup-a", ACTIVE); });
                var b = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue(); return other.prepareAuth(OP, "setup-a", ACTIVE); });
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
                assertThat(a.get(20, TimeUnit.SECONDS)).isEqualTo(OP);
                assertThat(b.get(20, TimeUnit.SECONDS)).isEqualTo(OP);
                assertThat(SystemCollections.CACHE_IDENTITY.on(database).countDocuments()).isOne();
                assertThat(SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).countDocuments()).isOne();
            }
        });
    }

    @Test
    void missingSchemaOrCounterFactsAreRefusedInsteadOfRecreated() {
        for (String damage : List.of("schema", "counter")) {
            withCache((root, database, binding) -> {
                root.prepareAuth(OP, "setup-a", ACTIVE);
                if (damage.equals("schema")) SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).deleteMany(new Document());
                else SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document(), new Document("$unset", new Document("authWriteFence", "")));
                assertThatThrownBy(() -> root.loadAuthScope(OP)).satisfies(MongoCacheDomainIT::unreadable);
                assertThatThrownBy(() -> root.prepareAuth(OP, "setup-a", ACTIVE)).satisfies(MongoCacheDomainIT::unreadable);
                assertThat(SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).countDocuments()).isEqualTo(damage.equals("schema") ? 0 : 1);
                if (damage.equals("counter")) assertThat(SystemCollections.CACHE_IDENTITY.on(database).find().first()).doesNotContainKey("authWriteFence");
            });
        }
    }

    @Test
    void aNewerAuthSchemaRefusesBothLoadAndControlledPreparation() {
        withCache((root, database, binding) -> {
            root.prepareAuth(OP, "setup-a", ACTIVE);
            SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).updateOne(new Document("_id", "auth"),
                    new Document("$set", new Document("installedVersion", 2)));
            assertThatThrownBy(() -> root.loadAuthScope(OP)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(MigrationError.DATA_NEWER_THAN_BINARY));
            assertThatThrownBy(() -> root.prepareAuth(OP, "setup-a", ACTIVE)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(MigrationError.DATA_NEWER_THAN_BINARY));
        });
    }

    @Test
    void authenticationResetIsIdempotentPreservesBusinessAndFencesTheOriginalScope() {
        withCache((root, database, binding) -> {
            root.prepareAuth(OP, "setup-a", ACTIVE);
            SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document(), new Document("$set", new Document("businessPhase", "READY")));
            var next = root.advanceAuthGeneration(OP, 2, "recovery-a", ACTIVE);
            assertThat(next.cacheIncarnation()).isEqualTo(OP.cacheIncarnation());
            assertThat(next.authGeneration()).isEqualTo(2);
            assertThatThrownBy(() -> root.loadAuthScope(OP)).satisfies(MongoCacheDomainIT::fenced);
            assertThat(root.loadAuthScope(next)).isEqualTo(next);
            Document before = SystemCollections.CACHE_IDENTITY.on(database).find().first();
            assertThat(before).containsEntry("businessPhase", "READY");
            assertThat(root.advanceAuthGeneration(OP, 2, "recovery-a", ACTIVE)).isEqualTo(next);
            assertThat(SystemCollections.CACHE_IDENTITY.on(database).find().first()).isEqualTo(before);
            assertThatThrownBy(() -> root.advanceAuthGeneration(OP, 2, "different-operation", ACTIVE)).satisfies(MongoCacheDomainIT::refused);
        });
    }

    @Test
    void losingTheResetGuardRollsBackGenerationSchemaOwnerAndFence() {
        withCache((root, database, binding) -> {
            root.prepareAuth(OP, "setup-a", ACTIVE);
            var beforeIdentity = SystemCollections.CACHE_IDENTITY.on(database).find().first();
            var beforeSchema = SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).find().first();
            var checks = new AtomicInteger();
            assertThatThrownBy(() -> root.advanceAuthGeneration(OP, 2, "recovery-a", () -> {
                if (checks.incrementAndGet() == 3) throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
            })).satisfies(MongoCacheDomainIT::refused);
            assertThat(SystemCollections.CACHE_IDENTITY.on(database).find().first()).isEqualTo(beforeIdentity);
            assertThat(SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).find().first()).isEqualTo(beforeSchema);
        });
    }

    @Test
    void aSupersededSnapshotCannotCommitItsOldGenerationProtectedWrite() {
        withCache((root, database, binding) -> {
            root.prepareAuth(OP, "setup-a", ACTIVE);
            var ready = new CountDownLatch(1); var release = new CountDownLatch(1); var first = new AtomicBoolean(true);
            CommandListener witness = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update") && event.getCommand().getString("update").getValue().equals("cache_identity")
                            && first.compareAndSet(true, false)) {
                        ready.countDown();
                        try { if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("identity update was not released"); }
                        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                    }
                }
            };
            try (var observed = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(witness).build()); var executor = Executors.newSingleThreadExecutor()) {
                var observedDatabase = observed.getDatabase(database.getName());
                var old = new MongoCacheIdentity(observed, observedDatabase, OP);
                var stale = executor.submit(() -> {
                    try {
                        old.transaction(session -> { old.fence(session); SystemCollections.OP_SESSIONS.on(observedDatabase)
                                .insertOne(session, new Document("_id", "protected-probe")); return null; });
                        return null;
                    } catch (TapstateException refusal) { return refusal; }
                });
                try {
                    assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                    root.advanceAuthGeneration(OP, 2, "recovery-a", ACTIVE);
                } finally { release.countDown(); }
                fenced(stale.get(20, TimeUnit.SECONDS));
                assertThat(SystemCollections.OP_SESSIONS.on(database).countDocuments()).isZero();
            }
        });
    }

    @Test
    void publishedAuthCannotRecreateLostRevocationsInTheOriginalGeneration() {
        withCache((root, database, binding) -> {
            var cloud = new CacheAuthScope(OP.clusterId(), 1, StorageBinding.Mode.CLOUD, OP.cacheIncarnation(), 1);
            root.prepareAuth(cloud, "setup-a", ACTIVE);
            SystemCollections.CLOUD_REVOCATIONS.on(database).drop();
            assertThatThrownBy(() -> root.loadAuthScope(cloud)).satisfies(MongoCacheDomainIT::unreadable);
            assertThatThrownBy(() -> root.prepareAuth(cloud, "setup-a", ACTIVE)).satisfies(MongoCacheDomainIT::unreadable);
            assertThat(database.listCollectionNames().into(new ArrayList<>())).doesNotContain("cloud_revocations");
            assertThat(SystemCollections.CACHE_IDENTITY.on(database).find().first()).containsEntry("authGeneration", 1L);
        });
    }

    @Test
    void anInitializerDelayedBeforeStructureCannotRepairAnAlreadyPublishedGeneration() {
        withCache((root, database, binding) -> {
            var cloud = new CacheAuthScope(OP.clusterId(), 1, StorageBinding.Mode.CLOUD, OP.cacheIncarnation(), 1);
            var ready = new CountDownLatch(1); var release = new CountDownLatch(1); var checks = new AtomicInteger();
            try (var other = connect(binding); var executor = Executors.newSingleThreadExecutor()) {
                var old = executor.submit(() -> {
                    try {
                        root.prepareAuth(cloud, "setup-a", () -> {
                            if (checks.incrementAndGet() == 4) {
                                ready.countDown();
                                try { if (!release.await(10, TimeUnit.SECONDS)) throw new AssertionError("old initializer not released"); }
                                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                            }
                        });
                        return null;
                    } catch (TapstateException refusal) { return refusal; }
                });
                try {
                    assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                    other.prepareAuth(cloud, "setup-a", ACTIVE);
                    SystemCollections.CLOUD_REVOCATIONS.on(database).drop();
                } finally { release.countDown(); }
                unreadable(old.get(20, TimeUnit.SECONDS));
                assertThat(database.listCollectionNames().into(new ArrayList<>())).doesNotContain("cloud_revocations");
            }
        });
    }

    @Test
    void preparationWorksWithOnePooledConnection() {
        withCache((unused, database, binding) -> {
            var single = new StorageBinding.Connection(binding.kind(), binding.uri() + "&maxPoolSize=1", binding.database(), null);
            try (var root = connect(single)) { assertThat(root.prepareAuth(OP, "setup-a", ACTIVE)).isEqualTo(OP); }
        });
    }

    @Test
    void emptyAuthCollectionsAndIndexesCanCommitWithTheirReadinessInOneLocalReadTransaction() {
        withCache((root, database, binding) -> {
            try (var client = MongoClients.create(MONGO.getReplicaSetUrl()); var session = client.startSession()) {
                var target = client.getDatabase(database.getName());
                new MongoCacheIdentity(client, target, OP).beginPreparation("setup-a", true, ACTIVE);
                session.withTransaction(() -> {
                    SystemCollections.CACHE_IDENTITY.on(target).updateOne(session, new Document("_id", "cluster"),
                            new Document("$set", new Document("authPhase", "READY")).append("$inc", new Document("authWriteFence", 1L)));
                    target.createCollection(session, SystemCollections.OP_SESSIONS.collectionName());
                    SystemCollections.OP_SESSIONS.on(target).createIndex(session, new Document("secretHash", 1).append("issuer", 1),
                            new com.mongodb.client.model.IndexOptions().name(SystemCollections.OP_SESSIONS.indexes().getFirst().indexName()));
                    SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(target).updateOne(session, new Document("_id", "auth"),
                            new Document("$set", new Document("installedVersion", 1).append("phase", "READY")));
                    return null;
                }, com.mongodb.TransactionOptions.builder().readConcern(com.mongodb.ReadConcern.LOCAL)
                        .writeConcern(com.mongodb.WriteConcern.MAJORITY.withJournal(true)).build());
                assertThat(root.loadAuthScope(OP)).isEqualTo(OP);
            }
        });
    }

    @Test
    void abortingTheSameSessionStructureTransactionRollsBackItsCollectionsAndIndex() {
        withCache((root, database, binding) -> {
            try (var client = MongoClients.create(MONGO.getReplicaSetUrl()); var session = client.startSession()) {
                var target = client.getDatabase(database.getName());
                new MongoCacheIdentity(client, target, OP).beginPreparation("setup-a", true, ACTIVE);
                assertThatThrownBy(() -> session.withTransaction(() -> {
                    SystemCollections.CACHE_IDENTITY.on(target).updateOne(session, new Document("_id", "cluster"),
                            new Document("$set", new Document("authPhase", "READY")));
                    target.createCollection(session, SystemCollections.OP_SESSIONS.collectionName());
                    SystemCollections.OP_SESSIONS.on(target).createIndex(session, new Document("secretHash", 1).append("issuer", 1),
                            new com.mongodb.client.model.IndexOptions().name(SystemCollections.OP_SESSIONS.indexes().getFirst().indexName()));
                    throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
                }, com.mongodb.TransactionOptions.builder().readConcern(com.mongodb.ReadConcern.LOCAL)
                        .writeConcern(com.mongodb.WriteConcern.MAJORITY.withJournal(true)).build())).satisfies(MongoCacheDomainIT::refused);
                assertThat(database.listCollectionNames().into(new ArrayList<>())).containsExactlyInAnyOrder("cache_identity", "schema_migrations");
                assertThat(SystemCollections.CACHE_IDENTITY.on(database).find().first()).containsEntry("authPhase", "PREPARING");
            }
        });
    }

    @Test
    void losingPreparationAuthorityAfterDdlRollsBackStructureAndReadyTogether() {
        withCache((root, database, binding) -> {
            var checks = new AtomicInteger();
            assertThatThrownBy(() -> root.prepareAuth(OP, "setup-a", () -> {
                if (checks.incrementAndGet() == 6) throw new TapstateException(StoreError.INITIALIZATION_REFUSED, Map.of(), null);
            })).satisfies(MongoCacheDomainIT::refused);
            assertThat(checks.get()).isEqualTo(6);
            assertThat(database.listCollectionNames().into(new ArrayList<>())).containsExactlyInAnyOrder("cache_identity", "schema_migrations");
            assertThat(SystemCollections.CACHE_IDENTITY.on(database).find().first()).containsEntry("authPhase", "PREPARING").containsEntry("authWriteFence", 0L);
            assertThat(SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).find().first()).containsEntry("installedVersion", 0);
            root.prepareAuth(OP, "setup-a", ACTIVE);
            assertThat(root.loadAuthScope(OP)).isEqualTo(OP);
        });
    }

    private static MongoCacheDomain connect(StorageBinding.Connection binding) { return MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "cache-auth-test"); }
    private static void fenced(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(IoError.CACHE_AUTH_FENCED)); }
    private static void refused(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(StoreError.INITIALIZATION_REFUSED)); }
    private static void unreadable(Throwable failure) {
        assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> { assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE); assertThat(error.getCause()).isNull(); });
    }
    private static void withCache(Scenario scenario) {
        try (var admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = admin.getDatabase("cache_auth_" + UUID.randomUUID().toString().replace("-", ""));
            String base = MONGO.getReplicaSetUrl();
            var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB,
                    base + (base.contains("?") ? "&" : "?") + "timeoutMS=5000", database.getName(), null);
            try (var root = connect(binding)) { scenario.run(root, database, binding); }
            catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }
    @FunctionalInterface private interface Scenario { void run(MongoCacheDomain root, MongoDatabase database, StorageBinding.Connection binding) throws Exception; }
}
