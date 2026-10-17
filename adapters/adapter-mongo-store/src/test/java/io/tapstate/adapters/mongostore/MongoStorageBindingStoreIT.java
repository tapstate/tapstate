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
        assertThat(f.domain.bindings().load()).isEmpty();
        assertThat(SystemCollections.SOURCES.on(f.database).countDocuments()).isZero();
        assertThat(SystemCollections.PDK_NOTE_SCOPES.on(f.database).countDocuments()).isZero();
        assertThat(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(f.database).find().first()).containsEntry("bindingVersion", 0L);
        assertThat(SystemCollections.STORAGE_SETUP.on(f.database).find().first()).containsEntry("phase", MongoConfigurationBootstrap.PREPARED);
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
