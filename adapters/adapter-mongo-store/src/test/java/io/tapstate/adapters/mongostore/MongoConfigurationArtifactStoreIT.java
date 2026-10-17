package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ArtifactMutation;
import io.tapstate.spi.store.ArtifactWrite;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineLayout;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Base64;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real replica-set witnesses for the split configuration resource and revision transaction. */
@RequiresDocker
class MongoConfigurationArtifactStoreIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final DslParser PARSER = new DslParser();

    @Test
    void separateCollectionsRetainEncryptedSourcesAndCanonicalReads() {
        withStore((store, database) -> {
            Resource source = source("input", "private-sentinel");
            Resource pipeline = pipeline("flow");
            store.saveAll(List.of(source, pipeline));

            assertThat(SystemCollections.SOURCES.on(database).countDocuments()).isEqualTo(1);
            assertThat(SystemCollections.PIPELINES.on(database).countDocuments()).isEqualTo(1);
            assertThat(SystemCollections.SOURCES.on(database).find().first().toJson())
                    .doesNotContain("private-sentinel");
            assertThat(store.get("input")).contains(source);
            assertThat(store.get("flow")).contains(pipeline);
            assertThat(store.list()).containsExactlyInAnyOrder(source, pipeline);
            assertThat(store.listStored("source")).hasSize(1).allSatisfy(row -> {
                assertThat(row.id()).isEqualTo("input");
                assertThat(row.contentHash()).isEqualTo(CanonicalHash.of(source));
            });
            assertThat(store.listStored("pipeline")).hasSize(1);
            assertThat(store.listStored("view")).isEmpty();
            assertThat(revision(database)).isEqualTo(1);
            assertThat(database.listCollectionNames().into(new java.util.ArrayList<>()))
                    .containsExactlyInAnyOrder("sources", "pipelines", "configuration_version");
        });
    }

    @Test
    void refusingASiblingRollsBackBothTablesAndTheRevision() {
        withStore((store, database) -> {
            Resource original = pipeline("existing");
            store.save(original);
            var refused = store.writeAll(List.of(ArtifactWrite.createOnly(source("fresh", "secret")),
                    ArtifactWrite.createOnly(pipeline("existing"))));
            assertThat(refused.appliedSuccessfully()).isFalse();
            assertThat(refused.refusal()).isEqualTo(ArtifactMutation.ALREADY_EXISTS);
            assertThat(store.get("fresh")).isEmpty();
            assertThat(store.get("existing")).contains(original);
            assertThat(revision(database)).isEqualTo(1);
        });
    }

    @Test
    void uniqueIndexFaultAbortsSiblingWritesRatherThanBecomingAnIdRefusal() {
        withStore((store, database) -> {
            store.save(pipeline("existing"));
            SystemCollections.PIPELINES.on(database).createIndex(new Document("kind", 1),
                    new IndexOptions().unique(true));
            assertThatThrownBy(() -> store.writeAll(List.of(
                    ArtifactWrite.createOnly(source("fresh", "secret")),
                    ArtifactWrite.createOnly(pipeline("another")))))
                    .isInstanceOfSatisfying(TapstateException.class,
                            coded -> assertThat(coded.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
            assertThat(store.get("fresh")).isEmpty();
            assertThat(store.get("another")).isEmpty();
            assertThat(revision(database)).isEqualTo(1);
        });
    }

    @Test
    void aReadDependencyInTheOtherTableGuardsTheWholeTransaction() {
        withStore((store, database) -> {
            Resource original = source("input", "first");
            store.save(original);
            Resource replacement = source("input", "changed");
            store.replace("input", CanonicalHash.of(original), replacement);

            var result = store.writeAll(List.of(ArtifactWrite.createOnly(pipeline("flow"))
                    .guardedBy(Map.of("input", CanonicalHash.of(original)))));
            assertThat(result.refusal()).isEqualTo(ArtifactMutation.VERSION_CONFLICT);
            assertThat(result.refusedId()).isEqualTo("input");
            assertThat(store.get("flow")).isEmpty();
            assertThat(revision(database)).isEqualTo(2);
        });
    }

    @Test
    void normalCreateReplaceDeleteAllContendOnConfigurationRevision() {
        withStore((store, database) -> {
            Resource original = pipeline("flow");
            assertThat(store.create(original)).isEqualTo(ArtifactMutation.CREATED);
            assertThat(revision(database)).isEqualTo(1);
            assertThat(store.create(source("flow", "secret"))).isEqualTo(ArtifactMutation.ALREADY_EXISTS);
            assertThat(revision(database)).isEqualTo(1);
            Resource changed = source("flow", "new-kind");
            assertThat(store.replace("flow", CanonicalHash.of(original), changed))
                    .isEqualTo(ArtifactMutation.REPLACED);
            assertThat(SystemCollections.PIPELINES.on(database).countDocuments()).isZero();
            assertThat(SystemCollections.SOURCES.on(database).countDocuments()).isEqualTo(1);
            assertThat(revision(database)).isEqualTo(2);
            assertThat(store.delete("flow", CanonicalHash.of(original))).isEqualTo(ArtifactMutation.VERSION_CONFLICT);
            assertThat(revision(database)).isEqualTo(2);
            assertThat(store.delete("flow", CanonicalHash.of(changed))).isEqualTo(ArtifactMutation.DELETED);
            assertThat(revision(database)).isEqualTo(3);
            assertThat(store.delete("flow", CanonicalHash.of(changed))).isEqualTo(ArtifactMutation.NOT_FOUND);
            assertThat(revision(database)).isEqualTo(3);
        });
    }

    @Test
    void distinctClientsRacingDifferentKindsHaveOnlyOneGlobalIdentityWinner() {
        withStore((store, database) -> {
            try (MongoClient otherClient = MongoClients.create(MONGO.getReplicaSetUrl());
                    var executor = Executors.newFixedThreadPool(2)) {
                var other = new MongoConfigurationArtifactStore(otherClient,
                        otherClient.getDatabase(database.getName()), new SourceConfigCipher(new byte[32]));
                for (int round = 0; round < 8; round++) {
                    String id = "race_" + round;
                    CountDownLatch ready = new CountDownLatch(2);
                    CountDownLatch start = new CountDownLatch(1);
                    var source = executor.submit(() -> {
                        ready.countDown();
                        if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start not released");
                        return store.create(source(id, "secret"));
                    });
                    var pipeline = executor.submit(() -> {
                        ready.countDown();
                        if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start not released");
                        return other.create(pipeline(id));
                    });
                    assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
                    start.countDown();
                    assertThat(List.of(source.get(20, TimeUnit.SECONDS), pipeline.get(20, TimeUnit.SECONDS)))
                            .containsExactlyInAnyOrder(ArtifactMutation.CREATED, ArtifactMutation.ALREADY_EXISTS);
                    assertThat(SystemCollections.SOURCES.on(database).countDocuments(new Document("_id", id))
                            + SystemCollections.PIPELINES.on(database).countDocuments(new Document("_id", id)))
                            .isEqualTo(1);
                }
                assertThat(revision(database)).isEqualTo(8);
            }
        });
    }

    @Test
    void ambiguousStoredIdentityIsCorruptionAndNeverSelectsOneTable() {
        withStore((store, database) -> {
            store.save(source("shared", "secret"));
            SystemCollections.PIPELINES.on(database).insertOne(MongoArtifactStore.toDocument(pipeline("shared")));
            assertThatThrownBy(() -> store.get("shared"))
                    .isInstanceOfSatisfying(TapstateException.class, error -> {
                        assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                        assertThat(error.args()).containsEntry("field", "identity");
                    });
            assertThatThrownBy(store::listStored).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> store.delete("shared", CanonicalHash.of(pipeline("shared"))))
                    .isInstanceOf(TapstateException.class);
            assertThat(revision(database)).isEqualTo(1);
        });
    }

    @Test
    void anUninitializedOrInvalidVersionRefusesWritesWithoutRepairingIt() {
        withStore((store, database) -> {
            var versions = SystemCollections.CONFIGURATION_VERSION.on(database);
            for (Object value : new Object[]{null, "0", -1L, Long.MAX_VALUE}) {
                versions.deleteMany(new Document());
                if (value != null) versions.insertOne(new Document("_id", ConfigurationVersion.ID)
                        .append("revision", value));
                assertThatThrownBy(() -> store.create(pipeline("forbidden")))
                        .isInstanceOfSatisfying(TapstateException.class,
                                error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
                assertThat(SystemCollections.PIPELINES.on(database).countDocuments()).isZero();
                assertThat(versions.countDocuments()).isEqualTo(value == null ? 0 : 1);
            }
        });
    }

    @Test
    void emptyWritesDoNotCreateOrTouchControlFacts() {
        withStore((store, database) -> {
            SystemCollections.CONFIGURATION_VERSION.on(database).deleteMany(new Document());
            assertThat(store.writeAll(List.of()).appliedSuccessfully()).isTrue();
            store.saveAll(List.of());
            assertThat(store.saveAll(List.of(), Map.of("absent", "hash"))).isEmpty();
            assertThat(SystemCollections.CONFIGURATION_VERSION.on(database).countDocuments()).isZero();
        });
    }

    @Test
    void theConfigurationDomainReopensWithTheSameKeyAndNoCacheResources() {
        withStore((unused, database) -> {
            installConfigFacts(database);
            String uri = configurationUri(database);
            Document key = SystemCollections.CLUSTER_KEYS.on(database).find().first();
            Resource original = source("input", "durable-secret");
            try (var domain = MongoConfigurationDomain.open(settings(uri), "cluster-a")) {
                domain.artifacts().save(original);
            }
            try (var replacement = MongoConfigurationDomain.open(settings(uri), "cluster-a")) {
                assertThat(replacement.artifacts().get("input")).contains(original);
                assertThat(replacement.artifacts().create(source("next", "next-secret")))
                        .isEqualTo(ArtifactMutation.CREATED);
            }
            Document reopened = SystemCollections.CLUSTER_KEYS.on(database).find().first();
            assertThat(reopened.get("keyId")).isEqualTo(key.get("keyId"));
            assertThat(reopened.get("material")).isEqualTo(key.get("material"));
            assertThat(reopened.getLong("sourceWriteFence")).isEqualTo(2);
            assertThat(SystemCollections.CLUSTER_KEYS.on(database).countDocuments()).isEqualTo(1);
            assertThat(database.listCollectionNames().into(new java.util.ArrayList<>()))
                    .containsExactlyInAnyOrder("configuration_version", "cluster_identity", "schema_migrations",
                            "cluster_keys", "storage_setup", "sources");
        });
    }

    @Test
    void missingKeysAreNeverRegeneratedByOpenOrAnExistingWriter() {
        withStore((unused, database) -> {
            installConfigFacts(database);
            String uri = configurationUri(database);
            try (var domain = MongoConfigurationDomain.open(settings(uri), "cluster-a")) {
                domain.artifacts().save(source("original", "secret"));
                SystemCollections.CLUSTER_KEYS.on(database).deleteMany(new Document());
                assertThatThrownBy(() -> domain.artifacts().save(source("forbidden", "secret")))
                        .isInstanceOfSatisfying(TapstateException.class,
                                error -> assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_INVALID));
                assertThat(revision(database)).isEqualTo(1);
            }
            assertThatThrownBy(() -> MongoConfigurationDomain.open(settings(uri), "cluster-a"))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_INVALID));
            assertThat(SystemCollections.CLUSTER_KEYS.on(database).countDocuments()).isZero();
            assertThat(SystemCollections.SOURCES.on(database).countDocuments()).isEqualTo(1);
        });
    }

    @Test
    void aDifferentValidKeyDoesNotBecomeAnImplicitRotation() {
        withStore((unused, database) -> {
            installConfigFacts(database);
            try (var domain = MongoConfigurationDomain.open(settings(configurationUri(database)), "cluster-a")) {
                byte[] replacement = new byte[32];
                java.util.Arrays.fill(replacement, (byte) 7);
                SystemCollections.CLUSTER_KEYS.on(database).updateOne(new Document("_id", "cluster"),
                        new Document("$set", new Document("keyId", SourceConfigCipher.fingerprint(replacement))
                                .append("material", Base64.getUrlEncoder().withoutPadding().encodeToString(replacement))));
                assertThatThrownBy(() -> domain.artifacts().create(source("forbidden", "secret")))
                        .isInstanceOfSatisfying(TapstateException.class,
                                error -> assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_INVALID));
                assertThat(revision(database)).isZero();
                assertThat(SystemCollections.SOURCES.on(database).countDocuments()).isZero();
            }
        });
    }

    @Test
    void legacyUnknownAndNewerLayoutsAreRefusedWithoutRunningOldMigrations() {
        withStore((unused, database) -> {
            installConfigFacts(database);
            String uri = configurationUri(database);
            var identity = SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database);
            identity.updateOne(new Document("_id", "cluster"), new Document("$unset", new Document("layoutId", "")));
            assertThatThrownBy(() -> MongoConfigurationDomain.open(settings(uri), "cluster-a"))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(identity.find().first()).doesNotContainKey("layoutId");
            identity.updateOne(new Document("_id", "cluster"),
                    new Document("$set", new Document("layoutId", MongoConfigurationDomain.LAYOUT)));
            assertThatThrownBy(() -> MongoConfigurationDomain.open(settings(uri), "wrong-cluster"))
                    .isInstanceOf(TapstateException.class);
            SystemCollections.SCHEMA_MIGRATIONS.on(database).updateOne(new Document("_id", "config"),
                    new Document("$set", new Document("installedVersion", 2)));
            assertThatThrownBy(() -> MongoConfigurationDomain.open(settings(uri), "cluster-a"))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(MigrationError.DATA_NEWER_THAN_BINARY));
            assertThat(database.listCollectionNames().into(new java.util.ArrayList<>()))
                    .doesNotContain("system_meta", "workload_claims", "operator_state", "srs_meta");
        });
    }

    @Test
    void malformedKeyAndEncryptedSourceFailBeforeTheDomainIsExposed() {
        withStore((store, database) -> {
            installConfigFacts(database);
            String uri = configurationUri(database);
            var keys = SystemCollections.CLUSTER_KEYS.on(database);
            keys.updateOne(new Document("_id", "cluster"),
                    new Document("$set", new Document("material", "not-base64!")));
            assertThatThrownBy(() -> MongoConfigurationDomain.open(settings(uri), "cluster-a"))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_INVALID));
            keys.updateOne(new Document("_id", "cluster"), new Document("$set", new Document("material",
                    Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]))));
            store.save(source("damaged", "secret"));
            SystemCollections.SOURCES.on(database).updateOne(new Document("_id", "damaged"),
                    new Document("$set", new Document("body.config", "bad-envelope")));
            assertThatThrownBy(() -> MongoConfigurationDomain.open(settings(uri), "cluster-a"))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
            assertThat(keys.countDocuments()).isEqualTo(1);
        });
    }

    @Test
    void concurrentBootstrapProcessesChooseOnePersistentKeyAndOneOperation() {
        withStore((unused, database) -> {
            SystemCollections.CONFIGURATION_VERSION.on(database).drop();
            try (var executor = Executors.newFixedThreadPool(2)) {
                CountDownLatch start = new CountDownLatch(1);
                var first = executor.submit(() -> {
                    if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start not released");
                    return MongoConfigurationBootstrap.prepare(settings(configurationUri(database)),
                            "cluster-a", "installation-a", "setup-a", 1);
                });
                var second = executor.submit(() -> {
                    if (!start.await(10, TimeUnit.SECONDS)) throw new AssertionError("start not released");
                    return MongoConfigurationBootstrap.prepare(settings(configurationUri(database)),
                            "cluster-a", "installation-a", "setup-a", 1);
                });
                start.countDown();
                try (var firstDomain = first.get(30, TimeUnit.SECONDS);
                        var secondDomain = second.get(30, TimeUnit.SECONDS)) {
                    Resource source = source("proof", "only-winning-key");
                    firstDomain.artifacts().save(source);
                    assertThat(secondDomain.artifacts().get("proof")).contains(source);
                }
            }
            Document key = SystemCollections.CLUSTER_KEYS.on(database).find().first();
            try (var retry = MongoConfigurationBootstrap.prepare(settings(configurationUri(database)),
                    "cluster-a", "installation-a", "setup-a", 1)) {
                assertThat(retry.artifacts().get("proof")).isPresent();
                assertThat(SystemCollections.CLUSTER_KEYS.on(database).find().first()).isEqualTo(key);
            }
            assertThat(SystemCollections.CLUSTER_KEYS.on(database).countDocuments()).isEqualTo(1);
            assertThat(SystemCollections.STORAGE_SETUP.on(database).find().first())
                    .containsEntry("phase", MongoConfigurationBootstrap.PREPARED)
                    .containsEntry("operationId", "setup-a").containsEntry("generation", 1L);
            assertThat(database.listCollectionNames().into(new java.util.ArrayList<>()))
                    .containsExactlyInAnyOrder("storage_setup", "cluster_identity", "cluster_keys",
                            "schema_migrations", "configuration_version", "sources");
        });
    }

    @Test
    void bootstrapCannotResetAnotherOperationOrACompletedPhase() {
        withStore((unused, database) -> {
            SystemCollections.CONFIGURATION_VERSION.on(database).drop();
            try (var domain = MongoConfigurationBootstrap.prepare(settings(configurationUri(database)),
                    "cluster-a", "installation-a", "setup-a", 1)) {
                domain.artifacts().save(source("original", "secret"));
            }
            Document key = SystemCollections.CLUSTER_KEYS.on(database).find().first();
            for (String[] identity : List.of(new String[]{"cluster-b", "installation-a", "setup-a"},
                    new String[]{"cluster-a", "installation-b", "setup-a"},
                    new String[]{"cluster-a", "installation-a", "setup-b"})) {
                assertThatThrownBy(() -> MongoConfigurationBootstrap.prepare(settings(configurationUri(database)),
                        identity[0], identity[1], identity[2], 1))
                        .isInstanceOfSatisfying(TapstateException.class,
                                error -> assertThat(error.code()).isEqualTo(StoreError.INITIALIZATION_REFUSED));
            }
            assertThatThrownBy(() -> MongoConfigurationBootstrap.prepare(settings(configurationUri(database)),
                    "cluster-a", "installation-a", "setup-a", 2)).isInstanceOf(TapstateException.class);
            SystemCollections.STORAGE_SETUP.on(database).updateOne(new Document("_id", "cluster"),
                    new Document("$set", new Document("phase", "COMPLETE")));
            assertThatThrownBy(() -> MongoConfigurationBootstrap.prepare(settings(configurationUri(database)),
                    "cluster-a", "installation-a", "setup-a", 1)).isInstanceOf(TapstateException.class);
            assertThat(SystemCollections.CLUSTER_KEYS.on(database).find().first()).isEqualTo(key);
            assertThat(revision(database)).isEqualTo(1);
        });
    }

    @Test
    void bootstrapDoesNotBrandAnOccupiedTargetOrRepairAMissingKey() {
        withStore((unused, database) -> {
            // Even one pre-existing control fact without the complete identity is an unknown target.
            assertThatThrownBy(() -> MongoConfigurationBootstrap.prepare(settings(configurationUri(database)),
                    "cluster-a", "installation-a", "setup-a", 1))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(StoreError.INITIALIZATION_REFUSED));
            assertThat(SystemCollections.CLUSTER_KEYS.on(database).countDocuments()).isZero();
            assertThat(SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).countDocuments()).isZero();
            SystemCollections.CONFIGURATION_VERSION.on(database).drop();
            try (var ignored = MongoConfigurationBootstrap.prepare(settings(configurationUri(database)),
                    "cluster-a", "installation-a", "setup-a", 1)) {
                assertThat(SystemCollections.CLUSTER_KEYS.on(database).countDocuments()).isEqualTo(1);
            }
            SystemCollections.CLUSTER_KEYS.on(database).deleteMany(new Document());
            assertThatThrownBy(() -> MongoConfigurationBootstrap.prepare(settings(configurationUri(database)),
                    "cluster-a", "installation-a", "setup-a", 1))
                    .isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_INVALID));
            assertThat(SystemCollections.CLUSTER_KEYS.on(database).countDocuments()).isZero();
        });
    }

    @Test
    void bootstrapNamespaceFailuresAreCodedAndDoNotWriteAnyFacts() {
        withStore((unused, database) -> {
            SystemCollections.CONFIGURATION_VERSION.on(database).drop();
            try (MongoClient admin = MongoClients.create(MONGO.getReplicaSetUrl())) {
                admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                        .append("mode", new Document("times", 1))
                        .append("data", new Document("failCommands", List.of("listCollections"))
                                .append("errorCode", 13)));
                try {
                    assertThatThrownBy(() -> MongoConfigurationBootstrap.prepare(settings(configurationUri(database)),
                            "cluster-a", "installation-a", "setup-a", 1))
                            .isInstanceOfSatisfying(TapstateException.class, coded -> {
                                assertThat(coded.code()).isEqualTo(IoError.STORE_UNAVAILABLE);
                                assertThat(coded.args()).containsExactlyEntriesOf(
                                        Map.of("detail", "MongoCommandException code=13"));
                                assertThat(coded.getCause()).isNull();
                            });
                } finally {
                    admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                            .append("mode", "off"));
                }
            }
            assertThat(database.listCollectionNames().into(new java.util.ArrayList<>())).isEmpty();
        });
    }

    @Test
    void layoutMutationsParticipateInTheSameConfigurationRevision() {
        withStore((unused, database) -> {
            installConfigFacts(database);
            try (var domain = MongoConfigurationDomain.open(settings(configurationUri(database)), "cluster-a")) {
                domain.artifacts().create(pipeline("flow"));
                PipelineLayout layout = new PipelineLayout("flow",
                        Map.of("step", new PipelineLayout.NodePosition(10, 20)),
                        new PipelineLayout.Viewport(1, 2, 1));
                domain.layouts().save(layout);
                assertThat(domain.layouts().get("flow")).contains(layout);
                assertThat(revision(database)).isEqualTo(2);
                domain.layouts().delete("flow");
                assertThat(domain.layouts().get("flow")).isEmpty();
                assertThat(revision(database)).isEqualTo(3);
            }
        });
    }

    @Test
    void aFailedLayoutWriteRollsBackTheConfigurationRevision() {
        withStore((unused, database) -> {
            installConfigFacts(database);
            try (var domain = MongoConfigurationDomain.open(settings(configurationUri(database)), "cluster-a")) {
                domain.artifacts().saveAll(List.of(pipeline("one"), pipeline("two")));
                PipelineLayout first = new PipelineLayout("one", Map.of(), new PipelineLayout.Viewport(0, 0, 1));
                domain.layouts().save(first);
                SystemCollections.CONFIG_PIPELINE_LAYOUTS.on(database).createIndex(new Document("viewport.zoom", 1),
                        new IndexOptions().unique(true));
                assertThatThrownBy(() -> domain.layouts().save(new PipelineLayout("two", Map.of(),
                        new PipelineLayout.Viewport(1, 1, 1))))
                        .isInstanceOfSatisfying(TapstateException.class,
                                error -> assertThat(error.code()).isEqualTo(IoError.STORE_UNAVAILABLE));
                assertThat(revision(database)).isEqualTo(2);
                assertThat(domain.layouts().get("one")).contains(first);
                assertThat(domain.layouts().get("two")).isEmpty();
            }
        });
    }

    private static void installConfigFacts(MongoDatabase database) {
        SystemCollections.STORAGE_SETUP.on(database).insertOne(new Document("_id", "cluster")
                .append("clusterId", "cluster-a").append("installationId", "installation-a")
                .append("operationId", "setup-a").append("generation", 1L).append("phase", MongoConfigurationBootstrap.PREPARED));
        MongoStorageBindingStore.prepareIndex(database);
        SystemCollections.CONFIG_CLUSTER_IDENTITY.on(database).insertOne(new Document("_id", "cluster")
                .append("clusterId", "cluster-a").append("layoutId", MongoConfigurationDomain.LAYOUT)
                .append("bindingVersion", 0L));
        SystemCollections.SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "config")
                .append("installedVersion", 1));
        byte[] key = new byte[32];
        SystemCollections.CLUSTER_KEYS.on(database).insertOne(new Document("_id", "cluster")
                .append("clusterId", "cluster-a").append("formatVersion", 1)
                .append("keyId", SourceConfigCipher.fingerprint(key))
                .append("material", Base64.getUrlEncoder().withoutPadding().encodeToString(key)));
    }

    private static String configurationUri(MongoDatabase database) {
        return MONGO.getReplicaSetUrl(database.getName());
    }

    private static MongoConnectionSettings settings(String uri) {
        return new MongoConnectionSettings(uri, null, Duration.ofSeconds(5));
    }

    private static long revision(MongoDatabase database) {
        return SystemCollections.CONFIGURATION_VERSION.on(database).find().first().getLong("revision");
    }

    private static Resource source(String id, String secret) {
        return PARSER.parse("""
                version: tapstate/v1
                kind: source
                id: %s
                connector: mongodb
                config:
                  password: %s
                """.formatted(id, secret));
    }

    private static Resource pipeline(String id) {
        return PARSER.parse("""
                version: tapstate/v1
                kind: pipeline
                id: %s
                source: []
                """.formatted(id));
    }

    private static void withStore(Scenario scenario) {
        try (MongoClient client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("config_split_" + UUID.randomUUID().toString().replace("-", ""));
            try {
                SystemCollections.CONFIGURATION_VERSION.on(database).insertOne(
                        new Document("_id", ConfigurationVersion.ID).append("revision", 0L));
                scenario.run(new MongoConfigurationArtifactStore(client, database,
                        new SourceConfigCipher(new byte[32])), database);
            } catch (Exception error) {
                throw new AssertionError(error);
            } finally {
                database.drop();
            }
        }
    }

    @FunctionalInterface
    private interface Scenario {
        void run(MongoConfigurationArtifactStore store, MongoDatabase database) throws Exception;
    }
}
