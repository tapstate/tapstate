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
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real replica-set witnesses for the split configuration resource and revision transaction. */
@RequiresDocker
class MongoConfigurationArtifactStoreIT {
    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
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
                if (value != null) versions.insertOne(new Document("_id", MongoConfigurationArtifactStore.REVISION_ID)
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
                        new Document("_id", MongoConfigurationArtifactStore.REVISION_ID).append("revision", 0L));
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
