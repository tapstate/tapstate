package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.IoError;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Paired Source/keyring BSON restore witnesses, not an implementation or certification of a backup service. */
@RequiresDocker
class SourceConfigKeyringRestoreIT {

    private static final String KEYRING_ID = "source-config-keyring";
    private static final String SECRET = "restore-config-secret-sentinel";
    private static final DslParser PARSER = new DslParser();

    @Container
    private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    @ParameterizedTest
    @EnumSource(SnapshotPhase.class)
    void pairedSnapshotsRestoreToAnIndependentDatabaseWithTheSameKeysAndLogicalSources(SnapshotPhase phase) {
        Snapshot captured = capture(phase);
        String target = freshName("paired_restore");
        try (MongoClient raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = raw.getDatabase(target);
            restore(database, captured);
            assertThat(SystemCollections.ARTIFACTS.on(database).find().into(new ArrayList<>()))
                    .containsExactlyInAnyOrderElementsOf(captured.artifacts);
            Document expectedKeyring = keyring(captured);
            try (MongoConnection connection = connection(target)) {
                connection.verify();
                assertThat(connection.sourceConfigKeyring().epoch()).isEqualTo(expectedKeyring.get("epoch", Number.class).longValue());
                assertThat(keyring(database)).isEqualTo(expectedKeyring);
                MongoArtifactStore store = new MongoArtifactStore(connection.client(),
                        SystemCollections.ARTIFACTS.on(connection.database()), connection.sourceConfigKeyring());
                for (Resource expected : List.of(source("old_source"), source("second_source"))) {
                    assertThat(store.get(expected.id())).contains(expected);
                    Document actual = SystemCollections.ARTIFACTS.on(database)
                            .find(new Document("_id", expected.id())).first();
                    assertThat(actual.getString("contentHash")).isEqualTo(CanonicalHash.of(expected));
                    assertThat(actual.get("body", Document.class).get("config"))
                            .isInstanceOf(String.class).asString().startsWith("tscfg:1:");
                    assertThat(actual.toJson()).doesNotContain(SECRET, "restored-public-value");
                }
                assertThat(store.get("non_source")).contains(nonSource());
                assertOriginalArtifacts(database, captured);
                // Restored readers can use retained read-only keys, but new writes must use the active key.
                store.save(source("new_source"));
                Document written = SystemCollections.ARTIFACTS.on(database)
                        .find(new Document("_id", "new_source")).first();
                assertThat(SourceConfigCipher.envelopeKeyId(written.get("body", Document.class).getString("config")))
                        .isEqualTo(expectedKeyring.getString("activeKeyId"));
                assertThat(keyring(database).getList("keys", Document.class))
                        .containsExactlyInAnyOrderElementsOf(expectedKeyring.getList("keys", Document.class));
                assertOriginalArtifacts(database, captured);
            }
            // A second fresh process-local keyring handle must still use the restored database winner.
            try (MongoConnection restarted = connection(target)) {
                restarted.verify();
                MongoArtifactStore store = new MongoArtifactStore(restarted.client(),
                        SystemCollections.ARTIFACTS.on(restarted.database()), restarted.sourceConfigKeyring());
                assertThat(store.get("old_source")).contains(source("old_source"));
                assertThat(store.get("new_source")).contains(source("new_source"));
                assertThat(keyring(database).getList("keys", Document.class))
                        .containsExactlyInAnyOrderElementsOf(expectedKeyring.getList("keys", Document.class));
                assertOriginalArtifacts(database, captured);
            }
        }
    }

    @ParameterizedTest
    @EnumSource(IncompleteRestore.class)
    void incompletePairingRefusesStartupWithoutGeneratingAReplacementOrChangingSourceBson(
            IncompleteRestore damage) {
        Snapshot captured = capture(SnapshotPhase.MIXED_AFTER_ROTATION);
        String target = freshName("incomplete_restore");
        try (MongoClient raw = MongoClients.create(MONGO.getReplicaSetUrl())) {
            MongoDatabase database = raw.getDatabase(target);
            restore(database, captured);
            switch (damage) {
                case MISSING_KEYRING -> SystemCollections.SYSTEM_META.on(database)
                        .deleteOne(new Document("_id", KEYRING_ID));
                case WRONG_KEYRING -> {
                    MongoDatabase other = raw.getDatabase(freshName("unrelated_keyring"));
                    new SourceConfigKeyringStore(other).loadOrCreateCipher();
                    SystemCollections.SYSTEM_META.on(database).replaceOne(new Document("_id", KEYRING_ID), keyring(other));
                }
                case MISSING_HISTORICAL_KEY -> {
                    Document ring = keyring(database);
                    String active = ring.getString("activeKeyId");
                    ring.put("keys", ring.getList("keys", Document.class).stream()
                            .filter(key -> active.equals(key.getString("id"))).toList());
                    SystemCollections.SYSTEM_META.on(database).replaceOne(new Document("_id", KEYRING_ID), ring);
                }
            }
            Document damagedKeyring = keyring(database);
            List<String> material = keyring(captured).getList("keys", Document.class).stream()
                    .map(key -> key.getString("material")).toList();
            TapstateErrorCode expectedCode = damage == IncompleteRestore.MISSING_KEYRING
                    ? StoreError.SOURCE_CONFIG_KEYRING_INVALID : IoError.DOCUMENT_UNREADABLE;
            try (MongoConnection connection = connection(target)) {
                assertThatThrownBy(connection::verify).isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(expectedCode);
                    assertThat(failure.getCause()).isNull();
                    StringWriter printed = new StringWriter();
                    failure.printStackTrace(new PrintWriter(printed));
                    assertThat(printed.toString()).doesNotContain(SECRET, "tscfg:");
                    material.forEach(value -> assertThat(printed.toString()).doesNotContain(value));
                });
            }
            assertThat(keyring(database)).isEqualTo(damagedKeyring);
            assertThat(SystemCollections.ARTIFACTS.on(database).find().into(new ArrayList<>()))
                    .containsExactlyInAnyOrderElementsOf(captured.artifacts);
        }
    }

    private Snapshot capture(SnapshotPhase phase) {
        String originalName = freshName("original_snapshot");
        try (MongoConnection original = connection(originalName)) {
            original.verify();
            MongoDatabase database = original.database();
            MongoArtifactStore store = new MongoArtifactStore(original.client(),
                    SystemCollections.ARTIFACTS.on(database), original.sourceConfigKeyring());
            store.saveAll(List.of(source("old_source"), source("second_source"), nonSource()));
            Document old = SystemCollections.ARTIFACTS.on(database).find(new Document("_id", "old_source")).first();
            String oldKey = SourceConfigCipher.envelopeKeyId(old.get("body", Document.class).getString("config"));
            if (phase != SnapshotPhase.BEFORE_ROTATION) {
                assertThat(original.sourceConfigKeyring().prepareRotation()).isEqualTo(2);
            }
            if (phase == SnapshotPhase.AFTER_ROTATION || phase == SnapshotPhase.MIXED_AFTER_ROTATION) {
                assertThat(original.sourceConfigKeyring().activatePrepared()).isEqualTo(3);
                assertThat(keyring(database).getString("activeKeyId")).isNotEqualTo(oldKey);
                assertThat(keyring(database).getList("keys", Document.class))
                        .anySatisfy(key -> assertThat(key).containsEntry("id", oldKey).containsEntry("state", "read-only"));
                if (phase == SnapshotPhase.MIXED_AFTER_ROTATION) {
                    // A paired snapshot may contain already-reencrypted and still-old envelopes.
                    // Restore the original authenticated envelope without inventing a new source id/hash.
                    SystemCollections.ARTIFACTS.on(database).replaceOne(new Document("_id", "old_source"), old);
                    assertThat(store.get("old_source")).contains(source("old_source"));
                    assertThat(SourceConfigCipher.envelopeKeyId(SystemCollections.ARTIFACTS.on(database)
                            .find(new Document("_id", "second_source")).first()
                            .get("body", Document.class).getString("config")))
                            .isEqualTo(keyring(database).getString("activeKeyId"));
                }
            }
            return new Snapshot(SystemCollections.ARTIFACTS.on(database).find().into(new ArrayList<>()),
                    SystemCollections.SYSTEM_META.on(database).find().into(new ArrayList<>()));
        }
    }

    private static void restore(MongoDatabase target, Snapshot snapshot) {
        // The witness restores these two collections into a new database. It does not export user
        // databases, indexes, sessions, history or a complete application backup.
        assertThat(SystemCollections.ARTIFACTS.on(target).countDocuments()).isZero();
        assertThat(SystemCollections.SYSTEM_META.on(target).countDocuments()).isZero();
        SystemCollections.ARTIFACTS.on(target).insertMany(snapshot.artifacts.stream()
                .map(Document::new).toList());
        SystemCollections.SYSTEM_META.on(target).insertMany(snapshot.systemMeta.stream()
                .map(Document::new).toList());
    }

    private static Document keyring(MongoDatabase database) {
        return SystemCollections.SYSTEM_META.on(database).find(new Document("_id", KEYRING_ID)).first();
    }

    private static void assertOriginalArtifacts(MongoDatabase database, Snapshot snapshot) {
        assertThat(SystemCollections.ARTIFACTS.on(database)
                .find(new Document("_id", new Document("$ne", "new_source"))).into(new ArrayList<>()))
                .containsExactlyInAnyOrderElementsOf(snapshot.artifacts);
    }

    private static Document keyring(Snapshot snapshot) {
        return snapshot.systemMeta.stream().filter(value -> KEYRING_ID.equals(value.get("_id"))).findFirst().orElseThrow();
    }

    private static MongoConnection connection(String database) {
        return new MongoConnection(new MongoConnectionSettings(
                MONGO.getReplicaSetUrl(database), null, Duration.ofSeconds(10)));
    }

    private static String freshName(String prefix) { return prefix + "_" + Long.toUnsignedString(System.nanoTime(), 16); }

    private static Resource source(String id) {
        return PARSER.parse("""
                version: tapstate/v1
                kind: source
                id: %s
                connector: unregistered
                metadata: { description: restored-source }
                config:
                  password: %s
                  nested: { publicValue: restored-public-value }
                """.formatted(id, SECRET));
    }

    private static Resource nonSource() {
        return PARSER.fromTree(Map.of("version", "tapstate/v1", "kind", "serve", "id", "non_source"));
    }

    private enum SnapshotPhase { BEFORE_ROTATION, PREPARED, AFTER_ROTATION, MIXED_AFTER_ROTATION }
    private enum IncompleteRestore { MISSING_KEYRING, WRONG_KEYRING, MISSING_HISTORICAL_KEY }
    private record Snapshot(List<Document> artifacts, List<Document> systemMeta) { }
}
