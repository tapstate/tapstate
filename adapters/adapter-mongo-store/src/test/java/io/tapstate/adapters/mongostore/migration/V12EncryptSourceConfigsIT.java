package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.adapters.mongostore.MongoArtifactStore;
import io.tapstate.adapters.mongostore.SourceConfigCipher;
import io.tapstate.adapters.mongostore.SourceConfigKeyringStore;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.DslParser;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.core.model.canonical.CanonicalWriter;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Carries legacy plaintext Source documents into the keyring-backed physical format. */
@RequiresDocker
class V12EncryptSourceConfigsIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final DslParser PARSER = new DslParser();
    private static final CanonicalWriter WRITER = new CanonicalWriter();

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    private static MongoClient client;

    @AfterAll
    static void closeClient() {
        if (client != null) client.close();
    }

    @Test
    void migrationEncryptsOnlySourceConfigAndKeepsLogicalHashAndRoundTrip() {
        MongoDatabase database = freshDatabase("v12_encrypt_sources");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        Resource source = PARSER.parse("""
                version: tapstate/v1
                kind: source
                id: orders
                connector: unknown
                config:
                  password: migration-secret
                  uri: mongodb://migration-user:migration-password@example.invalid/orders
                  nested:
                    token: nested-secret
                """);
        Resource pipeline = PARSER.parse("""
                version: tapstate/v1
                kind: pipeline
                id: orders-copy
                source: orders
                """);
        Document plaintextSource = stored(source);
        Document originalPipeline = stored(pipeline);
        artifacts.insertMany(java.util.List.of(plaintextSource, originalPipeline));
        seedVersion(database, 11);

        MigrationRunner.migrate(database);

        Document encryptedSource = artifacts.find(new Document("_id", "orders")).first();
        Object storedConfig = encryptedSource.get("body", Document.class).get("config");
        assertThat(storedConfig).isInstanceOf(String.class);
        assertThat((String) storedConfig).startsWith("tscfg:1:");
        assertThat(encryptedSource.toJson())
                .doesNotContain("migration-secret", "migration-user", "migration-password", "nested-secret");
        assertThat(encryptedSource.getString("contentHash")).isEqualTo(CanonicalHash.of(source));
        assertThat(artifacts.find(new Document("_id", "orders-copy")).first()).isEqualTo(originalPipeline);
        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(12);

        SourceConfigCipher cipher = new SourceConfigKeyringStore(database).loadExistingCipher();
        MongoArtifactStore store = new MongoArtifactStore(client, artifacts, cipher);
        assertThat(WRITER.write(store.get("orders").orElseThrow())).isEqualTo(WRITER.write(source));

        String firstEnvelope = (String) storedConfig;
        seedVersion(database, 11);
        MigrationRunner.migrate(database);
        assertThat(artifacts.find(new Document("_id", "orders")).first()
                .get("body", Document.class).getString("config")).isEqualTo(firstEnvelope);
    }

    @Test
    void invalidLogicalIdentityFailsWithoutLeakingConfigAndARepairCanResume() {
        MongoDatabase database = freshDatabase("v12_invalid_identity");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        Resource source = PARSER.parse("""
                version: tapstate/v1
                kind: source
                id: orders
                connector: mysql
                config:
                  password: migration-failure-secret
                """);
        Document corrupt = stored(source);
        corrupt.put("contentHash", "wrong-hash");
        artifacts.insertOne(corrupt);
        seedVersion(database, 11);

        assertThatThrownBy(() -> MigrationRunner.migrate(database))
                .isInstanceOfSatisfying(TapstateException.class, error -> {
                    assertThat(error.code()).isEqualTo(io.tapstate.adapters.mongostore.MigrationError.CHANGESET_FAILED);
                    StringWriter printed = new StringWriter();
                    error.printStackTrace(new PrintWriter(printed));
                    assertThat(printed.toString()).doesNotContain("migration-failure-secret");
                });
        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(11);
        assertThat(artifacts.find(new Document("_id", "orders")).first()
                .get("body", Document.class).get("config")).isInstanceOf(Document.class);

        artifacts.updateOne(new Document("_id", "orders"),
                new Document("$set", new Document("contentHash", CanonicalHash.of(source))));
        MigrationRunner.migrate(database);

        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(12);
        assertThat(artifacts.find(new Document("_id", "orders")).first()
                .get("body", Document.class).get("config")).isInstanceOf(String.class);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void restoredCiphertextWithARegressedSchemaNeverCreatesAnUnrelatedReplacementKey(boolean unknownEnvelope) {
        MongoDatabase database = freshDatabase("v12_incomplete_restore_" + unknownEnvelope);
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        MongoCollection<Document> systemMeta = SystemCollections.SYSTEM_META.on(database);
        Resource source = PARSER.parse("""
                version: tapstate/v1
                kind: source
                id: restored_source
                connector: unknown
                config: { password: incomplete-restore-secret }
                """);
        artifacts.insertOne(stored(source));
        seedVersion(database, 11);
        MigrationRunner.migrate(database);
        Document original = artifacts.find(new Document("_id", source.id())).first();
        Document keyring = systemMeta.find(new Document("_id", "source-config-keyring")).first();
        String material = keyring.getList("keys", Document.class).getFirst().getString("material");
        if (unknownEnvelope) {
            artifacts.updateOne(new Document("_id", source.id()), new Document("$set",
                    new Document("body.config", "tscfg:999:incomplete-restore-input")));
        }
        Document restored = artifacts.find(new Document("_id", source.id())).first();
        assertThat(systemMeta.deleteOne(new Document("_id", "source-config-keyring")).getDeletedCount())
                .isEqualTo(1);
        seedVersion(database, 11);

        assertThatThrownBy(() -> MigrationRunner.migrate(database))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(io.tapstate.adapters.mongostore.MigrationError.CHANGESET_FAILED);
                    StringWriter printed = new StringWriter();
                    failure.printStackTrace(new PrintWriter(printed));
                    assertThat(printed.toString()).doesNotContain(material, "incomplete-restore-secret",
                            "tscfg:", "incomplete-restore-input");
                });
        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(11);
        assertThat(artifacts.find(new Document("_id", source.id())).first()).isEqualTo(restored);
        assertThat(systemMeta.countDocuments(new Document("_id", "source-config-keyring")))
                .as("ciphertext needs its restored keys; startup must not manufacture a replacement")
                .isZero();

        // Repairing the actual pair, rather than accepting a newly generated key, permits reentry.
        systemMeta.insertOne(keyring);
        artifacts.replaceOne(new Document("_id", source.id()), original);
        MigrationRunner.migrate(database);
        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(12);
        assertThat(artifacts.find(new Document("_id", source.id())).first()).isEqualTo(original);
        assertThat(new MongoArtifactStore(client, artifacts,
                new SourceConfigKeyringStore(database).loadExistingCipher()).get(source.id())).contains(source);
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 11})
    void bothLegacyCanonicalAndStructuredSourcesMigrateAcrossEveryConnectorAndAnEmptyConfig(int legacyVersion) {
        MongoDatabase database = freshDatabase("v12_legacy_matrix_" + legacyVersion);
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        List<Resource> expected = new ArrayList<>();
        for (String connector : List.of("mongodb", "mongodb-atlas", "mysql", "oracle", "aws-rds-mysql", "unknown")) {
            expected.add(PARSER.parse("""
                    version: tapstate/v1
                    kind: source
                    id: legacy_%s
                    connector: %s
                    metadata: { description: retained-owner-facts }
                    config:
                      uri: mongodb://legacy-user:legacy-uri-secret@host.example/data
                      password: legacy-password-secret
                      nested: { extra: legacy-unmarked-value }
                      flags: [true, false, 123]
                    """.formatted(connector.replace('-', '_'), connector)));
        }
        expected.add(PARSER.parse("""
                version: tapstate/v1
                kind: source
                id: empty_legacy
                connector: unknown
                config: {}
                """));
        expected.add(PARSER.parse("""
                version: tapstate/v1
                kind: transform
                id: unchanged_transform
                type: map
                fields: { id: $id }
                """));
        artifacts.insertMany(expected.stream().map(resource -> legacyVersion == 1
                ? new Document("_id", resource.id()).append("kind", resource.kind())
                        .append("canonical", WRITER.write(resource)).append("contentHash", CanonicalHash.ofText(WRITER.write(resource)))
                : stored(resource)).toList());
        if (legacyVersion == 1) {
            for (Resource resource : expected) {
                assertThat(artifacts.find(new Document("_id", resource.id())).first().getString("contentHash"))
                        .isNotEqualTo(CanonicalHash.of(resource));
            }
        }
        seedVersion(database, legacyVersion);

        MigrationRunner.migrate(database);

        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(12);
        MongoArtifactStore store = new MongoArtifactStore(client, artifacts,
                new SourceConfigKeyringStore(database).loadExistingCipher());
        for (Resource resource : expected) {
            assertThat(store.get(resource.id())).contains(resource);
            Document actual = artifacts.find(new Document("_id", resource.id())).first();
            assertThat(actual).doesNotContainKey("canonical");
            assertThat(actual.getString("contentHash")).isEqualTo(CanonicalHash.of(resource));
            if (resource.kind().equals("source")) {
                assertThat(actual.get("body", Document.class).get("config"))
                        .isInstanceOf(String.class).asString().startsWith("tscfg:1:");
                assertThat(actual.toJson()).doesNotContain("legacy-user", "legacy-uri-secret",
                        "legacy-password-secret", "legacy-unmarked-value", "host.example");
            } else {
                // BSON decoding turns nested maps into Documents. Compare their encoded storage
                // shapes rather than Document-vs-Map Java equality for identical nested content.
                assertThat(actual.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry()))
                        .isEqualTo(stored(resource).toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry()));
            }
        }
        List<Document> once = artifacts.find().sort(new Document("_id", 1)).into(new ArrayList<>());
        Document keyring = SystemCollections.SYSTEM_META.on(database)
                .find(new Document("_id", "source-config-keyring")).first();
        seedVersion(database, 11);
        MigrationRunner.migrate(database);
        assertThat(artifacts.find().sort(new Document("_id", 1)).into(new ArrayList<>())).isEqualTo(once);
        assertThat(SystemCollections.SYSTEM_META.on(database)
                .find(new Document("_id", "source-config-keyring")).first()).isEqualTo(keyring);
    }

    @Test
    void aConcurrentPlaintextEditRefusesTheMigrationCasAndTheNextStartEncryptsTheWinningVersion() {
        MongoDatabase database = freshDatabase("v12_concurrent_edit");
        MongoCollection<Document> artifacts = SystemCollections.ARTIFACTS.on(database);
        Resource original = PARSER.parse("""
                version: tapstate/v1
                kind: source
                id: concurrent_source
                connector: mysql
                config: { password: stale-migration-secret }
                """);
        Resource replacement = PARSER.parse("""
                version: tapstate/v1
                kind: source
                id: concurrent_source
                connector: mysql
                metadata: { description: concurrent-winner }
                config: { password: winning-migration-secret }
                """);
        artifacts.insertOne(stored(original));
        seedVersion(database, 11);
        AtomicBoolean edited = new AtomicBoolean();
        CommandListener editBeforeEncryptionCas = new CommandListener() {
            @Override public void commandStarted(CommandStartedEvent event) {
                if (!event.getDatabaseName().equals(database.getName()) || !event.getCommandName().equals("update")) return;
                var command = event.getCommand();
                if (!command.getString("update").getValue().equals(SystemCollections.ARTIFACTS.collectionName())) return;
                var query = command.getArray("updates").get(0).asDocument().getDocument("q");
                if (query.containsKey("_id") && query.getString("_id").getValue().equals(original.id())
                        && edited.compareAndSet(false, true)) {
                    assertThat(artifacts.replaceOne(new Document("_id", original.id()), stored(replacement)).getModifiedCount())
                            .isEqualTo(1);
                }
            }
        };
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl()))
                .addCommandListener(editBeforeEncryptionCas).build();
        try (MongoClient migrating = MongoClients.create(settings)) {
            assertThatThrownBy(() -> MigrationRunner.migrate(migrating.getDatabase(database.getName())))
                    .isInstanceOfSatisfying(TapstateException.class, failure -> {
                        assertThat(failure.code()).isEqualTo(io.tapstate.adapters.mongostore.MigrationError.CHANGESET_FAILED);
                        assertThat(failure.getCause()).isInstanceOf(IllegalStateException.class)
                                .hasMessage("Source config changed while it was being encrypted: concurrent_source")
                                .hasNoCause();
                        StringWriter printed = new StringWriter();
                        failure.printStackTrace(new PrintWriter(printed));
                        assertThat(printed.toString()).doesNotContain("stale-migration-secret", "winning-migration-secret");
                    });
        }
        assertThat(edited).isTrue();
        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(11);
        assertThat(artifacts.find(new Document("_id", original.id())).first()
                .toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry()))
                .isEqualTo(stored(replacement).toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry()));

        MigrationRunner.migrate(database);

        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(12);
        MongoArtifactStore store = new MongoArtifactStore(client, artifacts,
                new SourceConfigKeyringStore(database).loadExistingCipher());
        assertThat(store.get(original.id())).contains(replacement);
        assertThat(artifacts.find(new Document("_id", original.id())).first().getString("contentHash"))
                .isEqualTo(CanonicalHash.of(replacement));
        assertThat(artifacts.find(new Document("_id", original.id())).first().toJson())
                .doesNotContain("stale-migration-secret", "winning-migration-secret");
    }

    private static Document stored(Resource resource) {
        return new Document("_id", resource.id())
                .append("kind", resource.kind())
                .append("body", new Document(WRITER.tree(resource)))
                .append("contentHash", CanonicalHash.of(resource));
    }

    private static void seedVersion(MongoDatabase database, int version) {
        SystemCollections.SYSTEM_META.on(database).replaceOne(
                new Document("_id", "schema"),
                new Document("_id", "schema").append("installedVersion", version),
                new com.mongodb.client.model.ReplaceOptions().upsert(true));
    }

    private static MongoDatabase freshDatabase(String name) {
        if (client == null) client = MongoClients.create(REPLICA_SET.getReplicaSetUrl());
        MongoDatabase database = client.getDatabase(name);
        database.drop();
        return database;
    }
}
