package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
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
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.PrintWriter;
import java.io.StringWriter;

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
