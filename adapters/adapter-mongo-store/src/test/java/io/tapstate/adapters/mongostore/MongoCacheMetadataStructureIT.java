package io.tapstate.adapters.mongostore;

import com.mongodb.ReadConcern;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Structure-only witnesses: no CACHE READY, NODE or runtime admission is published here. */
@RequiresDocker
class MongoCacheMetadataStructureIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final Duration RETENTION = Duration.ofDays(3);
    private static final TransactionOptions DDL = TransactionOptions.builder().readConcern(ReadConcern.LOCAL)
            .writeConcern(WriteConcern.MAJORITY.withJournal(true)).build();

    @Test void explicitPreparationCreatesOnlyTheApprovedMetadataStructuresAndConfiguredIndexes() {
        withFixture(f -> {
            f.prepare(() -> {}); f.structure().verify();
            assertThat(f.names()).containsExactlyInAnyOrder("source_schemas", "connector_catalog", "connector_specs", "connection_test_results",
                    "connector_artifacts.files", "connector_artifacts.chunks", "derived_schemas", "pipeline_rate_history");
            var ttl = SystemCollections.CACHE_PIPELINE_RATE_HISTORY.on(f.database).listIndexes().into(new ArrayList<>()).stream()
                    .filter(row -> row.containsKey("expireAfterSeconds")).findFirst().orElseThrow();
            assertThat(((Number) ttl.get("expireAfterSeconds")).longValue()).isEqualTo(RETENTION.toSeconds());
            var before = f.snapshot();
            f.prepare(() -> {}); f.structure().verify();
            assertThat(f.snapshot()).isEqualTo(before);
            assertThat(f.names()).doesNotContain("cache_identity", "schema_migrations", "workload_claims", "connections", "pipeline_state");
        });
    }

    @Test void ordinaryVerificationNeverCreatesMissingCollections() {
        withFixture(f -> {
            assertUnreadable(f.structure()::verify);
            assertThat(f.names()).isEmpty();
            f.prepare(() -> {});
            SystemCollections.CACHE_SOURCE_SCHEMAS.on(f.database).drop();
            var before = f.snapshot();
            assertUnreadable(f.structure()::verify); assertUnreadable(() -> f.prepare(() -> {}));
            assertThat(f.snapshot()).isEqualTo(before); assertThat(f.names()).doesNotContain("source_schemas");
        });
    }

    @Test void aChangedRetentionIsDiagnosedWithoutAnImplicitCollMod() {
        withFixture(f -> {
            f.prepare(() -> {}); var before = f.snapshot();
            assertUnreadable(() -> new MongoCacheMetadataStructure(f.database, Duration.ofDays(4)).verify());
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void missingGridFsIndexesAndUnexpectedTtlAreNotRepairedByOpenOrPreparationReplay() {
        withFixture(f -> {
            f.prepare(() -> {});
            SystemCollections.CACHE_CONNECTOR_ARTIFACTS.chunksOn(f.database).dropIndex("files_id_1_n_1");
            var before = f.snapshot();
            assertUnreadable(f.structure()::verify); assertUnreadable(() -> f.prepare(() -> {}));
            assertThat(f.snapshot()).isEqualTo(before);
        });
        withFixture(f -> {
            f.prepare(() -> {});
            SystemCollections.CACHE_CONNECTOR_CATALOG.on(f.database).createIndex(new Document("expiresAt", 1),
                    new com.mongodb.client.model.IndexOptions().expireAfter(60L, java.util.concurrent.TimeUnit.SECONDS));
            var before = f.snapshot(); assertUnreadable(f.structure()::verify);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void originalGuardLossRollsBackEveryCreatedCollectionAndIndex() {
        withFixture(f -> {
            var lastIndexRequest = new AtomicInteger(-1); var lastIndexCreated = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("createIndexes")
                            && event.getCommand().getString("createIndexes").getValue().equals("pipeline_rate_history")
                            && event.getCommand().getArray("indexes").stream().anyMatch(index -> index.asDocument().getString("name").getValue()
                                    .equals(SystemCollections.CACHE_PIPELINE_RATE_HISTORY.indexes().get(1).indexName()))) lastIndexRequest.set(event.getRequestId());
                }
                @Override public void commandSucceeded(CommandSucceededEvent event) {
                    if (event.getRequestId() == lastIndexRequest.get()) lastIndexCreated.set(true);
                }
            };
            try (var observed = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(listener).build())) {
                var original = new Fixture(observed, observed.getDatabase(f.database.getName()));
                assertUnreadable(() -> original.prepare(() -> { if (lastIndexCreated.get()) throw unreadable(); }));
            }
            assertThat(lastIndexCreated).as("the final declared index succeeded before the original guard was lost").isTrue();
            assertThat(f.names()).isEmpty();
            f.prepare(() -> {}); f.structure().verify();
        });
    }

    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) {
        assertThatThrownBy(action).isInstanceOfSatisfying(TapstateException.class, failure -> assertThat(failure.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE));
    }
    private static TapstateException unreadable() { return new TapstateException(IoError.DOCUMENT_UNREADABLE, Map.of("id", "cache-metadata", "field", "guard"), null); }
    private static void withFixture(Scenario scenario) {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("metadata_structure_" + UUID.randomUUID().toString().replace("-", ""));
            try { scenario.run(new Fixture(client, database)); }
            finally { database.drop(); }
        }
    }
    private record Fixture(MongoClient client, MongoDatabase database) {
        MongoCacheMetadataStructure structure() { return new MongoCacheMetadataStructure(database, RETENTION); }
        void prepare(Runnable active) {
            try (var session = client.startSession()) { session.withTransaction(() -> { structure().prepareInTransaction(session, active); return null; }, DDL); }
        }
        List<String> names() { return database.listCollectionNames().into(new ArrayList<>()); }
        Map<String, List<Document>> snapshot() {
            var result = new java.util.TreeMap<String, List<Document>>();
            for (var table : List.of(SystemCollections.CACHE_SOURCE_SCHEMAS, SystemCollections.CACHE_CONNECTOR_CATALOG,
                    SystemCollections.CACHE_CONNECTOR_SPECS, SystemCollections.CACHE_CONNECTION_TEST_RESULTS,
                    SystemCollections.CACHE_DERIVED_SCHEMAS, SystemCollections.CACHE_PIPELINE_RATE_HISTORY))
                if (names().contains(table.collectionName())) result.put(table.collectionName(), table.on(database).listIndexes().into(new ArrayList<>()));
            if (names().contains("connector_artifacts.files")) result.put("connector_artifacts.files", SystemCollections.CACHE_CONNECTOR_ARTIFACTS.indexTargetOn(database).listIndexes().into(new ArrayList<>()));
            if (names().contains("connector_artifacts.chunks")) result.put("connector_artifacts.chunks", SystemCollections.CACHE_CONNECTOR_ARTIFACTS.chunksOn(database).listIndexes().into(new ArrayList<>()));
            return result;
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture); }
}
