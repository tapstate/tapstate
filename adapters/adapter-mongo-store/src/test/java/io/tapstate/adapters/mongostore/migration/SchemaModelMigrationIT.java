package io.tapstate.adapters.mongostore.migration;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.adapters.mongostore.ChangeSet;
import io.tapstate.adapters.mongostore.MigrationError;
import io.tapstate.adapters.mongostore.MongoDerivedSchemaStore;
import io.tapstate.adapters.mongostore.MongoConnection;
import io.tapstate.adapters.mongostore.MongoConnectionSettings;
import io.tapstate.adapters.mongostore.MongoSchemaStore;
import io.tapstate.adapters.mongostore.SystemCollections;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Startup carries stored observations and complete drift history without opening a source. */
@RequiresDocker
class SchemaModelMigrationIT {

    @Container
    private static final MongoDBContainer REPLICA_SET =
            new MongoDBContainer(DockerImageName.parse("mongo:7.0"));

    private static MongoClient client;

    @AfterAll
    static void closeClient() {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void startupSplitsStoredDiscoveryWithoutOpeningItsUnavailableConnector() {
        MongoDatabase database = freshDatabase("schema_source_startup");
        MongoCollection<Document> sources = SystemCollections.SOURCE_SCHEMAS.on(database);
        Document old = source("source");
        sources.insertOne(old);
        sources.insertOne(source("source-other").append("tables", List.of()));
        SystemCollections.DERIVED_SCHEMAS.on(database).insertOne(history());

        migrateAtStartup(database);

        DiscoveredSourceModel read = new MongoSchemaStore(sources).get("source").orElseThrow();
        assertThat(read.connectorId()).isEqualTo("unavailable-connector");
        assertThat(read.discoveredAt()).isEqualTo(123L);
        assertThat(read.model().tables()).extracting(SourceTable::name).containsExactly("z.orders", "a");
        assertThat(read.model().tables().getFirst().fields().getFirst().type()).isEqualTo(TapstateType.INT64);
        assertThat(read.model().tables().getFirst().approximateRowCount()).isEqualTo(42L);
        assertThat(read.model().tables().getFirst().indexes().getFirst().unique()).isTrue();
        assertThat(new MongoSchemaStore(sources).get("source-other").orElseThrow().model().tables()).isEmpty();
        Document envelope = sources.find(new Document("_id", "source")).first();
        assertThat(envelope).doesNotContainKey("tables");
        assertThat(envelope.getInteger("modelVersion")).isEqualTo(2);
        List<Document> moved = sources.find(new Document("generation", envelope.getString("generation")))
                .filter(new Document("order", new Document("$exists", true)))
                .sort(new Document("order", 1)).into(new ArrayList<>());
        moved.forEach(table -> { table.remove("_id"); table.remove("generation"); table.remove("order"); });
        assertThat(moved).containsExactlyElementsOf(old.getList("tables", Document.class));
        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(6);
        assertThat(SystemCollections.DERIVED_SCHEMAS.on(database)
                .find(new Document("_id", "pipeline.src.orders")).first()).isNotNull();
    }

    @Test
    void preResolutionObservationsRetainUnknownTypesAndRediscoveryReplacesTheMigration() {
        MongoDatabase database = freshDatabase("schema_unresolved_source");
        MongoCollection<Document> sources = SystemCollections.SOURCE_SCHEMAS.on(database);
        Document old = source("source");
        old.remove("modelVersion");
        old.getList("tables", Document.class).getFirst().getList("fields", Document.class)
                .getFirst().remove("tapstateType");
        sources.insertOne(old);

        migrateAtStartup(database);

        MongoSchemaStore store = new MongoSchemaStore(sources);
        SourceField field = store.get("source").orElseThrow().model().tables().getFirst().fields().getFirst();
        assertThat(field.type()).isEqualTo(TapstateType.UNKNOWN);
        assertThat(field.unknownBecause()).contains("before a resolved type was kept");
        assertThat(field.dataType()).isEqualTo("bigint");
        store.save(new DiscoveredSourceModel("source", "new-connector", 999L, new SourceModel(List.of())));
        assertThat(sources.countDocuments()).isEqualTo(1);
        assertThat(store.get("source").orElseThrow().model().tables()).isEmpty();
    }

    @Test
    void startupCarriesEveryVersionBeforeAnyPipelineIsReadOrWritten() {
        MongoDatabase database = freshDatabase("schema_history_startup");
        MongoCollection<Document> histories = SystemCollections.DERIVED_SCHEMAS.on(database);
        Document old = history();
        histories.insertOne(old);

        migrateAtStartup(database);

        assertThat(histories.find(new Document("_id", "pipeline")).first()).isNull();
        for (Document step : old.getList("steps", Document.class)) {
            assertThat(histories.find(new Document("_id", "pipeline." + step.getString("step")))
                    .first().get("versions")).isEqualTo(step.get("versions"));
        }
        MongoDerivedSchemaStore store = new MongoDerivedSchemaStore(histories);
        assertThat(store.latest("pipeline", "src.orders").orElseThrow().version()).isEqualTo(1L);
        store.record("pipeline", "src.orders", Map.of("id", "STRING NULL"), "s2", "f2", "b2");
        assertThat(store.latest("pipeline", "src.orders").orElseThrow().version()).isEqualTo(2L);
    }

    @Test
    void bothChangesetsAreIdempotentAndLeaveCurrentModelsAndPinsUnchanged() {
        MongoDatabase database = freshDatabase("schema_idempotent");
        MongoCollection<Document> sources = SystemCollections.SOURCE_SCHEMAS.on(database);
        MongoCollection<Document> histories = SystemCollections.DERIVED_SCHEMAS.on(database);
        new MongoSchemaStore(sources).save(new DiscoveredSourceModel("current", "mysql", 456L,
                new SourceModel(List.of(new SourceTable("t", List.of(new SourceField("id", "int")),
                        List.of("id"), List.of())))));
        MongoDerivedSchemaStore store = new MongoDerivedSchemaStore(histories);
        store.record("current", "node", Map.of("id", "INT64 NULL"), "s", "f", "b");
        store.pin("current", "node", 0);
        List<Document> currentSources = snapshot(sources);
        List<Document> currentHistory = snapshot(histories);
        sources.insertOne(source("source"));
        histories.insertOne(history());

        changeSet(5).up(database, ChangeSet.Fence.HELD);
        changeSet(6).up(database, ChangeSet.Fence.HELD);
        List<Document> onceSources = snapshot(sources);
        List<Document> onceHistories = snapshot(histories);
        assertThat(onceSources).containsAll(currentSources);
        assertThat(onceHistories).containsAll(currentHistory);
        changeSet(5).up(database, ChangeSet.Fence.HELD);
        changeSet(6).up(database, ChangeSet.Fence.HELD);
        assertThat(snapshot(sources)).isEqualTo(onceSources);
        assertThat(snapshot(histories)).isEqualTo(onceHistories);
        assertThat(store.pinned("current", "node")).isPresent();
    }

    @Test
    void physicalSplitStopsAtEachLostFenceAndResumesWithoutPublishingHalfADiscovery() {
        for (int stop = 1; stop <= 3; stop++) {
            MongoDatabase database = freshDatabase("schema_source_fence_" + stop);
            MongoCollection<Document> sources = SystemCollections.SOURCE_SCHEMAS.on(database);
            Document original = source("source");
            sources.insertOne(original);
            int failAt = stop;
            AtomicInteger writes = new AtomicInteger();
            assertThatThrownBy(() -> changeSet(5).up(database, () -> {
                if (writes.incrementAndGet() == failAt) {
                    throw new IllegalStateException("lost lease");
                }
            })).isInstanceOf(IllegalStateException.class).hasMessage("lost lease");
            assertThat(sources.find(new Document("_id", "source")).first()).isEqualTo(original);
            assertThat(sources.countDocuments()).isEqualTo(stop);
            changeSet(5).up(database, ChangeSet.Fence.HELD);
            assertThat(sources.countDocuments()).isEqualTo(3);
            assertThat(new MongoSchemaStore(sources).get("source").orElseThrow().model().tables()).hasSize(2);
        }
    }

    @Test
    void logicalSplitStopsAtEachLostFenceAndResumesWithoutDroppingHistory() {
        for (int stop = 1; stop <= 3; stop++) {
            MongoDatabase database = freshDatabase("schema_history_fence_" + stop);
            MongoCollection<Document> histories = SystemCollections.DERIVED_SCHEMAS.on(database);
            Document original = history();
            histories.insertOne(original);
            int failAt = stop;
            AtomicInteger writes = new AtomicInteger();
            assertThatThrownBy(() -> changeSet(6).up(database, () -> {
                if (writes.incrementAndGet() == failAt) {
                    throw new IllegalStateException("lost lease");
                }
            })).isInstanceOf(IllegalStateException.class).hasMessage("lost lease");
            assertThat(histories.find(new Document("_id", "pipeline")).first()).isEqualTo(original);
            assertThat(histories.countDocuments()).isEqualTo(stop);
            changeSet(6).up(database, ChangeSet.Fence.HELD);
            assertThat(histories.countDocuments()).isEqualTo(2);
            assertThat(histories.find(new Document("_id", "pipeline.src.orders")).first().get("versions"))
                    .isEqualTo(original.getList("steps", Document.class).getFirst().get("versions"));
        }
    }

    @Test
    void malformedSourceRefusesStartupWithoutMarkingTheChangeComplete() {
        MongoDatabase database = freshDatabase("schema_bad_source");
        MongoCollection<Document> sources = SystemCollections.SOURCE_SCHEMAS.on(database);
        Document bad = source("source").append("tables", List.of(new Document("fields", List.of())));
        sources.insertOne(bad);

        Throwable failure = catchThrowable(() -> migrateAtStartup(database));

        assertThat(failure).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) failure).code()).isEqualTo(MigrationError.CHANGESET_FAILED);
        assertThat(failure.getMessage()).contains("source", "name");
        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(4);
        assertThat(snapshot(sources)).containsExactly(bad);
        sources.replaceOne(new Document("_id", "source"), source("source"));
        migrateAtStartup(database);
        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(6);
    }

    @Test
    void malformedHistoryRefusesStartupWithoutDiscardingTheOriginal() {
        MongoDatabase database = freshDatabase("schema_bad_history");
        MongoCollection<Document> histories = SystemCollections.DERIVED_SCHEMAS.on(database);
        Document bad = history();
        bad.getList("steps", Document.class).getLast().getList("versions", Document.class)
                .getFirst().put("columns", "not-an-array");
        histories.insertOne(bad);

        Throwable failure = catchThrowable(() -> migrateAtStartup(database));

        assertThat(failure).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) failure).code()).isEqualTo(MigrationError.CHANGESET_FAILED);
        assertThat(failure.getMessage()).contains("pipeline", "columns");
        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(5);
        assertThat(histories.find(new Document("_id", "pipeline")).first()).isEqualTo(bad);
    }

    @Test
    void aPreviousPartialSplitKeepsExtendedHistoryAndItsPin() {
        MongoDatabase database = freshDatabase("schema_extended_history");
        MongoCollection<Document> histories = SystemCollections.DERIVED_SCHEMAS.on(database);
        Document old = history();
        histories.insertOne(old);
        List<Document> extended = new ArrayList<>(old.getList("steps", Document.class)
                .getFirst().getList("versions", Document.class));
        extended.add(version(2, "STRING NULL"));
        Document split = new Document("_id", "pipeline.src.orders").append("versions", extended).append("pin", 1L);
        histories.insertOne(split);

        migrateAtStartup(database);

        assertThat(histories.find(new Document("_id", "pipeline.src.orders")).first()).isEqualTo(split);
        assertThat(histories.find(new Document("_id", "pipeline")).first()).isNull();
    }

    @Test
    void contradictorySplitHistoryRefusesStartupInsteadOfChoosingWhichHistoryToLose() {
        MongoDatabase database = freshDatabase("schema_conflicting_history");
        MongoCollection<Document> histories = SystemCollections.DERIVED_SCHEMAS.on(database);
        Document old = history();
        histories.insertOne(old);
        Document split = new Document("_id", "pipeline.src.orders")
                .append("versions", List.of(version(0, "BOOLEAN NULL")));
        histories.insertOne(split);

        Throwable failure = catchThrowable(() -> migrateAtStartup(database));

        assertThat(failure).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) failure).code()).isEqualTo(MigrationError.CHANGESET_FAILED);
        assertThat(histories.find(new Document("_id", "pipeline")).first()).isEqualTo(old);
        assertThat(histories.find(new Document("_id", "pipeline.src.orders")).first()).isEqualTo(split);
        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(5);
    }

    @Test
    void inspectionCountsPendingEnvelopesWithoutWriting() {
        MongoDatabase database = freshDatabase("schema_dry_run");
        MongoCollection<Document> sources = SystemCollections.SOURCE_SCHEMAS.on(database);
        MongoCollection<Document> histories = SystemCollections.DERIVED_SCHEMAS.on(database);
        sources.insertOne(source("source"));
        histories.insertOne(history());
        List<Document> beforeSources = snapshot(sources);
        List<Document> beforeHistory = snapshot(histories);

        assertThat(changeSet(5).dryRunSummary(database)).contains("1");
        assertThat(changeSet(6).dryRunSummary(database)).contains("1");
        assertThat(snapshot(sources)).isEqualTo(beforeSources);
        assertThat(snapshot(histories)).isEqualTo(beforeHistory);
    }

    @Test
    void aConflictingPhysicalSplitRefusesStartupWithoutOverwritingEitherObservation() {
        MongoDatabase database = freshDatabase("schema_conflicting_source");
        MongoCollection<Document> sources = SystemCollections.SOURCE_SCHEMAS.on(database);
        Document original = source("source");
        sources.insertOne(original);
        Document conflicting = new Document("_id", "source.migration-v5.z.orders")
                .append("name", "z.orders").append("fields", List.of()).append("generation", "migration-v5")
                .append("order", 0);
        sources.insertOne(conflicting);

        Throwable failure = catchThrowable(() -> migrateAtStartup(database));

        assertThat(failure).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) failure).code()).isEqualTo(MigrationError.CHANGESET_FAILED);
        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(4);
        assertThat(sources.find(new Document("_id", "source")).first()).isEqualTo(original);
        assertThat(sources.find(new Document("_id", conflicting.get("_id"))).first()).isEqualTo(conflicting);
    }

    @Test
    void anOldPhysicalEnvelopeMissingItsShapeCannotSilentlyDisappearFromTheReadModel() {
        MongoDatabase database = freshDatabase("schema_missing_source_shape");
        MongoCollection<Document> sources = SystemCollections.SOURCE_SCHEMAS.on(database);
        Document broken = new Document("_id", "source");
        sources.insertOne(broken);

        Throwable failure = catchThrowable(() -> migrateAtStartup(database));

        assertThat(failure).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) failure).code()).isEqualTo(MigrationError.CHANGESET_FAILED);
        assertThat(failure.getMessage()).contains("source", "connectorId");
        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(4);
        assertThat(snapshot(sources)).containsExactly(broken);
    }

    @Test
    void anOldLogicalEnvelopeMissingItsStepsCannotSilentlyResetTheDriftBaseline() {
        MongoDatabase database = freshDatabase("schema_missing_history_shape");
        MongoCollection<Document> histories = SystemCollections.DERIVED_SCHEMAS.on(database);
        Document broken = new Document("_id", "pipeline");
        histories.insertOne(broken);

        Throwable failure = catchThrowable(() -> migrateAtStartup(database));

        assertThat(failure).isInstanceOf(TapstateException.class);
        assertThat(((TapstateException) failure).code()).isEqualTo(MigrationError.CHANGESET_FAILED);
        assertThat(failure.getMessage()).contains("pipeline", "steps");
        assertThat(MigrationRunner.inspect(database).installed()).isEqualTo(5);
        assertThat(snapshot(histories)).containsExactly(broken);
    }

    private static void migrateAtStartup(MongoDatabase database) {
        try (MongoConnection connection = new MongoConnection(new MongoConnectionSettings(
                REPLICA_SET.getReplicaSetUrl(database.getName()), null, Duration.ofSeconds(10)))) {
            connection.verify();
        }
    }

    private static ChangeSet changeSet(int version) {
        return MigrationRunner.changeSets().stream().filter(candidate -> candidate.version() == version)
                .findFirst().orElseThrow();
    }

    private static List<Document> snapshot(MongoCollection<Document> collection) {
        return collection.find().sort(new Document("_id", 1)).into(new ArrayList<>());
    }

    private static Document source(String id) {
        return new Document("_id", id).append("modelVersion", 1).append("connectorId", "unavailable-connector")
                .append("discoveredAt", 123L).append("tables", List.of(
                        new Document("name", "z.orders")
                                .append("fields", List.of(new Document("name", "id").append("type", "bigint")
                                        .append("tapstateType", "INT64")))
                                .append("primaryKey", List.of("id"))
                                .append("indexes", List.of(new Document("name", "pk").append("fields", List.of("id"))
                                        .append("unique", true))).append("approximateRowCount", 42L),
                        new Document("name", "a").append("fields", List.of()).append("primaryKey", List.of())
                                .append("indexes", List.of())));
    }

    private static Document history() {
        return new Document("_id", "pipeline").append("steps", List.of(
                new Document("step", "src.orders").append("versions", List.of(
                        version(0, "INT64 NOT NULL"), version(1, "DECIMAL NULL"))),
                new Document("step", "other").append("versions", List.of(version(0, "STRING NULL")))));
    }

    private static Document version(long number, String type) {
        return new Document("version", number).append("columns", List.of(new Document("name", "id").append("type", type)))
                .append("statement", "s" + number).append("derivedFrom", "f" + number).append("derivedBy", "b" + number);
    }

    private static MongoDatabase freshDatabase(String name) {
        if (client == null) {
            client = MongoClients.create(REPLICA_SET.getReplicaSetUrl());
        }
        MongoDatabase database = client.getDatabase(name);
        database.drop();
        SystemCollections.SYSTEM_META.on(database).insertOne(new Document("_id", "schema").append("installedVersion", 4));
        return database;
    }
}
