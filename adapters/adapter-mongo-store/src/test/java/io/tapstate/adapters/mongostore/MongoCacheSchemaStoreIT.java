package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.DiscoveredSourceModel;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SchemaStore;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceIndex;
import io.tapstate.spi.store.SourceModel;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Source discoveries retain complete generations and their original CACHE publication authority. */
@RequiresDocker
class MongoCacheSchemaStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final CacheAuthScope AUTH = new CacheAuthScope("cluster-a", 1, StorageBinding.Mode.OP, "cache-a", 1);
    private static final Runnable ACTIVE = () -> {};
    private static DiscoveredSourceModel discovery(String id, String marker, long at) {
        var numeric = new io.tapstate.core.common.NumericType(64, true, false, false, null, null, 12, 2);
        var text = new io.tapstate.core.common.StringType(36L, false, true, 1L, 2);
        return new DiscoveredSourceModel(id, "example", at, new SourceModel(List.of(
                new SourceTable(marker + ".orders", List.of(new SourceField("id", "bigint", TapstateType.INT64, null, numeric),
                        new SourceField("note", "varchar(36)", TapstateType.STRING, null, null, text)), List.of("id"),
                        List.of(new SourceIndex("pk", List.of("id"), true)), 45L),
                new SourceTable(marker + "/empty", List.of(), List.of(), List.of(), null))));
    }

    @Test void completeSchemaAndDescriptorMetadataRoundTripWhileOtherConnectionsRemainIndependent() {
        withFixture(f -> {
            var store = f.store(); var original = discovery("source-a", "first", 123L); var other = discovery("source-b", "other", 124L);
            assertThat(store.get("source-a")).isEmpty(); store.save(original); store.save(other);
            assertThat(store.get("source-a")).contains(original); assertThat(store.get("source-b")).contains(other);
            store.save(discovery("source-a", "second", 125L)); assertThat(store.get("source-a")).contains(discovery("source-a", "second", 125L));
            assertThat(store.get("source-b")).contains(other); assertThat(f.rows().countDocuments()).isEqualTo(6);
            assertThat(f.envelope("source-a").get("_revision")).isInstanceOf(Long.class);
            assertThat(f.envelope("source-a").get("_writers", Document.class)).isEmpty();
        });
    }

    @Test void anEmptyCompleteDiscoveryIsDistinctFromAnUnpublishedPendingEnvelope() {
        withFixture(f -> {
            var empty = new DiscoveredSourceModel("source-a", "example", 1L, new SourceModel(List.of()));
            f.store().save(empty); assertThat(f.store().get("source-a")).contains(empty);
            assertThat(f.envelope("source-a")).containsEntry("tableCount", 0); assertThat(f.rows().countDocuments()).isEqualTo(1);
        });
    }

    @Test void aSchemaLargerThanOneBsonDocumentStagesTableBatchesOutsideItsSmallPublicationTransaction() {
        withFixture(f -> {
            var fields = new ArrayList<SourceField>(); String padding = "x".repeat(6000);
            for (int i = 0; i < 1000; i++) fields.add(new SourceField("field" + i + padding, "text", TapstateType.STRING, null));
            var tables = new ArrayList<SourceTable>(); for (int i = 0; i < 3; i++) tables.add(new SourceTable("table" + i, fields, List.of(), List.of()));
            var large = new DiscoveredSourceModel("large-source", "example", 1L, new SourceModel(tables));
            var inserted = new AtomicInteger(); var published = new AtomicInteger();
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
                if (tableInsert(e)) { assertThat(e.getCommand().containsKey("autocommit")).isFalse(); inserted.incrementAndGet(); }
                if (publishes(e)) { assertThat(e.getCommand().getBoolean("autocommit").getValue()).isFalse(); published.incrementAndGet(); }
            } };
            try (var observed = client(listener)) { f.store(observed, ACTIVE).save(large); }
            assertThat(inserted.get()).isGreaterThanOrEqualTo(1); assertThat(published).hasValue(1);
            assertThat(f.store().get("large-source")).contains(large); assertThat(f.rows().countDocuments()).isEqualTo(4);
        });
    }

    @Test void tableStagingCannotReplaceThePreviouslyPublishedDiscoveryOrExposeAPendingFirstDiscovery() {
        for (boolean existing : List.of(false, true)) withFixture(f -> {
            if (existing) f.store().save(discovery("source-a", "old", 1L));
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
                if (tableInsert(e) && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
            } };
            try (var observed = client(listener); var worker = Executors.newSingleThreadExecutor()) {
                var future = worker.submit(() -> f.store(observed, ACTIVE).save(discovery("source-a", "new", 2L)));
                try {
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
                    if (existing) assertThat(f.store().get("source-a")).contains(discovery("source-a", "old", 1L));
                    else assertThat(f.store().get("source-a")).isEmpty();
                } finally { proceed.countDown(); }
                future.get(15, TimeUnit.SECONDS); assertThat(f.store().get("source-a")).contains(discovery("source-a", "new", 2L));
            }
        });
    }

    @Test void aCompetingDiscoveryProtectsAnActiveStagedWriterAndBothPublishCompleteGenerations() {
        withFixture(f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1);
            try (var observed = client(beforePublication(paused, proceed)); var worker = Executors.newSingleThreadExecutor()) {
                var original = worker.submit(() -> f.store(observed, ACTIVE).save(discovery("source-a", "one", 1L)));
                try {
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue(); f.store().save(discovery("source-a", "two", 2L));
                    assertThat(f.store().get("source-a")).contains(discovery("source-a", "two", 2L));
                    assertThat(f.rows().countDocuments()).isEqualTo(5); assertThat(f.envelope("source-a").get("_writers", Document.class)).hasSize(1);
                } finally { proceed.countDown(); }
                original.get(15, TimeUnit.SECONDS); assertThat(f.store().get("source-a")).contains(discovery("source-a", "one", 1L));
                assertThat(f.rows().countDocuments()).isEqualTo(3);
            }
        });
    }

    @Test void anExpiredWriterRevokedByReclamationCannotPublishItsLateGeneration() {
        withFixture(f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1);
            try (var observed = client(beforePublication(paused, proceed)); var worker = Executors.newSingleThreadExecutor()) {
                var original = worker.submit(() -> f.store(observed, ACTIVE).save(discovery("source-a", "late", 1L)));
                try {
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue(); String writer = f.envelope("source-a").get("_writers", Document.class).keySet().iterator().next();
                    f.rows().updateOne(new Document("_id", "source-a"), new Document("$set", new Document("_writers." + writer, new Date(0))));
                    f.store().save(discovery("source-a", "current", 2L)); assertThat(f.rows().countDocuments()).isEqualTo(3);
                } finally { proceed.countDown(); }
                assertThatThrownBy(() -> original.get(15, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class)
                        .satisfies(e -> assertThat(((TapstateException) e.getCause()).code()).isEqualTo(IoError.SCHEMA_WRITE_CONTENTION));
                assertThat(f.store().get("source-a")).contains(discovery("source-a", "current", 2L)); assertThat(f.rows().countDocuments()).isEqualTo(3);
            }
        });
    }

    @Test void retirementBeforePublicationRefusesLateWriterWithoutMakingItsTablesVisible() {
        withFixture(f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1);
            try (var observed = client(beforePublication(paused, proceed)); var worker = Executors.newSingleThreadExecutor()) {
                var future = worker.submit(() -> f.store(observed, ACTIVE).save(discovery("source-a", "late", 1L)));
                try { assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue(); f.domain.retireBusiness(SCOPE, "retire-schema", ACTIVE); }
                finally { proceed.countDown(); }
                assertThatThrownBy(() -> future.get(15, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class)
                        .satisfies(e -> assertThat(((TapstateException) e.getCause()).code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED));
                assertThat(f.envelope("source-a")).doesNotContainKey("generation"); assertThat(f.admissions()).isEqualTo(1);
            }
        });
    }

    @Test void guardLossAfterEnvelopeReplacementRollsBackTheVisibleGenerationAndRootAdmission() {
        withFixture(f -> {
            f.store().save(discovery("source-a", "old", 1L)); long before = f.admissions(); var live = new AtomicBoolean(true);
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) { if (publishes(e)) live.set(false); } };
            try (var observed = client(listener)) {
                assertCode(() -> f.store(observed, guard(live)).save(discovery("source-a", "lost", 2L)), IoError.CACHE_BUSINESS_FENCED);
            }
            assertThat(f.store().get("source-a")).contains(discovery("source-a", "old", 1L)); assertThat(f.admissions()).isEqualTo(before + 1);
        });
    }

    @Test void anUnknownPublicationCommitReplyRetainsTheSameGenerationAndUploadedTables() {
        withFixture(f -> {
            var publishing = new AtomicBoolean(); var armed = new AtomicBoolean(); var commits = new AtomicInteger(); var inserts = new AtomicInteger();
            var publicationTransaction = new AtomicReference<BsonDocument>();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (tableInsert(e)) inserts.incrementAndGet();
                    if (publishes(e)) {
                        publishing.set(true);
                        publicationTransaction.set(new BsonDocument("lsid", e.getCommand().getDocument("lsid").clone())
                                .append("txnNumber", e.getCommand().getInt64("txnNumber")));
                    }
                    BsonDocument original = publicationTransaction.get();
                    if (e.getCommandName().equals("commitTransaction") && armed.get() && original != null
                            && original.getDocument("lsid").equals(e.getCommand().getDocument("lsid"))
                            && original.getInt64("txnNumber").equals(e.getCommand().getInt64("txnNumber"))) commits.incrementAndGet();
                }
                @Override public void commandSucceeded(CommandSucceededEvent e) {
                    if (e.getCommandName().equals("findAndModify") && publishing.get() && armed.compareAndSet(false, true)) {
                        f.client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                                .append("data", new Document("failCommands", List.of("commitTransaction"))
                                        .append("writeConcernError", new Document("code", 91).append("errmsg", "test reply lost"))
                                        .append("errorLabels", List.of("UnknownTransactionCommitResult"))));
                    }
                }
            };
            try (var observed = client(listener)) {
                try { f.store(observed, ACTIVE).save(discovery("source-a", "same", 1L)); }
                finally { f.client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
            }
            assertThat(inserts).hasValue(1); assertThat(commits.get()).isGreaterThanOrEqualTo(2);
            assertThat(f.store().get("source-a")).contains(discovery("source-a", "same", 1L)); assertThat(f.rows().countDocuments()).isEqualTo(3);
        });
    }

    @Test void missingTablesAndNoncanonicalControlFactsAreDiagnosedWithoutCoercionOrRawCauses() {
        withFixture(f -> {
            var store = f.store(); store.save(discovery("source-a", "old", 1L)); var saved = f.envelope("source-a");
            for (var mutation : List.of(new Document("_revision", 3), new Document("tableCount", 2.0d), new Document("discoveredAt", 1.5d),
                    new Document("cacheScope.extra", "private-sentinel"), new Document("_writers.bad", "private-sentinel"))) {
                f.rows().updateOne(new Document("_id", "source-a"), new Document("$set", mutation)); assertUnreadable(() -> store.get("source-a"));
                f.rows().replaceOne(new Document("_id", "source-a"), saved);
            }
            f.rows().deleteOne(new Document("order", 0)); assertUnreadable(() -> store.get("source-a"));
        });
    }

    @Test void missingTableFieldsNeverBecomeAnEmptyButApparentlyCompleteDiscoveredModel() {
        withFixture(f -> {
            f.store().save(discovery("source-a", "old", 1L));
            f.rows().updateOne(new Document("order", 0), new Document("$unset", new Document("fields", "")));
            assertUnreadable(() -> f.store().get("source-a"));
        });
    }

    @Test void tableBodiesKeepTheCurrentCanonicalShapeWithoutLegacyDefaultOrNumericTruncation() {
        withFixture(f -> {
            f.store().save(discovery("source-a", "old", 1L)); var filter = new Document("order", 0); var saved = f.rows().find(filter).first();
            for (var mutation : List.of(new Document("fields.0.tapstateType", "private-sentinel"), new Document("approximateRowCount", 45.5d),
                    new Document("indexes.0.extra", "private-sentinel"), new Document("fields.0.numericType.extra", "private-sentinel"))) {
                f.rows().updateOne(filter, new Document("$set", mutation)); assertUnreadable(() -> f.store().get("source-a"));
                f.rows().replaceOne(filter, saved);
            }
            for (String field : List.of("fields", "primaryKey", "indexes", "approximateRowCount", "fields.0.tapstateType", "fields.0.numericType.precision")) {
                f.rows().updateOne(filter, new Document("$unset", new Document(field, ""))); assertUnreadable(() -> f.store().get("source-a"));
                f.rows().replaceOne(filter, saved);
            }
            assertThat(f.store().get("source-a")).contains(discovery("source-a", "old", 1L));
        });
    }

    @Test void aStaleReclaimerCannotAdoptARecreatedEnvelopeAtTheSameRevisionAndDeleteItsNewTables() {
        withFixture(f -> {
            var observed = new CountDownLatch(1); var oldProceed = new CountDownLatch(1);
            var newPaused = new CountDownLatch(1); var newProceed = new CountDownLatch(1);
            var aggregated = new AtomicBoolean(); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (e.getCommandName().equals("aggregate") && e.getCommand().getString("aggregate").getValue().equals("source_schemas")) aggregated.set(true);
                }
                @Override public void commandSucceeded(CommandSucceededEvent e) {
                    if (e.getCommandName().equals("commitTransaction") && aggregated.get() && once.compareAndSet(false, true)) {
                        observed.countDown(); await(oldProceed);
                    }
                }
            };
            var newRoots = new AtomicInteger(); var newListener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
                if (e.getCommandName().equals("update") && e.getCommand().getString("update").getValue().equals("cache_identity") && newRoots.incrementAndGet() == 3) {
                    newPaused.countDown(); await(newProceed);
                }
            } };
            try (var oldClient = client(listener); var newClient = client(newListener); var workers = Executors.newFixedThreadPool(2)) {
                var older = workers.submit(() -> f.store(oldClient, ACTIVE).save(discovery("source-a", "old", 1L)));
                try {
                    assertThat(observed.await(10, TimeUnit.SECONDS)).isTrue(); assertThat(f.envelope("source-a")).containsEntry("_revision", 3L);
                    f.rows().deleteOne(new Document("_id", "source-a"));
                    var newer = workers.submit(() -> f.store(newClient, ACTIVE).save(discovery("source-a", "new", 2L)));
                    try {
                        assertThat(newPaused.await(10, TimeUnit.SECONDS)).isTrue(); assertThat(f.envelope("source-a")).containsEntry("_revision", 3L);
                        oldProceed.countDown(); older.get(15, TimeUnit.SECONDS);
                        assertThat(f.store().get("source-a")).contains(discovery("source-a", "new", 2L));
                    } finally { newProceed.countDown(); }
                    newer.get(15, TimeUnit.SECONDS); assertThat(f.store().get("source-a")).contains(discovery("source-a", "new", 2L));
                } finally { oldProceed.countDown(); newProceed.countDown(); }
            }
        });
    }

    @Test void malformedTableOrderCannotSilentlyTruncateOrReorderASchema() {
        withFixture(f -> {
            f.store().save(discovery("source-a", "old", 1L)); f.rows().updateOne(new Document("order", 0), new Document("$set", new Document("order", 0.5d)));
            assertUnreadable(() -> f.store().get("source-a"));
        });
    }

    @Test void explicitRediscoveryRebuildsAForeignEnvelopeWithoutAdoptingOrDeletingItsGeneration() {
        withFixture(f -> {
            f.store().save(discovery("source-a", "old", 1L));
            f.rows().updateMany(new Document(), new Document("$set", new Document("cacheScope.cacheIncarnation", "previous-cache")));
            assertThat(f.store().get("source-a")).isEmpty(); f.store().save(discovery("source-a", "new", 2L));
            assertThat(f.store().get("source-a")).contains(discovery("source-a", "new", 2L));
            assertThat(f.rows().countDocuments(new Document("cacheScope.cacheIncarnation", "previous-cache"))).isEqualTo(2);
            assertThat(f.rows().countDocuments()).isEqualTo(5);
        });
    }

    @Test void authOnlyResetKeepsTheSchemaWhileClosedOrMissingBusinessResourcesNeverRepairIt() {
        withFixture(f -> {
            var store = f.store(); store.save(discovery("source-a", "old", 1L)); f.domain.advanceAuthGeneration(AUTH, 2, "auth-reset", ACTIVE);
            assertThat(store.get("source-a")).contains(discovery("source-a", "old", 1L));
            f.rows().drop(); assertCode(() -> f.domain.schemas(SCOPE, ACTIVE), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.database.listCollectionNames().into(new ArrayList<>())).doesNotContain("source_schemas");
            f.domain.close(); assertCode(() -> store.get("source-a"), IoError.CACHE_BUSINESS_FENCED);
        });
    }

    @Test void exhaustedRevisionCannotCommitAPartialReservationOrRootAdmission() {
        withFixture(f -> {
            f.store().save(discovery("source-a", "old", 1L));
            f.rows().updateOne(new Document("_id", "source-a"), new Document("$set", new Document("_revision", Long.MAX_VALUE - 1)));
            var before = f.envelope("source-a"); long admissions = f.admissions();
            assertUnreadable(() -> f.store().save(discovery("source-a", "new", 2L)));
            assertThat(f.envelope("source-a")).isEqualTo(before); assertThat(f.admissions()).isEqualTo(admissions);
        });
    }

    private static Runnable guard(AtomicBoolean live) { return () -> { if (!live.get()) throw MongoCacheBusinessIdentity.businessFenced(); }; }
    private static CommandListener beforePublication(CountDownLatch paused, CountDownLatch proceed) {
        var roots = new AtomicInteger(); return new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
            if (e.getCommandName().equals("update") && e.getCommand().getString("update").getValue().equals("cache_identity") && roots.incrementAndGet() == 2) {
                paused.countDown(); await(proceed);
            }
        } };
    }
    private static boolean tableInsert(CommandStartedEvent e) { return e.getCommandName().equals("insert")
            && e.getCommand().getString("insert").getValue().equals("source_schemas")
            && e.getCommand().getArray("documents").get(0).asDocument().containsKey("generation"); }
    private static boolean publishes(CommandStartedEvent e) { return e.getCommandName().equals("findAndModify")
            && e.getCommand().getString("findAndModify").getValue().equals("source_schemas"); }
    private static MongoClient client(CommandListener listener) { return MongoClients.create(MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build()); }
    private static void await(CountDownLatch latch) { try { if (!latch.await(15, TimeUnit.SECONDS)) throw new AssertionError("test barrier did not open"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); } }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, IoError code) { assertThatThrownBy(action)
            .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(code)); }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) { assertThatThrownBy(action)
            .isInstanceOfSatisfying(TapstateException.class, e -> { assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                assertThat(e.getCause()).isNull(); assertThat(e.args().toString()).doesNotContain("private-sentinel"); }); }
    private static void withFixture(Scenario scenario) {
        String name = "cache_schemas_" + UUID.randomUUID().toString().replace("-", "");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(name), name, null);
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl()); var domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "schema-test")) {
            var database = client.getDatabase(name);
            try {
                domain.prepareAuth(AUTH, "auth-a", ACTIVE);
                try (var session = client.startSession()) { session.withTransaction(() -> {
                    new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).prepareInTransaction(session, ACTIVE);
                    SystemCollections.CACHE_WORKLOAD_CLAIMS.prepareCollectionOn(database, session); return null;
                }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build()); }
                SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"), new Document("$set", new Document("businessPhase", "READY")
                        .append("businessPreparationId", "business-a").append("claimAdmissionFence", 0L)));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1).append("phase", "READY")
                        .append("epoch", 1L).append("operationId", "business-a"));
                scenario.run(new Fixture(client, database, domain));
            } finally { database.drop(); }
        } catch (RuntimeException | Error e) { throw e; }
        catch (Exception e) { throw new AssertionError(e); }
    }
    private record Fixture(MongoClient client, MongoDatabase database, MongoCacheDomain domain) {
        SchemaStore store() { return domain.schemas(SCOPE, ACTIVE); }
        SchemaStore store(MongoClient original, Runnable guard) { return new MongoCacheSchemaStore(new MongoCacheMetadataAccess(original,
                original.getDatabase(database.getName()), SCOPE, SystemCollections.CACHE_SOURCE_SCHEMAS, guard)); }
        MongoCollection<Document> rows() { return SystemCollections.CACHE_SOURCE_SCHEMAS.on(database); }
        Document envelope(String id) { return rows().find(new Document("_id", id)).first(); }
        long admissions() { return SystemCollections.CACHE_IDENTITY.on(database).find().first().getLong("claimAdmissionFence"); }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
