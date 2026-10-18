package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.DerivedSchemaStore;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Per-step models use the original prepared Pipeline state and an actual live claim write. */
@RequiresDocker
class MongoCacheDerivedSchemaStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster-a", 1, "cache-a", "orders", 1);
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final Runnable ACTIVE = () -> {};
    private static Map<String, String> columns(String type) {
        var out = new LinkedHashMap<String, String>(); out.put("order.id", "bigint"); out.put("note", type); return out;
    }
    private static void record(DerivedSchemaStore store, String type) { store.record("orders", "source.orders", columns(type), "statement", "sources", "derivation"); }

    @Test void historyAppendsOnlyOnShapeChangeAndPreservesAPinDuringProvenanceRefresh() {
        withFixture(f -> {
            var writer = f.writer(); var reader = f.domain.pipelineDerivedSchemas(SCOPE);
            assertThat(reader.latest("orders", "source.orders")).isEmpty(); record(writer, "text"); writer.pin("orders", "source.orders", 0);
            writer.record("orders", "source.orders", columns("text"), "edited", "new-source", "v2");
            assertThat(reader.latest("orders", "source.orders").orElseThrow().version()).isZero();
            assertThat(reader.pinned("orders", "source.orders").orElseThrow().derivedBy()).isEqualTo("v2");
            record(writer, "varchar"); assertThat(reader.latest("orders", "source.orders").orElseThrow().version()).isEqualTo(1);
            assertThat(reader.pinned("orders", "source.orders").orElseThrow().schema()).isEqualTo(columns("text"));
            assertThat(reader.latest("orders", "source.orders").orElseThrow().schema().keySet()).containsExactly("order.id", "note");
            writer.pin("orders", "source.orders", 1); assertThat(reader.pinned("orders", "source.orders").orElseThrow().version()).isEqualTo(1);
            assertThat(f.rows().countDocuments()).isEqualTo(1); assertThat(f.claimWrites()).isEqualTo(5); assertThat(f.admissions()).isEqualTo(1);
        });
    }

    @Test void readOnlyViewsAndUnknownPinsCannotMutateEitherTheClaimStateOrModels() {
        withFixture(f -> {
            var reader = f.domain.pipelineDerivedSchemas(SCOPE); var before = f.snapshot();
            assertCode(() -> record(reader, "text"), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> reader.pin("orders", "source.orders", 0), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> reader.delete("orders"), IoError.CACHE_BUSINESS_FENCED); assertThat(f.snapshot()).isEqualTo(before);
            assertCode(() -> f.writer().pin("orders", "source.orders", 0), IoError.DOCUMENT_INVALID); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void recordingCapturesColumnOrderAndValuesBeforeACommandGuardCanMutateTheCaller() {
        withFixture(f -> {
            var columns = new LinkedHashMap<>(columns("text")); var once = new AtomicBoolean();
            var writer = new MongoCacheDerivedSchemaStore(new MongoCachePipelineAccess(f.client, f.database, SCOPE, WorkloadClaimFence.from(f.claim), () -> {
                if (once.compareAndSet(false, true)) { columns.clear(); columns.put("changed", "bad"); }
            }));
            writer.record("orders", "source.orders", columns, "statement", "sources", "derivation");
            assertThat(f.writer().latest("orders", "source.orders").orElseThrow().schema()).isEqualTo(columns("text"));
            assertThat(columns).containsOnlyKeys("changed");
        });
    }

    @Test void unrepresentableInputIsRejectedBeforeAnyDatabaseOrGuardCallback() {
        withFixture(f -> {
            var invalid = new LinkedHashMap<String, String>(); invalid.put("field", null); var guarded = new AtomicBoolean(); var before = f.snapshot();
            var writer = new MongoCacheDerivedSchemaStore(new MongoCachePipelineAccess(f.client, f.database, SCOPE, WorkloadClaimFence.from(f.claim), () -> guarded.set(true)));
            assertCode(() -> writer.record("orders", "step", invalid, "statement", "sources", "derivation"), IoError.DOCUMENT_INVALID);
            assertThat(guarded).isFalse(); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void sameColumnsInADifferentInputOrderDoNotMutateTheSchemaOfAnExistingPinnedVersion() {
        withFixture(f -> {
            var writer = f.writer(); record(writer, "text"); writer.pin("orders", "source.orders", 0);
            var reversed = new LinkedHashMap<String, String>(); reversed.put("note", "text"); reversed.put("order.id", "bigint");
            writer.record("orders", "source.orders", reversed, "new-statement", "new-source", "new-derivation");
            var pinned = writer.pinned("orders", "source.orders").orElseThrow();
            assertThat(pinned.version()).isZero(); assertThat(pinned.schema().keySet()).containsExactly("order.id", "note");
            assertThat(pinned.statement()).isEqualTo("new-statement");
        });
    }

    @Test void aKnownMissingPinnedVersionCannotBeReallocatedToAnotherModelByAnOrdinaryRecord() {
        withFixture(f -> {
            var writer = f.writer(); record(writer, "old"); record(writer, "held"); writer.pin("orders", "source.orders", 1);
            var versions = f.rows().find().first().getList("versions", Document.class);
            f.rows().updateOne(new Document(), new Document("$set", new Document("versions", List.of(versions.getFirst()))));
            assertThat(writer.pinned("orders", "source.orders")).isEmpty(); var before = f.snapshot();
            assertUnreadable(() -> record(writer, "replacement")); assertThat(f.snapshot()).isEqualTo(before);
            assertUnreadable(() -> writer.pin("orders", "source.orders", 0)); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void anOldExecutionCannotBorrowTheActuallyAdvancedClaimToWriteAPinOrHistory() {
        withFixture(f -> {
            var old = f.writer(); record(old, "text");
            var advanced = f.domain.workloadClaims(BUSINESS).advanceExecution(f.claim, 1, Set.of(OWNER.nodeId())).orElseThrow(); var before = f.snapshot();
            assertCode(() -> record(old, "old-execution"), IoError.WORKLOAD_CLAIM_FENCED);
            assertCode(() -> old.pin("orders", "source.orders", 0), IoError.WORKLOAD_CLAIM_FENCED); assertCode(() -> old.delete("orders"), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
            var actual = f.domain.pipelineDerivedSchemas(SCOPE, WorkloadClaimFence.from(advanced), ACTIVE); record(actual, "new-execution");
            assertThat(actual.latest("orders", "source.orders").orElseThrow().version()).isEqualTo(1);
        });
    }

    @Test void anExpiredClaimCannotAppendButTheOriginalModelRemainsReadable() {
        withFixture(f -> {
            var writer = f.writer(); record(writer, "text"); f.claims().updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)),
                    new Document("$set", new Document("leaseUntil", new Date(0)))); var before = f.snapshot();
            assertCode(() -> record(writer, "late"), IoError.WORKLOAD_CLAIM_FENCED); assertThat(f.snapshot()).isEqualTo(before);
            assertThat(f.domain.pipelineDerivedSchemas(SCOPE).latest("orders", "source.orders").orElseThrow().schema()).isEqualTo(columns("text"));
        });
    }

    @Test void aReplacementGenerationDoesNotAdoptTheOldHistoryPinOrLateWriter() {
        withFixture(f -> {
            var old = f.writer(); record(old, "old"); old.pin("orders", "source.orders", 0);
            var next = new PipelineIntentScope("cluster-a", 1, "cache-a", "orders", 2);
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database).updateOne(new Document("_id", "orders"),
                    new Document("$set", new Document("scope", MongoPipelinePreparationFacts.scope(next))));
            f.claims().updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)), new Document("$set", new Document("pipelineScope", MongoPipelinePreparationFacts.scope(next))
                    .append("pipelinePreparation", MongoCachePipelinePreparations.marker(UUID.randomUUID().toString(), io.tapstate.spi.store.PipelineCachePreparations.Phase.PREPARED))));
            var current = f.domain.pipelineDerivedSchemas(next, WorkloadClaimFence.from(f.claim), ACTIVE);
            assertThat(current.latest("orders", "source.orders")).isEmpty(); assertThat(current.pinned("orders", "source.orders")).isEmpty(); record(current, "new");
            assertThat(current.latest("orders", "source.orders").orElseThrow().version()).isZero(); assertThat(current.pinned("orders", "source.orders")).isEmpty();
            var before = f.snapshot(); assertCode(() -> old.latest("orders", "source.orders"), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> old.delete("orders"), IoError.CACHE_BUSINESS_FENCED); assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void concurrentClientsSerializeDifferentShapesWithoutDroppingHistoryOrItsExistingPin() {
        withFixture(f -> {
            record(f.writer(), "initial"); f.writer().pin("orders", "source.orders", 0); var start = new CountDownLatch(1);
            try (var second = MongoCacheDomain.connect(f.binding, Duration.ofSeconds(5), "second"); var workers = Executors.newFixedThreadPool(2)) {
                var other = second.pipelineDerivedSchemas(SCOPE, WorkloadClaimFence.from(f.claim), ACTIVE);
                var a = workers.submit(() -> { await(start); record(f.writer(), "one"); });
                var b = workers.submit(() -> { await(start); record(other, "two"); }); start.countDown();
                a.get(15, TimeUnit.SECONDS); b.get(15, TimeUnit.SECONDS);
                assertThat(f.rows().find().first().getList("versions", Document.class)).hasSize(3);
                assertThat(f.writer().pinned("orders", "source.orders").orElseThrow().schema()).isEqualTo(columns("initial"));
                assertThat(f.writer().latest("orders", "source.orders").orElseThrow().version()).isEqualTo(2);
            }
        });
    }

    @Test void originalGuardLossAfterThePayloadWriteRollsBackClaimStateAndHistoryTogether() {
        withFixture(f -> {
            record(f.writer(), "old"); var before = f.snapshot(); var live = new AtomicBoolean(true);
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
                if (updates(e, "derived_schemas")) live.set(false);
            } };
            try (var observed = client(listener)) {
                var writer = f.writer(observed, () -> { if (!live.get()) throw MongoCacheBusinessIdentity.businessFenced(); });
                assertCode(() -> record(writer, "lost"), IoError.CACHE_BUSINESS_FENCED);
            }
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void committedRetirementBeforeTheActualClaimWriteRefusesLateModelPublication() {
        withFixture(f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
                if (updates(e, "workload_claims") && once.compareAndSet(false, true)) { paused.countDown(); await(proceed); }
            } };
            try (var observed = client(listener); var worker = Executors.newSingleThreadExecutor()) {
                var future = worker.submit(() -> record(f.writer(observed, ACTIVE), "late"));
                try { assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue(); f.domain.retireBusiness(BUSINESS, "retire-models", ACTIVE); }
                finally { proceed.countDown(); }
                assertThatThrownBy(() -> future.get(15, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
                assertThat(f.rows().countDocuments()).isZero(); assertThat(f.claimWrites()).isZero();
            }
        });
    }

    @Test void malformedHistoryAndScopeDoNotUseOldDefaultsTruncateVersionsOrLeakDecoderCauses() {
        withFixture(f -> {
            record(f.writer(), "old"); f.writer().pin("orders", "source.orders", 0); var saved = f.rows().find().first();
            for (var mutation : List.of(new Document("versions.0.version", 0.5d), new Document("versions.0.extra", "private-sentinel"),
                    new Document("versions.0.columns.0.type", null), new Document("scope.stateGeneration", 1), new Document("pin", 0))) {
                f.rows().updateOne(new Document(), new Document("$set", mutation)); assertUnreadable(() -> f.writer().latest("orders", "source.orders"));
                f.rows().replaceOne(new Document(), saved);
            }
            f.rows().updateOne(new Document(), new Document("$unset", new Document("versions", "")));
            assertUnreadable(() -> f.writer().pinned("orders", "source.orders"));
        });
    }

    @Test void anUnheldPinAnswersEmptyAndNeverFallsBackToTheLatest() {
        withFixture(f -> {
            record(f.writer(), "old"); f.rows().updateOne(new Document(), new Document("$set", new Document("pin", 10L)));
            assertThat(f.writer().pinned("orders", "source.orders")).isEmpty();
            assertThat(f.writer().latest("orders", "source.orders")).isPresent();
        });
    }

    @Test void deletionRemovesOnlyTheOriginalGenerationAndReadViewsNeverRepairMissingStructures() {
        withFixture(f -> {
            record(f.writer(), "old"); Document other = new Document(f.rows().find().first()).append("_id", "orders.other");
            other.get("scope", Document.class).put("stateGeneration", 2L); f.rows().insertOne(other);
            f.writer().delete("orders"); assertThat(f.rows().countDocuments()).isEqualTo(1);
            assertThat(f.rows().find().first().getString("_id")).isEqualTo("orders.other");
            f.rows().drop(); assertCode(() -> f.writer().latest("orders", "source.orders"), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.database.listCollectionNames().into(new ArrayList<>())).doesNotContain("derived_schemas");
        });
    }

    private static boolean updates(CommandStartedEvent e, String table) { return e.getCommandName().equals("update") && e.getCommand().getString("update").getValue().equals(table); }
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
        String name = "cache_derived_" + UUID.randomUUID().toString().replace("-", "");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(name), name, null);
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl()); var domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "derived-test")) {
            var database = client.getDatabase(name);
            try {
                domain.prepareAuth(new CacheAuthScope("cluster-a", 1, StorageBinding.Mode.OP, "cache-a", 1), "auth-a", ACTIVE);
                try (var session = client.startSession()) { session.withTransaction(() -> {
                    new MongoCacheMetadataStructure(database, MongoRateHistoryStore.DEFAULT_RETENTION).prepareInTransaction(session, ACTIVE);
                    SystemCollections.CACHE_WORKLOAD_CLAIMS.prepareCollectionOn(database, session);
                    SystemCollections.CACHE_PIPELINE_STATE.prepareCollectionOn(database, session); return null;
                }, TransactionOptions.builder().readConcern(ReadConcern.LOCAL).writeConcern(WriteConcern.MAJORITY.withJournal(true)).build()); }
                SystemCollections.CACHE_IDENTITY.on(database).updateOne(new Document("_id", "cluster"), new Document("$set", new Document("businessPhase", "READY")
                        .append("businessPreparationId", "business-a").append("claimAdmissionFence", 0L)));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1)
                        .append("phase", "READY").append("epoch", 1L).append("operationId", "business-a"));
                SystemCollections.CACHE_PIPELINE_STATE.on(database).insertOne(new Document("_id", "orders").append("scope", MongoPipelinePreparationFacts.scope(SCOPE))
                        .append("stateJson", "NEW").append("epoch", 0L).append("touchMillis", 0L).append("runtimeWriteFence", 1L));
                var raw = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database));
                var retained = raw.acquire(KEY, OWNER, 1, Duration.ofSeconds(30)).claim(); raw.release(retained);
                SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)),
                        new Document("$set", new Document("pipelineScope", MongoPipelinePreparationFacts.scope(SCOPE)).append("pipelinePreparation",
                                MongoCachePipelinePreparations.marker(UUID.randomUUID().toString(), io.tapstate.spi.store.PipelineCachePreparations.Phase.PREPARED))));
                var claim = domain.workloadClaims(BUSINESS).acquirePipeline(SCOPE, OWNER, 1, Duration.ofSeconds(30)).claim();
                scenario.run(new Fixture(client, database, domain, binding, claim));
            } finally { database.drop(); }
        } catch (RuntimeException | Error e) { throw e; }
        catch (Exception e) { throw new AssertionError(e); }
    }
    private record Fixture(MongoClient client, MongoDatabase database, MongoCacheDomain domain, StorageBinding.Connection binding, WorkloadClaim claim) {
        DerivedSchemaStore writer() { return domain.pipelineDerivedSchemas(SCOPE, WorkloadClaimFence.from(claim), ACTIVE); }
        DerivedSchemaStore writer(MongoClient original, Runnable guard) { return new MongoCacheDerivedSchemaStore(new MongoCachePipelineAccess(original,
                original.getDatabase(database.getName()), SCOPE, WorkloadClaimFence.from(claim), guard)); }
        com.mongodb.client.MongoCollection<Document> rows() { return SystemCollections.CACHE_DERIVED_SCHEMAS.on(database); }
        com.mongodb.client.MongoCollection<Document> claims() { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database); }
        long claimWrites() { var row = claims().find().first(); return row.get("fencedPipelineWrites") == null ? 0 : row.getLong("fencedPipelineWrites"); }
        long admissions() { return SystemCollections.CACHE_IDENTITY.on(database).find().first().getLong("claimAdmissionFence"); }
        Map<String, List<Document>> snapshot() {
            var out = new java.util.TreeMap<String, List<Document>>();
            for (var table : List.of(SystemCollections.CACHE_DERIVED_SCHEMAS, SystemCollections.CACHE_WORKLOAD_CLAIMS, SystemCollections.CACHE_PIPELINE_STATE))
                out.put(table.collectionName(), table.on(database).find().sort(new Document("_id", 1)).into(new ArrayList<>()));
            return out;
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
