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
import com.mongodb.event.CommandSucceededEvent;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.RateSample;
import io.tapstate.spi.store.CacheAuthScope;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.RateHistoryStore;
import io.tapstate.spi.store.StorageBinding;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real history bytes, bounded queries and the original Pipeline claim share one CACHE domain. */
@RequiresDocker
class MongoCacheRateHistoryStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster-a", 1, "cache-a", "orders", 1);
    private static final CacheBusinessScope BUSINESS = new CacheBusinessScope("cluster-a", 1, "cache-a");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private static final WorkloadOwner OWNER = new WorkloadOwner("node-a", "boot-a");
    private static final Runnable ACTIVE = () -> {};
    private static final Instant T0 = Instant.now().truncatedTo(ChronoUnit.MILLIS);
    private static RateSample sample(long seconds, long count) { return new RateSample("orders", T0.plusSeconds(seconds), Map.of("records.out", count), Map.of("orders", 3L), T0.minusSeconds(100)); }

    @Test void sameTimeSamplesHaveStableDistinctKeysAndBoundedPagesAndBoundariesKeepTheOriginalNumbers() {
        withFixture(Duration.ofDays(15), f -> {
            var writer = f.writer(); writer.append(sample(0, 1)); writer.append(sample(0, 2)); writer.append(sample(1, 3)); writer.append(sample(2, 4));
            var reader = f.domain.pipelineRateHistory(SCOPE); var first = reader.readPage("orders", T0, T0.plusSeconds(3), null, 2);
            assertThat(first.entries()).extracting(e -> e.sample().counters().get("records.out")).containsExactly(1L, 2L); assertThat(first.hasMore()).isTrue();
            var second = reader.readPage("orders", T0, T0.plusSeconds(3), first.lastKey().orElseThrow(), 2);
            assertThat(second.entries()).extracting(e -> e.sample().counters().get("records.out")).containsExactly(3L, 4L); assertThat(second.hasMore()).isFalse();
            assertThat(reader.read("orders", first.entries().getFirst().key())).contains(first.entries().getFirst());
            assertThat(reader.predecessor("orders", T0.plusSeconds(1))).contains(first.entries().getLast());
            assertThat(reader.successor("orders", T0.plusSeconds(1))).contains(second.entries().getFirst());
            assertThat(reader.predecessor("orders", T0)).isEmpty(); assertThat(reader.successor("orders", T0.plusSeconds(3))).isEmpty();
            assertThat(first.entries().getFirst().sample()).isEqualTo(sample(0, 1)); assertThat(f.rows().countDocuments()).isEqualTo(4);
            assertThat(f.admissions()).isEqualTo(1); assertThat(f.claimWrites()).isEqualTo(4);
        });
    }

    @Test void readOnlyOrWrongPipelineRequestsCannotAppendDeleteOrSpendClaimWrites() {
        withFixture(Duration.ofDays(15), f -> {
            var reader = f.domain.pipelineRateHistory(SCOPE); var before = f.snapshot();
            assertCode(() -> reader.append(sample(0, 1)), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> reader.deleteAll("orders"), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> f.writer().append(new RateSample("other", T0, Map.of(), Map.of(), null)), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> reader.readPage("other", T0, T0.plusSeconds(1), null, 2), IoError.CACHE_BUSINESS_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void fractionalRetentionIsRefusedRatherThanSilentlyTruncatedToAnImmediateExpiry() {
        withFixture(Duration.ofDays(15), f -> {
            for (Duration invalid : java.util.Arrays.asList(Duration.ofMillis(500), Duration.ofMillis(1500), Duration.ZERO, Duration.ofSeconds(-1), null)) {
                assertCode(() -> { try (var ignored = MongoCacheDomain.connect(f.binding, Duration.ofSeconds(5), "invalid-retention", invalid)) { } }, StoreError.INVALID_HISTORY_RETENTION);
            }
            assertThat(f.ttl()).isEqualTo(Duration.ofDays(15).toSeconds());
        });
    }

    @Test void configuredRetentionIsCapturedAndOrdinaryReadsNeverCreateOrModifyIndexes() {
        withFixture(Duration.ofDays(3), f -> {
            var ddl = new AtomicInteger(); var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
                if (List.of("create", "createIndexes", "collMod", "dropIndexes").contains(e.getCommandName())) ddl.incrementAndGet();
            } };
            try (var observed = client(listener)) {
                var store = f.writer(observed, ACTIVE); assertThat(store.retention()).isEqualTo(Duration.ofDays(3)); store.append(sample(0, 1));
                assertThat(store.readPage("orders", T0, T0.plusSeconds(1), null, 1).entries()).hasSize(1);
                assertThat(ddl).hasValue(0); assertThat(f.ttl()).isEqualTo(Duration.ofDays(3).toSeconds());
                f.database.runCommand(new Document("collMod", "pipeline_rate_history").append("index", new Document("keyPattern", new Document("observedAt", 1))
                        .append("expireAfterSeconds", Duration.ofDays(4).toSeconds())));
                assertCode(() -> store.readPage("orders", T0, T0.plusSeconds(1), null, 1), StoreError.HISTORY_RETENTION_MISMATCH);
                assertThat(f.ttl()).isEqualTo(Duration.ofDays(4).toSeconds()); assertThat(ddl).hasValue(0);
            }
        });
    }

    @Test void anActuallyAdvancedExecutionFencesTheOldSamplerWithoutAffectingItsExistingSamples() {
        withFixture(Duration.ofDays(15), f -> {
            var old = f.writer(); old.append(sample(0, 1));
            var advanced = f.domain.workloadClaims(BUSINESS).advanceExecution(f.claim, 1, Set.of(OWNER.nodeId())).orElseThrow(); var before = f.snapshot();
            assertCode(() -> old.append(sample(1, 2)), IoError.WORKLOAD_CLAIM_FENCED); assertCode(() -> old.deleteAll("orders"), IoError.WORKLOAD_CLAIM_FENCED);
            assertThat(f.snapshot()).isEqualTo(before);
            f.domain.pipelineRateHistory(SCOPE, WorkloadClaimFence.from(advanced), ACTIVE).append(sample(1, 3));
            assertThat(f.domain.pipelineRateHistory(SCOPE).readPage("orders", T0, T0.plusSeconds(2), null, 3).entries()).hasSize(2);
        });
    }

    @Test void replacementStateGenerationsNeverMixHistoryOrAllowLateDeletion() {
        withFixture(Duration.ofDays(15), f -> {
            var old = f.writer(); old.append(sample(0, 1)); var next = new PipelineIntentScope("cluster-a", 1, "cache-a", "orders", 2);
            SystemCollections.CACHE_PIPELINE_STATE.on(f.database).updateOne(new Document(), new Document("$set", new Document("scope", MongoPipelinePreparationFacts.scope(next))));
            f.claims().updateOne(new Document("_id", MongoWorkloadClaimStore.id(KEY)), new Document("$set", new Document("pipelineScope", MongoPipelinePreparationFacts.scope(next))
                    .append("pipelinePreparation", MongoCachePipelinePreparations.marker(UUID.randomUUID().toString(), io.tapstate.spi.store.PipelineCachePreparations.Phase.PREPARED))));
            var current = f.domain.pipelineRateHistory(next, WorkloadClaimFence.from(f.claim), ACTIVE); current.append(sample(0, 2));
            assertThat(current.readPage("orders", T0, T0.plusSeconds(1), null, 10).entries()).extracting(e -> e.sample().counters().get("records.out")).containsExactly(2L);
            var before = f.snapshot(); assertCode(() -> old.deleteAll("orders"), IoError.CACHE_BUSINESS_FENCED);
            assertCode(() -> old.readPage("orders", T0, T0.plusSeconds(1), null, 10), IoError.CACHE_BUSINESS_FENCED); assertThat(f.snapshot()).isEqualTo(before);
            current.deleteAll("orders"); assertThat(f.rows().countDocuments()).isEqualTo(1);
            assertThat(f.rows().find().first().get("scope", Document.class)).containsEntry("stateGeneration", 1L);
        });
    }

    @Test void aTransactionRetryKeepsTheOriginalSampleIdAndSpendsOnlyOneClaimWrite() {
        withFixture(Duration.ofDays(15), f -> {
            var ids = new ArrayList<ObjectId>(); var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
                if (sampleInsert(e)) ids.add(e.getCommand().getArray("documents").getFirst().asDocument().getObjectId("_id").getValue());
            } };
            try (var observed = client(listener)) {
                failpoint(f.client, new Document("failCommands", List.of("insert")).append("errorCode", 112).append("errorLabels", List.of("TransientTransactionError")));
                try { f.writer(observed, ACTIVE).append(sample(0, 1)); } finally { clearFailpoint(f.client); }
                assertThat(ids).hasSize(2); assertThat(ids.getFirst()).isEqualTo(ids.getLast());
                assertThat(f.rows().countDocuments()).isEqualTo(1); assertThat(f.claimWrites()).isEqualTo(1); assertThat(f.rows().find().first().get("_id")).isEqualTo(ids.getFirst());
            }
        });
    }

    @Test void anUnknownCommitReplyRetriesOnlyTheOriginalSampleCommit() {
        withFixture(Duration.ofDays(15), f -> {
            var publishing = new AtomicBoolean(); var armed = new AtomicBoolean(); var inserts = new AtomicInteger(); var commits = new AtomicInteger();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent e) {
                    if (sampleInsert(e)) { publishing.set(true); inserts.incrementAndGet(); }
                    if (e.getCommandName().equals("commitTransaction") && armed.get()) commits.incrementAndGet();
                }
                @Override public void commandSucceeded(CommandSucceededEvent e) {
                    if (e.getCommandName().equals("insert") && publishing.get() && armed.compareAndSet(false, true)) failpoint(f.client,
                            new Document("failCommands", List.of("commitTransaction")).append("writeConcernError", new Document("code", 91).append("errmsg", "test reply lost"))
                                    .append("errorLabels", List.of("UnknownTransactionCommitResult")));
                }
            };
            try (var observed = client(listener)) {
                try { f.writer(observed, ACTIVE).append(sample(0, 1)); } finally { clearFailpoint(f.client); }
                assertThat(inserts).hasValue(1); assertThat(commits.get()).isGreaterThanOrEqualTo(2);
                assertThat(f.rows().countDocuments()).isEqualTo(1); assertThat(f.claimWrites()).isEqualTo(1);
            }
        });
    }

    @Test void losingTheOriginalGuardAfterInsertionRollsBackTheSampleAndBothWriteFences() {
        withFixture(Duration.ofDays(15), f -> {
            var before = f.snapshot(); var live = new AtomicBoolean(true); var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
                if (sampleInsert(e)) live.set(false);
            } };
            try (var observed = client(listener)) {
                assertCode(() -> f.writer(observed, () -> { if (!live.get()) throw MongoCacheBusinessIdentity.businessFenced(); }).append(sample(0, 1)), IoError.CACHE_BUSINESS_FENCED);
            }
            assertThat(f.snapshot()).isEqualTo(before);
        });
    }

    @Test void retirementCommittedBeforeTheClaimWriteRefusesALateSample() {
        withFixture(Duration.ofDays(15), f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() { @Override public void commandStarted(CommandStartedEvent e) {
                if (e.getCommandName().equals("update") && e.getCommand().getString("update").getValue().equals("workload_claims") && once.compareAndSet(false, true)) {
                    paused.countDown(); await(proceed);
                }
            } };
            try (var observed = client(listener); var worker = Executors.newSingleThreadExecutor()) {
                var future = worker.submit(() -> f.writer(observed, ACTIVE).append(sample(0, 1)));
                try { assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue(); f.domain.retireBusiness(BUSINESS, "retire-history", ACTIVE); }
                finally { proceed.countDown(); }
                assertThatThrownBy(() -> future.get(15, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class); assertThat(f.rows().countDocuments()).isZero();
            }
        });
    }

    @Test void corruptSamplesAndHeadersCannotBeTruncatedOrDefaultedByTheLegacyCodec() {
        withFixture(Duration.ofDays(15), f -> {
            var store = f.writer(); store.append(sample(0, 1)); var saved = f.rows().find().first();
            for (var mutation : List.of(new Document("counters", new Document("records.out", 1.5d)), new Document("scope.stateGeneration", 1), new Document("scope.extra", "private-sentinel"),
                    new Document("countingSince", "private-sentinel"), new Document("extra", "private-sentinel"))) {
                f.rows().updateOne(new Document(), new Document("$set", mutation));
                assertUnreadable(() -> store.readPage("orders", T0, T0.plusSeconds(1), null, 2)); f.rows().replaceOne(new Document(), saved);
            }
            f.rows().updateOne(new Document(), new Document("$unset", new Document("counters", "")));
            assertUnreadable(() -> store.readPage("orders", T0, T0.plusSeconds(1), null, 2));
        });
    }

    @Test void authResetKeepsTheOriginalHistoryButMissingCollectionsAreNeverRepaired() {
        withFixture(Duration.ofDays(15), f -> {
            var store = f.writer(); store.append(sample(0, 1)); f.domain.advanceAuthGeneration(new CacheAuthScope("cluster-a", 1, StorageBinding.Mode.OP, "cache-a", 1), 2, "auth-reset", ACTIVE);
            assertThat(store.readPage("orders", T0, T0.plusSeconds(1), null, 2).entries()).hasSize(1);
            f.rows().drop(); assertCode(() -> store.readPage("orders", T0, T0.plusSeconds(1), null, 2), IoError.DOCUMENT_UNREADABLE);
            assertThat(f.domain.pipelineStates(SCOPE).read("orders")).isPresent();
            assertThat(f.database.listCollectionNames().into(new ArrayList<>())).doesNotContain("pipeline_rate_history");
        });
    }

    private static boolean sampleInsert(CommandStartedEvent e) { return e.getCommandName().equals("insert") && e.getCommand().getString("insert").getValue().equals("pipeline_rate_history"); }
    private static MongoClient client(CommandListener listener) { return MongoClients.create(MongoClientSettings.builder()
            .applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(listener).build()); }
    private static void failpoint(MongoClient client, Document data) { client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1)).append("data", data)); }
    private static void clearFailpoint(MongoClient client) { client.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
    private static void await(CountDownLatch latch) { try { if (!latch.await(15, TimeUnit.SECONDS)) throw new AssertionError("test barrier did not open"); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); throw new AssertionError(e); } }
    private static void assertCode(org.assertj.core.api.ThrowableAssert.ThrowingCallable action, io.tapstate.core.common.TapstateErrorCode code) { assertThatThrownBy(action)
            .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(code)); }
    private static void assertUnreadable(org.assertj.core.api.ThrowableAssert.ThrowingCallable action) { assertThatThrownBy(action)
            .isInstanceOfSatisfying(TapstateException.class, e -> { assertThat(e.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE);
                assertThat(e.getCause()).isNull(); assertThat(e.args().toString()).doesNotContain("private-sentinel"); }); }
    private static void withFixture(Duration retention, Scenario scenario) {
        String name = "cache_rate_" + UUID.randomUUID().toString().replace("-", "");
        var binding = new StorageBinding.Connection(StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(name), name, null);
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl()); var domain = MongoCacheDomain.connect(binding, Duration.ofSeconds(5), "rate-test", retention)) {
            var database = client.getDatabase(name);
            try {
                domain.prepareAuth(new CacheAuthScope("cluster-a", 1, StorageBinding.Mode.OP, "cache-a", 1), "auth-a", ACTIVE);
                try (var session = client.startSession()) { session.withTransaction(() -> {
                    new MongoCacheMetadataStructure(database, retention).prepareInTransaction(session, ACTIVE);
                    SystemCollections.CACHE_WORKLOAD_CLAIMS.prepareCollectionOn(database, session); SystemCollections.CACHE_PIPELINE_STATE.prepareCollectionOn(database, session); return null;
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
                scenario.run(new Fixture(client, database, domain, claim, retention, binding));
            } finally { database.drop(); }
        } catch (RuntimeException | Error e) { throw e; }
        catch (Exception e) { throw new AssertionError(e); }
    }
    private record Fixture(MongoClient client, MongoDatabase database, MongoCacheDomain domain, WorkloadClaim claim, Duration retention, StorageBinding.Connection binding) {
        RateHistoryStore writer() { return domain.pipelineRateHistory(SCOPE, WorkloadClaimFence.from(claim), ACTIVE); }
        RateHistoryStore writer(MongoClient original, Runnable guard) { return new MongoCacheRateHistoryStore(new MongoCachePipelineAccess(original,
                original.getDatabase(database.getName()), SCOPE, WorkloadClaimFence.from(claim), guard, retention)); }
        com.mongodb.client.MongoCollection<Document> rows() { return SystemCollections.CACHE_PIPELINE_RATE_HISTORY.on(database); }
        com.mongodb.client.MongoCollection<Document> claims() { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database); }
        long claimWrites() { var row = claims().find().first(); return row.get("fencedPipelineWrites") == null ? 0 : row.getLong("fencedPipelineWrites"); }
        long admissions() { return SystemCollections.CACHE_IDENTITY.on(database).find().first().getLong("claimAdmissionFence"); }
        long ttl() { return ((Number) rows().listIndexes().into(new ArrayList<>()).stream().filter(d -> d.containsKey("expireAfterSeconds")).findFirst().orElseThrow().get("expireAfterSeconds")).longValue(); }
        Map<String, List<Document>> snapshot() {
            var out = new java.util.TreeMap<String, List<Document>>();
            for (var table : List.of(SystemCollections.CACHE_PIPELINE_RATE_HISTORY, SystemCollections.CACHE_WORKLOAD_CLAIMS, SystemCollections.CACHE_PIPELINE_STATE))
                out.put(table.collectionName(), table.on(database).find().sort(new Document("_id", 1)).into(new ArrayList<>()));
            return out;
        }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
