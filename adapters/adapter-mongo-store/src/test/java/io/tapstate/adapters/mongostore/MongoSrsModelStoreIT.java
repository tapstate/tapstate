package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import com.mongodb.event.CommandSucceededEvent;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.common.NumericType;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.common.TapstateType;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SourceField;
import io.tapstate.spi.store.SourceIndex;
import io.tapstate.spi.store.SourceTable;
import io.tapstate.spi.store.SrsLogBounds;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.SrsModel;
import io.tapstate.spi.store.SrsModelKey;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.core.event.Op;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Adapter witnesses with explicit business/chain facts; this does not exercise production domain preparation. */
@RequiresDocker
class MongoSrsModelStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"));
    private static final CacheBusinessScope SCOPE = new CacheBusinessScope("cluster-a", 1, "incarnation-a");
    private static final String CHAIN = "chain-a";
    private static final String RING = "srs.chain-a.orders";

    @Test
    void retirementBeforeOriginalClaimProofRejectsTheWholeDelayedModelPublication() {
        withBusiness(f -> {
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update") && event.getCommand().getString("update").getValue().equals("workload_claims")
                            && once.compareAndSet(false, true)) { paused.countDown(); awaitBarrier(proceed); }
                }
            };
            try (var observed = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(listener).build());
                 var domain = businessDomain(f.database()); var workers = Executors.newSingleThreadExecutor()) {
                var store = new MongoSrsModelStore(observed, observed.getDatabase(f.database().getName()), SCOPE);
                var originalChain = SystemCollections.SRS_CHAINS.on(f.database()).find().first();
                var result = workers.submit(() -> store.publish(model(0, table("id", null)), f.fence()));
                try {
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
                    domain.retireBusiness(SCOPE, "retire-a", () -> {});
                } finally { proceed.countDown(); }
                assertThatThrownBy(() -> result.get(15, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class)
                        .satisfies(error -> businessFenced(error.getCause()));
                assertThat(models(f.database()).countDocuments()).isZero();
                assertThat(SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()).countDocuments()).isZero();
                assertThat(SystemCollections.SRS_CHAINS.on(f.database()).find().first()).isEqualTo(originalChain);
                assertThat(new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()))
                        .read(f.claim().key()).orElseThrow().leased()).isFalse();
            }
        });
    }

    @Test
    void aPublicationAlreadyHoldingItsOriginalClaimCommitsBeforeRetirementCompletes() {
        withBusiness(f -> {
            var held = new CountDownLatch(1); var proceed = new CountDownLatch(1); var retirementTried = new CountDownLatch(1);
            var request = new AtomicInteger(-1); var once = new AtomicBoolean();
            var writerListener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update") && event.getCommand().getString("update").getValue().equals("workload_claims"))
                        request.compareAndSet(-1, event.getRequestId());
                }
                @Override public void commandSucceeded(CommandSucceededEvent event) {
                    if (event.getRequestId() == request.get() && once.compareAndSet(false, true)) { held.countDown(); awaitBarrier(proceed); }
                }
            };
            var retirementListener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update") && event.getCommand().getString("update").getValue().equals("workload_claims"))
                        retirementTried.countDown();
                }
            };
            try (var writer = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(writerListener).build());
                 var retirement = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(retirementListener).build()); var workers = Executors.newFixedThreadPool(2)) {
                var store = new MongoSrsModelStore(writer, writer.getDatabase(f.database().getName()), SCOPE);
                var result = workers.submit(() -> store.publish(model(0, table("id", null)), f.fence()));
                try {
                    assertThat(held.await(10, TimeUnit.SECONDS)).isTrue();
                    var retired = workers.submit(() -> {
                        try (var session = retirement.startSession()) {
                            return session.withTransaction(() -> {
                                new MongoCacheBusinessIdentity(retirement.getDatabase(f.database().getName()), SCOPE).retire(session, "retire-a");
                                return true;
                            }, com.mongodb.TransactionOptions.builder().readConcern(com.mongodb.ReadConcern.SNAPSHOT)
                                    .writeConcern(com.mongodb.WriteConcern.MAJORITY.withJournal(true)).build());
                        }
                    });
                    try { assertThat(retirementTried.await(10, TimeUnit.SECONDS)).isTrue(); }
                    finally { proceed.countDown(); }
                    assertThat(result.get(15, TimeUnit.SECONDS)).isEqualTo(model(0, table("id", null)));
                    assertThat(retired.get(15, TimeUnit.SECONDS)).isTrue();
                } finally { proceed.countDown(); }
                assertThat(models(f.database()).countDocuments()).isEqualTo(1);
                assertThat(SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()).countDocuments()).isEqualTo(1);
                assertThat(SystemCollections.CACHE_IDENTITY.on(f.database()).find().first()).containsEntry("businessPhase", "RETIRED");
                assertThat(new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()))
                        .read(f.claim().key()).orElseThrow().leased()).isFalse();
            }
        });
    }

    private static MongoCacheDomain businessDomain(MongoDatabase database) {
        return MongoCacheDomain.connect(new io.tapstate.spi.store.StorageBinding.Connection(
                io.tapstate.spi.store.StorageBinding.Connection.Kind.MONGODB, MONGO.getReplicaSetUrl(database.getName()), database.getName(), null),
                Duration.ofSeconds(5), "model-retirement-test");
    }
    private static void awaitBarrier(CountDownLatch latch) {
        try { if (!latch.await(15, TimeUnit.SECONDS)) throw new AssertionError("model test barrier did not open"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }

    @Test
    void aLiveClaimForAnotherCaptureOrRoleCannotTrimThisModelsRing() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            var log = modelLog(f); log.store(RING, 1, event(0, 7, f.fence()));
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            for (var key : List.of(new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.CAPTURE, "another-capture"),
                    new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, "capture-a"))) {
                var foreign = claims.acquire(key, new WorkloadOwner("node-b", "boot-b"), 1, Duration.ofSeconds(30)).claim();
                var before = snapshotFacts(f.database());
                assertThatThrownBy(() -> log.trim(RING, 1, WorkloadClaimFence.from(foreign)))
                        .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
                assertThat(snapshotFacts(f.database())).isEqualTo(before);
            }
        });
    }

    @Test
    void trimmingHistoricalModelsRequiresTheNewCapturesExplicitEpochBinding() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            var log = modelLog(f); log.store(RING, 1, event(0, 7, f.fence()));
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            assertThat(claims.release(f.claim())).isTrue();
            var next = claims.acquire(f.claim().key(), new WorkloadOwner("node-b", "boot-b"), 1, Duration.ofSeconds(30)).claim();
            var before = snapshotFacts(f.database());
            for (var fence : List.of(f.fence(), WorkloadClaimFence.from(next))) {
                assertThatThrownBy(() -> log.trim(RING, 1, fence)).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
                assertThat(snapshotFacts(f.database())).isEqualTo(before);
            }
            assertThat(new MongoSrsChainStore(f.client(), f.database(), SCOPE)
                    .openEpoch(CHAIN, 7, WorkloadClaimFence.from(next))).hasValue(8);
            log.trim(RING, 1, WorkloadClaimFence.from(next));
            assertThat(log.bounds(RING)).isEqualTo(new SrsLogBounds(1, 1));
            assertThat(log.load(RING, 1)).isEmpty();
            assertThat(f.models().require(key(0))).isEqualTo(model(0, table("id", null)));
        });
    }

    @Test
    void retirementBeforeTrimClaimProofKeepsEveryRowAndTheOriginalBoundary() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            modelLog(f).storeAll(RING, 1, List.of(event(0, 7, f.fence()), event(0, 7, f.fence())));
            var boundary = SystemCollections.SRS_LOG_BOUNDS.on(f.database()).find().first();
            var paused = new CountDownLatch(1); var proceed = new CountDownLatch(1); var once = new AtomicBoolean();
            var listener = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update") && event.getCommand().getString("update").getValue().equals("workload_claims")
                            && once.compareAndSet(false, true)) { paused.countDown(); awaitBarrier(proceed); }
                }
            };
            try (var observed = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(listener).build());
                 var domain = businessDomain(f.database()); var workers = Executors.newSingleThreadExecutor()) {
                var database = observed.getDatabase(f.database().getName());
                var models = new MongoSrsModelStore(observed, database, SCOPE);
                var log = new MongoSrsLogStore(observed, SystemCollections.SRS_EVENTS.on(database), SystemCollections.SRS_LOG_BOUNDS.on(database),
                        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database), models);
                var result = workers.submit(() -> log.trim(RING, 1, f.fence()));
                try {
                    assertThat(paused.await(10, TimeUnit.SECONDS)).isTrue();
                    domain.retireBusiness(SCOPE, "retire-a", () -> {});
                } finally { proceed.countDown(); }
                assertThatThrownBy(() -> result.get(15, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class)
                        .satisfies(error -> businessFenced(error.getCause()));
                assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isEqualTo(2);
                assertThat(SystemCollections.SRS_LOG_BOUNDS.on(f.database()).find().first()).isEqualTo(boundary);
            }
        });
    }

    @Test
    void actualTypedTableAndImmutableVersionSurviveReopenWithoutDiscoveryMeasurements() {
        withBusiness(f -> {
            var model = model(0, table("id", 999L));
            assertThat(model.table().approximateRowCount()).isNull();
            assertThat(f.models().publish(model, f.fence())).isEqualTo(model);
            Document before = models(f.database()).find().first();
            assertThat(f.models().publish(model(0, table("id", 123L)), f.fence())).isEqualTo(model);
            assertThat(models(f.database()).find().first()).isEqualTo(before);
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl())) {
                assertThat(new MongoSrsModelStore(other, other.getDatabase(f.database().getName()), SCOPE).require(key(0))).isEqualTo(model);
            }
            assertThat(new MongoSrsLogStore(f.client(), SystemCollections.SRS_EVENTS.on(f.database()),
                    SystemCollections.SRS_LOG_BOUNDS.on(f.database()), SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())).bounds(RING))
                    .isEqualTo(new SrsLogBounds(-1, -1));
            assertThat(f.database().listCollectionNames().into(new ArrayList<>())).doesNotContain("srs_meta", "system_meta", "sources", "cluster_keys");
        });
    }

    @Test
    void conflictingContentNeverOverwritesOrAdvancesTheOriginalClaimProof() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            Document original = models(f.database()).find().first(); Document claim = claim(f.database());
            assertThatThrownBy(() -> f.models().publish(model(0, table("changed", null)), f.fence()))
                    .isInstanceOfSatisfying(TapstateException.class, error -> assertThat(error.code()).isEqualTo(IoError.SRS_MODEL_CONFLICT));
            assertThat(models(f.database()).find().first()).isEqualTo(original);
            assertThat(claim(f.database())).isEqualTo(claim);
        });
    }

    @Test
    void twoClientsCannotPublishDifferentSchemasAtOneImmutableIdentity() {
        withBusiness(f -> {
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl()); var executor = Executors.newFixedThreadPool(2)) {
                var ready = new CountDownLatch(2); var start = new CountDownLatch(1);
                var independent = new MongoSrsModelStore(other, other.getDatabase(f.database().getName()), SCOPE);
                var a = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue(); return publish(f.models(), model(0, table("a", null)), f.fence()); });
                var b = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue(); return publish(independent, model(0, table("b", null)), f.fence()); });
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
                assertThat(List.of(a.get(20, TimeUnit.SECONDS), b.get(20, TimeUnit.SECONDS))).containsExactlyInAnyOrder(true, false);
            }
            assertThat(models(f.database()).countDocuments()).isOne();
            assertThat(claim(f.database()).getLong("fencedModelWrites")).isEqualTo(1L);
        });
    }

    @Test
    void versionsAreExactAndRequiredPredecessorsAreNotReplacedByLatestDiscovery() {
        withBusiness(f -> {
            var first = model(0, table("id", null)); var second = model(1, table("changed", null));
            assertThatThrownBy(() -> f.models().publish(second, f.fence())).satisfies(MongoSrsModelStoreIT::unavailable);
            assertThat(models(f.database()).countDocuments()).isZero();
            assertThat(SystemCollections.SRS_LOG_BOUNDS.on(f.database()).countDocuments()).isZero();
            f.models().publish(first, f.fence()); f.models().publish(second, f.fence());
            assertThat(f.models().require(key(0))).isEqualTo(first);
            assertThat(f.models().require(key(1))).isEqualTo(second);
            assertThatThrownBy(() -> f.models().require(key(2))).satisfies(MongoSrsModelStoreIT::unavailable);
            assertThat(models(f.database()).countDocuments()).isEqualTo(2);
        });
    }

    @Test
    void anAuthenticationOnlyResetKeepsBusinessModelsButANewBusinessIncarnationFencesTheOldView() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document(), new Document("$set", new Document("authGeneration", 2L)));
            assertThat(f.models().load(key(0))).isPresent();
            SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document(), new Document("$set", new Document("cacheIncarnation", "incarnation-b")));
            assertThatThrownBy(() -> f.models().load(key(0))).satisfies(MongoSrsModelStoreIT::businessFenced);
            assertThatThrownBy(() -> f.models().publish(model(1, table("new", null)), f.fence())).satisfies(MongoSrsModelStoreIT::businessFenced);
            assertThat(models(f.database()).countDocuments()).isOne();
        });
    }

    @Test
    void wrongCaptureEpochOrClaimCannotPublishAnActualModel() {
        withBusiness(f -> {
            var wrongEpoch = new SrsModel(new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 8, 0), RING, table("id", null), 0);
            assertThatThrownBy(() -> f.models().publish(wrongEpoch, f.fence())).satisfies(MongoSrsModelStoreIT::unavailable);
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("captureId", "other-capture")));
            assertThatThrownBy(() -> f.models().publish(model(0, table("id", null)), f.fence())).satisfies(MongoSrsModelStoreIT::unavailable);
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("captureId", "capture-a")));
            assertThat(new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())).release(f.claim())).isTrue();
            assertThatThrownBy(() -> f.models().publish(model(0, table("id", null)), f.fence())).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            assertThat(models(f.database()).countDocuments()).isZero();
            assertThat(SystemCollections.SRS_LOG_BOUNDS.on(f.database()).countDocuments()).isZero();
        });
    }

    @Test
    void postPublicationGuardLossRollsBackTheModelInitialBoundaryAndClaimProof() {
        withBusiness(f -> {
            Document claim = claim(f.database());
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.database().getName(),"insert","srs_models")) {
                var guarded = new MongoSrsModelStore(witness.client,witness.database,SCOPE).guardedBy(witness::refuseAfterCommand);
                assertThatThrownBy(() -> guarded.publish(model(0, table("id", null)), f.fence())).satisfies(MongoSrsModelStoreIT::businessFenced);
                witness.requireCommand();
            }
            assertThat(models(f.database()).countDocuments()).isZero();
            assertThat(SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()).countDocuments()).isZero();
            assertThat(SystemCollections.SRS_CHAINS.on(f.database()).find().first().getList("modelContextHeads", Document.class)).isEmpty();
            assertThat(SystemCollections.SRS_LOG_BOUNDS.on(f.database()).countDocuments()).isZero();
            assertThat(claim(f.database())).isEqualTo(claim);
        });
    }

    @Test
    void aKnownFullyTrimmedRingWithMissingBoundsCannotBeInitializedAgain() {
        withBusiness(f -> {
            var model = model(0, table("id", null)); f.models().publish(model, f.fence());
            var log = new MongoSrsLogStore(f.client(), SystemCollections.SRS_EVENTS.on(f.database()), SystemCollections.SRS_LOG_BOUNDS.on(f.database()),
                    SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            log.store(RING, 1, new SrsLogRecord("offset-1", Op.INSERT, 1, null, Map.of("id", 1), 1, f.fence(), 7));
            log.trim(RING, 1, f.fence()); SystemCollections.SRS_LOG_BOUNDS.on(f.database()).deleteMany(new Document());
            assertThatThrownBy(() -> f.models().publish(model, f.fence())).satisfies(MongoSrsModelStoreIT::unavailable);
            assertThat(models(f.database()).countDocuments()).isOne();
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isZero();
            assertThat(SystemCollections.SRS_LOG_BOUNDS.on(f.database()).countDocuments()).isZero();
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aCurrentDiscoveryCannotBePublishedAsTheFirstModelOfUnprovenHistory(boolean fullyTrimmed) {
        withBusiness(f -> {
            var log = new MongoSrsLogStore(f.client(), SystemCollections.SRS_EVENTS.on(f.database()), SystemCollections.SRS_LOG_BOUNDS.on(f.database()),
                    SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            log.store(RING, 1, new SrsLogRecord("old-offset", Op.INSERT, 1, null, Map.of("id", 1), 1, f.fence(), 7));
            if (fullyTrimmed) log.trim(RING, 1, f.fence());
            Document claim = claim(f.database());
            assertThatThrownBy(() -> f.models().publish(model(0, table("latest", null)), f.fence()))
                    .satisfies(MongoSrsModelStoreIT::unavailable);
            assertThat(models(f.database()).countDocuments()).isZero();
            assertThat(log.bounds(RING)).isEqualTo(new SrsLogBounds(1, fullyTrimmed ? 1 : -1));
            assertThat(claim(f.database())).isEqualTo(claim);
        });
    }

    @Test
    void changedDiscoveryAndCaptureContextDoNotRewriteAnEarlierModel() {
        withBusiness(f -> {
            var first = model(0, table("original", null)); f.models().publish(first, f.fence());
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("epoch", 8L)));
            var next = new SrsModel(new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 8, 0), RING, table("new", null), 0);
            f.models().publish(next, f.fence());
            assertThat(f.models().require(first.key())).isEqualTo(first);
            assertThat(f.models().require(next.key())).isEqualTo(next);
            assertThat(models(f.database()).countDocuments()).isEqualTo(2);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"contentHash", "schema.fields.0.tapstateType", "ddlSequence", "ringName"})
    void corruptModelPayloadOrCanonicalShapeNeverDefaultsToAnotherSchema(String field) {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            Object value = field.equals("ddlSequence") ? 9L : field.equals("ringName") ? "other-ring" : "raw-secret-sentinel";
            models(f.database()).updateOne(new Document(), new Document("$set", new Document(field, value)));
            assertThatThrownBy(() -> f.models().load(key(0))).satisfies(MongoSrsModelStoreIT::unreadable);
            assertThatThrownBy(() -> f.models().publish(model(0, table("id", null)), f.fence())).satisfies(MongoSrsModelStoreIT::unreadable);
            assertThat(models(f.database()).countDocuments()).isOne();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"missingEpoch", "fractionalEpoch", "missingOperation", "mismatchedOperation", "missingIdentityOperation"})
    void aBusinessSchemaWithoutItsOriginalMigrationOwnerCannotBeUsed(String damage) {
        withBusiness(f -> {
            var schemas = SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(f.database());
            switch (damage) {
                case "missingEpoch" -> schemas.updateOne(new Document(), new Document("$unset", new Document("epoch", "")));
                case "fractionalEpoch" -> schemas.updateOne(new Document(), new Document("$set", new Document("epoch", 1.5)));
                case "missingOperation" -> schemas.updateOne(new Document(), new Document("$unset", new Document("operationId", "")));
                case "mismatchedOperation" -> schemas.updateOne(new Document(), new Document("$set", new Document("operationId", "other-operation")));
                case "missingIdentityOperation" -> SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document(),
                        new Document("$unset", new Document("businessPreparationId", "")));
                default -> throw new AssertionError(damage);
            }
            assertThatThrownBy(() -> f.models().load(key(0))).satisfies(MongoSrsModelStoreIT::unreadable);
            assertThatThrownBy(() -> f.models().publish(model(0, table("id", null)), f.fence())).satisfies(MongoSrsModelStoreIT::unreadable);
            assertThat(models(f.database()).countDocuments()).isZero();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "missingVersion", "zeroEpoch", "withoutFence"})
    void modelAwareAppendRejectsAnUnprovenReferenceBeforeAnyEventCanLand(String damage) {
        withBusiness(f -> {
            if (!damage.equals("missing")) f.models().publish(model(0, table("id", null)), f.fence());
            Document before = claim(f.database());
            var record = new SrsLogRecord("offset", Op.INSERT, 1, null, Map.of("id", 1),
                    damage.equals("missingVersion") ? 1 : 0, damage.equals("withoutFence") ? null : f.fence(), damage.equals("zeroEpoch") ? 0 : 7);
            assertThatThrownBy(() -> modelLog(f).store(RING, 1, record)).satisfies(MongoSrsModelStoreIT::eventUnavailable);
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isZero();
            assertThat(claim(f.database())).isEqualTo(before);
        });
    }

    @Test
    void modelAwareHistoryUsesTheOriginalEpochAndVersionAndNeverSubstitutesANewerModel() {
        withBusiness(f -> {
            var log = modelLog(f);
            f.models().publish(model(0, table("original", null)), f.fence());
            log.store(RING, 1, event(0, 7, f.fence()));
            f.models().publish(new SrsModel(key(1), RING, table("changed", null), 2), f.fence());
            log.store(RING, 2, event(1, 7, f.fence()));
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("epoch", 8L)));
            var current = new SrsModel(new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 8, 0), RING, table("current", null), 3);
            f.models().publish(current, f.fence()); log.store(RING, 3, event(0, 8, f.fence()));
            assertThat(log.load(RING, 1)).contains(event(0, 7, f.fence()));
            models(f.database()).deleteOne(new Document("_id.modelVersion", 1L));
            assertThatThrownBy(() -> log.load(RING, 2)).satisfies(MongoSrsModelStoreIT::eventUnavailable);
            assertThatThrownBy(() -> log.readBatch(RING, 1, 3)).satisfies(MongoSrsModelStoreIT::eventUnavailable);
            assertThat(log.load(RING, 3)).contains(event(0, 8, f.fence()));
        });
    }

    @Test
    void oneMissingBatchModelRollsBackValidSiblingsAndTheOriginalClaimProof() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence()); Document before = claim(f.database());
            assertThatThrownBy(() -> modelLog(f).storeAll(RING, 1, List.of(event(0, 7, f.fence()), event(1, 7, f.fence()))))
                    .satisfies(MongoSrsModelStoreIT::eventUnavailable);
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isZero();
            assertThat(modelLog(f).bounds(RING)).isEqualTo(new SrsLogBounds(-1, -1));
            assertThat(claim(f.database())).isEqualTo(before);
        });
    }

    @Test
    void modelAwareReadsAndWritesRefuseLostBoundsEvenWhenAllEventsWereTrimmed() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence()); var log = modelLog(f);
            log.store(RING, 1, event(0, 7, f.fence())); log.trim(RING, 1, f.fence());
            SystemCollections.SRS_LOG_BOUNDS.on(f.database()).deleteMany(new Document());
            assertThatThrownBy(() -> log.bounds(RING)).satisfies(MongoSrsModelStoreIT::eventUnavailable);
            assertThatThrownBy(() -> log.readBatch(RING, 1, 2)).satisfies(MongoSrsModelStoreIT::eventUnavailable);
            assertThatThrownBy(() -> log.store(RING, 2, event(0, 7, f.fence()))).satisfies(MongoSrsModelStoreIT::eventUnavailable);
            assertThat(SystemCollections.SRS_LOG_BOUNDS.on(f.database()).countDocuments()).isZero();
        });
    }

    @Test
    void anOldModelAwarePortKeepsItsBusinessScopeAcrossAuthResetButCannotFollowANewIncarnation() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence()); var log = modelLog(f);
            log.store(RING, 1, event(0, 7, f.fence()));
            SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document(), new Document("$set", new Document("authGeneration", 2L)));
            assertThat(log.load(RING, 1)).isPresent();
            SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document(), new Document("$set", new Document("cacheIncarnation", "incarnation-b")));
            assertThatThrownBy(() -> log.load(RING, 1)).satisfies(MongoSrsModelStoreIT::businessFenced);
            assertThatThrownBy(() -> log.store(RING, 2, event(0, 7, f.fence()))).satisfies(MongoSrsModelStoreIT::businessFenced);
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isOne();
        });
    }

    @Test
    void theOriginalModelGuardCanAbortAfterEventAndBoundsWritesWithoutLeavingPartialEffects() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence()); Document before = claim(f.database());
            Document chainBefore = SystemCollections.SRS_CHAINS.on(f.database()).find().first();
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.database().getName(),"update","srs_log_bounds")) {
                var guarded = new MongoSrsModelStore(witness.client, witness.database, SCOPE).guardedBy(witness::refuseAfterCommand);
                var log = new MongoSrsLogStore(witness.client, SystemCollections.SRS_EVENTS.on(witness.database), SystemCollections.SRS_LOG_BOUNDS.on(witness.database),
                        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(witness.database), guarded);
                assertThatThrownBy(() -> log.store(RING, 1, event(0, 7, f.fence()))).satisfies(MongoSrsModelStoreIT::businessFenced);
                witness.requireCommand();
            }
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isZero();
            assertThat(modelLog(f).bounds(RING)).isEqualTo(new SrsLogBounds(-1, -1));
            assertThat(claim(f.database())).isEqualTo(before);
            assertThat(SystemCollections.SRS_CHAINS.on(f.database()).find().first()).isEqualTo(chainBefore);
        });
    }

    @Test
    void batchValidationReadsEachExactModelOnceRatherThanOncePerEvent() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            var queries = new AtomicInteger();
            var witness = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("find") && event.getCommand().getString("find").getValue().equals("srs_models")) queries.incrementAndGet();
                }
            };
            try (var observed = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(witness).build())) {
                var database = observed.getDatabase(f.database().getName());
                var log = new MongoSrsLogStore(observed, SystemCollections.SRS_EVENTS.on(database), SystemCollections.SRS_LOG_BOUNDS.on(database),
                        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database), new MongoSrsModelStore(observed, database, SCOPE));
                log.storeAll(RING, 1, java.util.Collections.nCopies(25, event(0, 7, f.fence())));
                assertThat(queries.get()).isLessThanOrEqualTo(4);
                queries.set(0);
                assertThat(log.readBatch(RING, 1, 25).records()).hasSize(25);
                assertThat(queries.get()).isLessThanOrEqualTo(4);
            }
        });
    }

    @Test
    void modelAwareConstructionRefusesAnotherClientsModelView() {
        withBusiness(f -> {
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl())) {
                assertThatThrownBy(() -> new MongoSrsLogStore(other, SystemCollections.SRS_EVENTS.on(f.database()),
                        SystemCollections.SRS_LOG_BOUNDS.on(f.database()), SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()), f.models()))
                        .isInstanceOf(IllegalArgumentException.class);
            }
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"append", "load", "batch"})
    void anEventCannotReferenceAModelBeforeItsDeclaredIntroduction(String operation) {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            f.models().publish(new SrsModel(key(1), RING, table("changed", null), 100), f.fence());
            var log = modelLog(f); var early = event(1, 7, f.fence());
            if (!operation.equals("append")) {
                new MongoSrsLogStore(f.client(), SystemCollections.SRS_EVENTS.on(f.database()), SystemCollections.SRS_LOG_BOUNDS.on(f.database()),
                        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())).store(RING, 1, early);
            }
            assertThatThrownBy(() -> {
                switch (operation) {
                    case "append" -> log.store(RING, 1, early);
                    case "load" -> log.load(RING, 1);
                    case "batch" -> log.readBatch(RING, 1, 1);
                    default -> throw new AssertionError(operation);
                }
            }).satisfies(MongoSrsModelStoreIT::eventUnavailable);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"append", "load", "batch"})
    void anOlderModelCannotDescribeAnEventAfterTheNextModelIntroducesItsSchema(String operation) {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            f.models().publish(new SrsModel(key(1), RING, table("changed", null), 100), f.fence());
            var log = modelLog(f); var stale = event(0, 7, f.fence());
            if (!operation.equals("append")) {
                new MongoSrsLogStore(f.client(), SystemCollections.SRS_EVENTS.on(f.database()), SystemCollections.SRS_LOG_BOUNDS.on(f.database()),
                        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())).store(RING, 101, stale);
            }
            assertThatThrownBy(() -> {
                switch (operation) {
                    case "append" -> log.store(RING, 101, stale);
                    case "load" -> log.load(RING, 101);
                    case "batch" -> log.readBatch(RING, 101, 1);
                    default -> throw new AssertionError(operation);
                }
            }).satisfies(MongoSrsModelStoreIT::eventUnavailable);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"history", "predecessor", "newCapture"})
    void aNewModelCannotMoveItsIntroductionBehindCommittedOrPredecessorHistory(String scenario) {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            SrsModel backwards;
            if (scenario.equals("predecessor")) {
                f.models().publish(new SrsModel(key(1), RING, table("second", null), 100), f.fence());
                backwards = new SrsModel(key(2), RING, table("third", null), 50);
            } else {
                modelLog(f).store(RING, 100, event(0, 7, f.fence()));
                if (scenario.equals("newCapture")) {
                    SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("epoch", 8L)));
                    backwards = new SrsModel(new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 8, 0), RING, table("new", null), 0);
                } else backwards = new SrsModel(key(1), RING, table("second", null), 1);
            }
            Document claim = claim(f.database()); long count = models(f.database()).countDocuments();
            assertThatThrownBy(() -> f.models().publish(backwards, f.fence())).satisfies(MongoSrsModelStoreIT::unavailable);
            assertThat(models(f.database()).countDocuments()).isEqualTo(count);
            assertThat(claim(f.database())).isEqualTo(claim);
        });
    }

    @Test
    void aModelOwnsItsIntroductionSequenceAndTheNextModelOwnsTheNextBoundary() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            f.models().publish(new SrsModel(key(1), RING, table("changed", null), 100), f.fence());
            var log = modelLog(f);
            log.storeAll(RING, 99, List.of(event(0, 7, f.fence()), event(1, 7, f.fence())));
            assertThat(log.readBatch(RING, 99, 2).records().keySet()).containsExactly(99L, 100L);
            assertThat(log.load(RING, 99)).get().extracting(SrsLogRecord::schemaVer).isEqualTo(0L);
            assertThat(log.load(RING, 100)).get().extracting(SrsLogRecord::schemaVer).isEqualTo(1L);
        });
    }

    @Test
    void eventProjectionResolvesTheExactHistoricalModelAndItsEffectiveRange() {
        withBusiness(f -> {
            var first=model(0,table("original",null));var second=new SrsModel(key(1),RING,table("changed",null),100);
            f.models().publish(first,f.fence());f.models().publish(second,f.fence());
            assertThat(f.models().requireEvent(first.key(),RING,99,f.fence())).isEqualTo(first);
            assertThat(f.models().requireEvent(second.key(),RING,100,f.fence())).isEqualTo(second);
            assertThatThrownBy(() -> f.models().requireEvent(first.key(),RING,100,f.fence())).satisfies(MongoSrsModelStoreIT::eventUnavailable);
            assertThatThrownBy(() -> f.models().requireEvent(second.key(),RING,99,f.fence())).satisfies(MongoSrsModelStoreIT::eventUnavailable);
            var wrongTable=new SrsModelKey(CHAIN,"another-table",SCOPE.cacheIncarnation(),7,0);
            assertThatThrownBy(() -> f.models().requireEvent(wrongTable,RING,99,f.fence())).satisfies(MongoSrsModelStoreIT::eventUnavailable);
            assertThatThrownBy(() -> f.models().requireEvent(first.key(),"another-ring",99,f.fence())).satisfies(MongoSrsModelStoreIT::eventUnavailable);
            var changedFence=new WorkloadClaimFence(f.fence().key(),f.fence().owner(),f.fence().claimGeneration()+1,f.fence().executionGeneration(),f.fence().topologyRevision());
            assertThatThrownBy(() -> f.models().requireEvent(first.key(),RING,99,changedFence)).isInstanceOfSatisfying(TapstateException.class,error -> assertThat(error.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
        });
    }

    @Test
    void historicalModelProjectionUsesTheRecordedCaptureFenceAfterItsLeaseEndsWithoutWritingClaims() {
        withBusiness(f -> {
            var original=model(0,table("original",null));f.models().publish(original,f.fence());
            var claims=new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            assertThat(claims.release(f.claim())).isTrue();var before=claim(f.database());
            assertThat(f.models().requireEvent(original.key(),RING,4,f.fence())).isEqualTo(original);
            assertThat(claim(f.database())).isEqualTo(before);
        });
    }

    @Test
    void losingTheSuccessorBodyCannotReopenThePreviousModelRangeForHotProjection() {
        withBusiness(f -> {
            var first=model(0,table("original",null));var second=new SrsModel(key(1),RING,table("changed",null),100);
            f.models().publish(first,f.fence());f.models().publish(second,f.fence());
            models(f.database()).deleteOne(new Document("_id.modelVersion",1L));
            assertThat(f.models().requireEvent(first.key(),RING,99,f.fence())).isEqualTo(first);
            assertThatThrownBy(() -> f.models().requireEvent(first.key(),RING,100,f.fence())).satisfies(MongoSrsModelStoreIT::eventUnavailable);
            assertThatThrownBy(() -> f.models().requireEvent(second.key(),RING,100,f.fence())).satisfies(MongoSrsModelStoreIT::eventUnavailable);
            assertThat(models(f.database()).countDocuments()).isEqualTo(1);
        });
    }

    @Test
    void actualVersionZeroIsRequiredBeforeOneAndCannotBeBackfilledAfterItIsLost() {
        withBusiness(f -> {
            assertThatThrownBy(() -> f.models().publish(new SrsModel(key(1), RING, table("no-initial", null), 1), f.fence()))
                    .satisfies(MongoSrsModelStoreIT::unavailable);
            f.models().publish(model(0, table("id", null)), f.fence());
            var log = modelLog(f); log.store(RING, 0, event(0, 7, f.fence()));
            assertThat(log.load(RING, 0)).contains(event(0, 7, f.fence()));
            f.models().publish(model(1, table("changed", null)), f.fence());
            log.store(RING, 1, event(1, 7, f.fence()));
            assertThat(log.readBatch(RING, 0, 2).records().keySet()).containsExactly(0L, 1L);
            models(f.database()).deleteOne(new Document("_id.modelVersion", 0L));
            assertThatThrownBy(() -> f.models().publish(model(0, table("id", null)), f.fence()))
                    .satisfies(MongoSrsModelStoreIT::unavailable);
        });
    }

    @Test
    void aMissingIntermediateModelCannotMakeAnOldVersionLookOpenEnded() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            f.models().publish(new SrsModel(key(1), RING, table("second", null), 100), f.fence());
            f.models().publish(new SrsModel(key(2), RING, table("third", null), 200), f.fence());
            var log = modelLog(f); log.store(RING, 1, event(0, 7, f.fence()));
            models(f.database()).deleteOne(new Document("_id.modelVersion", 1L));
            assertThatThrownBy(() -> log.load(RING, 1)).satisfies(MongoSrsModelStoreIT::eventUnavailable);
        });
    }

    @ParameterizedTest
    @CsvSource({"load,false", "batch,false", "load,true", "batch,true"})
    void aHistoricalCaptureCannotDescribeEventsBeyondTheNextCaptureStart(String operation, boolean initialMissing) {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            var log = modelLog(f); log.store(RING, 1, event(0, 7, f.fence()));
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("epoch", 8L)));
            var initial = new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 8, 0);
            f.models().publish(new SrsModel(initial, RING, table("initial", null), 2), f.fence());
            var later = new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 8, 1);
            f.models().publish(new SrsModel(later, RING, table("later", null), 100), f.fence());
            if (initialMissing) models(f.database()).deleteOne(new Document("_id.captureContext.epoch", 8L).append("_id.modelVersion", 0L));
            new MongoSrsLogStore(f.client(), SystemCollections.SRS_EVENTS.on(f.database()), SystemCollections.SRS_LOG_BOUNDS.on(f.database()),
                    SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())).store(RING, 3, event(0, 7, f.fence()));
            assertThatThrownBy(() -> {
                if (operation.equals("load")) log.load(RING, 3); else log.readBatch(RING, 3, 1);
            }).satisfies(MongoSrsModelStoreIT::eventUnavailable);
        });
    }

    @ParameterizedTest
    @CsvSource({"capture,load", "capture,batch", "version,load", "version,batch"})
    void losingTheOnlySuccessorPayloadMustNotReopenAnOldModelRange(String missing, String operation) {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            long badSequence;
            if (missing.equals("capture")) {
                modelLog(f).store(RING, 1, event(0, 7, f.fence()));
                SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("epoch", 8L)));
                var next = new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 8, 0);
                f.models().publish(new SrsModel(next, RING, table("next-capture", null), 2), f.fence());
                models(f.database()).deleteMany(new Document("_id.captureContext.epoch", 8L));
                badSequence = 3;
            } else {
                f.models().publish(new SrsModel(key(1), RING, table("next-version", null), 100), f.fence());
                models(f.database()).deleteOne(new Document("_id.modelVersion", 1L));
                badSequence = 101;
            }
            new MongoSrsLogStore(f.client(), SystemCollections.SRS_EVENTS.on(f.database()), SystemCollections.SRS_LOG_BOUNDS.on(f.database()),
                    SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())).store(RING, badSequence, event(0, 7, f.fence()));
            assertThatThrownBy(() -> {
                if (operation.equals("load")) modelLog(f).load(RING, badSequence); else modelLog(f).readBatch(RING, badSequence, 1);
            }).satisfies(MongoSrsModelStoreIT::eventUnavailable);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"publish", "append", "load", "batch", "bounds"})
    void losingKnownControlCannotBeTreatedAsAnUnpublishedContext(String operation) {
        withBusiness(f -> {
            var first = model(0, table("id", null)); f.models().publish(first, f.fence());
            var log = modelLog(f); log.store(RING, 1, event(0, 7, f.fence()));
            f.database().getCollection("srs_model_contexts").deleteMany(new Document());
            Document before = claim(f.database());
            assertThatThrownBy(() -> {
                switch (operation) {
                    case "publish" -> f.models().publish(first, f.fence());
                    case "append" -> log.store(RING, 2, event(0, 7, f.fence()));
                    case "load" -> log.load(RING, 1);
                    case "batch" -> log.readBatch(RING, 1, 1);
                    case "bounds" -> log.bounds(RING);
                    default -> throw new AssertionError(operation);
                }
            }).isInstanceOf(TapstateException.class);
            assertThat(claim(f.database())).isEqualTo(before);
        });
    }

    @Test
    void losingTheControlAndAllPayloadsBeforeTheFirstEventCannotRecreateHistory() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            f.database().getCollection("srs_model_contexts").deleteMany(new Document());
            models(f.database()).deleteMany(new Document());
            assertThatThrownBy(() -> f.models().publish(model(0, table("replacement", null)), f.fence()))
                    .satisfies(MongoSrsModelStoreIT::unavailable);
            assertThat(models(f.database()).countDocuments()).isZero();
        });
    }

    @ParameterizedTest
    @CsvSource({"string,publish", "number,publish", "list,publish", "string,load", "number,load", "list,load", "string,bounds", "number,bounds", "list,bounds"})
    void corruptPublishedContextPointerHasACodedRedactedFailure(String type, String operation) {
        withBusiness(f -> {
            var first = model(0, table("id", null)); f.models().publish(first, f.fence());
            Object broken = switch (type) { case "string" -> "raw-secret-sentinel"; case "number" -> 12L; default -> List.of("raw-secret-sentinel"); };
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("modelContextHeads.0.contextId", broken)));
            assertThatThrownBy(() -> {
                switch (operation) {
                    case "publish" -> f.models().publish(first, f.fence());
                    case "load" -> f.models().load(key(0));
                    case "bounds" -> modelLog(f).bounds(RING);
                    default -> throw new AssertionError(operation);
                }
            }).satisfies(MongoSrsModelStoreIT::unreadable);
        });
    }

    @Test
    void aRetainedContextPreventsAnotherChainFromClaimingARingAfterPayloadAndBoundsLoss() {
        withBusiness(f -> {
            f.models().publish(model(0, table("original", null)), f.fence());
            models(f.database()).deleteMany(new Document());
            SystemCollections.SRS_LOG_BOUNDS.on(f.database()).deleteMany(new Document());
            SystemCollections.SRS_CHAINS.on(f.database()).insertOne(new Document("_id", "chain-b").append("epoch", 8L)
                    .append("cacheIncarnation", SCOPE.cacheIncarnation()).append("captureId", "capture-b").append("modelContextHeads", List.of()).append("modelWriteFence", 0L));
            var other = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())).acquire(
                    new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.CAPTURE, "capture-b"), new WorkloadOwner("node-b", "boot-b"), 1, Duration.ofSeconds(30)).claim();
            var stolen = new SrsModel(new SrsModelKey("chain-b", "orders", SCOPE.cacheIncarnation(), 8, 0), RING, table("replacement", null), 0);
            assertThatThrownBy(() -> f.models().publish(stolen, WorkloadClaimFence.from(other))).satisfies(MongoSrsModelStoreIT::unavailable);
            assertThat(models(f.database()).countDocuments()).isZero();
            assertThat(SystemCollections.SRS_LOG_BOUNDS.on(f.database()).countDocuments()).isZero();
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"load", "batch"})
    void historicalEventsKeepTheirOriginalCaptureIdentityAfterALegalNewContext(String operation) {
        withBusiness(f -> {
            f.models().publish(model(0, table("old", null)), f.fence());
            var log = modelLog(f); var original = event(0, 7, f.fence()); log.store(RING, 1, original);
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("epoch", 8L).append("captureId", "capture-b")));
            var other = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())).acquire(
                    new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.CAPTURE, "capture-b"), new WorkloadOwner("node-b", "boot-b"), 1, Duration.ofSeconds(30)).claim();
            var fence = WorkloadClaimFence.from(other);
            var current = new SrsModel(new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 8, 0), RING, table("current", null), 2);
            f.models().publish(current, fence); log.store(RING, 2, event(0, 8, fence));
            if (operation.equals("load")) assertThat(log.load(RING, 1)).contains(original);
            else assertThat(log.readBatch(RING, 1, 2).records()).containsEntry(1L, original).containsEntry(2L, event(0, 8, fence));
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"publish", "load"})
    void anUnrepresentablePublishedVersionIsRejectedInsideTheCodedDecoder(String operation) {
        withBusiness(f -> {
            var first = model(0, table("id", null)); f.models().publish(first, f.fence());
            f.models().publish(new SrsModel(key(1), RING, table("changed", null), 100), f.fence());
            var controls = SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()); Document row = controls.find().first();
            row.put("publishedVersion", Long.MAX_VALUE); row.remove("contentHash");
            row.put("contentHash", io.tapstate.core.model.canonical.CanonicalHash.ofText(row.toJson()));
            controls.replaceOne(new Document("_id", row.get("_id")), row);
            assertThatThrownBy(() -> {
                if (operation.equals("publish")) f.models().publish(first, f.fence()); else f.models().load(key(0));
            }).satisfies(MongoSrsModelStoreIT::unreadable);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"publishedVersion", "captureFence.claimGeneration", "contentHash", "nextContext", "firstSequence"})
    void damagedOrderingFactsNeverDefaultToAnotherRange(String field) {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            Object bad = field.equals("contentHash") || field.equals("nextContext") ? "raw-secret-sentinel" : 0.5D;
            SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()).updateOne(new Document(), new Document("$set", new Document(field, bad)));
            assertThatThrownBy(() -> f.models().load(key(0))).satisfies(MongoSrsModelStoreIT::unreadable);
        });
    }

    @Test
    void anotherLiveClaimCannotReuseAnAlreadyPublishedNumericCaptureEpoch() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            assertThat(claims.release(f.claim())).isTrue();
            var replacement = claims.acquire(f.claim().key(), new WorkloadOwner("node-b", "boot-b"), 1, Duration.ofSeconds(30)).claim();
            Document before = claim(f.database());
            assertThatThrownBy(() -> f.models().publish(model(1, table("changed", null)), WorkloadClaimFence.from(replacement)))
                    .satisfies(MongoSrsModelStoreIT::unavailable);
            assertThat(models(f.database()).countDocuments()).isOne(); assertThat(claim(f.database())).isEqualTo(before);
        });
    }

    @Test
    void guardLossRollsBackBothContextLinksAndThePublishedChainPointer() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("epoch", 8L)));
            Document chainBefore = SystemCollections.SRS_CHAINS.on(f.database()).find().first();
            Document controlBefore = SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()).find().first(); Document claimBefore = claim(f.database());
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.database().getName(),"insert","srs_models")) {
                var guarded = new MongoSrsModelStore(witness.client,witness.database,SCOPE).guardedBy(witness::refuseAfterCommand);
                var next = new SrsModel(new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 8, 0), RING, table("next", null), 1);
                assertThatThrownBy(() -> guarded.publish(next, f.fence())).satisfies(MongoSrsModelStoreIT::businessFenced);
                witness.requireCommand();
            }
            assertThat(SystemCollections.SRS_CHAINS.on(f.database()).find().first()).isEqualTo(chainBefore);
            assertThat(SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()).find().first()).isEqualTo(controlBefore);
            assertThat(SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()).countDocuments()).isOne();
            assertThat(models(f.database()).countDocuments()).isOne(); assertThat(claim(f.database())).isEqualTo(claimBefore);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"publish", "bounds"})
    void thePublishedChainPointerStillProvesHistoryWhenAllRingFactsAreLost(String operation) {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            for (var collection : List.of(SystemCollections.SRS_MODELS, SystemCollections.SRS_MODEL_CONTEXTS, SystemCollections.SRS_LOG_BOUNDS)) {
                collection.on(f.database()).deleteMany(new Document());
            }
            assertThatThrownBy(() -> {
                if (operation.equals("publish")) f.models().publish(model(0, table("replacement", null)), f.fence()); else modelLog(f).bounds(RING);
            }).isInstanceOf(TapstateException.class);
            assertThat(models(f.database()).countDocuments()).isZero();
        });
    }

    @Test
    void anOldAppendAndANewCaptureBoundaryMustNotBothCommitAcrossDifferentClaims() {
        withBusiness(f -> {
            f.models().publish(model(0, table("original", null)), f.fence());
            modelLog(f).store(RING, 1, event(0, 7, f.fence()));
            var appendPaused = new CountDownLatch(1); var releaseAppend = new CountDownLatch(1); var closeIssued = new CountDownLatch(1);
            var once = new AtomicBoolean();
            var appendWitness = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update")
                            && java.util.Set.of("srs_chains", "srs_events").contains(event.getCommand().getString("update").getValue())
                            && once.compareAndSet(false, true)) {
                        appendPaused.countDown();
                        try { assertThat(releaseAppend.await(10, TimeUnit.SECONDS)).isTrue(); }
                        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
                    }
                }
            };
            var closeWitness = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update") && event.getCommand().getString("update").getValue().equals("srs_model_contexts")) closeIssued.countDown();
                }
            };
            try (var a = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(appendWitness).build());
                 var b = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl())).addCommandListener(closeWitness).build());
                 var executor = Executors.newFixedThreadPool(2)) {
                try {
                    var aDb = a.getDatabase(f.database().getName());
                    var log = new MongoSrsLogStore(a, SystemCollections.SRS_EVENTS.on(aDb), SystemCollections.SRS_LOG_BOUNDS.on(aDb),
                            SystemCollections.CACHE_WORKLOAD_CLAIMS.on(aDb), new MongoSrsModelStore(a, aDb, SCOPE));
                    var append = executor.submit(() -> {
                        try { log.store(RING, 101, event(0, 7, f.fence())); return true; }
                        catch (TapstateException refused) { assertThat(refused.code()).isEqualTo(IoError.SRS_EVENT_MODEL_UNAVAILABLE); return false; }
                    });
                    assertThat(appendPaused.await(10, TimeUnit.SECONDS)).isTrue();
                    SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("epoch", 8L).append("captureId", "capture-b")));
                    var claim = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())).acquire(
                            new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.CAPTURE, "capture-b"), new WorkloadOwner("node-b", "boot-b"), 1, Duration.ofSeconds(30)).claim();
                    var successor = executor.submit(() -> {
                        try {
                            var next = new SrsModel(new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 8, 0), RING, table("next", null), 100);
                            new MongoSrsModelStore(b, b.getDatabase(f.database().getName()), SCOPE).publish(next, WorkloadClaimFence.from(claim)); return true;
                        } catch (TapstateException refused) { assertThat(refused.code()).isEqualTo(IoError.SRS_MODEL_UNAVAILABLE); return false; }
                    });
                    assertThat(closeIssued.await(10, TimeUnit.SECONDS)).isTrue();
                    assertThat(successor.get(10, TimeUnit.SECONDS)).as("the successor publishes while the old append is paused before its shared write").isTrue();
                    releaseAppend.countDown();
                    boolean appended = append.get(20, TimeUnit.SECONDS); boolean transitioned = successor.get(20, TimeUnit.SECONDS);
                    assertThat(appended && transitioned).as("old seq101 and new frontier100 cannot both be durable").isFalse();
                    assertThat(appended || transitioned).as("one original transaction can complete without a deadlock").isTrue();
                } finally { releaseAppend.countDown(); }
            }
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"missing", "double", "negative", "max"})
    void anAppendWriteFenceCannotBeDefaultedOrTruncated(String damage) {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            var changes = damage.equals("missing") ? new Document("$unset", new Document("modelWriteFence", ""))
                    : new Document("$set", new Document("modelWriteFence", switch (damage) {
                        case "double" -> 0.5D; case "negative" -> -1L; default -> Long.MAX_VALUE;
                    }));
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), changes);
            Document before = claim(f.database());
            assertThatThrownBy(() -> modelLog(f).store(RING, 1, event(0, 7, f.fence()))).satisfies(MongoSrsModelStoreIT::unreadable);
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isZero();
            assertThat(claim(f.database())).isEqualTo(before);
        });
    }

    @Test
    void aMissingUnreferencedHeadPayloadDoesNotHideEarlierModelsWithProvenRanges() {
        withBusiness(f -> {
            f.models().publish(model(0, table("first", null)), f.fence());
            f.models().publish(new SrsModel(key(1), RING, table("second", null), 100), f.fence());
            var log = modelLog(f); log.store(RING, 1, event(0, 7, f.fence())); log.store(RING, 101, event(1, 7, f.fence()));
            f.models().publish(new SrsModel(key(2), RING, table("future", null), 200), f.fence());
            models(f.database()).deleteOne(new Document("_id.modelVersion", 2L));
            assertThat(f.models().require(key(0)).table().fields().getFirst().name()).isEqualTo("first");
            assertThat(f.models().require(key(1)).table().fields().getFirst().name()).isEqualTo("second");
            assertThat(log.load(RING, 1)).contains(event(0, 7, f.fence()));
            assertThat(log.load(RING, 101)).contains(event(1, 7, f.fence()));
            log.store(RING, 199, event(1, 7, f.fence()));
            assertThatThrownBy(() -> log.store(RING, 200, event(1, 7, f.fence()))).satisfies(MongoSrsModelStoreIT::eventUnavailable);
            assertThatThrownBy(() -> log.store(RING, 200, event(2, 7, f.fence()))).satisfies(MongoSrsModelStoreIT::eventUnavailable);
        });
    }

    @Test
    void aClosedContextBoundaryDoesNotRequireTheNextContextsUnreferencedSchemaBody() {
        withBusiness(f -> {
            f.models().publish(model(0, table("old", null)), f.fence()); var log = modelLog(f); log.store(RING, 1, event(0, 7, f.fence()));
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("epoch", 8L)));
            var next = new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 8, 0);
            f.models().publish(new SrsModel(next, RING, table("next", null), 2), f.fence());
            models(f.database()).deleteOne(new Document("_id.captureContext.epoch", 8L));
            assertThat(log.load(RING, 1)).contains(event(0, 7, f.fence()));
            assertThat(log.readBatch(RING, 1, 1).records()).containsEntry(1L, event(0, 7, f.fence()));
            assertThatThrownBy(() -> log.store(RING, 2, event(0, 8, f.fence()))).satisfies(MongoSrsModelStoreIT::eventUnavailable);
        });
    }

    @ParameterizedTest
    @ValueSource(longs = {0L, 2L})
    void anUnreferencedCorruptPayloadCannotBlockAnExactlyBoundEvent(long damagedVersion) {
        withBusiness(f -> {
            f.models().publish(model(0, table("first", null)), f.fence());
            f.models().publish(new SrsModel(key(1), RING, table("second", null), 100), f.fence());
            var log = modelLog(f); log.store(RING, 101, event(1, 7, f.fence()));
            f.models().publish(new SrsModel(key(2), RING, table("future", null), 200), f.fence());
            models(f.database()).updateOne(new Document("_id.modelVersion", damagedVersion),
                    new Document("$set", new Document("schema", new Document("corrupt", "raw-secret-sentinel"))));
            assertThat(f.models().require(key(1)).table().fields().getFirst().name()).isEqualTo("second");
            assertThat(log.load(RING, 101)).contains(event(1, 7, f.fence()));
            assertThat(log.readBatch(RING, 101, 1).records()).containsEntry(101L, event(1, 7, f.fence()));
            log.store(RING, 199, event(1, 7, f.fence()));
            assertThatThrownBy(() -> log.store(RING, 200, event(1, 7, f.fence()))).satisfies(MongoSrsModelStoreIT::eventUnavailable);
            assertThatThrownBy(() -> f.models().require(key(damagedVersion))).satisfies(MongoSrsModelStoreIT::unreadable);
        });
    }

    @Test
    void anAppendCannotAdvanceItsLastReadableChainFenceIntoAnUnreadableCounter() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("modelWriteFence", Long.MAX_VALUE - 1)));
            Document before = SystemCollections.SRS_CHAINS.on(f.database()).find().first(); Document claim = claim(f.database());
            assertThatThrownBy(() -> modelLog(f).store(RING, 1, event(0, 7, f.fence()))).satisfies(MongoSrsModelStoreIT::eventUnavailable);
            assertThat(SystemCollections.SRS_CHAINS.on(f.database()).find().first()).isEqualTo(before);
            assertThat(claim(f.database())).isEqualTo(claim);
            assertThat(f.models().require(key(0))).isEqualTo(model(0, table("id", null)));
        });
    }

    @Test
    void aNewSnapshotWaitsAtPreparedEpochZeroWithoutCreatingAnyFacts() {
        withBusiness(f -> {
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("epoch", 0L)));
            var before = snapshotFacts(f.database());
            assertThat(f.models().publishedForNewSnapshot(CHAIN, Map.of("orders", RING))).isEmpty();
            assertThat(snapshotFacts(f.database())).isEqualTo(before);
        });
    }

    @Test
    void newSnapshotCandidatesAreActualPublishedModelsWithoutAcquiringCaptureAuthority() {
        withBusiness(f -> {
            var first = model(0, table("original", null)); f.models().publish(first, f.fence());
            assertThat(new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database())).release(f.claim())).isTrue();
            var before = snapshotFacts(f.database());
            Map<String, SrsModel> selected;
            try (var other = MongoClients.create(MONGO.getReplicaSetUrl())) {
                selected = new MongoSrsModelStore(other, other.getDatabase(f.database().getName()), SCOPE)
                        .publishedForNewSnapshot(CHAIN, Map.of("orders", RING)).orElseThrow();
            }
            assertThat(selected).containsExactlyEntriesOf(Map.of("orders", first));
            assertThatThrownBy(() -> selected.clear()).isInstanceOf(UnsupportedOperationException.class);
            assertThat(snapshotFacts(f.database())).isEqualTo(before);
            assertThat(f.models().require(first.key())).isEqualTo(first);
        });
    }

    @Test
    void newSnapshotsSelectPublishedHeadsForTheWholeRequestedSetAndNeverChangeSavedReferences() {
        withBusiness(f -> {
            var first = model(0, table("original", null)); f.models().publish(first, f.fence());
            var orders = table("id", null);
            var customerTable = new SourceTable("customers", orders.fields(), orders.primaryKey(), orders.indexes());
            var customerKey = new SrsModelKey(CHAIN, "customers", SCOPE.cacheIncarnation(), 7, 0);
            String customerRing = "srs.chain-a.customers";
            var customers = new SrsModel(customerKey, customerRing, customerTable, 0);
            f.models().publish(customers, f.fence());
            var selected = Map.of("orders", RING, "customers", customerRing);
            var saved = f.models().publishedForNewSnapshot(CHAIN, selected).orElseThrow();
            var changed = model(1, table("changed", null)); f.models().publish(changed, f.fence());
            assertThat(f.models().publishedForNewSnapshot(CHAIN, selected)).hasValue(Map.of("orders", changed, "customers", customers));
            assertThat(saved).containsExactlyInAnyOrderEntriesOf(Map.of("orders", first, "customers", customers));
            assertThat(f.models().require(saved.get("orders").key())).isEqualTo(first);
            assertThat(f.models().publishedForNewSnapshot(CHAIN, Map.of("orders", RING, "waiting", "srs.chain-a.waiting"))).isEmpty();
        });
    }

    @Test
    void aNewSnapshotSelectsTheCurrentPublishedContextWithoutDependingOnUnreferencedOldPayloads() {
        withBusiness(f -> {
            f.models().publish(model(0, table("old", null)), f.fence());
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("epoch", 8L)));
            var current = new SrsModel(new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 8, 0), RING, table("current", null), 1);
            f.models().publish(current, f.fence());
            models(f.database()).updateOne(new Document("_id.captureContext.epoch", 7L),
                    new Document("$set", new Document("schema", new Document("corrupt", "raw-secret-sentinel"))));
            assertThat(f.models().publishedForNewSnapshot(CHAIN, Map.of("orders", RING))).hasValue(Map.of("orders", current));
            assertThatThrownBy(() -> f.models().require(key(0))).satisfies(MongoSrsModelStoreIT::unreadable);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"body", "control", "bounds", "anchor"})
    void aNewSnapshotCannotTreatKnownMissingPublishedFactsAsNotYetReady(String missing) {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            switch (missing) {
                case "body" -> models(f.database()).deleteMany(new Document());
                case "control" -> SystemCollections.SRS_MODEL_CONTEXTS.on(f.database()).deleteMany(new Document());
                case "bounds" -> SystemCollections.SRS_LOG_BOUNDS.on(f.database()).deleteMany(new Document());
                case "anchor" -> SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(),
                        new Document("$set", new Document("modelContextHeads", List.of())));
                default -> throw new AssertionError(missing);
            }
            var before = snapshotFacts(f.database());
            assertThatThrownBy(() -> f.models().publishedForNewSnapshot(CHAIN, Map.of("orders", RING))).satisfies(MongoSrsModelStoreIT::unavailable);
            assertThat(snapshotFacts(f.database())).isEqualTo(before);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"SRS_CHAINS", "SRS_MODELS", "SRS_MODEL_CONTEXTS", "SRS_EVENTS", "SRS_LOG_BOUNDS"})
    void losingAnEmptySnapshotResourceIsNotAnUnpublishedTable(String missing) {
        withBusiness(f -> {
            var resource = SystemCollections.valueOf(missing); resource.on(f.database()).drop();
            assertThatThrownBy(() -> f.models().publishedForNewSnapshot(CHAIN, Map.of("orders", RING))).satisfies(MongoSrsModelStoreIT::unreadable);
            assertThat(f.database().listCollectionNames().into(new ArrayList<>())).doesNotContain(resource.collectionName());
        });
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void retainedEventsOrTrimmedBoundsPreventUnpublishedSnapshotFallback(boolean trimmed) {
        withBusiness(f -> {
            var oldLog = new MongoSrsLogStore(f.client(), SystemCollections.SRS_EVENTS.on(f.database()),
                    SystemCollections.SRS_LOG_BOUNDS.on(f.database()), SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            oldLog.store(RING, 1, event(0, 7, f.fence())); if (trimmed) oldLog.trim(RING, 1, f.fence());
            var before = snapshotFacts(f.database());
            assertThatThrownBy(() -> f.models().publishedForNewSnapshot(CHAIN, Map.of("orders", RING))).satisfies(MongoSrsModelStoreIT::unavailable);
            assertThat(snapshotFacts(f.database())).isEqualTo(before);
        });
    }

    @Test
    void oneUnpublishedTableCannotHideAnotherRequestedTablesCorruptModel() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            models(f.database()).updateOne(new Document(), new Document("$set", new Document("schema", new Document("corrupt", "raw-secret-sentinel"))));
            assertThatThrownBy(() -> f.models().publishedForNewSnapshot(CHAIN, Map.of("a-waiting", "srs.chain-a.waiting", "orders", RING)))
                    .satisfies(MongoSrsModelStoreIT::unreadable);
        });
    }

    @Test
    void aPublishedRingCannotBeAliasedToAnUnpublishedSnapshotTable() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            assertThatThrownBy(() -> f.models().publishedForNewSnapshot(CHAIN, Map.of("customers", RING)))
                    .satisfies(MongoSrsModelStoreIT::unavailable);
            assertThatThrownBy(() -> f.models().publishedForNewSnapshot(CHAIN, Map.of("orders", "different-ring")))
                    .satisfies(MongoSrsModelStoreIT::unavailable);
        });
    }

    @Test
    void corruptBoundsAndLostBusinessContextNeverYieldSnapshotCandidates() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            SystemCollections.SRS_LOG_BOUNDS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("largestSequence", 0)));
            assertThatThrownBy(() -> f.models().publishedForNewSnapshot(CHAIN, Map.of("orders", RING))).satisfies(MongoSrsModelStoreIT::unreadable);
            SystemCollections.CACHE_IDENTITY.on(f.database()).updateOne(new Document(), new Document("$set", new Document("cacheIncarnation", "new-incarnation")));
            assertThatThrownBy(() -> f.models().publishedForNewSnapshot(CHAIN, Map.of("orders", RING))).satisfies(MongoSrsModelStoreIT::businessFenced);
        });
    }

    @Test
    void aSnapshotSelectionGuardIsCheckedBeforeReturningAnyCandidate() {
        withBusiness(f -> {
            f.models().publish(model(0, table("id", null)), f.fence());
            var before = snapshotFacts(f.database());
            try (var witness = new MongoCommandWitness(MONGO.getReplicaSetUrl(),f.database().getName(),"find","srs_models")) {
                var guarded = new MongoSrsModelStore(witness.client,witness.database,SCOPE).guardedBy(witness::refuseAfterCommand);
                assertThatThrownBy(() -> guarded.publishedForNewSnapshot(CHAIN, Map.of("orders", RING))).satisfies(MongoSrsModelStoreIT::businessFenced);
                witness.requireCommand();
            }
            assertThat(snapshotFacts(f.database())).isEqualTo(before);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"retained", "absent", "null"})
    void aRetainedRealCheckpointPreventsClassifyingLostPublishedHistoryAsWaiting(String checkpointBody) {
        withBusiness(f -> {
            SystemCollections.SRS_CHAINS.on(f.database()).deleteMany(new Document());
            var chains = new MongoSrsChainStore(f.client(), f.database(), SCOPE);
            chains.prepare(CHAIN, null, f.fence()); chains.requestTables(CHAIN, java.util.Set.of("orders"), f.fence());
            assertThat(chains.openEpoch(CHAIN, 0, f.fence())).hasValue(1);
            var first = new SrsModel(new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 1, 0), RING, table("id", null), 0);
            f.models().publish(first, f.fence()); assertThat(chains.publishServingTables(CHAIN, 1, java.util.Set.of("orders"), f.fence())).isTrue();
            var checkpoint = new io.tapstate.spi.store.SrsCaptureCheckpoint(new io.tapstate.core.event.ChainPosition(
                    io.tapstate.core.event.SourceOrder.snapshotRow(1), "snapshot-seam"),
                    Map.of("orders", new io.tapstate.spi.store.SrsTableCheckpoint(first.key(), -1)));
            assertThat(chains.checkpoint(CHAIN, 0, checkpoint, f.fence())).isPresent();
            for (var resource : List.of(SystemCollections.SRS_MODELS, SystemCollections.SRS_MODEL_CONTEXTS,
                    SystemCollections.SRS_LOG_BOUNDS, SystemCollections.SRS_EVENTS)) resource.on(f.database()).deleteMany(new Document());
            SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("modelContextHeads", List.of())));
            if (checkpointBody.equals("absent")) SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$unset", new Document("checkpoint", 1)));
            if (checkpointBody.equals("null")) SystemCollections.SRS_CHAINS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("checkpoint", null)));
            var before = snapshotFacts(f.database());
            assertThatThrownBy(() -> f.models().publishedForNewSnapshot(CHAIN, Map.of("orders", RING))).satisfies(failure -> {
                if (checkpointBody.equals("retained")) unavailable(failure); else unreadable(failure);
            });
            assertThat(snapshotFacts(f.database())).isEqualTo(before);
        });
    }

    private static Map<String, List<Document>> snapshotFacts(MongoDatabase database) {
        var facts = new java.util.TreeMap<String, List<Document>>();
        for (var resource : List.of(SystemCollections.CACHE_IDENTITY, SystemCollections.CACHE_SCHEMA_MIGRATIONS, SystemCollections.SRS_CHAINS,
                SystemCollections.SRS_MODELS, SystemCollections.SRS_MODEL_CONTEXTS, SystemCollections.SRS_EVENTS,
                SystemCollections.SRS_LOG_BOUNDS, SystemCollections.CACHE_WORKLOAD_CLAIMS)) {
            facts.put(resource.collectionName(), resource.on(database).find().sort(new Document("_id", 1)).into(new ArrayList<>()));
        }
        return facts;
    }

    private static SrsLogRecord event(long version, long epoch, WorkloadClaimFence fence) {
        return new SrsLogRecord("offset", Op.INSERT, 1, null, Map.of("id", 1), version, fence, epoch);
    }
    private static MongoSrsLogStore modelLog(Fixture f) { return new MongoSrsLogStore(f.client(), SystemCollections.SRS_EVENTS.on(f.database()),
            SystemCollections.SRS_LOG_BOUNDS.on(f.database()), SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()), f.models()); }
    private static void eventUnavailable(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> {
        assertThat(error.code()).isEqualTo(IoError.SRS_EVENT_MODEL_UNAVAILABLE); assertThat(error.getCause()).isNull();
    }); }

    private static boolean publish(MongoSrsModelStore store, SrsModel model, WorkloadClaimFence fence) {
        try { store.publish(model, fence); return true; }
        catch (TapstateException conflict) { if (conflict.code() != IoError.SRS_MODEL_CONFLICT) throw conflict; return false; }
    }
    private static SourceTable table(String name, Long count) {
        var number = new NumericType(64, true, false, false, new BigDecimal("-999.00"), new BigDecimal("999.00"), 18, 2);
        return new SourceTable("orders", List.of(new SourceField(name, "DECIMAL(18,2)", TapstateType.DECIMAL, null, number)),
                List.of(name), List.of(new SourceIndex("pk_orders", List.of(name), true)), count);
    }
    private static SrsModelKey key(long version) { return new SrsModelKey(CHAIN, "orders", SCOPE.cacheIncarnation(), 7, version); }
    private static SrsModel model(long version, SourceTable table) { return new SrsModel(key(version), RING, table, version); }
    private static com.mongodb.client.MongoCollection<Document> models(MongoDatabase database) { return SystemCollections.SRS_MODELS.on(database); }
    private static Document claim(MongoDatabase database) { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find().first(); }
    private static void unavailable(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
            error -> assertThat(error.code()).isEqualTo(IoError.SRS_MODEL_UNAVAILABLE)); }
    private static void businessFenced(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class,
            error -> assertThat(error.code()).isEqualTo(IoError.CACHE_BUSINESS_FENCED)); }
    private static void unreadable(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> {
        assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE); assertThat(error.getCause()).isNull();
        assertThat(error.getMessage()).doesNotContain("raw-secret-sentinel");
    }); }
    private static void withBusiness(Scenario scenario) {
        try (var client = MongoClients.create(MONGO.getReplicaSetUrl())) {
            var database = client.getDatabase("srs_model_" + UUID.randomUUID().toString().replace("-", ""));
            try {
                try (var session = client.startSession()) {
                    for (var table : List.of(SystemCollections.CACHE_IDENTITY, SystemCollections.CACHE_SCHEMA_MIGRATIONS, SystemCollections.SRS_CHAINS,
                            SystemCollections.SRS_MODELS, SystemCollections.SRS_MODEL_CONTEXTS, SystemCollections.SRS_EVENTS, SystemCollections.SRS_LOG_BOUNDS, SystemCollections.CACHE_WORKLOAD_CLAIMS)) {
                        table.prepareCollectionOn(database, session);
                        for (var index : table.indexes()) IndexEnsure.createInTransaction(session, table.on(database), index);
                    }
                }
                SystemCollections.CACHE_IDENTITY.on(database).insertOne(new Document("_id", "cluster").append("clusterId", SCOPE.clusterId())
                        .append("layoutId", MongoConfigurationDomain.LAYOUT).append("bindingVersion", 1L).append("cacheIncarnation", SCOPE.cacheIncarnation())
                        .append("businessPhase", "READY").append("businessPreparationId", "business-a").append("authGeneration", 1L).append("claimAdmissionFence", 0L));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1).append("phase", "READY").append("epoch", 1L).append("operationId", "business-a"));
                var claim = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database)).acquire(
                        new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.CAPTURE, "capture-a"), new WorkloadOwner("node-a", "boot-a"), 1, Duration.ofSeconds(30)).claim();
                SystemCollections.SRS_CHAINS.on(database).insertOne(new Document("_id", CHAIN).append("formatVersion", 1).append("epoch", 7L)
                        .append("cacheIncarnation", SCOPE.cacheIncarnation()).append("captureId", "capture-a")
                        .append("captureFence", WorkloadClaimDocuments.stored(WorkloadClaimFence.from(claim))).append("retention", null)
                        .append("requestedTables", List.of()).append("servingTables", List.of()).append("modelContextHeads", List.of()).append("modelWriteFence", 0L)
                        .append("checkpointRevision", 0L).append("checkpoint", null).append("checkpointAt", null).append("consumerCount", 0L).append("consumerRevision", 0L));
                scenario.run(new Fixture(client, database, new MongoSrsModelStore(client, database, SCOPE), claim));
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }
    private record Fixture(MongoClient client, MongoDatabase database, MongoSrsModelStore models, WorkloadClaim claim) {
        WorkloadClaimFence fence() { return WorkloadClaimFence.from(claim); }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
