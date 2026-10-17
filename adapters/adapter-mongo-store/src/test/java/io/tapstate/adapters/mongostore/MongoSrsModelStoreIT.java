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
            var checks = new AtomicInteger(); Document claim = claim(f.database());
            var guarded = f.models().guardedBy(() -> {
                if (checks.incrementAndGet() == 2) throw new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null);
            });
            assertThatThrownBy(() -> guarded.publish(model(0, table("id", null)), f.fence())).satisfies(MongoSrsModelStoreIT::businessFenced);
            assertThat(models(f.database()).countDocuments()).isZero();
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
            log.trim(RING, 1); SystemCollections.SRS_LOG_BOUNDS.on(f.database()).deleteMany(new Document());
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
            if (fullyTrimmed) log.trim(RING, 1);
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
            log.store(RING, 1, event(0, 7, f.fence())); log.trim(RING, 1);
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
            var boundsWritten = new AtomicBoolean(); var inBounds = new AtomicBoolean(); var checks = new AtomicInteger();
            var witness = new CommandListener() {
                @Override public void commandStarted(CommandStartedEvent event) {
                    if (event.getCommandName().equals("update")) inBounds.set(event.getCommand().getString("update").getValue().equals("srs_log_bounds"));
                }
                @Override public void commandSucceeded(CommandSucceededEvent event) {
                    if (event.getCommandName().equals("update") && inBounds.get()) boundsWritten.set(true);
                }
            };
            try (var observed = MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
                    .addCommandListener(witness).build())) {
                var database = observed.getDatabase(f.database().getName());
                var guarded = new MongoSrsModelStore(observed, database, SCOPE).guardedBy(() -> {
                    if (checks.incrementAndGet() == 2) {
                        assertThat(boundsWritten.get()).isTrue();
                        throw new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null);
                    }
                });
                var log = new MongoSrsLogStore(observed, SystemCollections.SRS_EVENTS.on(database), SystemCollections.SRS_LOG_BOUNDS.on(database),
                        SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database), guarded);
                assertThatThrownBy(() -> log.store(RING, 1, event(0, 7, f.fence()))).satisfies(MongoSrsModelStoreIT::businessFenced);
            }
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isZero();
            assertThat(modelLog(f).bounds(RING)).isEqualTo(new SrsLogBounds(-1, -1));
            assertThat(claim(f.database())).isEqualTo(before);
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
                assertThat(queries.get()).isEqualTo(4);
                queries.set(0);
                assertThat(log.readBatch(RING, 1, 25).records()).hasSize(25);
                assertThat(queries.get()).isEqualTo(4);
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
                            SystemCollections.SRS_MODELS, SystemCollections.SRS_EVENTS, SystemCollections.SRS_LOG_BOUNDS, SystemCollections.CACHE_WORKLOAD_CLAIMS)) {
                        table.prepareCollectionOn(database, session);
                        for (var index : table.indexes()) IndexEnsure.createInTransaction(session, table.on(database), index);
                    }
                }
                SystemCollections.CACHE_IDENTITY.on(database).insertOne(new Document("_id", "cluster").append("clusterId", SCOPE.clusterId())
                        .append("layoutId", MongoConfigurationDomain.LAYOUT).append("bindingVersion", 1L).append("cacheIncarnation", SCOPE.cacheIncarnation())
                        .append("businessPhase", "READY").append("businessPreparationId", "business-a").append("authGeneration", 1L));
                SystemCollections.CACHE_SCHEMA_MIGRATIONS.on(database).insertOne(new Document("_id", "business").append("installedVersion", 1).append("phase", "READY").append("epoch", 1L).append("operationId", "business-a"));
                SystemCollections.SRS_CHAINS.on(database).insertOne(new Document("_id", CHAIN).append("epoch", 7L)
                        .append("cacheIncarnation", SCOPE.cacheIncarnation()).append("captureId", "capture-a"));
                var claim = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database)).acquire(
                        new WorkloadClaimKey(SCOPE.clusterId(), WorkloadClaimType.CAPTURE, "capture-a"), new WorkloadOwner("node-a", "boot-a"), 1, Duration.ofSeconds(30)).claim();
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
