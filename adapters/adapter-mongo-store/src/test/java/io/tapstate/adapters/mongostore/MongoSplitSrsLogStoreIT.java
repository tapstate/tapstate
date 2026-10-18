package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.event.Bytes;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Op;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SrsLogBounds;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Events, bounds and the original claim proof commit together across independent cache collections. */
@RequiresDocker
class MongoSplitSrsLogStoreIT {
    @Container private static final MongoDBContainer MONGO = new MongoDBContainer(DockerImageName.parse("mongo:7.0"))
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");
    private static final String RING = "srs.chain-a.orders";
    private static final String OTHER = "srs.chain-a.customers";

    @Test
    void eventsAndBoundsStaySeparateAndRetainPortableRowsAfterRestartAndFullTrim() {
        withCache(f -> {
            var row = Map.<String, Object>of("amount", new ConvertedValue(new BigDecimal("123.4500"), "DECIMAL128"),
                    "bytes", new Bytes((byte) 0, new byte[]{1, 2, 3}));
            var record = new SrsLogRecord("offset-1", Op.INSERT, 1, null, row, 3, f.fence(), 7);
            f.log().storeAll(RING, 10, List.of(record, record));
            assertThat(f.log().load(RING, 10)).contains(record);
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isEqualTo(2);
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).find().into(new ArrayList<>()))
                    .allSatisfy(event -> assertThat(event.get("_id")).isInstanceOf(Document.class));
            assertThat(SystemCollections.SRS_LOG_BOUNDS.on(f.database()).find().first().get("_id")).isInstanceOf(String.class);
            f.log().trim(RING, 100, f.fence());
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isZero();
            try (var reopened = client()) {
                var log = log(reopened, reopened.getDatabase(f.database().getName()));
                assertThat(log.bounds(RING)).isEqualTo(new SrsLogBounds(11, 11));
                log.store(RING, 12, record);
                assertThat(log.readBatch(RING, 10, 5).records().keySet()).containsExactly(12L);
                assertThat(log.bounds(RING)).isEqualTo(new SrsLogBounds(12, 11));
            }
            assertThat(f.database().listCollectionNames().into(new ArrayList<>()))
                    .containsExactlyInAnyOrder("srs_events", "srs_log_bounds", "workload_claims");
        });
    }

    @Test
    void aBoundsWriteFailureRollsBackTheWholeEventBatchAndItsClaimProof() {
        withCache(f -> {
            SystemCollections.SRS_LOG_BOUNDS.on(f.database()).insertOne(new Document("_id", "srs-log-bounds:" + OTHER)
                    .append("largestSequence", 2L).append("trimmedThrough", -1L));
            SystemCollections.SRS_LOG_BOUNDS.on(f.database()).createIndex(new Document("largestSequence", 1), new IndexOptions().unique(true));
            Document claim = claim(f.database());
            assertThatThrownBy(() -> f.log().storeAll(RING, 1, List.of(record(1, f.fence()), record(2, f.fence())))).isInstanceOf(TapstateException.class);
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isZero();
            assertThat(f.log().bounds(RING)).isEqualTo(new SrsLogBounds(-1, -1));
            assertThat(claim(f.database())).isEqualTo(claim);
        });
    }

    @Test
    void aFailureInsideTheEventBatchLeavesNoPartialRunOrBounds() {
        withCache(f -> {
            SystemCollections.SRS_EVENTS.on(f.database()).createIndex(new Document("ts", 1), new IndexOptions().unique(true));
            Document claim = claim(f.database());
            assertThatThrownBy(() -> f.log().storeAll(RING, 1, List.of(record(1, f.fence()), record(1, f.fence()))))
                    .isInstanceOf(TapstateException.class);
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isZero();
            assertThat(SystemCollections.SRS_LOG_BOUNDS.on(f.database()).countDocuments()).isZero();
            assertThat(claim(f.database())).isEqualTo(claim);
        });
    }

    @Test
    void anUnclaimedSplitTrimCannotRetireAnyConfirmedRows() {
        withCache(f -> {
            f.log().storeAll(RING, 1, List.of(record(1, f.fence()), record(2, f.fence())));
            Document original = claim(f.database());
            assertThatThrownBy(() -> f.log().trim(RING, 1)).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            assertThat(f.log().bounds(RING)).isEqualTo(new SrsLogBounds(2, -1));
            assertThat(f.log().readBatch(RING, 1, 2).records()).hasSize(2);
            assertThat(claim(f.database())).isEqualTo(original);
        });
    }

    @Test
    void failedTrimRollsBackTheBoundaryAndDoesNotRetireAnyRows() {
        withCache(f -> {
            f.log().storeAll(RING, 1, List.of(record(1, f.fence()), record(2, f.fence())));
            Document beforeClaim = claim(f.database());
            var admin = f.client().getDatabase("admin");
            admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", new Document("times", 1))
                    .append("data", new Document("failCommands", List.of("delete")).append("errorCode", 121)));
            try { assertThatThrownBy(() -> f.log().trim(RING, 1, f.fence())).isInstanceOf(TapstateException.class); }
            finally { admin.runCommand(new Document("configureFailPoint", "failCommand").append("mode", "off")); }
            assertThat(f.log().bounds(RING)).isEqualTo(new SrsLogBounds(2, -1));
            assertThat(f.log().readBatch(RING, 1, 2).records().keySet()).containsExactly(1L, 2L);
            assertThat(claim(f.database())).isEqualTo(beforeClaim);
        });
    }

    @Test
    void missingBoundsAreNotRebuiltFromRetainedEventsByAnyOrdinaryOperation() {
        withCache(f -> {
            f.log().store(RING, 1, record(1, f.fence()));
            SystemCollections.SRS_LOG_BOUNDS.on(f.database()).deleteMany(new Document());
            assertThatThrownBy(() -> f.log().bounds(RING)).satisfies(MongoSplitSrsLogStoreIT::unreadable);
            assertThatThrownBy(() -> f.log().load(RING, 1)).satisfies(MongoSplitSrsLogStoreIT::unreadable);
            assertThatThrownBy(() -> f.log().readBatch(RING, 1, 2)).satisfies(MongoSplitSrsLogStoreIT::unreadable);
            assertThatThrownBy(() -> f.log().store(RING, 2, record(2, f.fence()))).satisfies(MongoSplitSrsLogStoreIT::unreadable);
            assertThatThrownBy(() -> f.log().trim(RING, 1, f.fence())).satisfies(MongoSplitSrsLogStoreIT::unreadable);
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isOne();
            assertThat(SystemCollections.SRS_LOG_BOUNDS.on(f.database()).countDocuments()).isZero();
        });
    }

    @Test
    void supersededCaptureCannotChangeEitherTheLogOrItsHighWaterMark() {
        withCache(f -> {
            f.log().store(RING, 1, record(1, f.fence()));
            var claims = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(f.database()));
            assertThat(claims.release(f.claim())).isTrue();
            var next = claims.acquire(f.claim().key(), new WorkloadOwner("node-b", "boot-b"), 8, Duration.ofSeconds(30)).claim();
            assertThatThrownBy(() -> f.log().store(RING, 2, record(2, f.fence()))).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            assertThatThrownBy(() -> f.log().trim(RING, 1, f.fence())).isInstanceOfSatisfying(TapstateException.class,
                    failure -> assertThat(failure.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            assertThat(f.log().bounds(RING)).isEqualTo(new SrsLogBounds(1, -1));
            assertThat(f.log().load(RING, 2)).isEmpty();
            f.log().store(RING, 2, record(2, WorkloadClaimFence.from(next)));
            assertThat(f.log().bounds(RING)).isEqualTo(new SrsLogBounds(2, -1));
        });
    }

    @Test
    void concurrentOriginalOwnerAppendsKeepEveryRowAndTheMaximumBoundary() {
        withCache(f -> {
            try (var other = client(); var executor = Executors.newFixedThreadPool(2)) {
                var ready = new CountDownLatch(2); var start = new CountDownLatch(1);
                var a = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue(); f.log().store(RING, 1, record(1, f.fence())); return true; });
                var b = executor.submit(() -> { ready.countDown(); assertThat(start.await(10, TimeUnit.SECONDS)).isTrue();
                    log(other, other.getDatabase(f.database().getName())).store(RING, 2, record(2, f.fence())); return true; });
                assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue(); start.countDown();
                assertThat(a.get(20, TimeUnit.SECONDS)).isTrue(); assertThat(b.get(20, TimeUnit.SECONDS)).isTrue();
            }
            assertThat(f.log().readBatch(RING, 1, 3).records().keySet()).containsExactly(1L, 2L);
            assertThat(f.log().bounds(RING)).isEqualTo(new SrsLogBounds(2, -1));
        });
    }

    @Test
    void boundedReadsAndTrimPreserveGapsAndOtherRings() {
        withCache(f -> {
            f.log().storeAll(RING, 10, List.of(record(1, f.fence()), record(2, f.fence()), record(3, f.fence())));
            f.log().store(OTHER, 11, record(4, f.fence()));
            SystemCollections.SRS_EVENTS.on(f.database()).deleteOne(new Document("_id", new Document("ring", RING).append("seq", 11L)));
            assertThat(f.log().readBatch(RING, 10, 3).records().keySet()).containsExactly(10L, 12L);
            f.log().trim(RING, 10, f.fence());
            assertThat(f.log().load(OTHER, 11)).isPresent();
            assertThat(f.log().bounds(RING)).isEqualTo(new SrsLogBounds(12, 10));
        });
    }

    @Test
    void aNoFenceBatchAlsoRollsBackEveryEventAndBoundaryOnAnEventWriteFailure() {
        withCache(f -> {
            SystemCollections.SRS_EVENTS.on(f.database()).createIndex(new Document("ts", 1), new IndexOptions().unique(true));
            Document claim = claim(f.database());
            var noFence = new SrsLogRecord("offset", Op.INSERT, 1, null, Map.of("id", 1), 3, 7);
            assertThatThrownBy(() -> f.log().storeAll(RING, 1, List.of(noFence, noFence))).isInstanceOf(TapstateException.class);
            assertThat(SystemCollections.SRS_EVENTS.on(f.database()).countDocuments()).isZero();
            assertThat(SystemCollections.SRS_LOG_BOUNDS.on(f.database()).countDocuments()).isZero();
            assertThat(claim(f.database())).isEqualTo(claim);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"claimGeneration", "executionGeneration", "topologyRevision"})
    void aCorruptFenceCounterCannotBeRoundedIntoAnotherCaptureIdentity(String field) {
        withCache(f -> {
            f.log().store(RING, 1, record(1, f.fence()));
            SystemCollections.SRS_EVENTS.on(f.database()).updateOne(new Document(),
                    new Document("$set", new Document("captureFence." + field, 1.5)));
            assertThatThrownBy(() -> f.log().load(RING, 1)).satisfies(MongoSplitSrsLogStoreIT::unreadable);
            assertThatThrownBy(() -> f.log().readBatch(RING, 1, 1)).satisfies(MongoSplitSrsLogStoreIT::unreadable);
        });
    }

    @Test
    void corruptionFailsClosedWithoutLeakingStoredEventOrBoundValues() {
        withCache(f -> {
            f.log().store(RING, 1, record(1, f.fence()));
            SystemCollections.SRS_LOG_BOUNDS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("largestSequence", "raw-secret-sentinel")));
            assertThatThrownBy(() -> f.log().bounds(RING)).satisfies(MongoSplitSrsLogStoreIT::unreadable);
            SystemCollections.SRS_LOG_BOUNDS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("largestSequence", 1L)));
            SystemCollections.SRS_EVENTS.on(f.database()).updateOne(new Document(), new Document("$set", new Document("op", "raw-secret-sentinel")));
            assertThatThrownBy(() -> f.log().readBatch(RING, 1, 1)).satisfies(MongoSplitSrsLogStoreIT::unreadable);
        });
    }

    private static SrsLogRecord record(long ts, WorkloadClaimFence fence) { return new SrsLogRecord("offset-" + ts, Op.INSERT, ts, null, Map.of("id", ts), 3, fence, 7); }
    private static Document claim(MongoDatabase database) { return SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database).find().first(); }
    private static MongoClient client() { return MongoClients.create(MongoClientSettings.builder().applyConnectionString(new ConnectionString(MONGO.getReplicaSetUrl()))
            .applyToConnectionPoolSettings(pool -> pool.maxSize(1)).build()); }
    private static MongoSrsLogStore log(MongoClient client, MongoDatabase database) { return new MongoSrsLogStore(client,
            SystemCollections.SRS_EVENTS.on(database), SystemCollections.SRS_LOG_BOUNDS.on(database), SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database)); }
    private static void unreadable(Throwable failure) { assertThat(failure).isInstanceOfSatisfying(TapstateException.class, error -> {
        assertThat(error.code()).isEqualTo(IoError.DOCUMENT_UNREADABLE); assertThat(error.getCause()).isNull();
        assertThat(error.getMessage()).doesNotContain("raw-secret-sentinel");
    }); }
    private static void withCache(Scenario scenario) {
        try (var client = client()) {
            var database = client.getDatabase("split_srs_" + UUID.randomUUID().toString().replace("-", ""));
            try {
                try (var session = client.startSession()) {
                    for (var table : List.of(SystemCollections.SRS_EVENTS, SystemCollections.SRS_LOG_BOUNDS, SystemCollections.CACHE_WORKLOAD_CLAIMS)) table.prepareCollectionOn(database, session);
                }
                var claim = new MongoWorkloadClaimStore(SystemCollections.CACHE_WORKLOAD_CLAIMS.on(database))
                        .acquire(new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, "capture-a"), new WorkloadOwner("node-a", "boot-a"), 7, Duration.ofSeconds(30)).claim();
                scenario.run(new Fixture(client, database, log(client, database), claim));
            } catch (Exception failure) { throw new AssertionError(failure); }
            finally { database.drop(); }
        }
    }
    private record Fixture(MongoClient client, MongoDatabase database, MongoSrsLogStore log, WorkloadClaim claim) {
        WorkloadClaimFence fence() { return WorkloadClaimFence.from(claim); }
    }
    @FunctionalInterface private interface Scenario { void run(Fixture fixture) throws Exception; }
}
