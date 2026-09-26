package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.event.ConvertedValue;
import io.tapstate.core.event.Op;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.SrsLogRecord;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Witnesses the durable change log against a real Mongo: a change round-trips by (ring, sequence), a run
 * of changes lands at consecutive sequences in one act, the largest sequence a ring has reached is
 * reported back, a trim drops the front of one ring only, and none of it reaches across ring names.
 *
 * <p>The cross-ring assertions are the ones that matter most here. Every key of one ring is a run of the
 * same index the other rings live in, so a bound that is off by one ring boundary reads or deletes
 * another table's changes -- and both failures look like ordinary success from inside the ring that
 * asked. Where Docker is absent this aborts on a developer machine and fails in CI, where a skip would
 * be a green build that ran nothing.
 */
@RequiresDocker
class MongoSrsLogStoreIT {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");
    private static final String RING = "srs.mc-1.orders";
    private static final String OTHER = "srs.mc-1.customers";

    /** The application name a case gives the client whose writes it holds on the server. */
    private static final String HELD_CLIENT = "held-old-owner";

    /** Test commands on, so a case can hold one client's write on the server and pin an interleaving. */
    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE)
            .withCommand("--replSet", "docker-rs", "--setParameter", "enableTestCommands=1");

    @Test
    void storeThenLoadRoundTripsAChange() {
        withStore(store -> {
            store.store(RING, 100L, new SrsLogRecord("bin.4:91827", Op.UPDATE, 42L,
                    Map.of("id", 1, "amount", "10.00"), Map.of("id", 1, "amount", "12.50"), 3L));

            SrsLogRecord read = store.load(RING, 100L).orElseThrow();
            assertThat(read.srcToken()).isEqualTo("bin.4:91827");
            assertThat(read.op()).isEqualTo(Op.UPDATE);
            assertThat(read.ts()).isEqualTo(42L);
            assertThat(read.before()).containsEntry("amount", "10.00");
            assertThat(read.after()).containsEntry("amount", "12.50");
            assertThat(read.schemaVer()).isEqualTo(3L);
        });
    }

    @Test
    void aChangeTheSourceStatedNoPositionAtReadsBackWithNone() {
        withStore(store -> {
            store.store(RING, 7L, new SrsLogRecord(null, Op.INSERT, 1L, null, Map.of("id", 1), 0L));

            SrsLogRecord read = store.load(RING, 7L).orElseThrow();
            assertThat(read.srcToken())
                    .as("the absence of a position is the record's meaning, so it must not come back as "
                            + "an empty string that a reader would treat as a position")
                    .isNull();
            assertThat(read.before()).isNull();
        });
    }

    @Test
    void loadOfASequenceTheLogNeverSawIsEmpty() {
        withStore(store -> assertThat(store.load(RING, 5L)).isEmpty());
    }

    @Test
    void storeAllLandsARunAtConsecutiveSequences() {
        withStore(store -> {
            store.storeAll(RING, 10L, List.of(
                    new SrsLogRecord("a", Op.INSERT, 1L, null, Map.of("id", 1), 0L),
                    new SrsLogRecord("b", Op.INSERT, 2L, null, Map.of("id", 2), 0L),
                    new SrsLogRecord("c", Op.INSERT, 3L, null, Map.of("id", 3), 0L)));

            assertThat(store.load(RING, 10L).orElseThrow().srcToken()).isEqualTo("a");
            assertThat(store.load(RING, 11L).orElseThrow().srcToken()).isEqualTo("b");
            assertThat(store.load(RING, 12L).orElseThrow().srcToken()).isEqualTo("c");
        });
    }

    @Test
    void storeAllOfAnEmptyRunWritesNothing() {
        withStore(store -> {
            store.storeAll(RING, 10L, List.of());

            assertThat(store.largestSequence(RING)).isEqualTo(-1L);
        });
    }

    @Test
    void largestSequenceIsMinusOneForARingTheLogHasNeverSeen() {
        withStore(store -> assertThat(store.largestSequence("srs.mc-1.unseen")).isEqualTo(-1L));
    }

    @Test
    void largestSequenceIsTheHighestThisRingReached() {
        withStore(store -> {
            store.storeAll(RING, 98L, List.of(
                    new SrsLogRecord("a", Op.INSERT, 1L, null, Map.of("id", 1), 0L),
                    new SrsLogRecord("b", Op.INSERT, 2L, null, Map.of("id", 2), 0L)));

            assertThat(store.largestSequence(RING))
                    .as("a rebuilt ring resumes numbering above this, so a sequence keeps naming the same "
                            + "change across a restart")
                    .isEqualTo(99L);
        });
    }

    @Test
    void largestSequenceReadsOnlyItsOwnRing() {
        withStore(store -> {
            store.store(OTHER, 500L, new SrsLogRecord("x", Op.INSERT, 1L, null, Map.of("id", 9), 0L));
            store.store(RING, 3L, new SrsLogRecord("a", Op.INSERT, 1L, null, Map.of("id", 1), 0L));

            assertThat(store.largestSequence(RING))
                    .as("every ring lives in the same index, so a bound that runs past this ring reports a "
                            + "sequence from another table -- and a rebuilt ring would then skip that far")
                    .isEqualTo(3L);
        });
    }

    @Test
    void trimDropsTheFrontOfOneRingAndLeavesTheRest() {
        withStore(store -> {
            store.storeAll(RING, 1L, List.of(
                    new SrsLogRecord("a", Op.INSERT, 1L, null, Map.of("id", 1), 0L, null, 1L),
                    new SrsLogRecord("b", Op.INSERT, 2L, null, Map.of("id", 2), 0L, null, 1L),
                    new SrsLogRecord("c", Op.INSERT, 3L, null, Map.of("id", 3), 0L, null, 1L)));
            store.store(OTHER, 1L, new SrsLogRecord("x", Op.INSERT, 1L, null, Map.of("id", 9), 0L));

            store.trim(RING, 2L, 1L);

            assertThat(store.load(RING, 1L)).isEmpty();
            assertThat(store.load(RING, 2L)).isEmpty();
            assertThat(store.load(RING, 3L).orElseThrow().srcToken()).isEqualTo("c");
            assertThat(store.load(OTHER, 1L))
                    .as("a trim cuts the ring it names; reaching into the ring beside it would drop changes "
                            + "another table's consumers have not read")
                    .isPresent();
        });
    }

    @Test
    void delayedTrimFromAnOldGenerationCannotDeleteAReusedSequence() {
        withStore(store -> {
            store.store(RING, 0L, new SrsLogRecord("old", Op.INSERT, 1L,
                    null, Map.of("id", 1), 0L, null, 1L));
            store.store(RING, 0L, new SrsLogRecord("new", Op.INSERT, 2L,
                    null, Map.of("id", 2), 0L, null, 2L));

            store.trim(RING, 0L, 1L);

            assertThat(store.load(RING, 0L))
                    .as("a delayed cut from an earlier ring generation must not delete a new change "
                            + "at the sequence that generation used")
                    .get().extracting(SrsLogRecord::srcToken).isEqualTo("new");
        });
    }

    @Test
    void aFullyConfirmedRingRetainsItsHighestSequenceAcrossRecreate() {
        withStore(store -> {
            store.storeAll(RING, 0L, List.of(
                    new SrsLogRecord("a", Op.INSERT, 1L, null, Map.of("id", 1), 0L, null, 1L),
                    new SrsLogRecord("b", Op.INSERT, 2L, null, Map.of("id", 2), 0L, null, 1L)));

            store.trim(RING, 1L, 1L);

            assertThat(store.load(RING, 0L)).isEmpty();
            assertThat(store.load(RING, 1L)).isPresent();
            assertThat(store.largestSequence(RING)).isEqualTo(1L);

            // A new physical subscription can keep the ring epoch. Its rebuilt ring resumes after the
            // retained high-water record, so a delayed cut from the old subscription stops before it.
            store.store(RING, 2L, new SrsLogRecord("new", Op.INSERT, 3L,
                    null, Map.of("id", 3), 0L, null, 1L));
            store.trim(RING, 1L, 1L);
            assertThat(store.load(RING, 2L).orElseThrow().srcToken()).isEqualTo("new");
        });
    }

    @Test
    void aGenerationTrimRetainsLegacyUntaggedRecords() {
        withStore(store -> {
            store.store(RING, 0L, new SrsLogRecord("legacy", Op.INSERT, 1L,
                    null, Map.of("id", 1), 0L));
            store.store(RING, 1L, new SrsLogRecord("current", Op.INSERT, 2L,
                    null, Map.of("id", 2), 0L, null, 2L));
            store.store(RING, 2L, new SrsLogRecord("high-water", Op.INSERT, 3L,
                    null, Map.of("id", 3), 0L, null, 2L));

            store.trim(RING, 1L, 2L);

            assertThat(store.load(RING, 0L)).isPresent();
            assertThat(store.load(RING, 1L)).isEmpty();
            assertThat(store.load(RING, 2L)).isPresent();
        });
    }

    @Test
    void aChangeWhoseValuesTheConnectorConvertedRoundTrips() {
        withStore(store -> {
            store.store(RING, 200L, new SrsLogRecord("bin.4:91827", Op.UPDATE, 42L, null,
                    Map.of("_id", new ConvertedValue("650f1a2b3c4d5e6f70819200", "OBJECT_ID"),
                            "amount", "12.50"),
                    3L));

            SrsLogRecord read = store.load(RING, 200L).orElseThrow();
            assertThat(read.after())
                    .as("a source whose key the connector converts carries one of these on every change, "
                            + "so a log that cannot store one stores nothing at all -- and the server has "
                            + "to accept the field names the encoding uses, which only a real one says")
                    .containsEntry("_id", new ConvertedValue("650f1a2b3c4d5e6f70819200", "OBJECT_ID"))
                    .containsEntry("amount", "12.50");
        });
    }

    @Test
    void anExactDecimalReloadsAsThePortableValueTheChangeCarried() {
        withStore(store -> {
            BigDecimal exact = new BigDecimal("1234567890.123456789012345678901234");
            store.store(RING, 201L, new SrsLogRecord("bin.4:91828", Op.INSERT, 43L, null,
                    Map.of("amount", new ConvertedValue(exact, "DECIMAL128")), 3L));

            Object amount = ((ConvertedValue) store.load(RING, 201L)
                    .orElseThrow()
                    .after()
                    .get("amount"))
                    .value();
            assertThat(amount)
                    .as("a durable reload must not replace the portable decimal with a Mongo driver value")
                    .isEqualTo(exact)
                    .isInstanceOf(BigDecimal.class);
        });
    }

    @Test
    void aSupersededCaptureGenerationCannotAppendEvenWhenTheOldProcessStillRuns() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            var database = client.getDatabase("tapstate_fence_" + System.nanoTime());
            var claimsCollection = database.getCollection("workload_claims");
            var logCollection = database.getCollection("srs_log");
            MongoWorkloadClaimStore claims = new MongoWorkloadClaimStore(claimsCollection);
            MongoSrsLogStore log = new MongoSrsLogStore(client, logCollection, claimsCollection);
            WorkloadClaimKey key =
                    new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, "capture-orders");

            WorkloadClaim first = claims.acquire(
                    key, new WorkloadOwner("node-a", "boot-a"), 7, Duration.ofSeconds(30)).claim();
            WorkloadClaimFence stale = WorkloadClaimFence.from(first);
            SrsLogRecord firstRecord = new SrsLogRecord(
                    "p1", Op.INSERT, 1L, null, Map.of("id", 1), 0L, stale);
            log.storeAll(RING, 1L, List.of(firstRecord));

            assertThat(claims.release(first)).isTrue();
            WorkloadClaim second = claims.acquire(
                    key, new WorkloadOwner("node-b", "boot-b"), 8, Duration.ofSeconds(30)).claim();
            assertThat(second.claimGeneration()).isEqualTo(first.claimGeneration() + 1);
            assertThat(second.owner()).isEqualTo(new WorkloadOwner("node-b", "boot-b"));

            assertThatThrownBy(() -> log.storeAll(RING, 2L, List.of(new SrsLogRecord(
                            "p2-stale", Op.INSERT, 2L, null, Map.of("id", 2), 0L, stale))))
                    .isInstanceOfSatisfying(TapstateException.class,
                            coded -> assertThat(coded.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
            assertThat(log.load(RING, 2L)).isEmpty();

            WorkloadClaimFence current = WorkloadClaimFence.from(second);
            log.storeAll(RING, 2L, List.of(new SrsLogRecord(
                    "p2", Op.INSERT, 2L, null, Map.of("id", 2), 0L, current)));
            assertThat(log.load(RING, 2L).orElseThrow().captureFence()).isEqualTo(current);
        }
    }

    /**
     * A takeover committed while an old owner's append is in flight leaves that append unable to land after
     * it -- the promise a capture fence makes.
     *
     * <p>The append used to prove its claim with a read, and under snapshot isolation a read conflicts with
     * no write: the old owner read its claim while it was live, the lease ran out, the new owner took the
     * claim over, and the old owner's append then committed after the takeover all the same. The
     * interleaving is pinned rather than raced for: the old owner's next write is held on the server for
     * longer than its lease has left, and the claim is taken over inside that pause.
     */
    @Test
    void anAppendInFlightWhenTheClaimIsTakenOverCannotLandAfterTheTakeover() throws Exception {
        String url = REPLICA_SET.getReplicaSetUrl();
        try (MongoClient admin = MongoClients.create(url);
             MongoClient oldOwner = MongoClients.create(MongoClientSettings.builder()
                     .applyConnectionString(new ConnectionString(url))
                     .applicationName(HELD_CLIENT)
                     .build())) {
            String databaseName = "tapstate_takeover_" + System.nanoTime();
            MongoWorkloadClaimStore claims =
                    new MongoWorkloadClaimStore(admin.getDatabase(databaseName).getCollection("workload_claims"));
            MongoSrsLogStore oldOwnersLog = new MongoSrsLogStore(oldOwner,
                    oldOwner.getDatabase(databaseName).getCollection("srs_log"),
                    oldOwner.getDatabase(databaseName).getCollection("workload_claims"));
            WorkloadClaimKey key = new WorkloadClaimKey("cluster-a", WorkloadClaimType.CAPTURE, "capture-orders");
            long leaseTakenAt = System.nanoTime();
            WorkloadClaim first = claims.acquire(
                    key, new WorkloadOwner("node-a", "boot-a"), 7, Duration.ofSeconds(1)).claim();
            WorkloadClaimFence stale = WorkloadClaimFence.from(first);
            holdNextWrite(admin, Duration.ofSeconds(4));
            ExecutorService threads = Executors.newFixedThreadPool(2);
            try {
                Future<Long> append = threads.submit(() -> {
                    oldOwnersLog.storeAll(RING, 1L, List.of(
                            new SrsLogRecord("p1", Op.INSERT, 1L, null, Map.of("id", 1), 0L, stale)));
                    return System.nanoTime();
                });
                // Past the old lease, with the old owner's append still held on the server.
                long pastTheLease = Duration.ofMillis(1500).toNanos() - (System.nanoTime() - leaseTakenAt);
                Thread.sleep(Math.max(0L, Duration.ofNanos(pastTheLease).toMillis()));
                Future<Long> takeover = threads.submit(() -> {
                    assertThat(claims.acquire(key, new WorkloadOwner("node-b", "boot-b"), 8, Duration.ofSeconds(30))
                            .acquired()).isTrue();
                    return System.nanoTime();
                });

                Throwable appendFailure = null;
                long appendReturnedAt = 0L;
                try {
                    appendReturnedAt = append.get(30, TimeUnit.SECONDS);
                } catch (ExecutionException ended) {
                    appendFailure = ended.getCause();
                }
                long takenOverAt = takeover.get(30, TimeUnit.SECONDS);

                if (appendFailure != null) {
                    assertThat(appendFailure).isInstanceOfSatisfying(TapstateException.class,
                            coded -> assertThat(coded.code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED));
                    assertThat(new MongoSrsLogStore(admin.getDatabase(databaseName).getCollection("srs_log"))
                            .load(RING, 1L)).isEmpty();
                } else {
                    assertThat(Duration.ofNanos(appendReturnedAt - takenOverAt))
                            .as("an append by the superseded owner committed after the takeover had")
                            .isLessThan(Duration.ofMillis(500));
                }
            } finally {
                threads.shutdownNow();
                releaseHeldWrites(admin);
            }
        }
    }

    /** Holds the held client's next write on the server for {@code hold} before it runs. */
    private static void holdNextWrite(MongoClient admin, Duration hold) {
        admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                .append("mode", new Document("times", 1))
                .append("data", new Document("failCommands", List.of("update"))
                        .append("blockConnection", true)
                        .append("blockTimeMS", hold.toMillis())
                        .append("appName", HELD_CLIENT)));
    }

    private static void releaseHeldWrites(MongoClient admin) {
        admin.getDatabase("admin").runCommand(new Document("configureFailPoint", "failCommand")
                .append("mode", "off"));
    }

    private static void withStore(Consumer<MongoSrsLogStore> body) {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getConnectionString())) {
            MongoCollection<Document> collection = client
                    .getDatabase("tapstate_test")
                    .getCollection("srs_log_" + System.nanoTime());
            body.accept(new MongoSrsLogStore(collection));
        }
    }
}
