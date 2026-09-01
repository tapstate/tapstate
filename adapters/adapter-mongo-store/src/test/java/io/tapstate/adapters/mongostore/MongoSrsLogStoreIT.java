package io.tapstate.adapters.mongostore;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import io.tapstate.core.event.Op;
import io.tapstate.spi.store.SrsLogRecord;
import io.tapstate.testsupport.RequiresDocker;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

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

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

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
                    new SrsLogRecord("a", Op.INSERT, 1L, null, Map.of("id", 1), 0L),
                    new SrsLogRecord("b", Op.INSERT, 2L, null, Map.of("id", 2), 0L),
                    new SrsLogRecord("c", Op.INSERT, 3L, null, Map.of("id", 3), 0L)));
            store.store(OTHER, 1L, new SrsLogRecord("x", Op.INSERT, 1L, null, Map.of("id", 9), 0L));

            store.trim(RING, 2L);

            assertThat(store.load(RING, 1L)).isEmpty();
            assertThat(store.load(RING, 2L)).isEmpty();
            assertThat(store.load(RING, 3L).orElseThrow().srcToken()).isEqualTo("c");
            assertThat(store.load(OTHER, 1L))
                    .as("a trim cuts the ring it names; reaching into the ring beside it would drop changes "
                            + "another table's consumers have not read")
                    .isPresent();
        });
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
