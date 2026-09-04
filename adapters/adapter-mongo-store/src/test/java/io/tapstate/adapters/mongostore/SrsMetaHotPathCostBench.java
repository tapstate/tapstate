package io.tapstate.adapters.mongostore;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import io.tapstate.core.event.ChainPosition;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.ConsumerOffset;
import io.tapstate.spi.store.SchemaVersion;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import io.tapstate.testsupport.RequiresDocker;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.bson.Document;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Measures what one cdc run pays the coordination record against a real endpoint, so that making the
 * record's writes rarer can be argued from a number rather than from the shape of the code.
 *
 * <p>The cdc write path touches one document — the chain's — and touches it three ways per run of
 * changes: the headroom bound reads it, the durable-frontier bound reads it again, and the advanced
 * read offset writes it. All three are paid synchronously on the connector's own callback thread, so
 * the source reads nothing further while they are outstanding: their sum is the run's floor, and the
 * reciprocal of that sum is the ceiling on how fast changes can be taken.
 *
 * <p>Two axes grow the document, and they grow it for reasons nothing trims: a consumer offset per
 * pipeline sharing the chain, and a schema version per DDL the source has ever emitted. Both are
 * walked here rather than assumed, because a read whose cost does not move with the document is a
 * different problem from one that does, and only the second gets worse in a long-lived deployment.
 *
 * <p>The absent read is the floor every other number sits on: it carries no document back, so what is
 * left is the round trip and the lookup. Reading a real number as the record's own cost without taking
 * that off would credit the record with the endpoint's latency.
 *
 * <p>This is an instrument, not a regression test: wall-clock numbers against a container do not
 * belong in a build gate. Its name keeps it out of the default surefire selection and out of the
 * failsafe one, so it costs a build nothing and is run deliberately:
 *
 * <pre>{@code mvn -o test -pl adapters/adapter-mongo-store -am -Dtest=SrsMetaHotPathCostBench \
 *   -DfailIfNoTests=false -Dapi.version=1.44}</pre>
 */
@RequiresDocker
class SrsMetaHotPathCostBench {

    private static final DockerImageName MONGO_IMAGE = DockerImageName.parse("mongo:7.0");

    /**
     * Consumer offsets on the chain — one per pipeline sharing it. Nothing trims these while the
     * pipelines exist, and every one of them is carried back on every read of the record.
     */
    private static final List<Integer> CONSUMERS = List.of(1, 4, 16);

    /**
     * Schema versions in the record's append-only history — one per DDL the source has emitted. This
     * axis only ever grows, so it is the one that decides what the read costs after a year rather than
     * on the day the chain was made.
     */
    private static final List<Integer> DDLS = List.of(0, 50, 500);

    /** Tables per consumer offset: the per-table cursor map each consumer carries. */
    private static final int TABLES = 8;

    /**
     * The table count a wide pipeline reads. Snapshot completion is a table name per finished table, so
     * this is the axis along which the marks grow -- and the per-table read cursor grows along it too.
     */
    private static final int WIDE_TABLES = 100;

    /**
     * How many changes one burst carries. Large enough that the steady state dominates the first few
     * round trips, small enough that the whole bench still finishes in a coffee break.
     */
    private static final int BURST = 2000;

    private static final int WARMUP = 20;
    private static final int RUNS = 200;

    @Container
    private static final MongoDBContainer REPLICA_SET = new MongoDBContainer(MONGO_IMAGE);

    @Test
    void whatOneCdcRunPaysTheCoordinationRecord() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("tapstate");
            MongoCollection<Document> collection = database.getCollection("srs_meta");
            MongoSrsMetaStore store = new MongoSrsMetaStore(collection);

            System.out.println("consumers,ddls,doc_bytes,path,p50_us,p99_us,max_us,ops_per_second");
            report(0, 0, 0, "ping", ping(database));
            report(0, 0, 0, "absent-read", absentRead(store));

            // One axis at a time: a cost that moves on both is attributable to neither if they are
            // only ever walked together.
            for (int consumers : CONSUMERS) {
                measureShape(store, collection, consumers, 0);
            }
            for (int ddls : DDLS) {
                if (ddls != 0) {
                    measureShape(store, collection, 1, ddls);
                }
            }
            // The shape a long-lived shared chain actually reaches, both axes at once -- and the A/B for
            // moving snapshot completion onto the consumer record. The hot path reads the consumers and
            // nothing else, so the marks ride along on a read that never looks at them: the pair below
            // differs in that one thing and in nothing else, which is the only way to say what carrying
            // them costs. Read the two `read-projected` rows against each other -- not against a number
            // from another machine or another day.
            measureShape(store, collection, 16, 500, 0);
            measureShape(store, collection, 16, 500, TABLES);

            // The same A/B at the table count where the marks could actually start to matter. They are a
            // table name each, so what they cost grows with how many tables a pipeline reads -- and the
            // per-table cursor beside them grows the same way, which is why both arms carry the wide
            // cursor and only the marks differ. Without this pair the reading would hold at eight tables
            // and say nothing about a hundred, which is the size this product is aimed at.
            measureShape(store, collection, 16, 0, 0, WIDE_TABLES);
            measureShape(store, collection, 16, 0, WIDE_TABLES, WIDE_TABLES);
        }
    }

    /**
     * The three touches of one run, against a record grown to the given shape. The read is measured
     * twice under one name: the hot path makes two of them per run today, and what a run pays is the
     * pair, not one of them.
     */
    /**
     * What a burst of changes actually sustains, end to end, rather than what adding two medians predicts.
     *
     * <p>The rest of this bench times one touch at a time and reports a median. A rate built by adding
     * two of those medians and inverting the sum is arithmetic, not a measurement: it assumes every
     * round trip pays the median, that nothing overlaps, and that the record's working set behaves the
     * same on the two-thousandth change as on the first. None of those is a safe assumption, and which
     * way they push is not obvious in advance -- connection reuse and the driver's pipelining make the
     * real rate higher, while the growth of the record and the write concern's durability make it lower.
     *
     * <p>So this drives the two touches the hot path actually makes per change -- one projected read of
     * the consumer cursors, one advance of the read offset -- back to back with nothing in between, and
     * times the whole run. The per-change touch count is not assumed here either; it is pinned by a case
     * elsewhere that counts the calls rather than timing them.
     *
     * <p>Two shapes, and the pair is the point. A chain with no schema history is what a young one looks
     * like; one with five hundred entries is what a long-lived one becomes, since nothing trims that
     * history. If the projection is doing its job the two rates are close, and if it ever stops the
     * second collapses.
     *
     * <p>What this does not include: any connector, any Jet vertex, any ring. It is the coordination
     * record's own ceiling -- the thing that has to be higher than the rate a source can deliver, not
     * an end-to-end throughput figure.
     */
    @Test
    void whatABurstOfChangesSustainsAgainstTheRecord() {
        try (MongoClient client = MongoClients.create(REPLICA_SET.getReplicaSetUrl())) {
            MongoDatabase database = client.getDatabase("tapstate");
            MongoCollection<Document> collection = database.getCollection("srs_meta");
            MongoSrsMetaStore store = new MongoSrsMetaStore(collection);

            System.out.println("consumers,ddls,doc_bytes,path,changes,elapsed_ms,sustained_ev_s");
            burst(store, collection, 16, 0);
            burst(store, collection, 16, 500);
        }
    }

    /** One burst against a record of the given shape, timed as a whole. */
    private static void burst(
            MongoSrsMetaStore store, MongoCollection<Document> collection, int consumers, int ddls) {
        String chain = "burst-c" + consumers + "-d" + ddls;
        seed(store, chain, consumers, ddls, 0, TABLES);
        long bytes = documentBytes(collection, chain);

        // Warmed so that what is timed is the rate a burst settles at, not the cost of getting there.
        for (int i = 1; i <= WARMUP; i++) {
            store.consumerOffsets(chain);
            store.advanceSourceReadOffset(chain, position(i));
        }

        long start = System.nanoTime();
        for (int i = 1; i <= BURST; i++) {
            store.consumerOffsets(chain);
            // Strictly ahead of everything written so far, so every write takes the matching path --
            // the repeated-advance path is a different cost and is measured on its own above.
            store.advanceSourceReadOffset(chain, position(WARMUP + i));
        }
        long elapsed = System.nanoTime() - start;

        System.out.printf(
                "%d,%d,%d,burst-sustained,%d,%d,%d%n",
                consumers, ddls, bytes, BURST, elapsed / 1_000_000L,
                BURST * 1_000_000_000L / Math.max(elapsed, 1));
    }

    private static void measureShape(
            MongoSrsMetaStore store, MongoCollection<Document> collection, int consumers, int ddls) {
        measureShape(store, collection, consumers, ddls, 0);
    }

    /**
     * The same, over a record whose consumers each carry {@code completed} finished tables. Completion is
     * recorded per pipeline, on the consumer's own record, so it grows the very sub-document the hot path
     * projects -- and what that costs is a measurement, not a deduction.
     */
    private static void measureShape(MongoSrsMetaStore store, MongoCollection<Document> collection,
            int consumers, int ddls, int completed) {
        measureShape(store, collection, consumers, ddls, completed, TABLES);
    }

    /** The same again, over consumers whose per-table cursor covers {@code tables} tables. */
    private static void measureShape(MongoSrsMetaStore store, MongoCollection<Document> collection,
            int consumers, int ddls, int completed, int tables) {
        String chain = "bench-c" + consumers + "-d" + ddls + "-k" + completed + "-t" + tables;
        seed(store, chain, consumers, ddls, completed, tables);
        long bytes = documentBytes(collection, chain);

        report(consumers, ddls, bytes, "read", read(store, chain));
        report(consumers, ddls, bytes, "read-projected", readProjected(store, chain));
        report(consumers, ddls, bytes, "advance-offset", advance(store, chain));
        report(consumers, ddls, bytes, "advance-repeated", advanceRepeated(store, chain));
    }

    /**
     * The narrow read the hot path actually makes -- the store's own method, not a projection written out
     * again here. Both bounds the hot path takes from this record are functions of the consumer offsets
     * alone, so the schema history would otherwise be carried back across the wire on every change and
     * then not looked at.
     *
     * <p>Going through the store rather than the driver is deliberate: a bench that re-wrote the
     * projection would measure one nothing ships, and would keep reporting the saving after a change to
     * the real one had taken it away.
     */
    private static long[] readProjected(MongoSrsMetaStore store, String chain) {
        for (int i = 0; i < WARMUP; i++) {
            store.consumerOffsets(chain);
        }
        long[] taken = new long[RUNS];
        for (int i = 0; i < RUNS; i++) {
            long start = System.nanoTime();
            List<ConsumerOffset> found = store.consumerOffsets(chain);
            taken[i] = System.nanoTime() - start;
            if (found.isEmpty()) {
                throw new IllegalStateException("the chain just seeded carried no cursors: " + chain);
            }
        }
        return taken;
    }

    /**
     * An advance to a position the record has already reached -- what a chain whose slowest sink is not
     * landing anything resolves on every run of changes.
     *
     * <p>It is measured because it is not free and does not look expensive. The advance is admitted by a
     * filter that requires a strictly greater position, so this one matches nothing and changes nothing;
     * the store then cannot tell "did not move forward" from "chain was never seeded" without looking, so
     * it takes a second trip, and that one is an unprojected read of the whole record. A repeat therefore
     * costs a write that writes nothing plus the most expensive read on the path -- which is why skipping
     * it in the caller is worth more than the write it appears to save.
     */
    private static long[] advanceRepeated(MongoSrsMetaStore store, String chain) {
        ChainPosition reached = position(RUNS + WARMUP + 1);
        store.advanceSourceReadOffset(chain, reached);
        // Every call below is now behind what the record holds, so every one takes the no-match path.
        ChainPosition behind = position(1);
        for (int i = 0; i < WARMUP; i++) {
            store.advanceSourceReadOffset(chain, behind);
        }
        long[] taken = new long[RUNS];
        for (int i = 0; i < RUNS; i++) {
            long start = System.nanoTime();
            store.advanceSourceReadOffset(chain, behind);
            taken[i] = System.nanoTime() - start;
        }
        return taken;
    }

    /**
     * Builds the record through the store's own mutators rather than by writing a document directly,
     * so what is measured is the shape production actually produces — including how the driver
     * serialises it — and not a hand-rolled approximation of it.
     */
    private static void seed(
            MongoSrsMetaStore store, String chain, int consumers, int ddls, int completed, int tables) {
        store.create(chain, "P7D");
        for (int c = 0; c < consumers; c++) {
            Map<String, Long> perTable = new LinkedHashMap<>();
            for (int t = 0; t < tables; t++) {
                perTable.put("table_" + t, (long) (t * 1000 + c));
            }
            store.upsertConsumerOffset(chain, new ConsumerOffset(
                    "pipeline-" + c, perTable,
                    new ChainPosition(new SourceOrder(1L, c), "acked-token-" + c)));
            // Through the real mutator, so what is measured is the document the sink actually leaves.
            for (int t = 0; t < completed; t++) {
                store.markSnapshotComplete(chain, "pipeline-" + c, "table_" + t);
            }
        }
        for (int d = 0; d < ddls; d++) {
            store.appendSchemaVersion(chain, new SchemaVersion(d + 1, columns(), d + 1));
        }
    }

    /** A schema of the size a real table's carries, so the history axis grows at a realistic rate. */
    private static Map<String, Object> columns() {
        Map<String, Object> schema = new LinkedHashMap<>();
        for (int i = 0; i < 20; i++) {
            schema.put("column_" + i, Map.of("type", "varchar", "length", 255, "nullable", true));
        }
        return schema;
    }

    private static long documentBytes(MongoCollection<Document> collection, String chain) {
        Document found = collection.find(new Document("_id", chain)).first();
        if (found == null) {
            throw new IllegalStateException("the chain just seeded was not there: " + chain);
        }
        return found.toBsonDocument().toString().length();
    }

    /**
     * A command that touches no storage at all. What is left is the trip out and back, so the gap
     * between this and everything else is what the endpoint actually does, and this number on its own
     * says how much of every other number belongs to the machine the bench ran on.
     */
    private static long[] ping(MongoDatabase database) {
        Document command = new Document("ping", 1);
        for (int i = 0; i < WARMUP; i++) {
            database.runCommand(command);
        }
        long[] taken = new long[RUNS];
        for (int i = 0; i < RUNS; i++) {
            long start = System.nanoTime();
            database.runCommand(command);
            taken[i] = System.nanoTime() - start;
        }
        return taken;
    }

    /** A read of a chain nothing ever created — the round trip and the lookup, carrying nothing back. */
    private static long[] absentRead(MongoSrsMetaStore store) {
        for (int i = 0; i < WARMUP; i++) {
            store.read("absent-" + i);
        }
        long[] taken = new long[RUNS];
        for (int i = 0; i < RUNS; i++) {
            long start = System.nanoTime();
            store.read("absent-run-" + i);
            taken[i] = System.nanoTime() - start;
        }
        return taken;
    }

    /** One read of the whole record, which both bounds on the hot path make separately. */
    private static long[] read(MongoSrsMetaStore store, String chain) {
        for (int i = 0; i < WARMUP; i++) {
            store.read(chain);
        }
        long[] taken = new long[RUNS];
        for (int i = 0; i < RUNS; i++) {
            long start = System.nanoTime();
            if (store.read(chain).isEmpty()) {
                throw new IllegalStateException("the chain just seeded was not there: " + chain);
            }
            taken[i] = System.nanoTime() - start;
        }
        return taken;
    }

    /** One advance of the read offset, which a run makes once — the write the record pays for. */
    private static long[] advance(MongoSrsMetaStore store, String chain) {
        for (int i = 0; i < WARMUP; i++) {
            store.advanceSourceReadOffset(chain, position(i));
        }
        long[] taken = new long[RUNS];
        for (int i = 0; i < RUNS; i++) {
            ChainPosition next = position(WARMUP + i);
            long start = System.nanoTime();
            store.advanceSourceReadOffset(chain, next);
            taken[i] = System.nanoTime() - start;
        }
        return taken;
    }

    private static ChainPosition position(int i) {
        return new ChainPosition(new SourceOrder(1L, i), "read-token-" + i);
    }

    private static void report(int consumers, int ddls, long bytes, String path, long[] taken) {
        long[] sorted = taken.clone();
        Arrays.sort(sorted);
        long p50 = sorted[(int) (sorted.length * 0.50)];
        long p99 = sorted[(int) (sorted.length * 0.99)];
        long max = sorted[sorted.length - 1];
        System.out.printf(
                "%d,%d,%d,%s,%d,%d,%d,%d%n",
                consumers, ddls, bytes, path, p50 / 1_000, p99 / 1_000, max / 1_000,
                1_000_000_000L / Math.max(p99, 1));
    }

    /**
     * The one thing in here that fails rather than prints: reading what every consumer has acked costs the
     * same however many of them there are.
     *
     * <p>The rest of this file is an instrument. It reports times, and a time is the wrong thing to assert
     * on -- a threshold in microseconds passes or fails on how busy the machine is, and the figure it would
     * be compared against is from another day and another machine. What the design actually claims is
     * structural: the slowest consumer is found in one round trip to the record, not in one per consumer.
     * That is a count, it is the same everywhere, and an implementation that asked per consumer shows
     * sixteen against one.
     *
     * <p>Counted at the driver rather than inside the store, because the store is the thing under test:
     * asking it how many times it went to the endpoint would be taking its own word for it. Only read
     * commands are counted -- the connection's own chatter is not a trip this code chose to make.
     *
     * <p>A first call is made and discarded before each count, or the first shape pays for whatever the
     * driver does once per collection and the comparison is between a cold read and a warm one.
     */
    @Test
    void takingTheSlowestConsumerCostsOneRoundTripHoweverManyThereAre() {
        CountingReads counting = new CountingReads();
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(REPLICA_SET.getReplicaSetUrl()))
                .addCommandListener(counting)
                .build();
        try (MongoClient client = MongoClients.create(settings)) {
            MongoCollection<Document> collection =
                    client.getDatabase("tapstate").getCollection("srs_meta");
            MongoSrsMetaStore store = new MongoSrsMetaStore(collection);

            long forOne = readsFor(counting, store, "count-c1", 1);
            long forSixteen = readsFor(counting, store, "count-c16", 16);

            assertThat(forOne)
                    .as("reads the hot path makes for a chain with one consumer -- the bound it needs is a "
                            + "function of the consumer offsets, and they are one record")
                    .isEqualTo(1);
            assertThat(forSixteen)
                    .as("and for a chain with sixteen: the same, because the slowest is found in the record "
                            + "rather than by asking each consumer. An implementation that asked per "
                            + "consumer reads sixteen here and one above -- the shape this catches and no "
                            + "timing on one machine reliably would")
                    .isEqualTo(forOne);
        }
    }

    /** The read commands one hot-path read costs, over a chain seeded with the given consumer count. */
    private static long readsFor(
            CountingReads counting, MongoSrsMetaStore store, String chain, int consumers) {
        seed(store, chain, consumers, 0, 0, TABLES);
        store.consumerOffsets(chain);
        counting.reset();
        store.consumerOffsets(chain);
        return counting.count();
    }

    /**
     * Counts the driver's read commands. Monitoring and handshake traffic is left out: that is the
     * connection keeping itself alive, not a trip this code asked for.
     */
    private static final class CountingReads implements CommandListener {

        private static final Set<String> READS = Set.of("find", "aggregate", "getMore", "count");

        private final AtomicLong seen = new AtomicLong();

        @Override
        public void commandStarted(CommandStartedEvent event) {
            if (READS.contains(event.getCommandName())) {
                seen.incrementAndGet();
            }
        }

        void reset() {
            seen.set(0);
        }

        long count() {
            return seen.get();
        }
    }
}
