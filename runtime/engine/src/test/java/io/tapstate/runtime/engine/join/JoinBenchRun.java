package io.tapstate.runtime.engine.join;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.EdgeConfig;
import io.tapstate.core.common.TapstateType;
import io.tapstate.core.event.Envelope;
import io.tapstate.core.sql.JoinPlan;
import io.tapstate.core.sql.SourceColumn;
import io.tapstate.core.sql.SourceTable;
import io.tapstate.core.sql.SqlFrontEnd;
import io.tapstate.spi.store.KeyedStateStore;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * What the join operator costs, measured on the operator alone.
 *
 * <p><b>No build runs this.</b> The name matches none of the surefire include patterns, so it is
 * reached only by naming it: {@code mvn -pl runtime/engine -am test -Dtest=JoinBenchRun}. Sizes, the
 * scenario list and where the state sits are system properties, listed on {@link Knob}.
 *
 * <p><b>Why the operator alone.</b> An end-to-end run cannot attribute what it measures: the sink, the
 * substrate and the operator share one clock, and which term moved is not recoverable from the total. So
 * the sink here counts and discards, there is no Jet job and there is no container - what is left in the
 * timed region is the mirror reads, the row assembly and the emission.
 *
 * <p><b>The estimate this was expected to confirm is the one it overturned, so it is not repeated here.</b>
 * The carrier was chosen under a figure that put a full phase's operator at a small fraction of the wall
 * clock, with the sink as the ceiling - and that figure was taken on a mechanism this product no longer
 * uses for its full phase. Measured on the path that ships, the full phase's operator is several times
 * slower than the sink it feeds. Isolating the operator is therefore not a way of magnifying a small
 * term; it is measuring the dominant one.
 *
 * <p><b>Every scenario carries an arm that fails if the number means nothing</b>, because each way this
 * measurement can be empty produces a fast, plausible number rather than an error:
 *
 * <ul>
 *   <li><b>Rows are counted against what the scenario must emit.</b> A carrier that emits nothing is
 *       the fastest carrier there is, and against a counting sink it leaves no other trace.
 *   <li><b>The tier is checked by what is resident, not by what it was configured to be.</b> The tier
 *       is a size on a map config; when it fails to bite, the run is the resident one wearing the cold
 *       label - a difference of orders of magnitude, reported as a mild one. So a cold run asserts most
 *       of its entries really did leave memory, and a resident run asserts none of them did.
 *   <li><b>{@link #r5} is a pair.</b> Its point is that an edit outside the projection costs nothing,
 *       and "emitted no rows" is equally what a key with no fact rows under it produces. So the same
 *       key is edited twice - once outside the projection, once inside - and the second must rebuild
 *       the whole bucket. Only the pair says the filter is what did it.
 * </ul>
 *
 * <p><b>Two things sit outside the clock on purpose</b>, both large enough to bury the operator: rows
 * are built before the clock starts, and every scenario runs twice with the second reported, so what is
 * timed is compiled rather than interpreted. The large-bucket scenarios build theirs as they load them
 * instead of holding the whole corpus, which is the same thing - their clock covers one later edit, so
 * no generator can reach it either way.
 *
 * <p><b>What the cold layer here is.</b> A map in this heap, so a read through it is a hash lookup
 * rather than a network round trip. That makes the <em>trip counts</em> real and the <em>cold latency
 * absent</em>: this measures how often the operator reaches the layer, never what reaching it costs on
 * a real one. What a trip costs against a real database was measured separately and is not re-derived
 * here.
 *
 * <p><b>The resident tier is not "no cold layer".</b> Every backed tier writes through on every write,
 * so a resident run still pays serialisation per write and differs from a cold one only in what is read
 * back. The {@code nostore} tier is what separates those two costs, and is why it exists.
 */
class JoinBenchRun {

    /** System properties this reads, all optional. */
    private static final class Knob {

        /** Which scenarios, comma-separated, or {@code all}. */
        static final String SCENARIOS = "joinbench.scenarios";

        /** Where the state sits: {@code nostore}, {@code hot}, {@code mixed} or {@code cold}. */
        static final String TIER = "joinbench.tier";

        /** How many dimension rows. */
        static final String DIMS = "joinbench.dims";

        /** How many fact rows per dimension row. */
        static final String FANOUT = "joinbench.fanout";

        /** How many fact rows sit under the one large key of the high fan-out scenarios. */
        static final String WIDE_FANOUT = "joinbench.wideFanout";

        /** How many incremental batches, and how many changes in each. */
        static final String BATCHES = "joinbench.batches";
        static final String BATCH = "joinbench.batch";

        /**
         * How many partitions the member is given. This is the second cross-cutting variable, reached
         * from the substrate's side rather than the key's.
         *
         * <p><b>What the variable is really asking is how many partitions one batch read splits
         * across</b>, because each partition asks the layer beneath for its own share - so a read of
         * many keys spread widely is many calls, and the same read over few partitions is few. Pinning
         * keys into one partition would ask the same question from the key's side, but it cannot be
         * done here without changing what the keys are: the fact key is rendered by the driver from
         * the row's own key column, and the reverse index is keyed by a record rather than a string,
         * which no key-side trick reaches at all. Turning the partition count instead moves exactly
         * the quantity in question and leaves every key as it was.
         */
        static final String PARTITIONS = "joinbench.partitions";

        /** How many changes are handed over in one call. See {@link JoinBenchRun#DELIVERY}. */
        static final String DELIVERY = "joinbench.delivery";

        private Knob() {
        }
    }

    private static final String PIPELINE = "bench";
    private static final String STEP = "join";
    private static final String FACT = "o";
    private static final String DIM = "c";
    private static final String OUT = "order_state";

    /** How many filler columns each side carries in the wide shape, on top of its named ones. */
    private static final int WIDE_FILLER = 23;

    /**
     * How many changes reach the vertex in one delivery. The substrate hands a vertex what its inbound
     * queue holds, so the default is that queue's default size rather than a number picked here - the
     * point of the bench is to feed the operator what the product feeds it.
     *
     * <p>It is a knob because it is the one variable the operator's mirror reads are a function of, and
     * <b>{@code 1} is the shape the operator had before it read a delivery's keys together</b>: at one
     * change per call the driver's read ahead does not engage at all. So the arm to compare against is
     * a property away rather than a build away, which is what keeps the two arms otherwise identical.
     */
    private static final int DELIVERY = intProperty(Knob.DELIVERY, EdgeConfig.DEFAULT_QUEUE_SIZE);

    @Test
    void measure() {
        String want = System.getProperty(Knob.SCENARIOS, "all");
        String tier = System.getProperty(Knob.TIER, "hot");
        Sizes sizes = new Sizes(intProperty(Knob.DIMS, 2_000), intProperty(Knob.FANOUT, 10),
                intProperty(Knob.WIDE_FANOUT, 20_000), intProperty(Knob.BATCHES, 20),
                intProperty(Knob.BATCH, 500));

        System.out.println("# joinbench tier=" + tier + " partitions="
                + System.getProperty(Knob.PARTITIONS, "271 (default)")
                + " delivery=" + DELIVERY + " " + sizes);
        System.out.println(Result.header());
        for (String name : scenarios(want)) {
            // Twice, reporting the second: the first is the interpreter, and reporting it would be
            // measuring the JVM's warm-up rather than the operator.
            run(name, tier, sizes);
            System.out.println(run(name, tier, sizes).row());
            System.out.flush();
        }
    }

    private static List<String> scenarios(String want) {
        List<String> all = List.of("F1", "F2", "I1", "I2", "I3", "I4", "R1", "R2", "R3", "R4", "R5");
        return "all".equals(want) ? all : List.of(want.split(","));
    }

    private static Result run(String name, String tier, Sizes sizes) {
        try (Rig rig = new Rig(name, tier)) {
            return switch (name) {
                case "F1" -> f1(rig, sizes);
                case "F2" -> f2(rig, sizes);
                case "I1" -> i1(rig, sizes);
                case "I2" -> i2(rig, sizes);
                case "I3" -> i3(rig, sizes);
                case "I4" -> i4(rig, sizes);
                case "R1" -> r1(rig, sizes);
                case "R2" -> r2(rig, sizes);
                case "R3" -> r3(rig, sizes);
                case "R4" -> r4(rig, sizes);
                case "R5" -> r5(rig, sizes);
                default -> throw new IllegalArgumentException("no scenario called " + name);
            };
        }
    }

    // ---------------------------------------------------------------- full phase

    /** The front of the stream, one fact row per dimension row, narrow rows. */
    private static Result f1(Rig rig, Sizes sizes) {
        Shape shape = Shape.narrow();
        rig.open(shape);
        List<SourceChange> dimensions = shape.dimensionReads(sizes.dims());
        List<SourceChange> facts = shape.factReads(sizes.dims(), 1);

        rig.settle();
        long began = System.nanoTime();
        rig.feed(dimensions);
        rig.feed(facts);
        long nanos = System.nanoTime() - began;

        return rig.result("F1", "1:1 narrow, dims then facts", nanos, sizes.dims());
    }

    /** The same, at a low fan-out and on the fifty-column row the throughput baseline was taken on. */
    private static Result f2(Rig rig, Sizes sizes) {
        Shape shape = Shape.wide();
        rig.open(shape);
        int facts = sizes.dims() * sizes.fanout();
        List<SourceChange> dimensions = shape.dimensionReads(sizes.dims());
        List<SourceChange> factRows = shape.factReads(sizes.dims(), sizes.fanout());

        rig.settle();
        long began = System.nanoTime();
        rig.feed(dimensions);
        rig.feed(factRows);
        long nanos = System.nanoTime() - began;

        return rig.result("F2", "1:" + sizes.fanout() + " wide, dims then facts", nanos, facts);
    }

    // ---------------------------------------------------------------- incremental

    /** Steady state, nothing but new fact rows arriving. */
    private static Result i1(Rig rig, Sizes sizes) {
        Shape shape = Shape.wide();
        rig.open(shape);
        rig.load(shape, sizes);

        int first = sizes.dims() * sizes.fanout();
        List<List<SourceChange>> work = new ArrayList<>();
        for (int b = 0; b < sizes.batches(); b++) {
            List<SourceChange> changes = new ArrayList<>(sizes.batch());
            for (int i = 0; i < sizes.batch(); i++) {
                int id = first + b * sizes.batch() + i;
                changes.add(shape.factInsert(id, id % sizes.dims()));
            }
            work.add(changes);
        }
        return rig.batched("I1", "inserts only", work, (long) sizes.batches() * sizes.batch());
    }

    /** A real change stream: the delete path maintains the index, and inserts alone never reach it. */
    private static Result i2(Rig rig, Sizes sizes) {
        Shape shape = Shape.wide();
        rig.open(shape);
        rig.load(shape, sizes);

        int existing = sizes.dims() * sizes.fanout();
        List<List<SourceChange>> work = new ArrayList<>();
        int fresh = existing;
        for (int b = 0; b < sizes.batches(); b++) {
            List<SourceChange> changes = new ArrayList<>(sizes.batch());
            for (int i = 0; i < sizes.batch(); i++) {
                int slot = b * sizes.batch() + i;
                int id = slot % existing;
                switch (i % 3) {
                    case 0 -> changes.add(shape.factInsert(fresh++, id % sizes.dims()));
                    // Neither of the other two moves the join key: that is I3, and mixing it in here
                    // would put its double emission into this scenario's rate.
                    case 1 -> changes.add(shape.factUpdate(id, id % sizes.dims(), id % sizes.dims()));
                    default -> changes.add(shape.factDelete(id, id % sizes.dims()));
                }
            }
            work.add(changes);
        }
        return rig.batched("I2", "insert / update / delete", work,
                (long) sizes.batches() * sizes.batch());
    }

    /**
     * A fact row's join key moving. Two published rows come of one change - the row under the old key
     * is a different row and nothing else would ever remove it - so this is the dearest single event.
     */
    private static Result i3(Rig rig, Sizes sizes) {
        Shape shape = Shape.wide();
        rig.open(shape);
        rig.load(shape, sizes);

        int existing = sizes.dims() * sizes.fanout();
        int moves = Math.min(sizes.batches() * sizes.batch(), existing);
        int perBatch = Math.max(1, moves / sizes.batches());
        List<List<SourceChange>> work = new ArrayList<>();
        int id = 0;
        for (int b = 0; b < sizes.batches(); b++) {
            List<SourceChange> changes = new ArrayList<>(perBatch);
            for (int i = 0; i < perBatch && id < moves; i++, id++) {
                // Each id is re-pointed once. Re-pointing one twice would file a second index entry
                // under a key it already sits in, which is a different scenario wearing this name.
                int was = id % sizes.dims();
                changes.add(shape.factUpdate(id, was, (was + 1) % sizes.dims()));
            }
            work.add(changes);
        }
        // Two rows per change: the removal under the old key, and the row under the new one.
        return rig.batched("I3", "join key re-pointed", work, 2L * id);
    }

    /** The same steady state over a two-column key: length-prefixed encoding and null poisoning. */
    private static Result i4(Rig rig, Sizes sizes) {
        Shape shape = Shape.compound();
        rig.open(shape);
        rig.load(shape, sizes);

        int first = sizes.dims() * sizes.fanout();
        List<List<SourceChange>> work = new ArrayList<>();
        for (int b = 0; b < sizes.batches(); b++) {
            List<SourceChange> changes = new ArrayList<>(sizes.batch());
            for (int i = 0; i < sizes.batch(); i++) {
                int id = first + b * sizes.batch() + i;
                changes.add(shape.factInsert(id, id % sizes.dims()));
            }
            work.add(changes);
        }
        return rig.batched("I4", "inserts, two-column key", work,
                (long) sizes.batches() * sizes.batch());
    }

    // ---------------------------------------------------------------- recompute

    /**
     * The unit cost of a rebuild. Without it the large one below cannot be attributed: a slow rebuild
     * of a million rows is either an expensive row or a great many of them, and only the pair says
     * which.
     */
    private static Result r1(Rig rig, Sizes sizes) {
        Shape shape = Shape.wide();
        rig.open(shape);
        rig.load(shape, sizes);

        int edits = Math.min(sizes.batches(), sizes.dims());
        List<SourceChange> work = new ArrayList<>(edits);
        for (int i = 0; i < edits; i++) {
            work.add(shape.dimensionRename(i));
        }

        rig.settle();
        long began = System.nanoTime();
        rig.feed(work);
        long nanos = System.nanoTime() - began;

        return rig.result("R1", "rebuild 1:" + sizes.fanout() + ", " + edits + " edits", nanos,
                (long) edits * sizes.fanout());
    }

    /** One dimension key holding a great many fact rows: the frailest shape this operator has. */
    private static Result r2(Rig rig, Sizes sizes) {
        Shape shape = Shape.wide();
        rig.open(shape);
        // One key, everything under it. Dimension first, so the load itself queues no rebuild.
        rig.feed(shape.dimensionReads(1));
        rig.loadFactsUnder(shape, 0, 0, sizes.wideFanout());

        rig.settle();
        long began = System.nanoTime();
        rig.feed(List.of(shape.dimensionRename(0)));
        long nanos = System.nanoTime() - began;

        return rig.result("R2", "rebuild 1:" + sizes.wideFanout() + ", one edit", nanos,
                sizes.wideFanout());
    }

    /**
     * What a real workload is shaped like: mostly small buckets with a few enormous ones. Measuring
     * only the two ends misses it, and the ends are what a synthetic corpus naturally holds.
     */
    private static Result r3(Rig rig, Sizes sizes) {
        Shape shape = Shape.wide();
        rig.open(shape);
        rig.feed(shape.dimensionReads(sizes.dims()));
        rig.feed(shape.factReads(sizes.dims(), sizes.fanout()));
        // One key given the whole large bucket on top of its ordinary share.
        rig.loadFactsUnder(shape, 0, sizes.dims() * sizes.fanout(), sizes.wideFanout());

        int edits = Math.min(sizes.batches(), sizes.dims());
        List<SourceChange> work = new ArrayList<>(edits);
        for (int i = 0; i < edits; i++) {
            work.add(shape.dimensionRename(i));
        }

        rig.settle();
        long began = System.nanoTime();
        rig.feed(work);
        long nanos = System.nanoTime() - began;

        // Key 0 is one of the edited ones and carries its ordinary share as well as the large bucket.
        long rows = (long) edits * sizes.fanout() + sizes.wideFanout();
        return rig.result("R3", (edits - 1) + " x 1:" + sizes.fanout() + " + 1 x 1:"
                + (sizes.wideFanout() + sizes.fanout()), nanos, rows);
    }

    /**
     * The seventh kind of event: a dimension row arriving for fact rows that were published without
     * one. They are found again only because a miss was written into the bucket too.
     */
    private static Result r4(Rig rig, Sizes sizes) {
        Shape shape = Shape.wide();
        rig.open(shape);
        // Facts first and no dimension at all, so every one of them is published as a miss.
        rig.loadFactsUnder(shape, 0, 0, sizes.wideFanout());

        rig.settle();
        long began = System.nanoTime();
        rig.feed(shape.dimensionReads(1));
        long nanos = System.nanoTime() - began;

        return rig.result("R4", sizes.wideFanout() + " miss rows turn to matches", nanos,
                sizes.wideFanout());
    }

    /**
     * The filter that decides how often the expensive thing happens at all - and the one number here
     * that is supposed to be zero.
     *
     * <p><b>Reported as a pair, because a zero on its own says nothing.</b> An edit outside the
     * projection emitting no rows, and a key with no fact rows under it emitting no rows, are the same
     * observation. So the same key is then edited inside the projection and must rebuild the whole
     * bucket; the second arm is what makes the first one mean the filter.
     */
    private static Result r5(Rig rig, Sizes sizes) {
        Shape shape = Shape.wide();
        rig.open(shape);
        rig.feed(shape.dimensionReads(1));
        rig.loadFactsUnder(shape, 0, 0, sizes.wideFanout());

        rig.settle();
        long began = System.nanoTime();
        for (int i = 0; i < sizes.batches(); i++) {
            rig.feed(List.of(shape.dimensionTouchUnpublished(0, i)));
        }
        long nanos = System.nanoTime() - began;

        long filtered = rig.emitted();
        if (filtered != 0) {
            throw new AssertionError("an edit outside the projection rebuilt something: " + filtered
                    + " rows");
        }
        // The other arm, on the same key, so the zero above is the filter rather than an empty bucket.
        // Not timed: it is here to be a witness, not a measurement.
        rig.feed(List.of(shape.dimensionRename(0)));
        long rebuilt = rig.emitted();
        if (rebuilt != sizes.wideFanout()) {
            throw new AssertionError("the paired arm did not rebuild the bucket, so the zero above is "
                    + "not the admission filter: " + rebuilt + " rows of " + sizes.wideFanout());
        }

        return rig.result("R5", sizes.batches() + " edits outside a 1:" + sizes.wideFanout()
                + " projection; the paired arm rebuilt " + rebuilt, nanos, 0);
    }

    // ---------------------------------------------------------------- the rig

    /** How large everything is. */
    private record Sizes(int dims, int fanout, int wideFanout, int batches, int batch) {

        @Override
        public String toString() {
            return "dims=" + dims + " fanout=" + fanout + " wideFanout=" + wideFanout + " batches="
                    + batches + " batch=" + batch;
        }
    }

    /**
     * One member, one cold layer and one carrier, standing for the length of one scenario.
     *
     * <p>A member per scenario rather than one for all of them: the state a scenario builds is what the
     * next one would be reading, and a rebuild that found another scenario's rows would be timing them.
     */
    private static final class Rig implements AutoCloseable {

        private final HazelcastInstance member;
        private final ColdLayer cold = new ColdLayer();
        private final BuiltinJoinExecutor executor;
        private final String tier;
        private final Counting sink = new Counting();
        private long emittedAtLastReset;

        private Rig(String scenario, String tier) {
            this.tier = tier;
            Config config = new Config();
            config.setClusterName("joinbench-" + scenario + "-" + System.nanoTime());
            String partitions = System.getProperty(Knob.PARTITIONS);
            if (partitions != null) {
                config.setProperty("hazelcast.partition.count", partitions);
            }
            config.getJetConfig().setEnabled(false);
            config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
            config.addMapConfig(mapConfigFor(tier));
            member = Hazelcast.newHazelcastInstance(config);
            JoinStateMapStoreFactory.bindTo(member, cold);
            executor = new BuiltinJoinExecutor(List.of("id"), OUT,
                    new ImapJoinStores(member, PIPELINE, STEP));
        }

        private void open(Shape shape) {
            executor.open(shape.plan());
        }

        /**
         * Loads {@code count} fact rows under one dimension key, one at a time.
         *
         * <p><b>Built as it is fed rather than up front, which every other load here does the other
         * way.</b> A million wide rows held as a list beside the same million in the map is twice the
         * memory for no gain: this is untimed setup, so the generator cannot reach any clock, and the
         * ladder that finds where the large bucket stops fitting is the whole reason the scenario
         * exists.
         */
        private void loadFactsUnder(Shape shape, int dimensionId, int from, int count) {
            for (int i = 0; i < count; i++) {
                feed(List.of(shape.factReadUnder(dimensionId, from + i)));
            }
        }

        /** Loads the ordinary corpus: every dimension row, then every fact row under it. */
        private void load(Shape shape, Sizes sizes) {
            feed(shape.dimensionReads(sizes.dims()));
            feed(shape.factReads(sizes.dims(), sizes.fanout()));
        }

        /**
         * Feeds {@code changes} the way the vertex does: a delivery at a time, draining after each.
         *
         * <p><b>Not the whole list in one call, which is what this was first written as.</b> The vertex
         * is handed one delivery per call and drains before taking the next, so a queue of work never
         * grows past a delivery's worth; a run that absorbed two hundred thousand changes before sending
         * any would build a queue that deep and be measuring the queue.
         *
         * <p><b>Nor one change per call, which is what it was written as next.</b> That was right while
         * the vertex was handed one item at a time; it is not now. The driver asks the fact mirror once
         * for the keys a delivery is about to ask about, so feeding it singly turns that one ask into
         * one per row - a round trip per row of the table, on a shape where the answer is always the
         * same. A bench that fed differently from the vertex would be measuring a batch size the
         * product never uses, in whichever direction it happened to differ.
         */
        private void feed(List<SourceChange> changes) {
            for (int from = 0; from < changes.size(); from += DELIVERY) {
                List<SourceChange> delivery =
                        changes.subList(from, Math.min(from + DELIVERY, changes.size()));
                // Called again with nothing until it says it is done: a rebuild outlives the change
                // that caused it, and the call carrying that change is not where it finishes.
                boolean done = executor.apply(delivery, sink);
                while (!done) {
                    done = executor.apply(List.of(), sink);
                }
            }
        }

        /** Puts the counters where the next timed region starts from. */
        private void settle() {
            cold.trips.reset();
            cold.keys.reset();
            cold.tripsWhere.values().forEach(LongAdder::reset);
            emittedAtLastReset = sink.rows;
        }

        /** Rows emitted since this was last asked, or since {@link #settle}. */
        private long emitted() {
            long since = sink.rows - emittedAtLastReset;
            emittedAtLastReset = sink.rows;
            return since;
        }

        /** Times each batch separately, so the shape of the distribution survives. */
        private Result batched(String name, String note, List<List<SourceChange>> work, long expected) {
            settle();
            long[] each = new long[work.size()];
            long total = 0;
            for (int b = 0; b < work.size(); b++) {
                long began = System.nanoTime();
                feed(work.get(b));
                each[b] = System.nanoTime() - began;
                total += each[b];
            }
            return result(name, note, total, expected).withBatches(each);
        }

        private Result result(String name, String note, long nanos, long expected) {
            long rows = emitted();
            if (rows != expected) {
                throw new AssertionError(name + " emitted " + rows + " rows where the scenario is "
                        + expected + " - the clock would be over the wrong work");
            }
            long resident = resident();
            long written = cold.entries();
            checkTierBit(name, resident, written);
            return new Result(name, tier, note, nanos, rows, cold.trips.sum(), cold.keys.sum(),
                    cold.breakdown(), resident, written);
        }

        /**
         * Whether the tier is the one that was asked for, read off what is actually in memory rather
         * than off the configuration that was meant to put it there.
         *
         * <p><b>Not read off the trip count, which was the first thing tried and is wrong.</b> A fact
         * row arriving for the first time is a miss in the map, and a miss on a read-through map reaches
         * the layer whatever the tier - so a resident run makes a trip per new key and "no trips" is
         * true of no tier at all.
         */
        private void checkTierBit(String name, long resident, long written) {
            if ("nostore".equals(tier)) {
                return;
            }
            if ("hot".equals(tier) && resident < written) {
                throw new AssertionError(name + " was asked for on the resident tier and holds "
                        + resident + " of " + written + " entries in memory, so this is a partly cold "
                        + "run under the resident name");
            }
            if ("cold".equals(tier) && resident > written / 2) {
                throw new AssertionError(name + " was asked for on the cold tier and still holds "
                        + resident + " of " + written + " entries in memory, so the eviction did not "
                        + "bite and this is the resident run under the cold name");
            }
        }

        /** How many entries the three maps hold in memory, across this member. */
        private long resident() {
            return owned(JoinMaps.factMirror(PIPELINE, STEP))
                    + owned(JoinMaps.dimensionMirror(PIPELINE, STEP, DIM))
                    + owned(JoinMaps.reverseIndex(PIPELINE, STEP, DIM));
        }

        private long owned(String map) {
            return member.getMap(map).getLocalMapStats().getOwnedEntryCount();
        }

        @Override
        public void close() {
            executor.close();
            member.shutdown();
        }
    }

    /**
     * What the tier name means as a map configuration.
     *
     * <p>{@code nostore} is here so the other three can be attributed. Every backed tier writes through
     * on every write, the resident one included - so "hot" means "nothing is read back", not "there is
     * no cold layer", and the gap between the two is what the write-through itself costs.
     */
    private static MapConfig mapConfigFor(String tier) {
        return switch (tier) {
            case "nostore" -> JoinMaps.stateMaps();
            case "hot" -> JoinMaps.backedStateMaps(Integer.MAX_VALUE - 1);
            case "mixed" -> JoinMaps.backedStateMaps(JoinMaps.DEFAULT_ENTRIES_HELD_IN_MEMORY);
            case "cold" -> JoinMaps.backedStateMaps(1);
            default -> throw new IllegalArgumentException("no tier called " + tier);
        };
    }

    /** A sink that takes everything and keeps only the count: what is left in the clock is the join. */
    private static final class Counting implements JoinSink {

        private long rows;

        @Override
        public boolean offer(Envelope change) {
            rows++;
            return true;
        }
    }

    /**
     * The layer behind the maps, in this heap.
     *
     * <p>It counts its own trips rather than reading them off the member's counters, because those are
     * per namespace and what a scenario wants is the whole operator's reaching. Both count the same
     * calls.
     */
    private static final class ColdLayer implements KeyedStateStore {

        private final Map<String, Map<String, byte[]>> byNamespace = new ConcurrentHashMap<>();
        private final LongAdder trips = new LongAdder();
        private final LongAdder keys = new LongAdder();
        /**
         * Trips split by which of the three maps was read.
         *
         * <p><b>A total on its own cannot be acted on.</b> The three are read for different reasons and
         * only one of them is the recompute's page reading; a number that adds them together says work
         * is happening without saying which work, and every way of making it smaller looks equally
         * plausible against it.
         */
        private final Map<String, LongAdder> tripsWhere = new ConcurrentHashMap<>();

        @Override
        public Optional<byte[]> load(String namespace, String key) {
            trips.increment();
            keys.increment();
            where(namespace).increment();
            return Optional.ofNullable(entries(namespace).get(key));
        }

        @Override
        public Map<String, byte[]> loadAll(String namespace, Collection<String> asked) {
            trips.increment();
            keys.add(asked.size());
            where(namespace).increment();
            Map<String, byte[]> found = new LinkedHashMap<>();
            Map<String, byte[]> entries = entries(namespace);
            for (String key : asked) {
                byte[] state = entries.get(key);
                if (state != null) {
                    found.put(key, state);
                }
            }
            return found;
        }

        @Override
        public void save(String namespace, String key, byte[] state) {
            entries(namespace).put(key, state);
        }

        @Override
        public void delete(String namespace, String key) {
            entries(namespace).remove(key);
        }

        @Override
        public void dropNamespace(String namespace) {
            byNamespace.remove(namespace);
        }

        @Override
        public long count(String namespace) {
            return entries(namespace).size();
        }

        private LongAdder where(String namespace) {
            return tripsWhere.computeIfAbsent(shortNameOf(namespace), ignored -> new LongAdder());
        }

        /** Which of the three maps a namespace is, by the suffix {@link JoinMaps} builds it with. */
        private static String shortNameOf(String namespace) {
            int dot = namespace.indexOf(".", JoinMaps.NAMESPACE_PREFIX.length());
            String tail = dot < 0 ? namespace : namespace.substring(dot + 1);
            if (tail.contains(".index.")) {
                return "index";
            }
            return tail.contains(".dim.") ? "dim" : "fact";
        }

        /** The split, in the order the three are read on the event path. */
        private String breakdown() {
            return "fact=" + at("fact") + " dim=" + at("dim") + " index=" + at("index");
        }

        private long at(String which) {
            LongAdder adder = tripsWhere.get(which);
            return adder == null ? 0 : adder.sum();
        }

        /** Everything this holds, over every namespace: what the resident count is measured against. */
        private long entries() {
            long held = 0;
            for (Map<String, byte[]> entries : byNamespace.values()) {
                held += entries.size();
            }
            return held;
        }

        private Map<String, byte[]> entries(String namespace) {
            return byNamespace.computeIfAbsent(namespace, ignored -> new ConcurrentHashMap<>());
        }
    }

    // ---------------------------------------------------------------- what the rows look like

    /**
     * One corpus: the plan, and rows that are the same every run.
     *
     * <p>The plan comes from SQL through the front end rather than being assembled here, so what is
     * measured is a plan the product can actually produce.
     *
     * <p>Rows are built here and handed over whole before any clock starts. Building a fifty-column row
     * costs microseconds, which is the same order as joining one - so generating inside the timed region
     * would be measuring the generator.
     */
    private static final class Shape {

        private final JoinPlan plan;
        private final List<String> factFiller;
        private final List<String> dimensionFiller;
        private final boolean compound;

        private Shape(JoinPlan plan, List<String> factFiller, List<String> dimensionFiller,
                boolean compound) {
            this.plan = plan;
            this.factFiller = factFiller;
            this.dimensionFiller = dimensionFiller;
            this.compound = compound;
        }

        private JoinPlan plan() {
            return plan;
        }

        /** Three published columns and nothing else: the throughput floor. */
        private static Shape narrow() {
            List<SourceColumn> orders = orderColumns(false);
            List<SourceColumn> customers = customerColumns(false);
            String sql = "SELECT o.id AS order_id, o.amount AS amount, c.name AS customer_name"
                    + " FROM orders o LEFT JOIN customers c ON o.cust_id = c.id";
            return new Shape(derive(sql, orders, customers), List.of(), List.of(), false);
        }

        /** Fifty columns across the two sides, which is what the published throughput was taken on. */
        private static Shape wide() {
            return wideOn(orderColumns(false), customerColumns(false), false);
        }

        /** The same rows over a two-column key: what the encoding and the null rule cost. */
        private static Shape compound() {
            return wideOn(orderColumns(true), customerColumns(true), true);
        }

        private static Shape wideOn(List<SourceColumn> orders, List<SourceColumn> customers,
                boolean compound) {
            List<String> factFiller = filler("f", orders);
            List<String> dimensionFiller = filler("d", customers);
            StringBuilder sql = new StringBuilder(
                    "SELECT o.id AS order_id, o.amount AS amount, c.name AS customer_name");
            for (String column : factFiller) {
                sql.append(", o.").append(column).append(" AS o_").append(column);
            }
            for (String column : dimensionFiller) {
                sql.append(", c.").append(column).append(" AS c_").append(column);
            }
            sql.append(" FROM orders o LEFT JOIN customers c ON o.cust_id = c.id");
            if (compound) {
                sql.append(" AND o.region = c.region");
            }
            return new Shape(derive(sql.toString(), orders, customers), factFiller, dimensionFiller,
                    compound);
        }

        private static List<SourceColumn> orderColumns(boolean compound) {
            List<SourceColumn> columns = new ArrayList<>(List.of(
                    new SourceColumn("id", TapstateType.INT64, false),
                    new SourceColumn("cust_id", TapstateType.INT64, true),
                    new SourceColumn("amount", TapstateType.DECIMAL, true)));
            if (compound) {
                columns.add(new SourceColumn("region", TapstateType.STRING, true));
            }
            return columns;
        }

        private static List<SourceColumn> customerColumns(boolean compound) {
            List<SourceColumn> columns = new ArrayList<>(List.of(
                    new SourceColumn("id", TapstateType.INT64, false),
                    new SourceColumn("name", TapstateType.STRING, true),
                    // Read for nothing and published by nothing: what R5 edits.
                    new SourceColumn("notes", TapstateType.STRING, true)));
            if (compound) {
                columns.add(new SourceColumn("region", TapstateType.STRING, true));
            }
            return columns;
        }

        /**
         * The filler columns, in the type mix a real fifty-column table holds: mostly text, then whole
         * numbers, then the three remaining kinds. A table of one type would be a row of one size.
         */
        private static List<String> filler(String prefix, List<SourceColumn> into) {
            List<String> names = new ArrayList<>(WIDE_FILLER);
            for (int i = 0; i < WIDE_FILLER; i++) {
                String name = prefix + "_c" + i;
                names.add(name);
                into.add(new SourceColumn(name, typeOf(i), true));
            }
            return List.copyOf(names);
        }

        private static TapstateType typeOf(int i) {
            if (i < 10) {
                return TapstateType.STRING;
            }
            if (i < 17) {
                return TapstateType.INT64;
            }
            if (i < 19) {
                return TapstateType.DECIMAL;
            }
            if (i < 21) {
                return TapstateType.DOUBLE;
            }
            return TapstateType.BOOLEAN;
        }

        private static JoinPlan derive(String sql, List<SourceColumn> orders,
                List<SourceColumn> customers) {
            return SqlFrontEnd.derive(sql, List.of(new SourceTable("orders", List.copyOf(orders)),
                    new SourceTable("customers", List.copyOf(customers))));
        }

        // -------- rows

        private Map<String, Object> dimensionRow(int id, int version) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", (long) id);
            row.put("name", "customer-" + id + "-v" + version);
            row.put("notes", "note-" + id + "-v" + version);
            if (compound) {
                row.put("region", region(id));
            }
            fill(row, dimensionFiller, id, version);
            return row;
        }

        private Map<String, Object> factRow(int id, int dimensionId) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("id", (long) id);
            row.put("cust_id", (long) dimensionId);
            row.put("amount", new BigDecimal(id + ".25"));
            if (compound) {
                row.put("region", region(dimensionId));
            }
            fill(row, factFiller, id, 0);
            return row;
        }

        private static String region(int dimensionId) {
            return "r" + (dimensionId % 8);
        }

        private void fill(Map<String, Object> row, List<String> columns, int id, int version) {
            for (int i = 0; i < columns.size(); i++) {
                row.put(columns.get(i), value(i, id, version));
            }
        }

        private static Object value(int i, int id, int version) {
            return switch (typeOf(i)) {
                // Twelve to forty characters, the length a text column of a real table holds.
                case STRING -> ("v" + version + "-col" + i + "-row" + id + "-0123456789abcdefghij")
                        .substring(0, Math.min(12 + (id + i) % 28, 24));
                case INT64 -> (long) id * 31 + i;
                case DECIMAL -> new BigDecimal((id % 1000) + "." + (10 + i % 89));
                case DOUBLE -> (id % 997) * 1.5d + i;
                default -> (id + i) % 2 == 0;
            };
        }

        // -------- changes

        private List<SourceChange> dimensionReads(int count) {
            List<SourceChange> changes = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                changes.add(new SourceChange(DIM,
                        Envelope.read(1L, "customers", dimensionRow(i, 0), null)));
            }
            return changes;
        }

        private List<SourceChange> factReads(int dimensions, int perDimension) {
            List<SourceChange> changes = new ArrayList<>(dimensions * perDimension);
            for (int i = 0; i < dimensions * perDimension; i++) {
                changes.add(new SourceChange(FACT,
                        Envelope.read(1L, "orders", factRow(i, i % dimensions), null)));
            }
            return changes;
        }

        /** One more fact row under {@code dimensionId}, numbered {@code id}. */
        private SourceChange factReadUnder(int dimensionId, int id) {
            return new SourceChange(FACT,
                    Envelope.read(1L, "orders", factRow(id, dimensionId), null));
        }

        private SourceChange factInsert(int id, int dimensionId) {
            return new SourceChange(FACT,
                    Envelope.insert(2L, "orders", factRow(id, dimensionId), null));
        }

        private SourceChange factUpdate(int id, int was, int now) {
            return new SourceChange(FACT,
                    Envelope.update(2L, "orders", factRow(id, was), factRow(id, now), null));
        }

        private SourceChange factDelete(int id, int dimensionId) {
            return new SourceChange(FACT,
                    Envelope.delete(2L, "orders", factRow(id, dimensionId), null));
        }

        /** An edit to a column the join publishes: every fact row under the key has to be built again. */
        private SourceChange dimensionRename(int id) {
            return new SourceChange(DIM, Envelope.update(3L, "customers", dimensionRow(id, 0),
                    dimensionRow(id, 1), null));
        }

        /**
         * An edit to a column the join reads for nothing. Nothing published can change, so nothing is
         * supposed to be rebuilt - the one number here that should be zero.
         */
        private SourceChange dimensionTouchUnpublished(int id, int version) {
            Map<String, Object> after = dimensionRow(id, 0);
            after.put("notes", "note-" + id + "-touch" + version);
            return new SourceChange(DIM,
                    Envelope.update(3L, "customers", dimensionRow(id, 0), after, null));
        }
    }

    // ---------------------------------------------------------------- reporting

    private record Result(String scenario, String tier, String note, long nanos, long rows, long trips,
                          long coldKeys, String tripsWhere, long resident, long written,
                          long[] batches) {

        private Result(String scenario, String tier, String note, long nanos, long rows, long trips,
                long coldKeys, String tripsWhere, long resident, long written) {
            this(scenario, tier, note, nanos, rows, trips, coldKeys, tripsWhere, resident, written,
                    null);
        }

        private Result withBatches(long[] each) {
            return new Result(scenario, tier, note, nanos, rows, trips, coldKeys, tripsWhere, resident,
                    written, each);
        }

        private static String header() {
            return String.join("\t", "scenario", "tier", "rows", "ms", "rows/s", "trips", "coldKeys",
                    "trips/batch", "p50ms", "p95ms", "p99ms", "resident", "written", "tripsWhere",
                    "note");
        }

        private String row() {
            // A rate is meaningless where the scenario is meant to emit nothing, so R5 reports none.
            String rate = rows == 0 ? "-" : String.format("%.0f", rows / (nanos / 1e9));
            String perBatch =
                    batches == null ? "-" : String.format("%.1f", trips / (double) batches.length);
            return String.join("\t", scenario, tier, Long.toString(rows),
                    String.format("%.1f", nanos / 1e6), rate, Long.toString(trips),
                    Long.toString(coldKeys), perBatch, percentile(50), percentile(95), percentile(99),
                    Long.toString(resident), Long.toString(written), tripsWhere, note);
        }

        private String percentile(int which) {
            if (batches == null || batches.length == 0) {
                return "-";
            }
            long[] sorted = batches.clone();
            Arrays.sort(sorted);
            int at = Math.max(0, Math.min(sorted.length - 1,
                    (int) Math.ceil(which / 100d * sorted.length) - 1));
            return String.format("%.2f", sorted[at] / 1e6);
        }
    }

    private static int intProperty(String name, int fallback) {
        String value = System.getProperty(name);
        return value == null ? fallback : Integer.parseInt(value);
    }
}
