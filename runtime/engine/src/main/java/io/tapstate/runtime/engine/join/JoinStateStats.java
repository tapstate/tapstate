package io.tapstate.runtime.engine.join;

import com.hazelcast.core.HazelcastInstance;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * How much of a join namespace's reading is being served from memory, and what the rest of it costs.
 *
 * <p><b>Two numbers, because the batch read makes them different questions.</b> A key that was not in
 * memory is a key that had to be fetched, and the share of reaching that lands there is what says a
 * namespace has outgrown what it is given to hold. A round trip is a fetch of a whole batch of such
 * keys. Before batching the two were one number and either name was correct for it; after batching they
 * diverge by up to the batch size, and taking round trips for the first is what would let the ratio
 * loosen by three orders of magnitude while everything went on reporting healthy.
 *
 * <ul>
 *   <li><b>{@code keysFromCold} is what a pressure ratio is computed on</b>, against {@code accesses}.
 *       Both count keys, so the two sides of the ratio mean the same thing.
 *   <li><b>{@code trips} is what says the batching is real.</b> It answers a different question - how
 *       many times the store was reached - and a run where it tracks {@code keysFromCold} one for one
 *       is a run where the batch read is not being used, which is invisible in every other number.
 * </ul>
 *
 * <p>One instance lives per Hazelcast member, resolved through {@link HazelcastInstance#getUserContext()},
 * because the two halves are counted on different threads: a read is made from the processor's own
 * thread, and the trip to the layer behind it is made by the substrate on a partition thread.
 *
 * <p>Counts are per namespace and per member lifetime. They are not state and are not rebuilt after a
 * restart - a run that has just begun has served nothing yet, and carrying a previous run's ratio into
 * it would describe a cliff that is no longer there.
 */
public final class JoinStateStats {

    private static final String USER_CONTEXT_KEY = JoinStateStats.class.getName();

    private final Map<String, Counters> byNamespace = new ConcurrentHashMap<>();

    /** Reached through {@link #of} in a run; directly only where there is no member to reach it on. */
    JoinStateStats() {
    }

    /** The counters shared by this member's processors and the stores behind their maps, created once. */
    public static JoinStateStats of(HazelcastInstance member) {
        return (JoinStateStats) member.getUserContext()
                .computeIfAbsent(USER_CONTEXT_KEY, ignored -> new JoinStateStats());
    }

    /** Counts {@code keys} reaches for {@code namespace}'s state - read or write, in memory or not. */
    public void access(String namespace, long keys) {
        counters(namespace).accesses.add(keys);
    }

    /**
     * Counts one trip to the layer behind memory, made for {@code keys} keys and costing {@code nanos}.
     * Keys and trips are added to different counters on purpose - see the class note.
     */
    public void backfill(String namespace, long keys, long nanos) {
        Counters counters = counters(namespace);
        counters.keysFromCold.add(keys);
        counters.trips.increment();
        counters.backfillNanos.add(nanos);
    }

    /**
     * Marks that one dimension key of {@code namespace} was found to hold {@code pages} pages of fact
     * keys. What is kept is the deepest ever reported rather than the last: the last is whichever key
     * happened to be walked most recently, and says nothing about the one that is large.
     */
    public void widestBucket(String namespace, long pages) {
        counters(namespace).widestBucket.accumulate(pages);
    }

    /** The most pages any one dimension key of {@code namespace} has been found to hold. */
    public long widestBucket(String namespace) {
        return counters(namespace).widestBucket.get();
    }

    /**
     * How large a fan-out has to be before rebuilding it is worth showing anybody.
     *
     * <p><b>Without a threshold this number is noise, and noise is how a number like this comes to be
     * ignored.</b> Every edit to any dimension row rebuilds something; reporting each one puts a
     * constant stream in front of whoever is watching, and the one report that mattered arrives in the
     * middle of it. Below this many rows a rebuild is over in well under a second, which is not a wait
     * anybody has to be told about.
     */
    public static final long REPORT_FANOUT_ABOVE = 10_000L;

    /**
     * Records that a rebuild of {@code dimensionKey}'s fan-out has sent {@code rowsDone} rows of about
     * {@code rowsExpected}, or ignores it as too small to be worth anyone's attention.
     *
     * <p>What this makes visible is a wait that otherwise looks like health: while a large fan-out is
     * being rebuilt the job runs, nothing errors, and the target holds half the old value and half the
     * new one. The threshold is applied here rather than where the number is produced, because which
     * rebuilds are worth surfacing is a reporting decision and every carrier would otherwise write its
     * own copy of it.
     */
    public void recomputing(String namespace, String dimensionKey, long rowsDone, long rowsExpected) {
        if (rowsExpected < REPORT_FANOUT_ABOVE) {
            return;
        }
        Counters counters = counters(namespace);
        counters.recomputeKey = dimensionKey;
        counters.recomputeExpected = rowsExpected;
        counters.recomputeDone = rowsDone;
    }

    /** The dimension key of the last large rebuild reported for {@code namespace}, or null. */
    public String recomputeKey(String namespace) {
        return counters(namespace).recomputeKey;
    }

    /** How many rows that rebuild had sent when it last reported. */
    public long recomputeDone(String namespace) {
        return counters(namespace).recomputeDone;
    }

    /** About how many rows that rebuild has in total - an estimate, never a count. */
    public long recomputeExpected(String namespace) {
        return counters(namespace).recomputeExpected;
    }

    /** How much reaching {@code namespace} has had, in keys. */
    public long accesses(String namespace) {
        return counters(namespace).accesses.sum();
    }

    /** How much of that reaching went to the layer behind memory, in keys. */
    public long keysFromCold(String namespace) {
        return counters(namespace).keysFromCold.sum();
    }

    /** How many times the layer behind memory was reached, whatever each reach asked for. */
    public long trips(String namespace) {
        return counters(namespace).trips.sum();
    }

    /** What reaching the layer behind memory has cost {@code namespace} so far. */
    public long backfillNanos(String namespace) {
        return counters(namespace).backfillNanos.sum();
    }

    private Counters counters(String namespace) {
        return byNamespace.computeIfAbsent(namespace, ignored -> new Counters());
    }

    private static final class Counters {

        private final LongAdder accesses = new LongAdder();
        private final LongAdder keysFromCold = new LongAdder();
        private final LongAdder trips = new LongAdder();
        private final LongAdder backfillNanos = new LongAdder();
        private final LongAccumulator widestBucket = new LongAccumulator(Long::max, 0L);
        // Written from a processor thread and read from whoever is publishing metrics, so the three are
        // volatile. They are read one at a time rather than as a set: a reader that caught the key of
        // one rebuild beside the progress of the next would be reporting a row count against the wrong
        // key, and the three are only ever meant as an indication that a large one is under way.
        private volatile String recomputeKey;
        private volatile long recomputeDone;
        private volatile long recomputeExpected;
    }
}
