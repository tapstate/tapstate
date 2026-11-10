package io.tapstate.runtime.engine.nest;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.lifecycle.HistogramBounds;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;

/**
 * How much of a nest namespace's reading is being served from memory, and what the rest of it costs. The
 * two halves of that are counted on different threads and could not otherwise meet: a read is made from
 * the processor's own thread, while the trip to the layer behind it is made by the substrate on a
 * partition thread, and neither can see the other's counter.
 *
 * <p>One instance lives per Hazelcast member, resolved through {@link HazelcastInstance#getUserContext()},
 * so both sides reach the same one without either holding a reference across a boundary that has to stay
 * serializable.
 *
 * <p>Counts are per namespace and per member lifetime. They are not state and are not rebuilt after a
 * restart, which is right for what they are - a run that has just begun has served nothing yet, and
 * carrying a previous run's ratio into it would describe a cliff that is no longer there. What must not be
 * counted this way is how much state there <em>is</em>: a count of keys cannot be rebuilt after a restart
 * without reading the keyspace, which is the warm-up the whole layer exists to avoid, so that one is
 * always asked of the map itself and never remembered here.
 */
public final class NestStateStats {

    private static final String USER_CONTEXT_KEY = NestStateStats.class.getName();

    private final Map<String, Counters> byNamespace = new ConcurrentHashMap<>();

    private NestStateStats() {
    }

    /** The counters shared by this member's processors and the stores behind their maps, created once. */
    public static NestStateStats of(HazelcastInstance member) {
        return (NestStateStats) member.getUserContext()
                .computeIfAbsent(USER_CONTEXT_KEY, ignored -> new NestStateStats());
    }

    /** Counts one reach for {@code namespace}'s state - read or write, in memory or not. */
    public void access(String namespace) {
        counters(namespace).accesses.increment();
    }

    /** Counts one reach that was not in memory, and what going to the layer behind it cost. */
    public void backfill(String namespace, long nanos) {
        Counters counters = counters(namespace);
        counters.backfills.increment();
        counters.backfillNanos.add(nanos);
    }

    public void readKeys(String namespace, long keys) {
        Counters value = counters(namespace);
        synchronized (value) { value.readKeys.add(keys); value.publishKeys(); }
    }
    public void writeKeys(String namespace, long keys) {
        Counters value = counters(namespace);
        synchronized (value) { value.writeKeys.add(keys); value.publishKeys(); }
    }
    public void coldKeys(String namespace, long keys) {
        Counters value = counters(namespace);
        synchronized (value) { value.coldKeys.add(keys); value.publishKeys(); }
    }
    public io.tapstate.core.lifecycle.NestKeyAccess keyAccess(String namespace) {
        Counters counted = counters(namespace);
        synchronized (counted) { return counted.keyAccess(); }
    }

    /** One replaceable publisher per member/namespace; remote loaders keep an idle owner current. */
    void publishKeys(String namespace, java.util.function.Consumer<io.tapstate.core.lifecycle.NestKeyAccess> publisher) {
        Counters value = counters(namespace);
        synchronized (value) { value.publisher = publisher; value.publishKeys(); }
    }

    void refreshKeys(String namespace) {
        Counters value = counters(namespace);
        synchronized (value) { value.publishKeys(); }
    }

    /**
     * Marks that one key of {@code namespace} was holding {@code pending} changes for something that has not
     * arrived. What is kept is the deepest ever reported rather than the last, because the last is whichever
     * key happened to be touched most recently and says nothing about the one that is stuck.
     *
     * <p>Reported by whoever already worked the number out for the limit, so that the two cannot disagree
     * about what one key holds. A mark taken anywhere else would have to count the queue a second time, and
     * a queue is inside an entry - counting it again is the one cost this layer is built to avoid.
     */
    public void holding(String namespace, long pending) {
        counters(namespace).pendingHighWater.accumulate(pending);
    }

    /**
     * Counts one commit group of {@code namespace}'s state that became durable: {@code keys} keys in one store
     * transaction, costing {@code roundTrips} store round trips and {@code nanos} from issue to acknowledgement.
     * Called on the thread that made the commit, which is not the one that publishes it.
     */
    public void committed(String namespace, long keys, long roundTrips, long nanos) {
        Counters counters = counters(namespace);
        synchronized (counters) {
            counters.keys.observe(keys, keys);
            counters.roundTrips.observe(roundTrips, roundTrips);
            counters.commitDuration.observe(nanos / 1_000_000_000.0, nanos);
        }
    }

    long commitGroups(String namespace) {
        Counters counters = byNamespace.get(namespace);
        return counters == null ? 0L : counters.keys.count();
    }

    /** How {@code namespace}'s commits have gone so far, or null when it has committed nothing yet. */
    public Commits commits(String namespace) {
        Counters counters = byNamespace.get(namespace);
        if (counters == null) return null;
        synchronized (counters) {
            if (counters.keys.count() == 0) return null;
            return new Commits(counters.keys.count(), counters.keys.read(), counters.keys.sum(),
                    counters.roundTrips.read(), counters.roundTrips.sum(), counters.commitDuration.read(),
                    counters.commitDuration.sum(), counters.countingSinceMillis);
        }
    }

    /**
     * One namespace's commits as read together: the number of groups, then per instrument the bucket counts
     * over its registered bounds and the sum of what was observed - keys, round trips, and nanoseconds.
     */
    public record Commits(long groups, long[] keyBuckets, long keySum, long[] roundTripBuckets, long roundTripSum,
            long[] durationBuckets, long durationNanos, long countingSinceMillis) {

        /** Two readings are equal when every bucket is: arrays compare by identity unless told otherwise. */
        @Override
        public boolean equals(Object other) {
            return other instanceof Commits that && groups == that.groups && keySum == that.keySum
                    && roundTripSum == that.roundTripSum && durationNanos == that.durationNanos
                    && countingSinceMillis == that.countingSinceMillis
                    && java.util.Arrays.equals(keyBuckets, that.keyBuckets)
                    && java.util.Arrays.equals(roundTripBuckets, that.roundTripBuckets)
                    && java.util.Arrays.equals(durationBuckets, that.durationBuckets);
        }

        @Override
        public int hashCode() {
            int hash = java.util.Objects.hash(groups, keySum, roundTripSum, durationNanos, countingSinceMillis);
            hash = 31 * hash + java.util.Arrays.hashCode(keyBuckets);
            hash = 31 * hash + java.util.Arrays.hashCode(roundTripBuckets);
            return 31 * hash + java.util.Arrays.hashCode(durationBuckets);
        }

        @Override
        public String toString() {
            return "Commits[groups=" + groups + ", keyBuckets=" + java.util.Arrays.toString(keyBuckets)
                    + ", keySum=" + keySum + ", roundTripBuckets=" + java.util.Arrays.toString(roundTripBuckets)
                    + ", roundTripSum=" + roundTripSum + ", durationBuckets="
                    + java.util.Arrays.toString(durationBuckets) + ", durationNanos=" + durationNanos
                    + ", countingSinceMillis=" + countingSinceMillis + "]";
        }
    }

    /** Counts {@code documents} whole documents sent from {@code namespace}'s level since the run began. */
    public void rendered(String namespace, long documents) {
        counters(namespace).rootsRendered.add(documents);
    }

    /** How often {@code namespace} was reached for, how much of that went behind the map, at what cost. */
    public Counted counted(String namespace) {
        Counters counters = byNamespace.get(namespace);
        return counters == null
                ? new Counted(0, 0, 0, 0, 0, 0)
                : new Counted(counters.accesses.sum(), counters.backfills.sum(), counters.backfillNanos.sum(),
                        counters.pendingHighWater.get(), counters.rootsRendered.sum(), counters.countingSinceMillis);
    }

    /** Whether anything has been counted for {@code namespace} at all - absence being distinct from zero. */
    public boolean knows(String namespace) {
        return byNamespace.containsKey(namespace);
    }

    /**
     * Drops what was counted for {@code namespace}, for a pipeline whose state has been let go of. Left
     * behind, the counts would go on describing a run that is over, and a reader cannot tell a count that
     * is standing still from one that is merely quiet.
     */
    public void forget(String namespace) {
        byNamespace.remove(namespace);
    }

    private Counters counters(String namespace) {
        return byNamespace.computeIfAbsent(namespace, ignored -> new Counters());
    }

    /** One namespace's counts, as read together. Nanoseconds, because the trips being timed are short. */
    public record Counted(long accesses, long backfills, long backfillNanos, long pendingHighWater,
            long rootsRendered, long countingSinceMillis) {
        public Counted(long accesses, long backfills, long backfillNanos, long pendingHighWater) {
            this(accesses, backfills, backfillNanos, pendingHighWater, 0, 0);
        }
    }

    /**
     * One namespace's counters. Adders rather than atomics for the three that are summed: these are written
     * far more often than read. The mark is not summed and so cannot be one - what is wanted is the largest
     * anything reported, which an accumulator keeps for the same price.
     */
    private static final class Counters {

        private final LongAdder accesses = new LongAdder();
        private final LongAdder backfills = new LongAdder();
        private final LongAdder backfillNanos = new LongAdder();
        private final LongAdder readKeys = new LongAdder();
        private final LongAdder writeKeys = new LongAdder();
        private final LongAdder coldKeys = new LongAdder();
        private java.util.function.Consumer<io.tapstate.core.lifecycle.NestKeyAccess> publisher;
        private io.tapstate.core.lifecycle.NestKeyAccess keyAccess() {
            return new io.tapstate.core.lifecycle.NestKeyAccess(readKeys.sum(), writeKeys.sum(), coldKeys.sum(), "local");
        }
        private void publishKeys() { if (publisher != null) publisher.accept(keyAccess()); }
        private final LongAdder rootsRendered = new LongAdder();
        private final Distribution keys = new Distribution(HistogramBounds.STATE_COMMIT_KEYS);
        private final Distribution roundTrips = new Distribution(HistogramBounds.STATE_COMMIT_ROUND_TRIPS);
        private final Distribution commitDuration = new Distribution(HistogramBounds.STATE_COMMIT_DURATION);

        /**
         * When these counts began, in epoch milliseconds: what a count of work done accumulates from, so a
         * reader can tell a restart from a decrease. Fixed when the namespace is first counted on this member.
         */
        private final long countingSinceMillis = System.currentTimeMillis();
        private final LongAccumulator pendingHighWater = new LongAccumulator(Math::max, 0L);
    }

    /**
     * Bucket counts over one instrument's registered bounds, and the sum of what was observed in the unit the
     * caller keeps it in. Adders, for the reason the other counters are: written on every commit, read only
     * when a reading is left.
     */
    private static final class Distribution {

        private final java.util.List<Double> bounds;
        private final LongAdder[] buckets;
        private final LongAdder count = new LongAdder();
        private final LongAdder sum = new LongAdder();

        private Distribution(HistogramBounds instrument) {
            this.bounds = instrument.bounds();
            this.buckets = new LongAdder[instrument.buckets()];
            for (int i = 0; i < buckets.length; i++) {
                buckets[i] = new LongAdder();
            }
        }

        /** One observation: {@code value} decides the bucket, in the bounds' unit; {@code kept} is summed. */
        private void observe(double value, long kept) {
            int bucket = 0;
            while (bucket < bounds.size() && value > bounds.get(bucket)) {
                bucket++;
            }
            buckets[bucket].increment();
            count.increment();
            sum.add(kept);
        }

        private long count() {
            return count.sum();
        }

        private long sum() {
            return sum.sum();
        }

        private long[] read() {
            long[] read = new long[buckets.length];
            for (int i = 0; i < buckets.length; i++) {
                read[i] = buckets[i].sum();
            }
            return read;
        }
    }
}
