package io.tapstate.runtime.engine.join;

import com.hazelcast.core.HazelcastInstance;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
    }
}
