package io.tapstate.runtime.engine.nest;

import com.hazelcast.core.HazelcastInstance;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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

    /** How often {@code namespace} was reached for, how much of that went behind the map, at what cost. */
    public Counted counted(String namespace) {
        Counters counters = byNamespace.get(namespace);
        return counters == null
                ? new Counted(0, 0, 0)
                : new Counted(counters.accesses.sum(), counters.backfills.sum(), counters.backfillNanos.sum());
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
    public record Counted(long accesses, long backfills, long backfillNanos) {
    }

    /** One namespace's counters. Adders rather than atomics: these are written far more often than read. */
    private static final class Counters {

        private final LongAdder accesses = new LongAdder();
        private final LongAdder backfills = new LongAdder();
        private final LongAdder backfillNanos = new LongAdder();
    }
}
