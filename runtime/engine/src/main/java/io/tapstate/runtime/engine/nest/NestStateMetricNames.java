package io.tapstate.runtime.engine.nest;

import io.tapstate.core.lifecycle.NestStateReading;
import java.util.Map;
import java.util.OptionalLong;

/**
 * What a nest state reading is called among a run's statistics, and how to read one back. Naming and
 * parsing sit together because they are one contract with two ends: a run leaves readings under these
 * names and the engine picks them out again by them, and the two drifting apart would show up as a
 * pipeline that simply reports no state - not as anything failing.
 */
public final class NestStateMetricNames {

    /** What every reading's name begins with, before the kind and the namespace it concerns. */
    public static final String PREFIX = "nestState.";

    /** How many keys the namespace holds. */
    public static final String ENTRIES = "entries";

    /** The established logical access accounting, including reads and writes. */
    public static final String ACCESSES = "accesses";

    /** Completed cold loader callbacks, independent of the number of keys in each callback. */
    public static final String BACKFILLS = "backfills";

    /** Requested key accesses, distinct within each logical operation. */
    public static final String KEY_ACCESSES = "keyAccesses";

    /** Keys requested by completed cold loader callbacks, including absent stored values. */
    public static final String COLD_KEYS = "coldKeys";

    /** How long those took in total. */
    public static final String BACKFILL_MILLIS = "backfillMillis";

    /** The deepest one key has been seen holding for something that has not arrived. */
    public static final String PENDING_HIGH_WATER = "pendingHighWater";

    private NestStateMetricNames() {
    }

    /**
     * The reading {@code kinds} describes, with {@code stored} answering what the layer behind the memory
     * holds. Legacy kinds absent from the statistics retain their zero defaults. Key counters require
     * both instruments; an older or incomplete reading cannot supply a measured cold share.
     *
     * <p>Assembling it here rather than where the statistics are read keeps the two ends of the contract in
     * one file. A kind published under a name nobody reads back is invisible in the way that costs most: the
     * metric is present and at zero, exactly as it would be for a namespace where nothing ever happened.
     */
    public static NestStateReading readingFrom(Map<String, Long> kinds, OptionalLong stored) {
        NestStateReading reading = new NestStateReading(
                kinds.getOrDefault(ENTRIES, 0L),
                kinds.getOrDefault(ACCESSES, 0L),
                kinds.getOrDefault(BACKFILLS, 0L),
                kinds.getOrDefault(BACKFILL_MILLIS, 0L),
                kinds.getOrDefault(PENDING_HIGH_WATER, 0L),
                stored);
        return kinds.containsKey(KEY_ACCESSES) && kinds.containsKey(COLD_KEYS)
                ? reading.withKeyCounts(kinds.get(KEY_ACCESSES), kinds.get(COLD_KEYS)) : reading;
    }

    /** The name a reading of {@code kind} about {@code namespace} is left under. */
    public static String nameOf(String kind, String namespace) {
        return PREFIX + kind + "." + namespace;
    }

    /**
     * The kind and namespace a reading named {@code metric} concerns, or {@code null} when it is not one
     * of these. The kind is read up to the first separator and the namespace is the whole of the rest: a
     * namespace has separators of its own and a kind never does, so only splitting from the left is safe.
     */
    public static Reading readingOf(String metric) {
        if (!metric.startsWith(PREFIX)) {
            return null;
        }
        String rest = metric.substring(PREFIX.length());
        int split = rest.indexOf('.');
        return split < 0 ? null : new Reading(rest.substring(0, split), rest.substring(split + 1));
    }

    /** Which kind of reading, and which namespace it is about. */
    public record Reading(String kind, String namespace) {
    }
}
