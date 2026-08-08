package io.tapstate.runtime.engine.nest;

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

    /** How many times its state was read. */
    public static final String ACCESSES = "accesses";

    /** How many of those went to the layer behind the map. */
    public static final String BACKFILLS = "backfills";

    /** How long those took in total. */
    public static final String BACKFILL_MILLIS = "backfillMillis";

    private NestStateMetricNames() {
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
