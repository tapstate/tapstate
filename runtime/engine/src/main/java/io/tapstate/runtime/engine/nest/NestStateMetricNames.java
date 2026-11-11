package io.tapstate.runtime.engine.nest;

import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.StateCommitReading;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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

    /** Whole documents the root level has sent since the run began; only a rendering level has it. */
    public static final String ROOTS_RENDERED = "rootsRendered";

    /** When that count began, in epoch milliseconds, so it can travel as an accumulating count. */
    public static final String ROOTS_RENDERED_SINCE = "rootsRenderedSince";

    /**
     * How the namespace's state was made durable: commit groups, the sums behind the three distributions,
     * when the counting began, and - with the bucket index appended - each distribution's bucket counts.
     * Entries memory let go of travel beside them.
     */
    public static final String COMMIT_GROUPS = "commitGroups";
    public static final String COMMIT_KEYS_SUM = "commitKeys";
    public static final String COMMIT_ROUND_TRIPS_SUM = "commitRoundTrips";
    public static final String COMMIT_NANOS_SUM = "commitNanos";
    public static final String COMMIT_SINCE = "commitSince";
    public static final String COMMIT_KEYS_BUCKET = "commitKeysBucket";
    public static final String COMMIT_ROUND_TRIPS_BUCKET = "commitRoundTripsBucket";
    public static final String COMMIT_NANOS_BUCKET = "commitNanosBucket";
    public static final String EVICTIONS = "evictions";
    /** Requested key accesses, distinct within each logical operation. */
    public static final String KEY_ACCESSES = "keyAccesses";


    /** How long those took in total. */
    public static final String BACKFILL_MILLIS = "backfillMillis";

    /** The deepest one key has been seen holding for something that has not arrived. */
    public static final String PENDING_HIGH_WATER = "pendingHighWater";
    public static final String READ_KEYS = "readKeys";
    public static final String WRITE_KEYS = "writeKeys";
    public static final String COLD_KEYS = "coldKeys";

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

    /**
     * The commit reading the kinds carry, or null when any part of it is missing: a distribution short a
     * bucket is not a distribution short one bucket, it is a collection that has not finished arriving.
     */
    static StateCommitReading commitsFrom(Map<String, Long> kinds) {
        Long groups = kinds.get(COMMIT_GROUPS);
        Long since = kinds.get(COMMIT_SINCE);
        Long keySum = kinds.get(COMMIT_KEYS_SUM);
        Long tripSum = kinds.get(COMMIT_ROUND_TRIPS_SUM);
        Long nanos = kinds.get(COMMIT_NANOS_SUM);
        if (groups == null || since == null || keySum == null || tripSum == null || nanos == null) {
            return null;
        }
        List<Long> keys = buckets(kinds, COMMIT_KEYS_BUCKET, HistogramBounds.STATE_COMMIT_KEYS);
        List<Long> trips = buckets(kinds, COMMIT_ROUND_TRIPS_BUCKET, HistogramBounds.STATE_COMMIT_ROUND_TRIPS);
        List<Long> took = buckets(kinds, COMMIT_NANOS_BUCKET, HistogramBounds.STATE_COMMIT_DURATION);
        if (!complete(keys, groups) || !complete(trips, groups) || !complete(took, groups)) {
            return null;
        }
        return new StateCommitReading(groups,
                HistogramBounds.STATE_COMMIT_KEYS.value(groups, keySum, keys),
                HistogramBounds.STATE_COMMIT_ROUND_TRIPS.value(groups, tripSum, trips),
                HistogramBounds.STATE_COMMIT_DURATION.value(groups, nanos / 1_000_000_000.0, took),
                kinds.getOrDefault(EVICTIONS, 0L), Instant.ofEpochMilli(since));
    }

    private static boolean complete(List<Long> counts, long groups) {
        return counts != null && counts.stream().mapToLong(Long::longValue).sum() == groups;
    }

    private static List<Long> buckets(Map<String, Long> kinds, String kind, HistogramBounds bounds) {
        List<Long> counts = new ArrayList<>(bounds.buckets());
        for (int i = 0; i < bounds.buckets(); i++) {
            Long count = kinds.get(kind + i);
            if (count == null) {
                return null;
            }
            counts.add(count);
        }
        return counts;
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
