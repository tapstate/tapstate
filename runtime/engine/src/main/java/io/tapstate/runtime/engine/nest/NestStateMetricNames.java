package io.tapstate.runtime.engine.nest;

import io.tapstate.core.lifecycle.NestStateReading;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
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

    /** How many times its state was read. */
    public static final String ACCESSES = "accesses";

    /** How many of those went to the layer behind the map. */
    public static final String BACKFILLS = "backfills";

    /** How long those took in total. */
    public static final String BACKFILL_MILLIS = "backfillMillis";

    /** The deepest one key has been seen holding for something that has not arrived. */
    public static final String PENDING_HIGH_WATER = "pendingHighWater";

    private NestStateMetricNames() {
    }

    /**
     * The reading {@code kinds} describes, with {@code stored} answering what the layer behind the memory
     * holds. A kind the statistics do not carry reads as zero: a run publishes every kind on every reading,
     * so a missing one is a run that has not been collected from yet rather than a namespace at zero.
     *
     * <p>Assembling it here rather than where the statistics are read keeps the two ends of the contract in
     * one file. A kind published under a name nobody reads back is invisible in the way that costs most: the
     * metric is present and at zero, exactly as it would be for a namespace where nothing ever happened.
     */
    public static NestStateReading readingFrom(Map<String, Long> kinds, OptionalLong stored) {
        return readingFrom(kinds, stored, Optional.empty());
    }

    /** The same reading with the actual time of an asynchronously sampled stored count. */
    public static NestStateReading readingFrom(Map<String, Long> kinds, OptionalLong stored,
            Optional<Instant> storedObservedAt) {
        return new NestStateReading(
                kinds.getOrDefault(ENTRIES, 0L),
                kinds.getOrDefault(ACCESSES, 0L),
                kinds.getOrDefault(BACKFILLS, 0L),
                kinds.getOrDefault(BACKFILL_MILLIS, 0L),
                kinds.getOrDefault(PENDING_HIGH_WATER, 0L),
                stored, storedObservedAt);
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
