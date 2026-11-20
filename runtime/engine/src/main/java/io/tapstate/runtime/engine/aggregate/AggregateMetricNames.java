package io.tapstate.runtime.engine.aggregate;

import java.util.List;

/**
 * What an aggregate step's readings are called among a run's statistics. Each name is a kind and then the
 * step, and the step is the only thing a name varies by: a reading about one group is the answer of a
 * check, never a statistic, because the number of groups is decided by the data.
 *
 * <p>The sizes are counted by a pass over the step's stored state and are as of the last pass that
 * finished, so a step that has not finished one reports none of them. That absence is "not measured yet",
 * which a zero would not say.
 */
public final class AggregateMetricNames {

    /** Input rows that are in a group now. Counted by a pass. */
    public static final String ROWS = "aggregateRows.";
    /** Groups holding at least one row. Counted by a pass. */
    public static final String GROUPS = "aggregateGroups.";
    /** Every entry the step keeps, on both sides. Counted by a pass. */
    public static final String STATE_ENTRIES = "aggregateStateEntries.";
    /**
     * Records kept only to answer a second delivery: rows that are gone, and groups a row left. They go
     * once deliveries can no longer reach back to them, so a number that only grows says they cannot.
     * Counted by a pass.
     */
    public static final String WAITING = "aggregateTombstones.";
    /** Rows in the group that holds the most. Counted by a pass. */
    public static final String LARGEST_GROUP = "aggregateLargestGroupRows.";
    /** Groups that cannot be given exactly now and are held at their last good values. Counted by a pass. */
    public static final String FAILED_GROUPS = "aggregateFailedGroups.";
    /** Input events applied since the step started. */
    public static final String INPUTS = "aggregateInputs.";
    /** Commits to the step's state since it started; read against the inputs, how many one write absorbs. */
    public static final String STATE_WRITES = "aggregateStateWrites.";
    /** Input events not applied since the step started: an equal order with another payload, or another input's key. */
    public static final String CONFLICTS = "aggregateOrderConflicts.";

    /** Every kind, in the order they are reported. */
    public static final List<String> KINDS = List.of(ROWS, GROUPS, STATE_ENTRIES, WAITING, LARGEST_GROUP,
            FAILED_GROUPS, INPUTS, STATE_WRITES, CONFLICTS);

    private AggregateMetricNames() {
    }

    /** The name the reading of {@code kind} for {@code step} is left under. */
    public static String nameOf(String kind, String step) {
        return kind + step;
    }

    /** {@code metric} itself when it is one of these readings, or {@code null} when it is not. */
    public static String readingOf(String metric) {
        for (String kind : KINDS) {
            if (metric.startsWith(kind) && metric.length() > kind.length()) {
                return metric;
            }
        }
        return null;
    }
}
