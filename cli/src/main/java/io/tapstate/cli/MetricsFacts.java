package io.tapstate.cli;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The cells of the metrics face the status answer reads, pulled out of the open map in the one place their
 * key names are written down.
 *
 * <p><strong>This class exists to be the only file that changes when those keys change.</strong> The metric
 * model is due to be reshaped: the dimensions come out of the key string instead of being spelled into it,
 * the names follow a different convention, and what the driven-record count counts is re-pinned to a
 * different boundary. Three rules of the status answer read three of these cells. Read inline, that
 * reshaping would mean re-deriving three separate rules from the new model, inside a file whose subject is
 * wording; read through here, it is one class with one set of names in it.
 *
 * <p>Absence is carried, not flattened to zero. A count that is not published and a count of nought are
 * different facts — one says nothing was driven, the other says nobody is counting — and a rule that turned
 * the second into the first would report a run as having moved nothing on the strength of a metric that was
 * never wired.
 *
 * @param reconcileFailuresInARow how many convergence passes in a row have thrown, or null when no
 *                      streak is running -- which is the ordinary state and is not the same as nought.
 *                      It is a streak and not a total: one clean pass ends it. What used to stand in this
 *                      slot was a cell named errorCount that carried this streak *and* the pipeline's own
 *                      state written as a number, and the rule below always wanted this half
 * @param recordCount   records the live job has driven to its sinks, or null when there is no live job to
 *                      ask — which is a different answer from nought and is kept apart from it
 * @param stalledChains chains whose durable position has not advanced, mapped to how long, in milliseconds.
 *                      Only entries above zero are kept: zero is the healthy reading and carrying it would
 *                      make "a chain is stuck" true of every running pipeline
 */
record MetricsFacts(Long reconcileFailuresInARow, Long recordCount, Map<String, Long> stalledChains) {

    /**
     * How many convergence passes in a row have thrown, published only while a streak is running.
     *
     * <p>Not "errorCount", which is what this key used to be called while it carried two different
     * quantities: this streak, and the pipeline's state as a one-or-nought. The second is now a real
     * counter of its own under a dimensioned name, and a key left called count while holding only a
     * streak would have the sentence the rule below prints start lying the day somebody trusted the name.
     */
    private static final String RECONCILE_STREAK = "reconcileFailuresInARow";

    /** Records the live job has driven to its sinks; absent when the pipeline has no live job. */
    private static final String RECORD_COUNT = "recordCount";

    /** How long one chain's durable position has stood still, keyed by chain after the dot. */
    private static final String STALLED_PREFIX = "frontierStalledMillis.";

    MetricsFacts {
        stalledChains = stalledChains == null ? Map.of() : Map.copyOf(stalledChains);
    }

    /** Reads the three cells out of one metrics answer; every one of them may legitimately be absent. */
    static MetricsFacts of(Map<String, Long> metrics) {
        Map<String, Long> stalled = new LinkedHashMap<>();
        metrics.forEach((name, value) -> {
            if (name.startsWith(STALLED_PREFIX) && value != null && value > 0) {
                stalled.put(name.substring(STALLED_PREFIX.length()), value);
            }
        });
        return new MetricsFacts(metrics.get(RECONCILE_STREAK), metrics.get(RECORD_COUNT), stalled);
    }
}
