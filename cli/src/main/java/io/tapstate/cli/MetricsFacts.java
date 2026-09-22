package io.tapstate.cli;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

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
 *                      Only entries at or above the status threshold are kept: shorter pauses are ordinary
 *                      time between frontier advances and do not mean a chain has stopped
 * @param movement      what the run had moved and how far behind it stood, read off the measured facts
 *                      rather than the open map, or null when the face carried neither -- which is how a
 *                      pipeline with no live job reads, and is kept apart from a run that moved nothing
 */
record MetricsFacts(Long reconcileFailuresInARow, Long recordCount, Map<String, Long> stalledChains,
        MovementReading movement) {

    /** A minute pinned separates an ordinary frontier pause from a chain worth diagnosing as stopped. */
    static final Duration CHAIN_STALL_THRESHOLD = Duration.ofMinutes(1);

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

    /**
     * Rows the run has moved, as the measured fact rather than the open map's cell: one point per table
     * and direction, each stamped with when it was observed, which is what a rate needs and the map has
     * not got. Its points are summed over tables, per direction.
     */
    static final String RECORDS_FACT = "tapstate.pipeline.records";

    /** How far behind each table stands, in seconds, one point per table. */
    static final String LAG_FACT = "tapstate.pipeline.lag";

    /** The attribute a records point carries its direction under, and the one a lag point names its table under. */
    static final String DIRECTION_ATTRIBUTE = "direction";
    static final String TABLE_ATTRIBUTE = "tapstate.table.id";

    MetricsFacts {
        stalledChains = stalledChains == null ? Map.of() : Map.copyOf(stalledChains);
    }

    MetricsFacts(Long reconcileFailuresInARow, Long recordCount, Map<String, Long> stalledChains) {
        this(reconcileFailuresInARow, recordCount, stalledChains, null);
    }

    /** Reads the three cells out of one metrics answer; every one of them may legitimately be absent. */
    static MetricsFacts of(Map<String, Long> metrics) {
        return of(metrics, List.of());
    }

    /** The same, with the movement read off the measured facts beside the map. */
    static MetricsFacts of(Map<String, Long> metrics, List<MetricsOutcome.FactPoint> facts) {
        Map<String, Long> stalled = new LinkedHashMap<>();
        metrics.forEach((name, value) -> {
            if (name.startsWith(STALLED_PREFIX) && value != null
                    && value >= CHAIN_STALL_THRESHOLD.toMillis()) {
                stalled.put(name.substring(STALLED_PREFIX.length()), value);
            }
        });
        return new MetricsFacts(metrics.get(RECONCILE_STREAK), metrics.get(RECORD_COUNT), stalled, movementOf(facts));
    }

    /**
     * The movement reading off the facts, or null when they carry neither a records point nor a lag point.
     * Records are summed over every table per direction -- the overflow point a folded table set carries
     * counts too, since it is the remainder and not a duplicate. The reading's time is the latest any of
     * its points was observed; they are one observation, so this is that observation's time.
     */
    static MovementReading movementOf(List<MetricsOutcome.FactPoint> facts) {
        Map<String, Long> records = new TreeMap<>();
        Map<String, Long> lag = new TreeMap<>();
        Instant observedAt = null;
        boolean any = false;
        for (MetricsOutcome.FactPoint point : facts) {
            if (RECORDS_FACT.equals(point.name())) {
                String direction = point.attributes().get(DIRECTION_ATTRIBUTE);
                if (direction == null || point.value() == null) {
                    continue;
                }
                records.merge(direction, point.value(), Long::sum);
            } else if (LAG_FACT.equals(point.name())) {
                String table = point.attributes().get(TABLE_ATTRIBUTE);
                if (table == null || point.value() == null) {
                    continue;
                }
                lag.put(table, point.value());
            } else {
                continue;
            }
            any = true;
            if (point.observedAt() != null && (observedAt == null || point.observedAt().isAfter(observedAt))) {
                observedAt = point.observedAt();
            }
        }
        return any ? new MovementReading(observedAt, records, lag) : null;
    }
}
