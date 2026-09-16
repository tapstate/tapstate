package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.metrics.Metric;
import com.hazelcast.jet.core.metrics.Metrics;
import java.util.HashMap;
import java.util.Map;

/**
 * Publishes a sink's delivery readings as run statistics of the job it belongs to, one per table and
 * operation. The engine already collects a job's statistics on a cadence of its own and hands them out
 * with the job, so a reading left here is readable from outside the run without a second channel to keep
 * alive.
 *
 * <p>A run's statistics are bare numbers under a flat name, so what a reading is <em>about</em> has to
 * ride in the name. That is this seam's limitation and not the measurement's: what comes out the far side
 * is split back into the dimensions it was taken with, and the name here never reaches anybody reading a
 * metric. <strong>The table goes last</strong>, and that is load-bearing rather than tidy — a table's name
 * can contain a dot (a schema-qualified one usually does), so it is the one part that cannot be followed
 * by anything else without the split becoming a guess. The operation in front of it is one of a closed
 * set of symbols with no dot in any of them.
 *
 * <p>Readings can only be taken from the job's own threads, which is why a sink driven outside a running
 * job is given a gauge that reads nothing instead of this one. The handle for a name is kept once
 * obtained — it belongs to the sink rather than to whichever thread ran it — so a reading costs a lookup
 * and a write.
 */
final class JetDeliveryGauge implements DeliveryGauge {

    /** What a per-table, per-operation delivery count is named, with {@code <op>.<table>} appended. */
    static final String DELIVERED_PREFIX = "recordsOut.";

    /** What a per-table newest-settled event time is named, with the table appended. */
    static final String REACHED_PREFIX = "outEventTime.";

    /** What the moment this sink began counting is named. One per sink, with nothing appended. */
    static final String SINCE_METRIC = "outCountingSince";

    private final Map<String, Metric> deliveredByKey = new HashMap<>();
    private final Map<String, Metric> reachedByTable = new HashMap<>();
    private Metric since;

    @Override
    public void delivered(Map<String, Map<String, Long>> rowsByTableAndOp) {
        rowsByTableAndOp.forEach((table, byOp) -> byOp.forEach((op, rows) ->
                deliveredByKey.computeIfAbsent(op + "." + table, JetDeliveryGauge::deliveredMetricFor)
                        .set(rows)));
    }

    @Override
    public void reached(Map<String, Long> newestEventTimeByTable) {
        newestEventTimeByTable.forEach((table, eventTime) ->
                reachedByTable.computeIfAbsent(table, JetDeliveryGauge::reachedMetricFor).set(eventTime));
    }

    @Override
    public void countingSince(long epochMillis) {
        if (since == null) {
            since = Metrics.metric(SINCE_METRIC);
        }
        since.set(epochMillis);
    }

    /**
     * The operation and table a delivery count named {@code metric} concerns, or {@code null} when it is
     * not one of these. The split is at the first dot after the prefix and nowhere else: everything past
     * it is the table, dots and all.
     */
    static Delivered deliveredOf(String metric) {
        if (!metric.startsWith(DELIVERED_PREFIX)) {
            return null;
        }
        String rest = metric.substring(DELIVERED_PREFIX.length());
        int split = rest.indexOf('.');
        if (split <= 0 || split == rest.length() - 1) {
            return null;
        }
        return new Delivered(rest.substring(0, split), rest.substring(split + 1));
    }

    /** The table a newest-settled reading named {@code metric} concerns, or {@code null} when it is not one. */
    static String reachedTableOf(String metric) {
        if (!metric.startsWith(REACHED_PREFIX)) {
            return null;
        }
        String table = metric.substring(REACHED_PREFIX.length());
        return table.isEmpty() ? null : table;
    }

    /** What one delivery count is about: the source operation, and the table it was taken over. */
    record Delivered(String op, String table) {
    }

    private static Metric deliveredMetricFor(String opAndTable) {
        return Metrics.metric(DELIVERED_PREFIX + opAndTable);
    }

    private static Metric reachedMetricFor(String table) {
        return Metrics.metric(REACHED_PREFIX + table);
    }
}
