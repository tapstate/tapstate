package io.tapstate.runtime.engine;

import io.tapstate.core.lifecycle.HistogramValue;
import java.util.HashMap;
import java.util.List;
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
 * and, through {@link JobStatistic}, a write only where the number moved. The readings arrive as whole
 * maps of running totals, and a batch settling rows of one table leaves every other table's numbers
 * exactly as they were.
 */
final class JetDeliveryGauge implements DeliveryGauge {

    /** What a per-table, per-operation delivery count is named, with {@code <op>.<table>} appended. */
    static final String DELIVERED_PREFIX = "recordsOut.";

    /** What a per-table settled payload size is named, with the table appended. */
    static final String CARRIED_PREFIX = "bytesOut.";

    /** What a per-table newest-settled event time is named, with the table appended. */
    static final String REACHED_PREFIX = "outEventTime.";

    /** What the moment this sink began counting is named. One per sink, with nothing appended. */
    static final String SINCE_METRIC = "outCountingSince";

    /**
     * What the three parts of a table's delivery-duration distribution are named. A run's statistics hold
     * one number per name, so a distribution travels as its count, its sum in milliseconds and one number
     * per bucket, {@code <index>.<table>} appended to the bucket prefix — the index in front for the reason
     * the operation goes in front of the table above: the table is the one part that may hold a dot.
     */
    static final String DURATION_COUNT_PREFIX = "outDeliveryCount.";
    static final String DURATION_SUM_PREFIX = "outDeliverySumMillis.";
    static final String DURATION_BUCKET_PREFIX = "outDeliveryBucket.";

    private final Map<String, JobStatistic> deliveredByKey = new HashMap<>();
    private final Map<String, JobStatistic> carriedByTable = new HashMap<>();
    private final Map<String, JobStatistic> reachedByTable = new HashMap<>();
    private final Map<String, JobStatistic> durationParts = new HashMap<>();
    private JobStatistic since;

    @Override
    public void delivered(Map<String, Map<String, Long>> rowsByTableAndOp) {
        rowsByTableAndOp.forEach((table, byOp) -> byOp.forEach((op, rows) ->
                deliveredByKey.computeIfAbsent(op + "." + table, JetDeliveryGauge::deliveredMetricFor)
                        .set(rows)));
    }

    @Override
    public void carried(Map<String, Long> bytesByTable) {
        bytesByTable.forEach((table, bytes) ->
                carriedByTable.computeIfAbsent(table, JetDeliveryGauge::carriedMetricFor).set(bytes));
    }

    @Override
    public void reached(Map<String, Long> newestEventTimeByTable) {
        newestEventTimeByTable.forEach((table, eventTime) ->
                reachedByTable.computeIfAbsent(table, JetDeliveryGauge::reachedMetricFor).set(eventTime));
    }

    @Override
    public void took(Map<String, HistogramValue> durationByTable) {
        durationByTable.forEach((table, histogram) -> {
            part(DURATION_COUNT_PREFIX + table).set(histogram.count());
            part(DURATION_SUM_PREFIX + table).set(Math.round(histogram.sum() * 1000.0));
            List<Long> buckets = histogram.bucketCounts();
            for (int index = 0; index < buckets.size(); index++) {
                part(DURATION_BUCKET_PREFIX + index + "." + table).set(buckets.get(index));
            }
        });
    }

    private JobStatistic part(String name) {
        return durationParts.computeIfAbsent(name, JobStatistic::new);
    }

    @Override
    public boolean readableOnlyOnAJobThread() {
        return true;
    }

    @Override
    public void countingSince(long epochMillis) {
        if (since == null) {
            since = new JobStatistic(SINCE_METRIC);
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

    /**
     * The table a settled payload size named {@code metric} concerns, or {@code null} when it is not one.
     * Everything past the prefix is the table, dots and all, for the reason the newest-settled reading
     * beside it takes the whole remainder: a schema-qualified table name contains dots.
     */
    static String carriedTableOf(String metric) {
        if (!metric.startsWith(CARRIED_PREFIX)) {
            return null;
        }
        String table = metric.substring(CARRIED_PREFIX.length());
        return table.isEmpty() ? null : table;
    }

    /** The table a newest-settled reading named {@code metric} concerns, or {@code null} when it is not one. */
    static String reachedTableOf(String metric) {
        if (!metric.startsWith(REACHED_PREFIX)) {
            return null;
        }
        String table = metric.substring(REACHED_PREFIX.length());
        return table.isEmpty() ? null : table;
    }

    /** The table a duration count named {@code metric} concerns, or {@code null} when it is not one. */
    static String durationCountTableOf(String metric) {
        return tableAfter(metric, DURATION_COUNT_PREFIX);
    }

    /** The table a duration sum named {@code metric} concerns, or {@code null} when it is not one. */
    static String durationSumTableOf(String metric) {
        return tableAfter(metric, DURATION_SUM_PREFIX);
    }

    /**
     * The bucket index and table a duration bucket named {@code metric} concerns, or {@code null} when it
     * is not one. Split at the first dot after the prefix and nowhere else: the index never holds a dot,
     * the table may.
     */
    static DurationBucket durationBucketOf(String metric) {
        if (!metric.startsWith(DURATION_BUCKET_PREFIX)) {
            return null;
        }
        String rest = metric.substring(DURATION_BUCKET_PREFIX.length());
        int split = rest.indexOf('.');
        if (split <= 0 || split == rest.length() - 1) {
            return null;
        }
        try {
            return new DurationBucket(Integer.parseInt(rest.substring(0, split)), rest.substring(split + 1));
        } catch (NumberFormatException notAnIndex) {
            return null;
        }
    }

    private static String tableAfter(String metric, String prefix) {
        if (!metric.startsWith(prefix)) {
            return null;
        }
        String table = metric.substring(prefix.length());
        return table.isEmpty() ? null : table;
    }

    /** What one delivery count is about: the source operation, and the table it was taken over. */
    record Delivered(String op, String table) {
    }

    /** What one duration bucket is about: which bucket, of which table's distribution. */
    record DurationBucket(int index, String table) {
    }

    private static JobStatistic deliveredMetricFor(String opAndTable) {
        return new JobStatistic(DELIVERED_PREFIX + opAndTable);
    }

    private static JobStatistic carriedMetricFor(String table) {
        return new JobStatistic(CARRIED_PREFIX + table);
    }

    private static JobStatistic reachedMetricFor(String table) {
        return new JobStatistic(REACHED_PREFIX + table);
    }
}
