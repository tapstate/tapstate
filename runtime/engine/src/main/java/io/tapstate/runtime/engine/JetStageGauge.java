package io.tapstate.runtime.engine;

import com.hazelcast.jet.core.metrics.Metric;
import com.hazelcast.jet.core.metrics.Metrics;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.Stage;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Publishes a stage's timing distribution as run statistics of the job it belongs to, the way
 * {@link JetDeliveryGauge} publishes a sink's deliveries: one bare number per name, the distribution split
 * into its count, its sum in microseconds and one number per bucket, plus the moment timing began. The
 * stage rides in the name; it is a closed set of words with no dot in any of them, so the names split
 * back without a guess.
 *
 * <p>Readings can only be taken from the job's own threads, which is why a processor driven outside a
 * running job is given a gauge that reads nothing instead of this one. The handle for a name is kept once
 * obtained — it belongs to the processor rather than to whichever thread ran it.
 */
final class JetStageGauge implements StageGauge {

    static final String PREFIX = "stage.";
    static final String COUNT = ".count";
    static final String SUM_MICROS = ".sumMicros";
    static final String BUCKET = ".bucket.";
    static final String SINCE = ".since";

    private final Map<String, Metric> parts = new HashMap<>();

    @Override
    public void took(Stage stage, HistogramValue distribution, long countingSinceMillis) {
        String name = PREFIX + stage.attributeValue();
        part(name + COUNT).set(distribution.count());
        part(name + SUM_MICROS).set(Math.round(distribution.sum() * 1_000_000.0));
        List<Long> buckets = distribution.bucketCounts();
        for (int index = 0; index < buckets.size(); index++) {
            part(name + BUCKET + index).set(buckets.get(index));
        }
        part(name + SINCE).set(countingSinceMillis);
    }

    private Metric part(String name) {
        return parts.computeIfAbsent(name, Metrics::metric);
    }

    /** What one statistic is about: which stage, and which part of its distribution. */
    record Part(String stage, String kind, int bucket) {
    }

    /**
     * The stage and part a statistic named {@code metric} concerns, or {@code null} when it is not one of
     * these. The stage is read up to the first dot after the prefix — a stage word never holds one.
     */
    static Part partOf(String metric) {
        if (!metric.startsWith(PREFIX)) {
            return null;
        }
        String rest = metric.substring(PREFIX.length());
        int split = rest.indexOf('.');
        if (split <= 0) {
            return null;
        }
        String stage = rest.substring(0, split);
        String tail = rest.substring(split);
        if (tail.equals(COUNT) || tail.equals(SUM_MICROS) || tail.equals(SINCE)) {
            return new Part(stage, tail, -1);
        }
        if (tail.startsWith(BUCKET)) {
            try {
                return new Part(stage, BUCKET, Integer.parseInt(tail.substring(BUCKET.length())));
            } catch (NumberFormatException notAnIndex) {
                return null;
            }
        }
        return null;
    }
}
