package io.tapstate.runtime.engine;

import io.tapstate.core.lifecycle.Stage;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

/**
 * Publishes a stage's timing distribution as run statistics of the job it belongs to, the way
 * {@link JetDeliveryGauge} publishes a sink's deliveries: one bare number per name, the distribution split
 * into its count, its sum in microseconds and one number per bucket, plus the moment timing began. The
 * stage rides in the name; it is a closed set of words with no dot in any of them, so the names split
 * back without a guess.
 *
 * <p>Handles are acquired on the owning processor thread. A submitted worker uses a completely
 * prebound set of thread-safe handles and performs no metric lookup. Accumulators and cached values
 * still have one writer. Outside a running job the timer uses a gauge that reads nothing.
 *
 * <p>What is written is what moved. One unit of work changes the count, the sum and one bucket, so
 * publishing the whole distribution writes sixteen numbers that already hold that value — once per row
 * through a transform, on the cooperative thread. {@link JobStatistic} keeps the last value written under
 * each name and skips the rest.
 */
final class JetStageGauge implements StageGauge {

    static final String PREFIX = "stage.";
    static final String COUNT = ".count";
    static final String SUM_MICROS = ".sumMicros";
    static final String BUCKET = ".bucket.";
    static final String SINCE = ".since";

    private final boolean workerPublication;
    JetStageGauge() { this(false); }
    JetStageGauge(boolean workerPublication) { this.workerPublication = workerPublication; }

    /** Acquire all handles on the owning processor thread before publishing work to another thread. */
    void bind(Stage stage, int bucketCount) {
        String name = PREFIX + stage.attributeValue();
        bindPart(name + COUNT); bindPart(name + SUM_MICROS); bindPart(name + SINCE);
        JobStatistic[] handles = new JobStatistic[bucketCount];
        for (int index = 0; index < bucketCount; index++) handles[index] = bindPart(name + BUCKET + index);
        bucketsByStage.put(stage, handles);
    }

    private final Map<String, JobStatistic> parts = new HashMap<>();
    private final Map<Stage, JobStatistic[]> bucketsByStage = new EnumMap<>(Stage.class);

    @Override
    public void took(Stage stage, long count, long sumNanos, long[] bucketCounts, long countingSinceMillis) {
        String name = PREFIX + stage.attributeValue();
        part(name + COUNT).set(count);
        // Microseconds, which is what the reader adds up across processors and divides back to seconds.
        part(name + SUM_MICROS).set(Math.round(sumNanos / 1_000.0));
        // The bucket handles by index, so a unit of work does not build seventeen names to find them.
        JobStatistic[] buckets = bucketsByStage.computeIfAbsent(stage, ignored -> {
            JobStatistic[] handles = new JobStatistic[bucketCounts.length];
            for (int index = 0; index < handles.length; index++) {
                handles[index] = part(name + BUCKET + index);
            }
            return handles;
        });
        for (int index = 0; index < bucketCounts.length; index++) {
            buckets[index].set(bucketCounts[index]);
        }
        part(name + SINCE).set(countingSinceMillis);
    }

    private JobStatistic bindPart(String name) {
        return parts.computeIfAbsent(name, value -> new JobStatistic(value, workerPublication));
    }

    private JobStatistic part(String name) {
        var found = parts.get(name);
        if (found != null) return found;
        if (workerPublication) throw new IllegalStateException("worker stage handle was not bound by its processor");
        var created = new JobStatistic(name); parts.put(name, created); return created;
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
