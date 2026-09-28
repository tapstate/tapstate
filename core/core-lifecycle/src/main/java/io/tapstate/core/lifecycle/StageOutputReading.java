package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Complete collected ordinary-output refusals and completed retry intervals of one job execution. */
public record StageOutputReading(Map<String, Sample> byStage) {
    public static final StageOutputReading NONE = new StageOutputReading(Map.of());

    public record Sample(long refused, HistogramValue retryDuration, Instant countingSince, Instant observedAt) {
        public Sample {
            Objects.requireNonNull(countingSince, "countingSince");
            Objects.requireNonNull(observedAt, "observedAt");
            if (refused < 0 || countingSince.isAfter(observedAt)
                    || retryDuration != null && (retryDuration.count() <= 0
                    || retryDuration.count() > refused
                    || !Double.isFinite(retryDuration.sum()) || retryDuration.sum() < 0
                    || retryDuration.bucketCounts().stream().anyMatch(value -> value < 0)
                    || !retryDuration.bounds().equals(HistogramBounds.STAGE_OUTPUT_RETRY_DURATION.bounds()))) {
                throw new IllegalArgumentException("invalid collected output retry reading");
            }
            if (refused == 0 && retryDuration == null) {
                throw new IllegalArgumentException("quiet output pressure stays absent");
            }
            if (retryDuration != null) {
                long count = 0;
                for (long bucket : retryDuration.bucketCounts()) {
                    count = Math.addExact(count, bucket);
                }
                if (count != retryDuration.count()) {
                    throw new IllegalArgumentException("output retry buckets must cover every completed interval");
                }
            }
        }
    }

    public StageOutputReading {
        byStage = byStage == null ? Map.of() : Map.copyOf(byStage);
        for (String stage : byStage.keySet()) {
            if (!Stage.attributeValues().contains(stage) || Stage.SINK.attributeValue().equals(stage)) {
                throw new IllegalArgumentException("output retry readings require a wrapped business stage");
            }
        }
    }
}
