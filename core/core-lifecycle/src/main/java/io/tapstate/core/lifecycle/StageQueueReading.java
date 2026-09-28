package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** Collected input queue occupancy and sampled peaks of one complete current job execution. */
public record StageQueueReading(Map<String, Sample> byStage) {

    public static final StageQueueReading NONE = new StageQueueReading(Map.of());

    public record Sample(QueueReading queue, Instant observedAt) {
        public Sample {
            Objects.requireNonNull(queue, "queue");
            Objects.requireNonNull(observedAt, "observedAt");
            if (queue.highWater() == 0) {
                throw new IllegalArgumentException("a stage queue with no observed occupancy stays absent");
            }
        }
    }

    public StageQueueReading {
        byStage = byStage == null ? Map.of() : Map.copyOf(byStage);
        for (String stage : byStage.keySet()) {
            if (!Stage.attributeValues().contains(stage)) {
                throw new IllegalArgumentException("stage queue readings require a known stage");
            }
        }
    }
}
