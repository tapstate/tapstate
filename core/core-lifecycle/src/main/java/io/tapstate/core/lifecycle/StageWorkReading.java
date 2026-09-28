package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/** A complete collected job reading of executing business units, not executor utilization. */
public record StageWorkReading(Map<String, Long> activeByStage, Instant observedAt) {

    public static final StageWorkReading NONE = new StageWorkReading(Map.of(), null);

    public StageWorkReading {
        activeByStage = activeByStage == null ? Map.of() : Map.copyOf(activeByStage);
        for (Map.Entry<String, Long> entry : activeByStage.entrySet()) {
            if (!Stage.attributeValues().contains(entry.getKey()) || entry.getValue() == null
                    || entry.getValue() <= 0) {
                throw new IllegalArgumentException("active work requires a known stage and a positive count");
            }
        }
        if (!activeByStage.isEmpty()) {
            Objects.requireNonNull(observedAt, "observedAt");
            long total = 0;
            for (long value : activeByStage.values()) {
                total = Math.addExact(total, value);
            }
        }
    }

    public long totalActive() {
        return activeByStage.values().stream().mapToLong(Long::longValue).sum();
    }
}
