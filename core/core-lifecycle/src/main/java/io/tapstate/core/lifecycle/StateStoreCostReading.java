package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Complete live-job costs for backed state namespaces, aggregated across the job's current members. */
public record StateStoreCostReading(Instant countingSince,
        Map<String, OperationCost> operations, Map<String, CodecCost> codecs) {

    public static final Set<String> OPERATIONS = Set.of("load", "load_all", "save", "delete");
    public static final Set<String> CODECS = Set.of("encode", "decode");

    public StateStoreCostReading {
        Objects.requireNonNull(countingSince, "countingSince");
        operations = Map.copyOf(Objects.requireNonNull(operations, "operations"));
        codecs = Map.copyOf(Objects.requireNonNull(codecs, "codecs"));
        if (!OPERATIONS.containsAll(operations.keySet()) || !CODECS.containsAll(codecs.keySet())) {
            throw new IllegalArgumentException("state-store cost names belong to fixed operation and codec sets");
        }
    }

    public record OperationCost(long completed, long failed, long durationNanos, long payloadBytes) {
        public OperationCost {
            if (completed < 0 || failed < 0 || durationNanos < 0 || payloadBytes < 0) {
                throw new IllegalArgumentException("state-store operation costs are non-negative");
            }
        }
    }

    public record CodecCost(long count, long bytes) {
        public CodecCost {
            if (count < 0 || bytes < 0) {
                throw new IllegalArgumentException("state-store serialization costs are non-negative");
            }
        }
    }
}
