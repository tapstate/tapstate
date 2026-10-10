package io.tapstate.runtime.engine.join;

import io.tapstate.runtime.engine.ProcessorBufferBounds;
import java.util.List;
import java.util.Map;

/** Transient row staging of the concrete update and projection processors drawn for a join. */
final class JoinBufferBounds {
    private JoinBufferBounds() { }

    static ProcessorBufferBounds updates(int dimensions) {
        if (dimensions < 0) {
            throw new IllegalArgumentException("a dimension count cannot be negative");
        }
        return new ProcessorBufferBounds(Map.of(
                "recompute-mirror-read", dimensions == 0 ? 0L
                        : (long) JoinDriver.DEFAULT_KEYS_PER_READ + ReverseIndex.DEFAULT_PAGE_SIZE,
                "current-dimension-rows", (long) dimensions,
                "current-projected-row", 1L), Map.of(
                "source-change-batch", 1L,
                "primed-mirror-read", 1L,
                "primed-mirror-held", 1L,
                // Either the old/new primary-key pair, or one removal per changed dimension and the final row.
                "pending-fact-changelog", Math.max(2L, dimensions + 1L)), List.of(
                "join cached and durable rows are outside transient record staging",
                "join historical-writer and bucket bookkeeping remains unmeasured by record capacity"));
    }

    static ProcessorBufferBounds projection(int dimensions) {
        return new ProcessorBufferBounds(Map.of("current-dimension-rows", (long) dimensions), Map.of(
                "arrival-batch", 1L, "mirror-read", 1L, "projected-results", 1L, "pending-output-copy", 1L),
                List.of("join projection publishes exactly one current row per arrival"));
    }
}
