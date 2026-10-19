package io.tapstate.spi.store;

import java.time.Duration;
import java.time.Instant;

/** Controlled successor preparation from retained original CACHE history, never a missing-key repair. */
public interface PipelineCacheRebuilds {
    /** The original external writers and hot state are already retired; shared consumers detached first. */
    PipelineRebuildReceipt prepare(PipelineRebuildPreparation original, WorkloadClaimFence currentNode, Duration ttl, Instant touchTime);
}
