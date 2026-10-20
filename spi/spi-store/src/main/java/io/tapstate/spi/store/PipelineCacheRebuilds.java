package io.tapstate.spi.store;

import java.time.Duration;
import java.time.Instant;

/** Controlled successor preparation from retained original CACHE history, never a missing-key repair. */
public interface PipelineCacheRebuilds {
    /** Reclaims one bounded batch from frozen old locations under the original successor receipt. */
    default long reclaim(PipelineRebuildReceipt original) {
        throw new io.tapstate.core.common.TapstateException(IoError.WORKLOAD_CLAIM_FENCED, java.util.Map.of(), null);
    }

    /** The original external writers and hot state are already retired; shared consumers detached first. */
    PipelineRebuildReceipt prepare(PipelineRebuildPreparation original, WorkloadClaimFence currentNode, Duration ttl, Instant touchTime);
}
