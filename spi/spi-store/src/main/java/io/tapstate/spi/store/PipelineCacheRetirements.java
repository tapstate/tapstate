package io.tapstate.spi.store;

/** CACHE retirement under a current existing node session and the exact original CONFIG operation. */
public interface PipelineCacheRetirements {
    /** Verifies an existing exact retired receipt without performing retirement or acquiring a pipeline claim. */
    default java.util.Optional<PipelineRetirementReceipt> completed(PipelineRetirementPreparation original, WorkloadClaimFence currentNode) {
        throw new io.tapstate.core.common.TapstateException(IoError.PIPELINE_INTENT_FENCED,java.util.Map.of(),null);
    }
    PipelineRetirementReceipt retire(PipelineRetirementPreparation original,WorkloadClaimFence currentNode);

    /** A fresh verified same-scope isolation step freezes its candidate before CACHE retries; it grants no business authority. */
    default PipelineRetirementReceipt retire(PipelineRetirementPreparation original,WorkloadClaimFence currentNode,
            java.util.Optional<PipelineRetirementRequest.CacheOriginal> isolated) {
        throw new io.tapstate.core.common.TapstateException(IoError.PIPELINE_INTENT_FENCED,java.util.Map.of(),null);
    }
}
