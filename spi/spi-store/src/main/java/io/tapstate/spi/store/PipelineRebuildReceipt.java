package io.tapstate.spi.store;

/** A verified original CACHE transition result; it does not itself publish CONFIG ACTIVE or start work. */
public interface PipelineRebuildReceipt {
    PipelineRebuildPreparation preparation();
    WorkloadClaimFence issuerNode();
    WorkloadClaimFence claim();
    long checkpointEpoch();
}
