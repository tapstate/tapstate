package io.tapstate.spi.store;

/** CONFIG preparation of an explicit full rebuild; existing CACHE history remains independently fenced. */
public interface PipelineStateRebuilds {
    PipelineStateRebuilds guardedBy(Runnable requireActive);

    /**
     * The caller already verified original writers and in-flight work isolated. Freeze the exact
     * confirmation and move only its original mapping into REBUILDING. This is no CACHE isolation proof.
     */
    PipelineRebuildPreparation begin(PipelineRebuildRequest original);

    /** Verify the exact existing pending operation and original current definitions; never creates facts. */
    void requirePending(PipelineRebuildPreparation original);
}
