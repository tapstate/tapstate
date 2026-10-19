package io.tapstate.spi.store;

/** CONFIG preparation of an explicit full rebuild; existing CACHE history remains independently fenced. */
public interface PipelineStateRebuilds {
    PipelineStateRebuilds guardedBy(Runnable requireActive);

    /**
     * The caller already verified original writers and in-flight work isolated. Freeze the exact
     * confirmation and move only its original mapping into REBUILDING. This is no CACHE isolation proof.
     */
    PipelineRebuildPreparation begin(PipelineRebuildRequest original);

    /** Publish the prepared successor and consume this operation's one-shot intent flags atomically. */
    PipelinePreparation complete(PipelineRebuildReceipt original);

    /** Read only this original committed result, including after process restart or response loss. */
    java.util.Optional<PipelinePreparation> completed(PipelineRebuildRequest original);

    /** Verify the exact existing pending operation and original current definitions; never creates facts. */
    void requirePending(PipelineRebuildPreparation original);
}
