package io.tapstate.spi.store;

import java.util.Optional;
import java.util.Map;
import java.util.List;
import java.util.Set;

/** Logged-capture membership is independent from each consumer's cursor and the source checkpoint. */
public interface SrsConsumerStore {
    SrsConsumerStore guardedBy(Runnable requireActive);
    /** Reads the complete original control closure without loading lost payloads or granting recovery authority. */
    default SharedSrsRecoverySnapshot inspectSharedRecovery(CapturePreparation previous) {
        throw new io.tapstate.core.common.TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE,Map.of("chain",previous.chainId()),null);
    }
    /** Loads coherent original-generation relations, including detach markers, for lifecycle retirement only. */
    default List<SrsConsumerMembership> forPipeline(PipelineIntentScope original) {
        throw new io.tapstate.core.common.TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null);
    }

    Optional<SrsConsumerMembership> find(SrsConsumerKey key);
    /** Bounded exact original relation only; no scan of other consumers, cursors or model bodies. */
    default boolean attached(SrsConsumerRun original,String table){throw new io.tapstate.core.common.TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE,Map.of("chain",original.miningChainId()),null);}
    SrsConsumerTopology attached(String miningChainId);
    Optional<ConsumerOffset> cursor(SrsConsumerContext context);
    Optional<SrsConsumerProgress> progress(SrsConsumerContext context);

    /** Freezes the original current view and one store-issued completed CONFIG full. */
    default SrsFreshFullPreparation inspectFreshFull(CompletedPipelineFull full, SrsConsumerRun original, long expectedProgressRevision,
            WorkloadClaimFence originalNode, WorkloadClaimFence originalCapture) {
        throw new io.tapstate.core.common.TapstateException(IoError.SRS_CONSUMER_UNAVAILABLE,
                Map.of("chain", original.miningChainId()), null);
    }

    /**
     * Pins the original snapshot under one fixed membership, progress revision and Pipeline claim.
     * An identical held pin is success without returning or adopting a newer progress revision.
     * Existing references, row order and replay anchor cannot be replaced; new selected tables may add references.
     */
    boolean pinSnapshot(SrsConsumerContext context, long membershipRevision, long expectedProgressRevision,
                        SrsSnapshotPin snapshot, WorkloadClaimFence originalPipelineFence);


    /** Begins one immutable writer roster per original execution without resetting retained sink progress. */
    boolean beginWriterRun(SrsConsumerContext context, long membershipRevision, String runId,
                           Map<String,List<String>> expectedWritersByTable, WorkloadClaimFence originalPipelineFence);

    /** Monotonically folds one original writer's report and its confirmed cursor in the same cache transaction. */
    boolean advanceWriter(SrsConsumerContext context, long membershipRevision, String runId, String table, String writerId,
                          WriterProgress progress, WorkloadClaimFence originalPipelineFence);

    /**
     * Saves the original first arrival and its initial read cursor together. A retained confirmed place is never raised by a later arrival.
     * A held place covering the requested floor succeeds without adopting a later progress revision; a new place uses only the original expected revision.
     */
    boolean startReadingAfter(SrsConsumerContext context, long membershipRevision, long expectedProgressRevision, String table, long sequence,
                              WorkloadClaimFence originalPipelineFence);

    /**
     * Records the supplied per-table first arrivals in one original-claim transaction and progress CAS.
     * Every table keeps its separately sampled floor. Existing covering floors are unchanged; adding any
     * missing floor requires the original expected revision. No subset is written on refusal.
     */
    boolean startReadingAfter(SrsConsumerContext context, long membershipRevision, long expectedProgressRevision,
                              Map<String, Long> sequences, WorkloadClaimFence originalPipelineFence);

    /** Advances only this consumer's read cursor; it cannot acknowledge a sink or advance the source checkpoint. */
    boolean advanceReadSequence(SrsConsumerContext context, long membershipRevision, String table, long sequence,
                                WorkloadClaimFence originalPipelineFence);

    /** Uses the original expected relation revision and Pipeline claim; existing progress is preserved. */
    Optional<SrsConsumerMembership> attach(SrsConsumerContext context, long expectedRevision, Set<String> tables,
                                          WorkloadClaimFence originalPipelineFence);

    /** A confirmed original rebuild may detach its retired consumer under the current node session. */
    default Optional<SrsConsumerMembership> detachForRebuild(PipelineRebuildPreparation original,SrsConsumerMembership membership,
            WorkloadClaimFence currentNode) {
        return detach(membership.context(),membership.revision(),original.request().originalClaim());
    }

    /** A fixed verified deletion step retires only this original physical generation's relation. */
    default Optional<SrsConsumerMembership> detachForRetirement(PipelineRetirementPreparation original,SrsConsumerMembership membership,
            WorkloadClaimFence currentNode,Optional<PipelineRetirementRequest.CacheOriginal> isolated) {
        throw new io.tapstate.core.common.TapstateException(IoError.PIPELINE_INTENT_FENCED,Map.of(),null);
    }

    /** Removes only this cursor and leaves a revision marker so a late original attach cannot revive it. */
    Optional<SrsConsumerMembership> detach(SrsConsumerContext context, long expectedRevision,
                                          WorkloadClaimFence originalPipelineFence);
}
