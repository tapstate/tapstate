package io.tapstate.spi.store;

import java.util.Optional;
import java.util.Set;

/** Logged-capture membership is independent from each consumer's cursor and the source checkpoint. */
public interface SrsConsumerStore {
    SrsConsumerStore guardedBy(Runnable requireActive);
    Optional<SrsConsumerMembership> find(SrsConsumerKey key);
    SrsConsumerTopology attached(String miningChainId);
    Optional<ConsumerOffset> cursor(SrsConsumerContext context);
    Optional<SrsConsumerProgress> progress(SrsConsumerContext context);

    /**
     * Pins the original snapshot under one fixed membership, progress revision and Pipeline claim.
     * An identical held pin is success without returning or adopting a newer progress revision.
     * Existing references, row order and replay anchor cannot be replaced; new selected tables may add references.
     */
    boolean pinSnapshot(SrsConsumerContext context, long membershipRevision, long expectedProgressRevision,
                        SrsSnapshotPin snapshot, WorkloadClaimFence originalPipelineFence);


    /** Uses the original expected relation revision and Pipeline claim; existing progress is preserved. */
    Optional<SrsConsumerMembership> attach(SrsConsumerContext context, long expectedRevision, Set<String> tables,
                                          WorkloadClaimFence originalPipelineFence);

    /** Removes only this cursor and leaves a revision marker so a late original attach cannot revive it. */
    Optional<SrsConsumerMembership> detach(SrsConsumerContext context, long expectedRevision,
                                          WorkloadClaimFence originalPipelineFence);
}
