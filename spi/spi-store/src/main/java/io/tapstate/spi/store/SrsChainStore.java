package io.tapstate.spi.store;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

/** Explicit logged-capture control. No method infers a capture claim from a chain name. */
public interface SrsChainStore {
    SrsChainStore guardedBy(Runnable requireActive);

    /** Explicit first preparation only; retained models or consumers prevent recreating a lost root. */
    SrsChainState prepare(String miningChainId, String retention, WorkloadClaimFence originalCaptureFence);

    /** Loads the chain and its exact checkpoint models without synthesizing schema history. */
    Optional<SrsChainState> load(String miningChainId);

    /** Unions requests under the original live capture claim; an older narrow callback cannot certify them. */
    SrsChainState requestTables(String miningChainId, Set<String> tables, WorkloadClaimFence originalCaptureFence);

    /** A retry of the same expected epoch and original fence returns its allocation, never a later one. */
    OptionalLong openEpoch(String miningChainId, long expectedEpoch, WorkloadClaimFence originalCaptureFence);

    /** Publishes an immutable callback selection only when it covers every current request. */
    boolean publishServingTables(String miningChainId, long epoch, Set<String> tables, WorkloadClaimFence originalCaptureFence);

    /** Original expected revision is fixed across retries; conflict never adopts a newer checkpoint. */
    Optional<SrsChainState> checkpoint(String miningChainId, long expectedRevision,
                                      SrsCaptureCheckpoint checkpoint, WorkloadClaimFence originalCaptureFence);
}
