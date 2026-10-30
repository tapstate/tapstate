package io.tapstate.adapters.mongostore;

import io.tapstate.spi.store.WorkloadClaimFence;

/** Retained execution facts never borrow current authority or outrun its monotonic coordinates. */
final class MongoPositionFences {
    private MongoPositionFences() {}
    static boolean retained(WorkloadClaimFence original, WorkloadClaimFence current) {
        return original.key().equals(current.key()) && original.claimGeneration() <= current.claimGeneration()
                && original.executionGeneration() <= current.executionGeneration() && original.topologyRevision() <= current.topologyRevision()
                && (original.claimGeneration() != current.claimGeneration() || original.owner().equals(current.owner()));
    }
}
