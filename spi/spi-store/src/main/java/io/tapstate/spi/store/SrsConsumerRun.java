package io.tapstate.spi.store;

import java.io.Serializable;
import java.util.Objects;

/** Serializable original consumer coordinates; no store, lease deadline or later-scope lookup travels with them. */
public record SrsConsumerRun(PipelineIntentScope scope, String miningChainId, String sourceId, long membershipRevision,
        String ownerNodeId, String ownerBootId, long claimGeneration, long executionGeneration, long topologyRevision)
        implements Serializable {
    public SrsConsumerRun {
        Objects.requireNonNull(scope,"scope");
        new SrsConsumerKey(miningChainId,scope.pipelineId(),sourceId);
        new WorkloadOwner(ownerNodeId,ownerBootId);
        if (membershipRevision < 1 || membershipRevision == Long.MAX_VALUE || claimGeneration < 1 || claimGeneration == Long.MAX_VALUE
                || executionGeneration < 1 || executionGeneration == Long.MAX_VALUE || topologyRevision < 0 || topologyRevision == Long.MAX_VALUE)
            throw new IllegalArgumentException("consumer execution requires its original bounded membership and claim");
    }
    public static SrsConsumerRun of(PipelineIntentScope scope,SrsConsumerMembership membership) {
        Objects.requireNonNull(membership,"membership"); var fence = membership.commandFence();
        if (membership.detached() || !scope.clusterId().equals(fence.key().clusterId()) || !scope.pipelineId().equals(fence.key().resourceId())
                || !scope.pipelineId().equals(membership.context().key().pipelineId()) || scope.stateGeneration() != membership.context().stateGeneration())
            throw new IllegalArgumentException("consumer membership belongs to a different original Pipeline scope");
        return new SrsConsumerRun(scope,membership.context().key().miningChainId(),membership.context().key().sourceId(),membership.revision(),
                fence.owner().nodeId(),fence.owner().bootId(),fence.claimGeneration(),fence.executionGeneration(),fence.topologyRevision());
    }
    public SrsConsumerContext context() { return new SrsConsumerContext(new SrsConsumerKey(miningChainId,scope.pipelineId(),sourceId),scope.stateGeneration()); }
    public CacheBusinessScope cacheScope() { return new CacheBusinessScope(scope.clusterId(),scope.bindingVersion(),scope.cacheIncarnation()); }
    public WorkloadClaimFence originalClaim() {
        return new WorkloadClaimFence(new WorkloadClaimKey(scope.clusterId(),WorkloadClaimType.PIPELINE_ACTUATION,scope.pipelineId()),
                new WorkloadOwner(ownerNodeId,ownerBootId),claimGeneration,executionGeneration,topologyRevision);
    }
}
