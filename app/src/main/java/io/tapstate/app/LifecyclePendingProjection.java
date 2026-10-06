package io.tapstate.app;

import com.mongodb.MongoException;
import io.tapstate.control.core.PipelineExplanation.Pending;
import io.tapstate.control.core.PipelineExplanation.PendingReason;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/** Qualifies a historical terminal decision on a control read without changing local queue facts. */
final class LifecyclePendingProjection {
    private final LifecyclePendingRegistry pending;
    private final StorePort store;
    private final ObservationScopeRegistry scopes;
    private final ClusterMembershipGate membership;
    private final String clusterId;

    LifecyclePendingProjection(LifecyclePendingRegistry pending, StorePort store, ObservationScopeRegistry scopes,
            ClusterMembershipGate membership, String clusterId) {
        this.pending = Objects.requireNonNull(pending, "pending");
        this.store = Objects.requireNonNull(store, "store");
        this.scopes = Objects.requireNonNull(scopes, "scopes");
        this.membership = Objects.requireNonNull(membership, "membership");
        this.clusterId = Objects.requireNonNull(clusterId, "clusterId");
    }

    Optional<Pending> pending(String pipelineId) {
        LifecyclePendingRegistry.Projection read = pending.projection(pipelineId);
        var candidate = read.terminalNoop();
        if (read.pending() == null || candidate == null
                || read.pending().reason() != PendingReason.START_PENDING
                        && read.pending().reason() != PendingReason.START_CAPACITY
                || candidate.intent().targetState() != PipelineState.RUNNING
                || candidate.context().owner() == null || candidate.context().binding() == null
                || !candidate.context().binding().known()) {
            return pending.pending(pipelineId);
        }
        PipelineState actual = StateJson.parse(candidate.checkpoint().stateJson());
        if (actual != PipelineState.FAILED && actual != PipelineState.COMPLETED) {
            return pending.pending(pipelineId);
        }
        var owner = candidate.context().owner();
        if (!clusterId.equals(owner.key().clusterId()) || !pipelineId.equals(owner.key().resourceId())
                || owner.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || owner.executionGeneration() <= 0) {
            return pending.pending(pipelineId);
        }
        ClusterMembership committed = membership.committed();
        var visible = membership.visibleSnapshot();
        if (!localCurrent(owner, committed, visible)) { return pending.pending(pipelineId); }
        try {
            Proof before = readProof(pipelineId, candidate, committed);
            if (before == null) { return pending.pending(pipelineId); }
            Proof after = readProof(pipelineId, candidate, committed);
            if (!before.equals(after) || !localCurrent(owner, committed, visible)) {
                return pending.pending(pipelineId);
            }
            AtomicBoolean suppress = new AtomicBoolean();
            // All IO is over before either memory gate; a replacement entry cannot revive this receipt.
            scopes.withBindingIdentity(pipelineId, candidate.context().binding(),
                    () -> pending.withProjection(pipelineId, read, () -> {
                        if (committed.equals(membership.committed())
                                && visible.equals(membership.visibleSnapshot())) { suppress.set(true); }
                    }));
            return suppress.get() ? Optional.empty() : pending.pending(pipelineId);
        } catch (TapstateException | MongoException unavailable) {
            // Unavailable or corrupt cold proof does not turn the still queued reconcile into a NOOP.
            return pending.pending(pipelineId);
        }
    }

    private boolean localCurrent(ObservationScopeRecovery.Owner owner, ClusterMembership expected,
            ClusterMembershipGate.VisibleSnapshot visible) {
        return expected != null && clusterId.equals(expected.clusterId())
                && expected.revision() == owner.topologyRevision()
                && expected.activeNodeIds().contains(owner.owner().nodeId())
                && visible.nodeIds().contains(owner.owner().nodeId())
                && membership.businessEligible() && expected.equals(membership.committed())
                && visible.equals(membership.visibleSnapshot());
    }

    /** One bounded exact-key proof pass; the caller repeats it once to detect changes. */
    private Proof readProof(String pipelineId, LifecyclePendingRegistry.TerminalNoop candidate,
            ClusterMembership committed) {
        if (!store.desired().read(pipelineId).filter(candidate.intent()::equals).isPresent()
                || !store.state().read(pipelineId).filter(candidate.checkpoint()::equals).isPresent()) {
            return null;
        }
        var artifact = store.artifacts().get(pipelineId).filter(found -> "pipeline".equals(found.kind())).orElse(null);
        var incarnation = store.artifacts().pipelineIncarnationId(pipelineId).orElse(null);
        var generation = store.workloadClaims().currentGeneration(clusterId, pipelineId);
        if (artifact == null || incarnation == null || generation.isEmpty() || generation.getAsLong() <= 0) {
            return null;
        }
        ObservationStore.Scope scope = new ObservationStore.Scope(incarnation, generation.getAsLong());
        if (!candidate.context().binding().matchesScope(scope)) { return null; }
        var owner = candidate.context().owner();
        var reading = store.workloadClaims().read(owner.key()).orElse(null);
        if (reading == null || !reading.leased()
                || !owner.equals(ObservationScopeRecovery.Owner.of(reading.claim()))
                || reading.claim().executionGeneration() != scope.executionGeneration()
                || !store.clusterMembership().read(clusterId).filter(committed::equals).isPresent()) {
            return null;
        }
        WorkloadClaimKey node = new WorkloadClaimKey(clusterId, WorkloadClaimType.NODE_SESSION,
                owner.owner().nodeId());
        var session = store.workloadClaims().read(node).orElse(null);
        if (session == null || !session.leased() || !session.claim().key().equals(node)
                || !session.claim().owner().equals(owner.owner())) {
            return null;
        }
        return new Proof(scope, CanonicalHash.of(artifact), WorkloadClaimFence.from(session.claim()));
    }

    private record Proof(ObservationStore.Scope scope, String artifactHash, WorkloadClaimFence nodeSession) { }
}
