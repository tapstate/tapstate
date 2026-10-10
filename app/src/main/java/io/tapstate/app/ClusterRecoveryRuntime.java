package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ClusterCapacityLimits;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.DesiredStateFingerprint;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.ExecutionCohortGuard;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.scheduler.RebuildAdmission;
import io.tapstate.runtime.srs.CaptureError;
import io.tapstate.runtime.srs.CaptureStartupException;
import io.tapstate.spi.store.*;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Composes the durable queue into the existing convergence admission and real execution allocator. */
final class ClusterRecoveryRuntime implements RebuildAdmission, PipelineExecutionAdmission {
    private static final Logger LOG = LoggerFactory.getLogger(ClusterRecoveryRuntime.class);
    private static final int PAGE_SIZE = 100;
    private final StorePort stores;
    private final DagSource dags;
    private final PipelineCaptureCoordinator captures;
    private final Engine engine;
    private final PipelineActuationOwnership actuation;
    private final ClusterWorkloadClaims claims;
    private final ClusterMembershipGate membership;
    private final HazelcastInstance member;
    private final ClusterProperties properties;
    private final ClusterCapacityLimits limits;
    private final String defaultDatabase;
    private final WorkloadOwner owner;
    private final Duration detectionWindow;
    private final Map<String, Allocation> allocations = new HashMap<>();
    private final Map<String, FailedExecution> failures = new HashMap<>();
    private final Map<String, WorkloadClaim> failedStops = new HashMap<>();
    private final Map<String, WorkloadClaimFence> notedFailures = new HashMap<>();
    private WorkloadClaim recoveryClaim;

    private record Allocation(DagSource.PlanningFacts facts, ClusterExecutionProfile profile,
            ClusterRecoveryKey key, ClusterCapacityReservation capacity, boolean recovery, PendingPipelineResume resume) {
        Allocation(DagSource.PlanningFacts facts, ClusterExecutionProfile profile,
                ClusterRecoveryKey key, ClusterCapacityReservation capacity, boolean recovery) {
            this(facts, profile, key, capacity, recovery, null);
        }
    }
    private record FailedExecution(long executionGeneration, WorkloadClaimFence authority, ClusterRecoveryDiagnostic diagnostic,
            CaptureStartupFailure sourceFailure, Throwable executionCause) {
        boolean belongsTo(WorkloadClaim claim) {
            return claim != null && authority != null && authority.sameAuthorityAs(WorkloadClaimFence.from(claim));
        }
    }

    ClusterRecoveryRuntime(StorePort stores, DagSource dags, PipelineCaptureCoordinator captures, Engine engine,
            PipelineActuationOwnership actuation, ClusterWorkloadClaims claims, ClusterMembershipGate membership,
            HazelcastInstance member, ClusterProperties properties, ClusterCapacityLimits limits, String defaultDatabase,
            Duration detectionWindow) {
        this.stores = stores;
        this.dags = dags;
        this.captures = captures;
        this.engine = engine;
        this.actuation = actuation;
        this.claims = claims;
        this.membership = membership;
        this.member = member;
        this.properties = properties;
        this.limits = limits;
        this.defaultDatabase = defaultDatabase;
        this.detectionWindow = detectionWindow;
        Object node = member.getUserContext().get(HazelcastConfiguration.NODE_SESSION_CONTEXT_KEY);
        owner = active() ? ((WorkloadClaim) Objects.requireNonNull(node, "node session")).owner() : null;
    }

    private boolean active() { return properties.getProfile() != ClusterProperties.Profile.SINGLE; }

    @Override public boolean admits(String pipelineId) {
        if (!active()) { return false; }
        recordFailure(pipelineId);
        return permitted(pipelineId);
    }

    @Override public boolean admitsMissingJob(String pipelineId) {
        if (!active()) { return true; }
        WorkloadClaim current = actuation.currentClaim(pipelineId).orElse(null);
        if (current == null) { return false; }
        if (current.executionGeneration() == 0) { return true; }
        if (pendingResume(pipelineId).isPresent()) { return true; }
        Allocation local = allocations.get(pipelineId);
        // A start waiting for a shared ring resumes its already allocated, still-owned ordinary step.
        if (local != null && !local.recovery() && local.capacity().executionGeneration() != null
                && local.capacity().nativeJobId() == null
                && local.capacity().executionGeneration() == current.executionGeneration()
                && local.capacity().pipelineClaim().sameAuthorityAs(WorkloadClaimFence.from(current))) {
            return true;
        }
        return permitted(pipelineId);
    }

    private boolean permitted(String pipelineId) {
        if (!membership.businessEligible()) { return false; }
        Optional<ClusterRecoveryItem> item = item(pipelineId);
        return item.filter(value -> !value.status().terminal() && value.permit() != null)
                .filter(value -> stores.desired().read(pipelineId)
                        .map(DesiredStateFingerprint::of).filter(value.event().intentFingerprint()::equals).isPresent())
                .isPresent();
    }

    @Override public boolean prepare(String pipelineId, DagSource.PlannedStart planned,
            PipelineActuationOwnership ownership) {
        if (!active()) { return PipelineExecutionAdmission.super.prepare(pipelineId, planned, ownership); }
        if (!ownership.mayStart(pipelineId) || !membership.submissionEligible(member.getCluster().getMembers())) {
            return false;
        }
        DagSource.PlanningFacts facts = planned.planningFacts().orElseThrow(() -> unproven(pipelineId, "compiled geometry absent"));
        if (!facts.unknownInputs().isEmpty()) { throw unproven(pipelineId, String.join(", ", facts.unknownInputs())); }
        if (!Set.copyOf(facts.plannedStableIds()).equals(membership.visibleNodeIds())) { return false; }
        ClusterExecutionProfile profile = stores.clusterProfiles().profile(properties.getId()).orElse(null);
        ArtifactIdentity artifact = stores.artifacts().identity(pipelineId).orElse(null);
        DesiredState intent = stores.desired().read(pipelineId).orElse(null);
        WorkloadClaim current = ownership.currentClaim(pipelineId).orElse(null);
        if (profile == null || artifact == null || intent == null || current == null
                || current.profileGeneration() != profile.generation()) { return false; }
        ClusterRecoveryKey key = new ClusterRecoveryKey(properties.getId(), pipelineId, artifact.incarnation());
        ClusterRecoveryItem item = stores.clusterRecovery().read(key).orElse(null);
        PendingPipelineResume resume = pendingResume(pipelineId).orElse(null);
        ClusterCapacityReservation resumedCapacity = resume == null ? null
                : stores.clusterCapacity().resumeReservation(resume).orElse(null);
        Allocation pending = allocations.get(pipelineId);
        boolean pendingCapacity = pendingCapacity(pending, current) || (resumedCapacity != null
                && resumedCapacity.executionGeneration() != null && resumedCapacity.nativeJobId() == null
                && resumedCapacity.executionGeneration() == current.executionGeneration());
        boolean pendingRecovery = pendingRecovery(item, current);
        if (pendingCapacity || pendingRecovery) {
            Set<String> frozen = current.executionNodeIds();
            Set<String> proposed = facts.perMemberUpperBounds().keySet();
            if (!frozen.equals(proposed)) { throw changedCohort(pipelineId, frozen, proposed); }
            requirePendingCohort(pipelineId, current);
            Map<String, io.tapstate.core.lifecycle.ClusterCapacityDemand> frozenDemand = pendingCapacity
                    ? resumedCapacity != null ? resumedCapacity.demandByNode() : pending.capacity().demandByNode()
                    : item.permit().demandByNode();
            if (!frozenDemand.equals(facts.perMemberUpperBounds())) {
                throw unproven(pipelineId, "the allocated resource demand differs from the compiled start");
            }
        }
        if (item != null && !item.status().terminal()
                && item.event().intentFingerprint().equals(DesiredStateFingerprint.of(intent))
                && (resume == null || item.event().originalExecutionGeneration() > resume.originalClaim().executionGeneration())) {
            if (item.permit() == null || !item.permit().demandByNode().equals(facts.perMemberUpperBounds())
                    || !item.targetProfile().equals(profile)) { return false; }
            allocations.put(pipelineId, new Allocation(facts, profile, key, null, true));
            return true;
        }
        ClusterCapacityStore.Result reserved = resume == null
                ? stores.clusterCapacity().reserve(current, profile, artifact.incarnation(),
                        DesiredStateFingerprint.of(intent), facts.perMemberUpperBounds(), limits, properties.getWorkloadClaimTtl())
                : stores.clusterCapacity().reserveResume(resume, current, profile, artifact.incarnation(),
                        DesiredStateFingerprint.of(intent), facts.perMemberUpperBounds(), limits, properties.getWorkloadClaimTtl());
        if (reserved.outcome() == ClusterCapacityStore.Outcome.CAPACITY_REFUSED) {
            var violation = reserved.violations().getFirst();
            throw new TapstateException(LifecycleError.CLUSTER_CAPACITY_REFUSED,
                    Map.of("node", Objects.requireNonNull(reserved.refusedNode(), "refused node"),
                            "resource", violation.resource(), "occupied", violation.occupied(),
                            "requested", violation.requested(), "limit", violation.limit()), null);
        }
        if (reserved.outcome() == ClusterCapacityStore.Outcome.UNKNOWN_DEMAND) {
            throw unproven(pipelineId, "existing execution occupancy is unproven");
        }
        if (reserved.reservation() == null || (reserved.outcome() != ClusterCapacityStore.Outcome.APPLIED
                && reserved.outcome() != ClusterCapacityStore.Outcome.ALREADY_RESERVED)) { return false; }
        allocations.put(pipelineId, new Allocation(facts, profile, key, reserved.reservation(), false, resume));
        return true;
    }

    @Override public PipelineActuationOwnership.Execution begin(String pipelineId, DagSource.PlannedStart planned,
            PipelineActuationOwnership ownership) {
        if (!active()) { return PipelineExecutionAdmission.super.begin(pipelineId, planned, ownership); }
        Allocation allocation = allocations.get(pipelineId);
        if (allocation == null) { return PipelineActuationOwnership.Execution.refused(); }
        WorkloadClaim current = ownership.currentClaim(pipelineId).orElse(null);
        if (pendingExecution(pipelineId).isPresent() && current != null
                && !current.executionNodeIds().equals(membership.visibleNodeIds())) {
            throw changedCohort(pipelineId, current.executionNodeIds(), membership.visibleNodeIds());
        }
        if (pendingExecution(pipelineId).isPresent() && current != null) {
            requirePendingCohort(pipelineId, current);
        }
        WorkloadClaim qualifiedClaim = null;
        if (allocation.resume() != null && allocation.capacity().executionGeneration() != null) {
            ArtifactIdentity artifact = stores.artifacts().identity(pipelineId).orElse(null);
            DesiredState intent = stores.desired().read(pipelineId).orElse(null);
            if (current == null || artifact == null || intent == null) { return PipelineActuationOwnership.Execution.refused(); }
            var checked = stores.clusterCapacity().reserveResume(allocation.resume(), current, allocation.profile(),
                    artifact.incarnation(), DesiredStateFingerprint.of(intent), allocation.facts().perMemberUpperBounds(),
                    limits, properties.getWorkloadClaimTtl());
            if (checked.outcome() == ClusterCapacityStore.Outcome.UNKNOWN_DEMAND) {
                throw unproven(pipelineId, "the accepted allocated resume cannot be qualified for submission");
            }
            if (checked.reservation() == null || (checked.outcome() != ClusterCapacityStore.Outcome.APPLIED
                    && checked.outcome() != ClusterCapacityStore.Outcome.ALREADY_RESERVED)) {
                return PipelineActuationOwnership.Execution.refused();
            }
            var selected = stores.clusterCapacity().resumeSourceRequirements(allocation.resume());
            if (selected.isPresent() && !selected.orElseThrow().equals(allocation.facts().selectedSourceIds())) {
                throw unproven(pipelineId, "the allocated resume's frozen source selection differs from its compiled start");
            }
            allocation = new Allocation(allocation.facts(), allocation.profile(), allocation.key(), checked.reservation(),
                    false, allocation.resume());
            allocations.put(pipelineId, allocation);
            qualifiedClaim = checked.advancedPipelineClaim();
        }
        Allocation admitted = allocation;
        WorkloadClaim adopted = qualifiedClaim;
        PipelineActuationOwnership.Execution execution = ownership.beginExecution(pipelineId, (expected, topology, nodes) -> {
            if (!nodes.equals(admitted.facts().perMemberUpperBounds().keySet())) { return Optional.empty(); }
            if (admitted.recovery()) {
                ClusterRecoveryItem item = stores.clusterRecovery().read(admitted.key()).orElse(null);
                if (item == null || item.permit() == null) { return Optional.empty(); }
                if (item.hasAllocatedSuccessor() && item.successor().pipelineClaim().sameAuthorityAs(WorkloadClaimFence.from(expected))
                        && item.successor().submittedAt() == null) { return Optional.of(expected); }
                var advanced = stores.clusterRecovery().advanceExecution(fence(item), expected, nodes,
                        admitted.facts().selectedSourceIds()).advancedPipelineClaim();
                if (advanced != null) { forgetFailure(pipelineId); }
                return Optional.ofNullable(advanced);
            }
            ClusterCapacityReservation reservation = admitted.capacity();
            if (reservation.executionGeneration() != null) {
                return reservation.executionGeneration() == expected.executionGeneration()
                        && reservation.pipelineClaim().sameAuthorityAs(WorkloadClaimFence.from(expected))
                        && reservation.nativeJobId() == null ? Optional.ofNullable(adopted == null ? expected : adopted) : Optional.empty();
            }
            ClusterCapacityStore.Result advanced = stores.clusterCapacity().advanceExecution(reservation, expected, nodes);
            if (advanced.reservation() != null) {
                allocations.put(pipelineId, new Allocation(admitted.facts(), admitted.profile(), admitted.key(),
                        advanced.reservation(), false, admitted.resume()));
            }
            if (advanced.advancedPipelineClaim() != null) { forgetFailure(pipelineId); }
            return Optional.ofNullable(advanced.advancedPipelineClaim());
        });
        if (execution.allowed() && !admitted.recovery()) {
            Allocation allocated = allocations.get(pipelineId);
            WorkloadClaim held = ownership.currentClaim(pipelineId).orElse(null);
            if (allocated == null || held == null) { return PipelineActuationOwnership.Execution.refused(); }
            var recorded = allocated.resume() == null
                    ? stores.clusterCapacity().recordExecutionSources(allocated.capacity(), WorkloadClaimFence.from(held),
                            allocated.facts().selectedSourceIds())
                    : stores.clusterCapacity().recordResumeSources(allocated.resume(), allocated.capacity(), WorkloadClaimFence.from(held),
                            allocated.facts().selectedSourceIds());
            if (recorded.outcome() == ClusterCapacityStore.Outcome.UNKNOWN_DEMAND) {
                throw unproven(pipelineId, "the execution's compiled source selection cannot be durably qualified");
            }
            if (recorded.outcome() != ClusterCapacityStore.Outcome.APPLIED
                    && recorded.outcome() != ClusterCapacityStore.Outcome.ALREADY_RESERVED) {
                return PipelineActuationOwnership.Execution.refused();
            }
        }
        return execution;
    }

    @Override public void guard(String pipelineId, PipelineActuationOwnership.Execution execution, DAG dag) {
        if (!active()) { return; }
        Allocation allocation = Objects.requireNonNull(allocations.get(pipelineId), "admitted plan");
        ExecutionCohortGuard.install(dag, member, pipelineId, allocation.facts().plannedStableIds(),
                allocation.profile().generation(), allocation.profile().profile().hash());
    }

    @Override public void submitted(String pipelineId, PipelineActuationOwnership.Execution execution, String nativeJobId) {
        if (!active()) { return; }
        Allocation allocation = allocations.get(pipelineId);
        WorkloadClaim claim = actuation.currentClaim(pipelineId).orElse(null);
        if (allocation == null || claim == null) { return; }
        if (allocation.recovery()) {
            stores.clusterRecovery().read(allocation.key()).filter(item -> item.permit() != null)
                    .ifPresent(item -> stores.clusterRecovery().recordSubmission(fence(item), WorkloadClaimFence.from(claim), nativeJobId));
        } else {
            var submitted = stores.clusterCapacity().submitted(allocation.capacity(), WorkloadClaimFence.from(claim), nativeJobId);
            if (submitted.reservation() != null) {
                allocations.put(pipelineId, new Allocation(allocation.facts(), allocation.profile(), allocation.key(),
                        submitted.reservation(), false, allocation.resume()));
            }
        }
    }

    @Override public void recordFailure(String pipelineId) {
        if (!active()) { return; }
        WorkloadClaim failedClaim = actuation.currentClaim(pipelineId).orElse(null);
        Throwable failure = PipelineFailures.current(pipelineId, failedClaim, true, engine, captures).orElse(null);
        FailedExecution recorded = failures.get(pipelineId);
        if (failure == null && recorded != null && recorded.belongsTo(failedClaim)) {
            failure = recorded.executionCause();
        }
        if (failure != null) {
            TapstateException coded = PipelineFailures.codedCause(failure);
            if (ordinaryFailure(coded)
                    || containsCode(failure, io.tapstate.adapters.pdk.ConnectorError.WRITE_FAILED)
                    || containsCode(failure, io.tapstate.adapters.pdk.ConnectorError.CAPTURE_FAILED)
                    || containsCode(failure, io.tapstate.adapters.pdk.ConnectorError.READ_FAILED)) {
                actuation.recordClassifiedFailure(pipelineId, false);
            } else if (ClusterRebuildAdmission.isMembershipChangedBeforeStart(failure)) {
                actuation.recordClassifiedFailure(pipelineId, true);
            } else if (containsCode(failure, EngineError.OUT_OF_MEMORY)) {
                // The lost local member cannot be queried for a live membership view.
                actuation.recordClassifiedFailure(pipelineId, true);
            } else if (failedClaim != null && failedClaim.originalMembersPresent(liveMembers())
                    .filter(present -> !present).isPresent()) {
                actuation.recordClassifiedFailure(pipelineId, true);
            }
        }
        actuation.recordFailure(pipelineId, ClusterRebuildAdmission.MAX_ATTEMPTS * properties.getWorkloadClaimTtl().toNanos(),
                detectionWindow.toNanos());
        if (failure != null) {
            FailedExecution previous = failures.get(pipelineId);
            if (previous == null || !previous.belongsTo(failedClaim)) {
                failures.put(pipelineId, failedExecution(pipelineId, failedClaim, failure));
            }
        }
        WorkloadClaim current = actuation.currentClaim(pipelineId).orElse(null);
        if (current != null && verdictRecorded(current)) {
            // A generic native result can disappear during cleanup. The durable FAILED verdict still
            // names that execution; the original diagnosis is preferred whenever it was observed.
            failures.compute(pipelineId, (ignored, previous) -> previous != null && previous.belongsTo(current)
                    ? previous : failedExecution(pipelineId, current, null));
            boolean noted = persistFailure(pipelineId, current);
            WorkloadClaim stopped = failedStops.get(pipelineId);
            if (noted && sameAuthority(stopped, current)) { retireStopped(pipelineId, stopped); }
        }
    }

    @Override public void afterFailedStop(String pipelineId) { recordFailure(pipelineId); }

    @Override public void failedAfterAllocation(String pipelineId, PipelineActuationOwnership.Execution execution,
            TapstateException failure) {
        if (!active() || execution.fence() == null) { return; }
        WorkloadClaim claim = actuation.currentClaim(pipelineId).orElse(null);
        if (claim == null || !claim.owner().equals(owner) || claim.claimGeneration() != execution.fence().claimGeneration()
                || claim.executionGeneration() != execution.fence().executionGeneration()
                || claim.profileGeneration() != execution.fence().profileGeneration()) { return; }
        failures.compute(pipelineId, (ignored, previous) -> previous != null && previous.belongsTo(claim)
                ? previous : failedExecution(pipelineId, claim, failure));
        recordFailure(pipelineId);
    }

    @Override public void refused(String pipelineId, TapstateException failure) {
        if (!active()) { return; }
        WorkloadClaim current = actuation.currentClaim(pipelineId).orElse(null);
        failures.put(pipelineId, new FailedExecution(actuation.heldExecutionGeneration(pipelineId),
                current == null ? null : WorkloadClaimFence.from(current),
                ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.REBUILD_REFUSED,
                        failure, diagnosticPositions(pipelineId), "Correct the reported configuration or capacity; the source position has not been reset."), null, null));
    }

    @Override public Optional<PipelineActuationOwnership.Execution> pendingExecution(String pipelineId) {
        if (!active()) { return Optional.empty(); }
        WorkloadClaim current = actuation.currentClaim(pipelineId).orElse(null);
        if (current == null || !current.owner().equals(owner) || current.executionGeneration() < 1
                || current.contextExecutionGeneration() != current.executionGeneration()
                || current.executionClaimGeneration() != current.claimGeneration()) { return Optional.empty(); }
        Allocation local = allocations.get(pipelineId);
        if (!pendingCapacity(local, current)) {
            boolean resumed = pendingResume(pipelineId)
                    .flatMap(stores.clusterCapacity()::resumeReservation)
                    .filter(capacity -> pendingResumeCapacity(capacity, current)).isPresent();
            ClusterRecoveryItem queued = local != null && local.recovery()
                    ? stores.clusterRecovery().read(local.key()).orElse(null) : item(pipelineId).orElse(null);
            if (!resumed && !pendingRecovery(queued, current)) { return Optional.empty(); }
        }
        return Optional.of(new PipelineActuationOwnership.Execution(true,
                new ExecutionFence(pipelineId, current.claimGeneration(), current.executionGeneration(), current.profileGeneration()),
                current.topologyRevision()));
    }

    private static boolean pendingCapacity(Allocation allocation, WorkloadClaim current) {
        return current != null && allocation != null && !allocation.recovery() && allocation.capacity() != null
                && allocation.capacity().executionGeneration() != null && allocation.capacity().nativeJobId() == null
                && allocation.capacity().executionGeneration() == current.executionGeneration()
                && allocation.capacity().pipelineClaim().sameAuthorityAs(WorkloadClaimFence.from(current));
    }

    private static boolean pendingResumeCapacity(ClusterCapacityReservation capacity, WorkloadClaim current) {
        return current != null && capacity != null && capacity.executionGeneration() != null && capacity.nativeJobId() == null
                && capacity.executionGeneration() == current.executionGeneration()
                && capacity.pipelineClaim().sameAuthorityAs(WorkloadClaimFence.from(current));
    }

    @Override public Optional<PendingPipelineResume> prepareResume(
            String pipelineId, DesiredState intent, long acceptedStateEpoch) {
        if (!active()) { return Optional.empty(); }
        WorkloadClaim current = actuation.currentClaim(pipelineId).orElse(null);
        var nativeRun = engine.nativeRun(pipelineId).orElse(null);
        ClusterExecutionProfile profile = stores.clusterProfiles().profile(properties.getId()).orElse(null);
        ArtifactIdentity artifact = stores.artifacts().identity(pipelineId).orElse(null);
        if (current == null || nativeRun == null || profile == null || artifact == null
                || intent.targetState() != PipelineState.RUNNING || !artifact.contentHash().equals(intent.revision())
                || !artifact.incarnation().equals(current.executionIncarnation())
                || current.executionProfile() == null || !profile.equals(current.executionProfile())
                || current.contextExecutionGeneration() != current.executionGeneration()
                || current.executionClaimGeneration() != current.claimGeneration()
                || current.executionMembers().isEmpty() || !matchesNative(nativeRun, WorkloadClaimFence.from(current))
                || !nativeRun.initialized() || !nativeMembersMatch(nativeRun, current)
                || !actuation.proveExecution(new ExecutionFence(pipelineId, current.claimGeneration(),
                        current.executionGeneration(), current.profileGeneration()))) {
            throw unproven(pipelineId, "the paused native execution's original authority and member context are unproven");
        }
        return Optional.of(new PendingPipelineResume(acceptedStateEpoch, DesiredStateFingerprint.of(intent), current,
                nativeRun.nativeJobId(), nativeRun.initialization().orElseThrow().runtimeExecutionId()));
    }

    @Override public Optional<PendingPipelineResume> pendingResume(String pipelineId) {
        if (!active()) { return Optional.empty(); }
        DesiredState intent = stores.desired().read(pipelineId).orElse(null);
        ArtifactIdentity artifact = stores.artifacts().identity(pipelineId).orElse(null);
        ClusterExecutionProfile profile = stores.clusterProfiles().profile(properties.getId()).orElse(null);
        if (intent == null || artifact == null || profile == null || intent.targetState() != PipelineState.RUNNING
                || !artifact.contentHash().equals(intent.revision())) { return Optional.empty(); }
        return stores.state().pendingResume(pipelineId)
                .filter(receipt -> receipt.originalClaim().key().clusterId().equals(properties.getId())
                        && receipt.originalClaim().key().resourceId().equals(pipelineId)
                        && receipt.intentFingerprint().equals(DesiredStateFingerprint.of(intent))
                        && receipt.originalClaim().executionIncarnation().equals(artifact.incarnation())
                        && profile.equals(receipt.originalClaim().executionProfile()));
    }

    @Override public boolean acceptsPendingResume(PendingPipelineResume receipt) {
        return pendingResume(receipt.originalClaim().key().resourceId()).filter(receipt::sameRequestAs).isPresent();
    }

    @Override public ResumeDisposition resumeDisposition(PendingPipelineResume receipt, boolean loadDelivered) {
        String pipeline = receipt.originalClaim().key().resourceId();
        if (!acceptsPendingResume(receipt) || !membership.businessEligible()) { return ResumeDisposition.WAIT; }
        if (resumeCompleted(receipt)) { return ResumeDisposition.WAIT; }
        if (!membership.submissionEligible(member.getCluster().getMembers())) { return ResumeDisposition.WAIT; }
        var nativeRun = engine.nativeRun(pipeline).orElse(null);
        WorkloadClaim current = actuation.currentClaim(pipeline).orElse(null);
        ClusterCapacityReservation linked = stores.clusterCapacity().resumeReservation(receipt).orElse(null);
        if (linked != null && linked.executionGeneration() != null && nativeRun != null
                && matchesNative(nativeRun, linked.pipelineClaim())) {
            if (current == null || !linked.pipelineClaim().sameAuthorityAs(WorkloadClaimFence.from(current))) {
                if (current != null && stores.clusterCapacity().resumeAuthorityRetired(receipt, current, linked.pipelineClaim())) {
                    throw unproven(pipeline, "the linked resume was submitted under a different native authority");
                }
                return ResumeDisposition.WAIT;
            }
            if (linked.nativeJobId() == null && current != null
                    && linked.pipelineClaim().sameAuthorityAs(WorkloadClaimFence.from(current))) {
                stores.clusterCapacity().submitted(linked, WorkloadClaimFence.from(current), nativeRun.nativeJobId());
            }
            return ResumeDisposition.WAIT;
        }
        if (nativeRun != null && receipt.originalNativeJobId().equals(nativeRun.nativeJobId())
                && matchesNative(nativeRun, WorkloadClaimFence.from(receipt.originalClaim()))
                && (nativeRun.status() == JobStatus.SUSPENDED || nativeRun.status() == JobStatus.RUNNING)) {
            DesiredState intent = stores.desired().read(pipeline).orElseThrow();
            boolean sameAuthority = current != null
                    && WorkloadClaimFence.from(receipt.originalClaim()).sameAuthorityAs(WorkloadClaimFence.from(current));
            boolean sameCohort = receipt.originalClaim().executionMembers().equals(completeLiveMembers(pipeline));
            if (loadDelivered && !intent.reassemble() && sameAuthority && sameCohort) {
                if (!actuation.proveExecution(new ExecutionFence(pipeline, current.claimGeneration(),
                        current.executionGeneration(), current.profileGeneration()))) { return ResumeDisposition.WAIT; }
                String observed = nativeRun.initialization().map(value -> value.runtimeExecutionId()).orElse(null);
                return nativeRun.status() == JobStatus.SUSPENDED || receipt.originalRuntimeExecutionId().equals(observed)
                        ? ResumeDisposition.HELD : ResumeDisposition.WAIT;
            }
            return ResumeDisposition.RECOMPILE;
        }
        if (nativeRun != null && nativeRun.status() != JobStatus.FAILED && nativeRun.status() != JobStatus.COMPLETED) {
            throw unproven(pipeline, "a different native execution is already carrying the accepted resume");
        }
        return ResumeDisposition.START;
    }

    @Override public boolean resumeCompleted(PendingPipelineResume receipt) {
        String pipeline = receipt.originalClaim().key().resourceId();
        if (!acceptsPendingResume(receipt) || !membership.businessEligible()) { return false; }
        WorkloadClaim current = actuation.currentClaim(pipeline).orElse(null);
        var nativeRun = engine.nativeRun(pipeline).orElse(null);
        if (current == null || nativeRun == null || !nativeRun.initialized()
                || (nativeRun.status() != JobStatus.RUNNING && nativeRun.status() != JobStatus.COMPLETED)
                || !matchesNative(nativeRun, WorkloadClaimFence.from(current)) || !nativeMembersMatch(nativeRun, current)
                || !actuation.proveExecution(new ExecutionFence(pipeline, current.claimGeneration(),
                        current.executionGeneration(), current.profileGeneration()))) { return false; }
        ClusterCapacityReservation linked = stores.clusterCapacity().resumeReservation(receipt).orElse(null);
        if (linked == null) {
            if (!receipt.originalNativeJobId().equals(nativeRun.nativeJobId())
                    || !WorkloadClaimFence.from(receipt.originalClaim()).sameAuthorityAs(WorkloadClaimFence.from(current))
                    || receipt.originalRuntimeExecutionId().equals(nativeRun.initialization().orElseThrow().runtimeExecutionId())) {
                return false;
            }
        } else {
            if (linked.executionGeneration() == null || linked.executionGeneration() != current.executionGeneration()
                    || !linked.pipelineClaim().sameAuthorityAs(WorkloadClaimFence.from(current))) { return false; }
            if (linked.nativeJobId() == null) {
                var submitted = stores.clusterCapacity().submitted(linked, WorkloadClaimFence.from(current), nativeRun.nativeJobId());
                linked = submitted.reservation();
            }
            if (linked == null || !nativeRun.nativeJobId().equals(linked.nativeJobId())) { return false; }
        }
        WorkloadClaimFence fence = WorkloadClaimFence.from(current);
        Optional<Set<String>> selected = stores.clusterCapacity().resumeSourceRequirements(receipt);
        if (selected.isEmpty() && linked == null) {
            selected = item(pipeline).filter(item -> item.hasAllocatedSuccessor()
                            && item.successor().executionGeneration() == receipt.originalClaim().executionGeneration()
                            && item.successor().pipelineClaim().sameAuthorityAs(WorkloadClaimFence.from(receipt.originalClaim()))
                            && item.successor().sourceRequirementsRecorded())
                    .map(item -> item.successor().requiredSourceIds());
        }
        if (selected.isEmpty()) { throw unproven(pipeline, "the resumed execution's frozen compiled source selection is absent"); }
        Set<String> sources = selected.orElseThrow();
        return captures.requiredSources(pipeline, fence).equals(sources)
                && captures.startupProofs(pipeline, fence).keySet().equals(sources);
    }

    private static boolean matchesNative(Engine.NativeRun run, WorkloadClaimFence fence) {
        return run.claimGeneration() == fence.claimGeneration() && run.executionGeneration() == fence.executionGeneration()
                && run.profileGeneration() == fence.profileGeneration();
    }

    private static boolean nativeMembersMatch(Engine.NativeRun run, WorkloadClaim claim) {
        if (!run.initialized() || claim.contextExecutionGeneration() != claim.executionGeneration()
                || claim.executionClaimGeneration() != run.claimGeneration() || claim.executionMembers().isEmpty()) { return false; }
        Map<String, ClusterExecutionMember> observed = new LinkedHashMap<>();
        for (var processor : run.initialization().orElseThrow().processors().values()) {
            if (processor.nodeId() == null || processor.bootId() == null || processor.memberUuid() == null
                    || processor.memberCount() != claim.executionMembers().size()
                    || !processor.jobId().equals(run.nativeJobId())
                    || !processor.runtimeExecutionId().equals(run.initialization().orElseThrow().runtimeExecutionId())
                    || processor.claimGeneration() != run.claimGeneration()
                    || processor.executionGeneration() != run.executionGeneration()
                    || processor.profileGeneration() != run.profileGeneration()) { return false; }
            ClusterExecutionMember identity = new ClusterExecutionMember(processor.nodeId(), processor.bootId(), processor.memberUuid());
            ClusterExecutionMember previous = observed.putIfAbsent(identity.nodeId(), identity);
            if (previous != null && !previous.equals(identity)) { return false; }
        }
        return observed.equals(claim.executionMembers());
    }

    private static boolean pendingRecovery(ClusterRecoveryItem item, WorkloadClaim current) {
        return current != null && item != null && !item.status().terminal() && item.hasAllocatedSuccessor()
                && item.successor().submittedAt() == null
                && item.successor().pipelineClaim().sameAuthorityAs(WorkloadClaimFence.from(current));
    }

    private static TapstateException changedCohort(String pipeline, Set<String> original, Set<String> actual) {
        return new TapstateException(EngineError.EXECUTION_COHORT_CHANGED_BEFORE_START,
                Map.of("pipeline", pipeline, "reason", "live-membership", "planned", original.stream().sorted().toList().toString(),
                        "actual", actual.stream().sorted().toList().toString()), null);
    }

    private void requirePendingCohort(String pipelineId, WorkloadClaim current) {
        if (current.executionMembers().isEmpty()
                || current.contextExecutionGeneration() != current.executionGeneration()) {
            throw unproven(pipelineId, "the allocated execution's original member identities are absent");
        }
        Map<String, ClusterExecutionMember> live = completeLiveMembers(pipelineId);
        if (!current.executionMembers().equals(live)) {
            var byNode = java.util.Comparator.comparing(ClusterExecutionMember::nodeId);
            throw new TapstateException(EngineError.EXECUTION_COHORT_CHANGED_BEFORE_START,
                    Map.of("pipeline", pipelineId, "reason", "member-incarnation",
                            "planned", current.executionMembers().values().stream().sorted(byNode).toList().toString(),
                            "actual", live.values().stream().sorted(byNode).toList().toString()), null);
        }
    }

    private Map<String, ClusterExecutionMember> completeLiveMembers(String pipelineId) {
        Map<String, ClusterExecutionMember> live = new LinkedHashMap<>();
        for (var peer : member.getCluster().getMembers()) {
            if (peer.isLiteMember()) { continue; }
            String node = peer.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE);
            String boot = peer.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE);
            if (node == null || node.isBlank() || boot == null || boot.isBlank() || peer.getUuid() == null) {
                throw unproven(pipelineId, "a live data member has incomplete runtime identity");
            }
            if (live.putIfAbsent(node, new ClusterExecutionMember(node, boot, peer.getUuid().toString())) != null) {
                throw unproven(pipelineId, "live data members have duplicate stable node identity");
            }
        }
        if (!live.keySet().equals(membership.visibleNodeIds())) {
            throw unproven(pipelineId, "the live member identities differ from the eligible member view");
        }
        return Map.copyOf(live);
    }

    @Override public void stopped(String pipelineId, WorkloadClaim stoppedClaim, boolean jobOver) {
        if (!active() || !jobOver || stoppedClaim == null) { return; }
        WorkloadClaim current = actuation.currentClaim(pipelineId).orElse(null);
        boolean failed = stores.state().read(pipelineId)
                .map(checkpoint -> StateJson.parse(checkpoint.stateJson()) == PipelineState.FAILED).orElse(false);
        if (failed && sameAuthority(stoppedClaim, current)
                && current.executionGeneration() > 0
                && current.contextExecutionGeneration() == current.executionGeneration()
                && (!verdictRecorded(current) || !persistFailure(pipelineId, current))) {
            // Keep the existing detector and renewal alive until a later membership publication can
            // classify this stopped run. Only this captured execution may be retired afterwards.
            failedStops.put(pipelineId, stoppedClaim);
            return;
        }
        retireStopped(pipelineId, stoppedClaim);
    }

    private void retireStopped(String pipelineId, WorkloadClaim stoppedClaim) {
        if (!actuation.retireStoppedExecution(stoppedClaim)) { return; }
        failedStops.remove(pipelineId);
        Allocation allocation = allocations.get(pipelineId);
        if (allocation != null && allocation.capacity() != null) { stores.clusterCapacity().release(allocation.capacity()); }
    }

    @Override public boolean mayComplete(String pipelineId) {
        if (!active()) { return true; }
        ClusterRecoveryItem item = item(pipelineId).orElse(null);
        if (item == null || item.status().terminal()) { return true; }
        WorkloadClaim current = actuation.currentClaim(pipelineId).orElse(null);
        if (current == null) { return false; }
        if (!item.executionAliases().contains(current.executionGeneration())) { return true; }
        return item.permit() != null && observeStartup(item, fence(item));
    }

    @Override public void retain(Collection<String> pipelineIds) {
        if (!active() || !membership.businessEligible()) { return; }
        if (!holdRecoveryClaim()) { return; }
        Map<String, ClusterExecutionMember> live = liveMembers();
        ClusterExecutionProfile profile = stores.clusterProfiles().profile(properties.getId()).orElseThrow();
        for (String pipeline : pipelineIds) {
            try { discover(pipeline, profile, live); }
            catch (TapstateException unavailable) { LOG.warn("Recovery discovery for {} failed [{}]", pipeline, unavailable.code().code(), unavailable); }
        }
        for (int offset = 0; ; offset += PAGE_SIZE) {
            List<ClusterRecoveryItem> page = stores.clusterRecovery().list(properties.getId(), offset, PAGE_SIZE);
            for (ClusterRecoveryItem item : page) {
                if (!item.status().terminal()) {
                    try { reconcile(item, profile); }
                    catch (TapstateException unavailable) { LOG.warn("Recovery step for {} failed [{}]", item.event().key().pipelineId(), unavailable.code().code(), unavailable); }
                }
            }
            if (page.size() < PAGE_SIZE) { break; }
        }
        allocations.entrySet().removeIf(entry -> !pipelineIds.contains(entry.getKey()));
        failures.keySet().retainAll(pipelineIds);
        notedFailures.keySet().retainAll(pipelineIds);
        failedStops.entrySet().removeIf(entry -> !pipelineIds.contains(entry.getKey())
                || !sameAuthority(entry.getValue(), actuation.currentClaim(entry.getKey()).orElse(null)));
    }

    private boolean holdRecoveryClaim() {
        var topology = membership.committed();
        if (topology == null) { return false; }
        WorkloadClaimKey key = new WorkloadClaimKey(properties.getId(), WorkloadClaimType.CLUSTER_RECOVERY, properties.getId());
        var attempt = claims.acquire(key, owner, topology.revision(), properties.getWorkloadClaimTtl());
        recoveryClaim = attempt.filter(WorkloadClaimAttempt::acquired).map(WorkloadClaimAttempt::claim)
                .filter(claim -> claim.owner().equals(owner)).orElse(null);
        return recoveryClaim != null;
    }

    private void discover(String pipeline, ClusterExecutionProfile profile, Map<String, ClusterExecutionMember> live) {
        DesiredState desired = stores.desired().read(pipeline).orElse(null);
        var checkpoint = stores.state().read(pipeline).orElse(null);
        ArtifactIdentity artifact = stores.artifacts().identity(pipeline).orElse(null);
        WorkloadClaim execution = stores.workloadClaims().read(new WorkloadClaimKey(properties.getId(),
                WorkloadClaimType.PIPELINE_ACTUATION, pipeline)).map(WorkloadClaimReading::claim).orElse(null);
        if (desired == null || checkpoint == null || artifact == null || execution == null) { return; }
        if (pendingResume(pipeline).filter(receipt -> receipt.originalClaim().executionGeneration()
                == execution.executionGeneration()).isPresent()) { return; }
        var cause = PipelineRecoveryDiscovery.cause(desired, StateJson.parse(checkpoint.stateJson()), artifact, execution,
                profile, engine.hasLiveJob(pipeline), execution.originalMembersPresent(live).filter(present -> !present).isPresent());
        if (cause.isEmpty()) { return; }
        ClusterRecoveryEvent event = new ClusterRecoveryEvent(new ClusterRecoveryKey(properties.getId(), pipeline, artifact.incarnation()),
                cause.get(), execution.executionGeneration(), execution.executionRevision(), execution.executionTopologyRevision(),
                execution.executionProfile(), execution.executionProfile() == null, profile, membership.committed().revision(),
                DesiredStateFingerprint.of(desired), positions(pipeline));
        stores.clusterRecovery().enqueue(event, ClusterRecoveryFence.enqueue(event, WorkloadClaimFence.from(recoveryClaim)));
    }

    private void reconcile(ClusterRecoveryItem item, ClusterExecutionProfile profile) {
        ClusterRecoveryFence expected = coordinatorFence(item);
        ArtifactIdentity artifact = stores.artifacts().identity(item.event().key().pipelineId()).orElse(null);
        DesiredState desired = stores.desired().read(item.event().key().pipelineId()).orElse(null);
        if (artifact == null) { stores.clusterRecovery().removeDeleted(expected); return; }
        if (!artifact.incarnation().equals(item.event().key().incarnation()) || desired == null
                || !DesiredStateFingerprint.of(desired).equals(item.event().intentFingerprint())) {
            stores.clusterRecovery().cancel(expected); return;
        }
        PendingPipelineResume explicit = pendingResume(item.event().key().pipelineId()).orElse(null);
        if (explicit != null && item.executionAliases().contains(explicit.originalClaim().executionGeneration())) {
            stores.clusterRecovery().cancel(expected); return;
        }
        // A compatible addition does not replace an already successful native execution. Consume its
        // matching proof on the topology it actually used before retargeting unfinished work.
        if (item.permit() != null && item.targetProfile().equals(profile)) {
            if (consumeFailure(item, expected) || observeStartup(item, expected)) { return; }
        }
        if (!item.targetProfile().equals(profile) || item.targetTopologyRevision() != membership.committed().revision()) {
            var retargeted = stores.clusterRecovery().retarget(expected, profile, membership.committed().revision());
            if (retargeted.item() == null || retargeted.outcome() != ClusterRecoveryMutation.APPLIED) { return; }
            item = retargeted.item(); expected = coordinatorFence(item);
        }
        if (item.permit() != null) {
            if (consumeFailure(item, expected)) { return; }
            // Only the store can prove both expiration and retirement. Renewal first would move the
            // deadline every pass and keep an orphaned recovery slot occupied forever.
            var retired = stores.clusterRecovery().releaseExpiredPermit(expected);
            if (retired.outcome() == ClusterRecoveryMutation.APPLIED) { return; }
            if (retired.outcome() != ClusterRecoveryMutation.WAITING_PERMIT
                    && retired.outcome() != ClusterRecoveryMutation.SUCCESSOR_STILL_AUTHORIZED) { return; }
            var adopted = stores.clusterRecovery().resumePermit(expected, item.permit().reservationId(), properties.getWorkloadClaimTtl());
            if (adopted.item() == null || adopted.outcome() != ClusterRecoveryMutation.APPLIED) { return; }
            item = adopted.item(); expected = coordinatorFence(item);
            if (observeStartup(item, expected)) { return; }
            FailedExecution failure = failures.get(item.event().key().pipelineId());
            if (failure != null && failure.executionGeneration() == item.executionFrontier()) {
                boolean allocated = item.hasAllocatedSuccessor();
                WorkloadClaimFence pipelineClaim = allocated ? item.successor().pipelineClaim() : null;
                boolean currentFailure = allocated ? pipelineClaim.sameAuthorityAs(failure.authority())
                        : failure.diagnostic().reason() == ClusterRecoveryDiagnostic.Reason.REBUILD_REFUSED
                                && failure.belongsTo(actuation.currentClaim(item.event().key().pipelineId()).orElse(null));
                if (!currentFailure) { return; }
                stores.clusterRecovery().fail(expected, pipelineClaim, failure.diagnostic(),
                        failure.diagnostic().reason() == ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED
                                ? ClusterRecoveryStore.FailureStage.SOURCE_POSITION_REJECTION
                                : !allocated ? ClusterRecoveryStore.FailureStage.BEFORE_EXECUTION_ADVANCE
                                : ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, properties.getWorkloadClaimTtl());
            }
            return;
        }
        DagSource.PlannedStart plan;
        try { plan = dags.prepareStart(item.event().key().pipelineId(), defaultDatabase).plan(); }
        catch (TapstateException rejected) {
            var diagnostic = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.REBUILD_REFUSED, rejected,
                    item.event().resumePositions(), "Correct the reported prerequisite before retrying this pipeline.");
            stores.clusterRecovery().fail(expected, null, diagnostic, ClusterRecoveryStore.FailureStage.BEFORE_EXECUTION_ADVANCE,
                    properties.getWorkloadClaimTtl()); return;
        }
        DagSource.PlanningFacts facts = plan.planningFacts().orElse(null);
        Map<String, io.tapstate.core.lifecycle.ClusterCapacityDemand> demand = facts == null || !facts.unknownInputs().isEmpty()
                ? Map.of() : facts.perMemberUpperBounds();
        stores.clusterRecovery().acquirePermit(expected, demand, limits, properties.getWorkloadClaimTtl(),
                properties.getWorkloadClaimTtl(), ClusterRecoveryStore.DEFAULT_MAX_CONCURRENT_REBUILDS);
    }

    private boolean observeStartup(ClusterRecoveryItem item, ClusterRecoveryFence expected) {
        if (!item.hasAllocatedSuccessor() || item.successor().failureNote() != null) { return false; }
        String pipeline = item.event().key().pipelineId();
        var successor = item.successor();
        WorkloadClaimFence liveFence = stores.workloadClaims().read(successor.pipelineClaim().key())
                .filter(WorkloadClaimReading::leased).map(WorkloadClaimReading::claim).map(WorkloadClaimFence::from)
                .filter(successor.pipelineClaim()::sameAuthorityAs).orElse(null);
        if (liveFence == null) { return false; }
        var nativeRun = engine.nativeRun(pipeline).filter(run -> run.executionGeneration() == successor.executionGeneration()
                && run.claimGeneration() == successor.pipelineClaim().claimGeneration()
                && run.profileGeneration() == successor.profile().generation()).orElse(null);
        if (nativeRun == null || (nativeRun.status() != JobStatus.RUNNING && nativeRun.status() != JobStatus.COMPLETED)) { return false; }
        if (item.successor().submittedAt() == null) {
            var submitted = stores.clusterRecovery().recordSubmission(expected, liveFence, nativeRun.nativeJobId());
            if (submitted.outcome() != ClusterRecoveryMutation.APPLIED) { return false; }
            item = submitted.item(); expected = fence(item, expected.recoveryClaim());
        }
        if (!nativeRun.initialized()) { return false; }
        var fence = liveFence;
        Map<String, CaptureStartupProof> proof = captures.startupProofs(pipeline, fence);
        if (!item.successor().sourceRequirementsRecorded()
                || !proof.keySet().equals(item.successor().requiredSourceIds())
                || !proof.keySet().equals(captures.requiredSources(pipeline, fence))) { return false; }
        Map<String, CaptureResumeWitness> witnesses = new LinkedHashMap<>();
        Map<String, ClusterRecoveryPosition> requested = new LinkedHashMap<>();
        Map<String, ClusterRecoveryPosition> acceptedPositions = new LinkedHashMap<>();
        proof.forEach((source, value) -> {
            witnesses.put(source, value.witness());
            requested.put(source, value.requestedPosition());
            acceptedPositions.put(source, value.acceptedPosition());
        });
        Instant initialized = nativeRun.initialization().orElseThrow().processors().values().stream()
                .map(io.tapstate.core.lifecycle.ProcessorRuntimeContext::initializedAt).max(Instant::compareTo).orElseThrow();
        Instant accepted = proof.values().stream().map(CaptureStartupProof::acceptedAt).max(Instant::compareTo).orElse(initialized);
        var receipt = new ClusterRecoveryStartupReceipt(fence, nativeRun.nativeJobId(), initialized, witnesses,
                requested, acceptedPositions, accepted, nativeRun.status() == JobStatus.COMPLETED);
        var recorded = stores.clusterRecovery().recordStartup(expected, fence, receipt);
        if (recorded.outcome() != ClusterRecoveryMutation.APPLIED) { return false; }
        var completed = stores.clusterRecovery().complete(new ClusterRecoveryFence(recorded.item().event().key(),
                recorded.item().itemRevision(), recorded.item().event().intentFingerprint(), recorded.item().targetProfile(),
                recorded.item().executionFrontier(), expected.recoveryClaim()));
        if (completed.outcome() == ClusterRecoveryMutation.APPLIED && completed.item().status() == ClusterRecoveryStatus.RECOVERED) {
            forgetFailure(pipeline);
            return true;
        }
        return false;
    }

    private boolean consumeFailure(ClusterRecoveryItem item, ClusterRecoveryFence expected) {
        ClusterRecoveryFailureNote note = item.hasAllocatedSuccessor() ? item.successor().failureNote() : null;
        if (note == null) { return false; }
        var result = stores.clusterRecovery().fail(expected, note.pipelineClaim(), note.diagnostic(), note.stage(),
                properties.getWorkloadClaimTtl());
        return result.outcome() == ClusterRecoveryMutation.APPLIED || result.outcome() == ClusterRecoveryMutation.TERMINAL
                || result.outcome() == ClusterRecoveryMutation.SUCCESSOR_STILL_AUTHORIZED;
    }

    private FailedExecution failedExecution(String pipeline, WorkloadClaim claim, Throwable failure) {
        TapstateException coded = PipelineFailures.codedCause(failure);
        CaptureStartupFailure source = coded instanceof CaptureStartupException remote ? remote.failure() : null;
        if (source == null && coded != null && claim != null) {
            source = captures.startupFailures(pipeline, WorkloadClaimFence.from(claim)).values().stream()
                    .filter(fact -> fact.code().equals(coded.code().code()) && fact.params().equals(coded.args()))
                    .findFirst().orElse(null);
        }
        String code;
        Map<String, Object> params;
        Map<String, ClusterRecoveryPosition> positions = new LinkedHashMap<>(diagnosticPositions(pipeline));
        String disposition = "Inspect the original coded failure and retained source position before retrying this pipeline.";
        if (source != null) {
            code = source.code(); params = source.params(); disposition = source.disposition();
            String sourceId = source.witness().sourceId();
            if (source.requestedPosition() == null) { positions.remove(sourceId); }
            else { positions.put(sourceId, source.requestedPosition()); }
        } else if (coded != null) {
            code = coded.code().code(); params = coded.args();
        } else {
            var observed = PipelineFailures.of(pipeline, failure);
            code = observed.code(); params = new LinkedHashMap<>(observed.params());
        }
        var diagnostic = new ClusterRecoveryDiagnostic(sourcePositionRejected(code)
                ? ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED : ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                code, params, positions, disposition);
        return new FailedExecution(claim == null ? 0 : claim.executionGeneration(),
                claim == null ? null : WorkloadClaimFence.from(claim), diagnostic, source, failure);
    }

    /** Report under PIPE authority before its retirement; a recovery coordinator is not required. */
    private boolean persistFailure(String pipeline, WorkloadClaim claim) {
        WorkloadClaimFence authority = WorkloadClaimFence.from(claim);
        ClusterRecoveryItem current = item(pipeline).orElse(null);
        for (int retry = 0; retry < 2; retry++) {
            if (current == null || current.status().terminal() || !current.hasAllocatedSuccessor()
                    || !current.successor().pipelineClaim().sameAuthorityAs(authority)) { return true; }
            if (authority.sameAuthorityAs(notedFailures.get(pipeline)) || current.successor().failureNote() != null) { return true; }
            FailedExecution failure = failures.get(pipeline);
            if (failure == null || !failure.belongsTo(claim)) { return false; }
            var expected = new ClusterRecoveryPipelineFence(current.event().key(), current.itemRevision(),
                    current.event().intentFingerprint(), current.targetProfile(), authority);
            var result = stores.clusterRecovery().recordFailureNote(expected, failure.diagnostic(),
                    failure.diagnostic().reason() == ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED
                            ? ClusterRecoveryStore.FailureStage.SOURCE_POSITION_REJECTION
                            : ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, failure.sourceFailure());
            if (result.outcome() == ClusterRecoveryMutation.APPLIED || result.outcome() == ClusterRecoveryMutation.DUPLICATE) {
                notedFailures.put(pipeline, authority);
                return true;
            }
            if (result.outcome() != ClusterRecoveryMutation.STALE_ITEM) { return false; }
            current = result.item();
        }
        return false;
    }

    private void forgetFailure(String pipeline) {
        failures.remove(pipeline); notedFailures.remove(pipeline); failedStops.remove(pipeline);
    }

    private static boolean verdictRecorded(WorkloadClaim claim) {
        return claim.executionGeneration() > 0 && claim.contextExecutionGeneration() == claim.executionGeneration()
                && claim.failureClaimGeneration() == claim.executionClaimGeneration() && claim.failureClaimGeneration() > 0;
    }

    private static boolean sameAuthority(WorkloadClaim expected, WorkloadClaim actual) {
        return expected != null && actual != null && WorkloadClaimFence.from(expected).sameAuthorityAs(WorkloadClaimFence.from(actual));
    }

    private static boolean ordinaryFailure(TapstateException coded) {
        if (coded == null) { return false; }
        if (coded instanceof CaptureStartupException) { return true; }
        if (coded.code() instanceof EngineError error) {
            return switch (error) {
                case FRONTIER_ORDER_NOT_ENCODABLE, VIEW_KEY_MISSING_FROM_BEFORE_IMAGE, ROUTING_KEY_MISSING,
                        KEY_CHANGE_ON_PARALLEL_NODE -> true;
                default -> false;
            };
        }
        if (coded.code() instanceof CaptureError error) {
            return switch (error) {
                case CLAIM_LOST, NO_RING_TO_ATTACH, CLUSTER_REFUSED_WRITES, CLUSTER_REFUSED_THE_READ -> false;
                default -> true;
            };
        }
        return coded.code() != IoError.WORKLOAD_CLAIM_FENCED && coded.code() != IoError.STORE_UNAVAILABLE
                && coded.code() != IoError.STORE_UNAUTHORIZED;
    }

    private static boolean sourcePositionRejected(String code) {
        return code.equals(CaptureError.START_FROM_OUTSIDE_WINDOW.code())
                || code.equals(io.tapstate.adapters.pdk.ConnectorError.RESUME_POSITION_REJECTED.code());
    }

    private Optional<ClusterRecoveryItem> item(String pipeline) {
        return stores.artifacts().identity(pipeline).flatMap(identity -> stores.clusterRecovery().read(
                new ClusterRecoveryKey(properties.getId(), pipeline, identity.incarnation())));
    }

    private Map<String, ClusterRecoveryPosition> positions(String pipeline) {
        return captures.resumePositions(pipeline, stores.artifacts());
    }

    private Map<String, ClusterRecoveryPosition> diagnosticPositions(String pipeline) {
        WorkloadClaim claim = actuation.currentClaim(pipeline).orElse(null);
        if (claim != null) {
            Map<String, ClusterRecoveryPosition> requested = new LinkedHashMap<>();
            stores.meta().captureResumePreparations(WorkloadClaimFence.from(claim)).forEach(prepared -> {
                if (prepared.requestedPosition() != null) {
                    requested.put(prepared.witness().sourceId(), prepared.requestedPosition());
                }
            });
            if (!requested.isEmpty()) { return Map.copyOf(requested); }
        }
        return item(pipeline).map(value -> value.event().resumePositions()).orElse(Map.of());
    }

    private Map<String, ClusterExecutionMember> liveMembers() {
        Map<String, ClusterExecutionMember> live = new LinkedHashMap<>();
        for (var peer : member.getCluster().getMembers()) {
            if (!peer.isLiteMember()) {
                String node = peer.getAttribute("tapstate.node-id");
                String boot = peer.getAttribute("tapstate.boot-id");
                if (node != null && boot != null) { live.put(node, new ClusterExecutionMember(node, boot, peer.getUuid().toString())); }
            }
        }
        return Map.copyOf(live);
    }

    private ClusterRecoveryFence coordinatorFence(ClusterRecoveryItem item) {
        return fence(item, WorkloadClaimFence.from(recoveryClaim));
    }

    private static ClusterRecoveryFence fence(ClusterRecoveryItem item) {
        return fence(item, item.permit().recoveryClaim());
    }

    private static ClusterRecoveryFence fence(ClusterRecoveryItem item, WorkloadClaimFence recoveryClaim) {
        return new ClusterRecoveryFence(item.event().key(), item.itemRevision(), item.event().intentFingerprint(),
                item.targetProfile(), item.executionFrontier(), recoveryClaim);
    }

    private static TapstateException unproven(String pipeline, String reason) {
        return new TapstateException(LifecycleError.CLUSTER_CAPACITY_UNPROVEN,
                Map.of("pipeline", pipeline, "reason", reason), null);
    }

    private static boolean containsCode(Throwable failure, io.tapstate.core.common.TapstateErrorCode code) {
        Throwable cause = failure;
        for (int depth = 0; cause != null && depth < 32; depth++) {
            if (cause instanceof TapstateException coded && coded.code() == code) { return true; }
            if (cause.getMessage() != null && cause.getMessage().contains(code.code())) { return true; }
            cause = cause.getCause() == cause ? null : cause.getCause();
        }
        return false;
    }
}
