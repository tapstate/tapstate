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
            ClusterRecoveryKey key, ClusterCapacityReservation capacity, boolean recovery) { }
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
        Allocation local = allocations.get(pipelineId);
        // A start waiting for a shared ring resumes its already allocated, still-owned ordinary step.
        if (local != null && !local.recovery() && local.capacity().executionGeneration() != null
                && local.capacity().nativeJobId() == null
                && local.capacity().executionGeneration() == current.executionGeneration()
                && local.capacity().pipelineClaim().equals(WorkloadClaimFence.from(current))) {
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
        if (item != null && !item.status().terminal()
                && item.event().intentFingerprint().equals(DesiredStateFingerprint.of(intent))) {
            if (item.permit() == null || !item.permit().demandByNode().equals(facts.perMemberUpperBounds())
                    || !item.targetProfile().equals(profile)) { return false; }
            allocations.put(pipelineId, new Allocation(facts, profile, key, null, true));
            return true;
        }
        ClusterCapacityStore.Result reserved = stores.clusterCapacity().reserve(current, profile, artifact.incarnation(),
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
        allocations.put(pipelineId, new Allocation(facts, profile, key, reserved.reservation(), false));
        return true;
    }

    @Override public PipelineActuationOwnership.Execution begin(String pipelineId, DagSource.PlannedStart planned,
            PipelineActuationOwnership ownership) {
        if (!active()) { return PipelineExecutionAdmission.super.begin(pipelineId, planned, ownership); }
        Allocation allocation = allocations.get(pipelineId);
        if (allocation == null) { return PipelineActuationOwnership.Execution.refused(); }
        return ownership.beginExecution(pipelineId, (expected, topology, nodes) -> {
            if (!nodes.equals(allocation.facts().perMemberUpperBounds().keySet())) { return Optional.empty(); }
            if (allocation.recovery()) {
                ClusterRecoveryItem item = stores.clusterRecovery().read(allocation.key()).orElse(null);
                if (item == null || item.permit() == null) { return Optional.empty(); }
                if (item.hasAllocatedSuccessor() && item.successor().pipelineClaim().sameAuthorityAs(WorkloadClaimFence.from(expected))
                        && item.successor().submittedAt() == null) { return Optional.of(expected); }
                var advanced = stores.clusterRecovery().advanceExecution(fence(item), expected, nodes,
                        allocation.facts().selectedSourceIds()).advancedPipelineClaim();
                if (advanced != null) { forgetFailure(pipelineId); }
                return Optional.ofNullable(advanced);
            }
            ClusterCapacityReservation reservation = allocation.capacity();
            if (reservation.executionGeneration() != null) {
                return reservation.executionGeneration() == expected.executionGeneration()
                        && reservation.pipelineClaim().equals(WorkloadClaimFence.from(expected))
                        && reservation.nativeJobId() == null ? Optional.of(expected) : Optional.empty();
            }
            ClusterCapacityStore.Result advanced = stores.clusterCapacity().advanceExecution(reservation, expected, nodes);
            if (advanced.reservation() != null) {
                allocations.put(pipelineId, new Allocation(allocation.facts(), allocation.profile(), allocation.key(),
                        advanced.reservation(), false));
            }
            if (advanced.advancedPipelineClaim() != null) { forgetFailure(pipelineId); }
            return Optional.ofNullable(advanced.advancedPipelineClaim());
        });
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
                        submitted.reservation(), false));
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
