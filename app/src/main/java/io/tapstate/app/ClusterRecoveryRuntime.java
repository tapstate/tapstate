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
import io.tapstate.runtime.scheduler.RebuildAdmission;
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
    private WorkloadClaim recoveryClaim;

    private record Allocation(DagSource.PlanningFacts facts, ClusterExecutionProfile profile,
            ClusterRecoveryKey key, ClusterCapacityReservation capacity, boolean recovery) { }
    private record FailedExecution(long executionGeneration, ClusterRecoveryDiagnostic diagnostic) { }

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
                if (item.successor() != null && item.successor().pipelineClaim().equals(WorkloadClaimFence.from(expected))
                        && item.successor().submittedAt() == null) { return Optional.of(expected); }
                var advanced = stores.clusterRecovery().advanceExecution(fence(item), expected, nodes,
                        allocation.facts().selectedSourceIds()).advancedPipelineClaim();
                if (advanced != null) { failures.remove(pipelineId); }
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
            if (advanced.advancedPipelineClaim() != null) { failures.remove(pipelineId); }
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
        long failedGeneration = failedClaim == null ? 0 : failedClaim.executionGeneration();
        Throwable failure = PipelineFailures.current(pipelineId, failedClaim, true, engine, captures).orElse(null);
        if (failure != null) {
            String code = PipelineFailures.of(pipelineId, failure).code();
            if (code.startsWith("connector.") || code.equals(io.tapstate.runtime.srs.CaptureError.START_FROM_OUTSIDE_WINDOW.code())
                    || containsCode(failure, io.tapstate.adapters.pdk.ConnectorError.WRITE_FAILED)
                    || containsCode(failure, io.tapstate.adapters.pdk.ConnectorError.CAPTURE_FAILED)
                    || containsCode(failure, io.tapstate.adapters.pdk.ConnectorError.READ_FAILED)) {
                actuation.recordClassifiedFailure(pipelineId, false);
            } else if (ClusterRebuildAdmission.isMembershipChangedBeforeStart(failure)) {
                actuation.recordClassifiedFailure(pipelineId, true);
            } else if (failedClaim != null && failedClaim.originalMembersPresent(liveMembers())
                    .filter(present -> !present).isPresent()) {
                actuation.recordClassifiedFailure(pipelineId, true);
            }
        }
        actuation.recordFailure(pipelineId, ClusterRebuildAdmission.MAX_ATTEMPTS * properties.getWorkloadClaimTtl().toNanos(),
                detectionWindow.toNanos());
        if (failure != null) {
            var coded = PipelineFailures.of(pipelineId, failure);
            failures.put(pipelineId, new FailedExecution(failedGeneration,
                    new ClusterRecoveryDiagnostic(coded.code().equals(io.tapstate.runtime.srs.CaptureError.START_FROM_OUTSIDE_WINDOW.code())
                            ? ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED : ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                    coded.code(), new LinkedHashMap<>(coded.params()), diagnosticPositions(pipelineId),
                    "Inspect the original coded failure and retained source position before retrying this pipeline.")));
        }
    }

    @Override public void refused(String pipelineId, TapstateException failure) {
        if (!active()) { return; }
        failures.put(pipelineId, new FailedExecution(actuation.heldExecutionGeneration(pipelineId),
                ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.REBUILD_REFUSED,
                        failure, diagnosticPositions(pipelineId), "Correct the reported configuration or capacity; the source position has not been reset.")));
    }

    @Override public void stopped(String pipelineId, WorkloadClaim stoppedClaim, boolean jobOver) {
        if (!active() || !jobOver || stoppedClaim == null) { return; }
        actuation.retireStoppedExecution(stoppedClaim);
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
        if (item.permit() != null && item.targetProfile().equals(profile) && observeStartup(item, expected)) { return; }
        if (!item.targetProfile().equals(profile) || item.targetTopologyRevision() != membership.committed().revision()) {
            var retargeted = stores.clusterRecovery().retarget(expected, profile, membership.committed().revision());
            if (retargeted.item() == null || retargeted.outcome() != ClusterRecoveryMutation.APPLIED) { return; }
            item = retargeted.item(); expected = coordinatorFence(item);
        }
        if (item.permit() != null) {
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
                WorkloadClaimFence pipelineClaim = item.successor() == null ? null : item.successor().pipelineClaim();
                stores.clusterRecovery().fail(expected, pipelineClaim, failure.diagnostic(),
                        failure.diagnostic().reason() == ClusterRecoveryDiagnostic.Reason.SOURCE_POSITION_REJECTED
                                ? ClusterRecoveryStore.FailureStage.SOURCE_POSITION_REJECTION
                                : item.successor() == null ? ClusterRecoveryStore.FailureStage.BEFORE_EXECUTION_ADVANCE
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
        if (item.successor() == null) { return false; }
        String pipeline = item.event().key().pipelineId();
        var successor = item.successor();
        var nativeRun = engine.nativeRun(pipeline).filter(run -> run.executionGeneration() == successor.executionGeneration()
                && run.claimGeneration() == successor.pipelineClaim().claimGeneration()
                && run.profileGeneration() == successor.profile().generation()).orElse(null);
        if (nativeRun == null || (nativeRun.status() != JobStatus.RUNNING && nativeRun.status() != JobStatus.COMPLETED)) { return false; }
        if (item.successor().submittedAt() == null) {
            var submitted = stores.clusterRecovery().recordSubmission(expected, item.successor().pipelineClaim(), nativeRun.nativeJobId());
            if (submitted.outcome() != ClusterRecoveryMutation.APPLIED) { return false; }
            item = submitted.item(); expected = fence(item, expected.recoveryClaim());
        }
        if (!nativeRun.initialized()) { return false; }
        var fence = item.successor().pipelineClaim();
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
            failures.remove(pipeline);
            return true;
        }
        return false;
    }

    private Optional<ClusterRecoveryItem> item(String pipeline) {
        return stores.artifacts().identity(pipeline).flatMap(identity -> stores.clusterRecovery().read(
                new ClusterRecoveryKey(properties.getId(), pipeline, identity.incarnation())));
    }

    private Map<String, ClusterRecoveryPosition> positions(String pipeline) {
        Map<String, ClusterRecoveryPosition> positions = new LinkedHashMap<>();
        captures.resumeWitnesses(pipeline, stores.artifacts()).forEach((source, witness) ->
                witness.requestedPosition(witness.miningChainId()).ifPresent(position -> positions.put(source, position)));
        return Map.copyOf(positions);
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
