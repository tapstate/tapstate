package io.tapstate.app;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ClusterCapacityDemand;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.ProcessorRuntimeContext;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.runtime.engine.ExecutionShape;
import io.tapstate.runtime.engine.NativeExecutionStartup;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ClusterRecoveryRuntimeTest {
    private static final Set<String> NODES = Set.of("a", "b", "c");
    private static final Set<String> EXPANDED_NODES = Set.of("a", "b", "c", "d");
    private static final Map<String, ClusterExecutionMember> ORIGINAL_MEMBERS = Map.of(
            "a", new ClusterExecutionMember("a", "boot-a", "00000000-0000-4000-8000-000000000001"),
            "b", new ClusterExecutionMember("b", "boot-b", "00000000-0000-4000-8000-000000000002"),
            "c", new ClusterExecutionMember("c", "boot-c", "00000000-0000-4000-8000-000000000003"));
    private static final Map<String, ClusterExecutionMember> REJOINED_MEMBERS = Map.of(
            "a", ORIGINAL_MEMBERS.get("a"),
            "b", new ClusterExecutionMember("b", "boot-b-next", "00000000-0000-4000-8000-000000000004"),
            "c", ORIGINAL_MEMBERS.get("c"));
    private static final Instant NOW = Instant.parse("2026-10-10T06:00:00Z");
    private final StorePort store = mock(StorePort.class);
    private final ClusterCapacityStore capacity = mock(ClusterCapacityStore.class);
    private final PipelineActuationOwnership ownership = mock(PipelineActuationOwnership.class);
    private final ClusterMembershipGate gate = mock(ClusterMembershipGate.class);
    private final ClusterWorkloadClaims workloads = mock(ClusterWorkloadClaims.class);
    private final PipelineCaptureCoordinator captures = mock(PipelineCaptureCoordinator.class);
    private final Engine engine = mock(Engine.class);
    private final WorkloadOwner owner = new WorkloadOwner("a", "boot-a");
    private final ClusterExecutionProfile profile = new ClusterExecutionProfile("cluster", 2,
            new ExecutionProfile(1, Map.of("runtime", "test")));
    private final ClusterCapacityDemand demand = new ClusterCapacityDemand(1, 0, 1, 2, 4, 5);
    private final WorkloadClaim initial = claim(0);
    private Cluster cluster;
    private ClusterRecoveryRuntime runtime;

    @BeforeEach void setup() {
        var member = mock(HazelcastInstance.class);
        cluster = mock(Cluster.class);
        when(member.getCluster()).thenReturn(cluster);
        useLiveMembers(ORIGINAL_MEMBERS);
        var context = new java.util.concurrent.ConcurrentHashMap<String, Object>();
        context.put(HazelcastConfiguration.NODE_SESSION_CONTEXT_KEY, new WorkloadClaim(
                new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "a"), owner, 1, 0, 0,
                NOW.plusSeconds(60), 0, 0, Set.of(), 0, false, 2));
        when(member.getUserContext()).thenReturn(context);
        when(gate.businessEligible()).thenReturn(true);
        when(gate.submissionEligible(anyCollection())).thenReturn(true);
        when(gate.visibleNodeIds()).thenReturn(NODES);
        when(ownership.mayStart("p")).thenReturn(true);
        when(ownership.currentClaim("p")).thenReturn(Optional.of(initial));
        var profiles = mock(ClusterProfileStore.class);
        var artifacts = mock(ArtifactStore.class);
        var desired = mock(DesiredStore.class);
        var recovery = mock(ClusterRecoveryStore.class);
        when(store.clusterCapacity()).thenReturn(capacity);
        when(store.clusterProfiles()).thenReturn(profiles);
        when(store.artifacts()).thenReturn(artifacts);
        when(store.desired()).thenReturn(desired);
        when(store.clusterRecovery()).thenReturn(recovery);
        when(store.meta()).thenReturn(mock(SrsMetaStore.class));
        when(store.state()).thenReturn(mock(StateStore.class));
        when(capacity.recordExecutionSources(any(), any(), anySet())).thenAnswer(call ->
                new ClusterCapacityStore.Result(ClusterCapacityStore.Outcome.APPLIED, call.getArgument(0), null, List.of()));
        when(profiles.profile("cluster")).thenReturn(Optional.of(profile));
        when(artifacts.identity("p")).thenReturn(Optional.of(new ArtifactIdentity("p", "inc", "a".repeat(64))));
        when(desired.read("p")).thenReturn(Optional.of(new DesiredState("p", PipelineState.RUNNING, "a".repeat(64))));
        when(recovery.read(any())).thenReturn(Optional.empty());
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        properties.setId("cluster");
        runtime = new ClusterRecoveryRuntime(store, mock(DagSource.class), captures,
                engine, ownership, workloads, gate, member, properties,
                new ClusterCapacityProperties().limits(), "default", Duration.ofSeconds(30));
        when(ownership.beginExecution(eq("p"), any())).thenAnswer(call -> {
            var issuer = call.<PipelineActuationOwnership.ExecutionAdvance>getArgument(1);
            return issuer.advance(initial, 7, NODES).map(advanced -> {
                when(ownership.currentClaim("p")).thenReturn(Optional.of(advanced));
                return new PipelineActuationOwnership.Execution(true,
                        new ExecutionFence("p", advanced.claimGeneration(), advanced.executionGeneration(), advanced.profileGeneration()),
                        advanced.topologyRevision());
            }).orElseGet(PipelineActuationOwnership.Execution::refused);
        });
    }

    @Test void anAcceptedStampedStartWaitsForItsActualPriorAuthorityRetirement() {
        SubmittedStart start = stampedStart(false, false);
        assertThat(runtime.admitsMissingJob("p")).isFalse();
        assertThat(runtime.prepare("p", plan(List.of()), ownership)).isFalse();
        verify(capacity, never()).reserve(any(), any(), anyString(), anyString(), anyMap(), any(), any());

        when(capacity.submittedExecution(start.current())).thenReturn(Optional.of(
                new ClusterCapacityStore.SubmittedExecution(start.previous(), true)));
        assertThat(runtime.admitsMissingJob("p")).isTrue();
        verify(capacity, never()).advanceExecution(any(), any(), anySet());
    }

    @Test void theSameSubmittedStartCannotBypassAutomaticRecoveryForAnotherMissingJob() {
        stampedStart(true, true);
        assertThat(runtime.admitsMissingJob("p")).isFalse();
        assertThat(runtime.prepare("p", plan(List.of()), ownership)).isFalse();
        verify(capacity, never()).reserve(any(), any(), anyString(), anyString(), anyMap(), any(), any());
        verify(capacity, never()).advanceExecution(any(), any(), anySet());
    }

    @Test void aSupersedingStateEpochCannotContinueTheOldStampedStart() {
        stampedStart(true, false);
        when(store.state().read("p")).thenReturn(Optional.of(new io.tapstate.core.lifecycle.CheckpointDoc("p", "RUNNING", 3, NOW)));
        assertThat(runtime.admitsMissingJob("p")).isFalse();
        verify(capacity, never()).submittedExecution(any());
        verify(capacity, never()).reserve(any(), any(), anyString(), anyString(), anyMap(), any(), any());
    }

    @Test void anUnknownSubmittedHistoryRefusesTheAcceptedStartBeforeAnotherAllocation() {
        stampedStart(true, false);
        when(capacity.submittedExecution(any())).thenReturn(Optional.empty());
        assertThatThrownBy(() -> runtime.admitsMissingJob("p")).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(LifecycleError.CLUSTER_CAPACITY_UNPROVEN));
        verify(capacity, never()).reserve(any(), any(), anyString(), anyString(), anyMap(), any(), any());
        verify(capacity, never()).advanceExecution(any(), any(), anySet());
    }

    @Test void aStampedStartCannotBorrowAnotherArtifactIncarnationsSubmission() {
        stampedStart(true, false);
        when(store.artifacts().identity("p")).thenReturn(Optional.of(new ArtifactIdentity("p", "inc-next", "a".repeat(64))));
        assertThatThrownBy(() -> runtime.admitsMissingJob("p")).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(LifecycleError.CLUSTER_CAPACITY_UNPROVEN));
        verify(capacity, never()).reserve(any(), any(), anyString(), anyString(), anyMap(), any(), any());
    }

    private SubmittedStart stampedStart(boolean retired, boolean sameIntent) {
        WorkloadClaim current = allocatedClaim(1, ORIGINAL_MEMBERS);
        DesiredState intent = new DesiredState("p", PipelineState.RUNNING, "a".repeat(64), false, null, true, 0L);
        when(store.desired().read("p")).thenReturn(Optional.of(intent));
        when(store.state().read("p")).thenReturn(Optional.of(new io.tapstate.core.lifecycle.CheckpointDoc("p", "RUNNING", 1, NOW)));
        when(ownership.currentClaim("p")).thenReturn(Optional.of(current));
        String fingerprint = io.tapstate.core.lifecycle.DesiredStateFingerprint.of(sameIntent ? intent
                : new DesiredState("p", PipelineState.RUNNING, "a".repeat(64)));
        ClusterCapacityReservation previous = new ClusterCapacityReservation("previous", "cluster", "p", "inc", fingerprint,
                profile, WorkloadClaimFence.from(current), NODES.stream().collect(java.util.stream.Collectors.toMap(
                        node -> node, node -> demand)), NOW.minusSeconds(60), NOW.minusSeconds(1), 1L, "prior-native-job");
        when(capacity.submittedExecution(current)).thenReturn(Optional.of(new ClusterCapacityStore.SubmittedExecution(previous, retired)));
        return new SubmittedStart(current, previous);
    }

    private record SubmittedStart(WorkloadClaim current, ClusterCapacityReservation previous) {}

    @Test void unknownOwnedResourceDemandNeverReservesOrAllocates() {
        DagSource.PlannedStart planned = plan(List.of("script output has no proven bound"));
        assertThatThrownBy(() -> runtime.prepare("p", planned, ownership)).isInstanceOfSatisfying(TapstateException.class,
                failure -> assertThat(failure.code()).isEqualTo(LifecycleError.CLUSTER_CAPACITY_UNPROVEN));
        verifyNoInteractions(capacity);
        verify(ownership, never()).beginExecution(anyString(), any());
    }

    @Test void anAtomicCapacityRefusalCarriesItsActualNodeWithoutSpendingAGeneration() {
        when(capacity.reserve(any(), any(), anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(new ClusterCapacityStore.Result(ClusterCapacityStore.Outcome.CAPACITY_REFUSED, null, null,
                        List.of(new io.tapstate.core.lifecycle.ClusterCapacityLimits.Violation("writers", 2, 1, 2)), "b"));
        assertThatThrownBy(() -> runtime.prepare("p", plan(List.of()), ownership))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(LifecycleError.CLUSTER_CAPACITY_REFUSED);
                    assertThat(failure.args()).containsEntry("node", "b").containsEntry("occupied", 2L);
                });
        verify(capacity, never()).advanceExecution(any(), any(), anySet());
        verify(ownership, never()).beginExecution(anyString(), any());
    }

    @Test void ordinarySubmissionUsesTheCapacityTransactionsExistingIssuer() {
        ClusterCapacityReservation reservation = reservation(null, initial);
        WorkloadClaim advanced = claim(1);
        ClusterCapacityReservation allocated = reservation(1L, advanced);
        when(capacity.reserve(any(), any(), anyString(), anyString(), anyMap(), any(), any()))
                .thenReturn(new ClusterCapacityStore.Result(ClusterCapacityStore.Outcome.APPLIED, reservation, null, List.of()));
        when(capacity.advanceExecution(reservation, initial, NODES))
                .thenReturn(new ClusterCapacityStore.Result(ClusterCapacityStore.Outcome.APPLIED, allocated, advanced, List.of()));
        DagSource.PlannedStart planned = plan(List.of());
        assertThat(runtime.prepare("p", planned, ownership)).isTrue();
        assertThat(runtime.begin("p", planned, ownership).fence().executionGeneration()).isEqualTo(1);
        verify(capacity).advanceExecution(reservation, initial, NODES);
    }

    @Test void aPendingOrdinaryAllocationContinuesAfterOnlyAcquisitionTopologyRefresh() {
        PendingStart pending = ordinaryPendingStart();
        pending.held().set(atTopology(pending.held().get(), 8));
        when(gate.committed()).thenReturn(new ClusterMembership("cluster", 8, NODES, 2));

        assertThat(runtime.admitsMissingJob("p")).isTrue();
        assertThat(runtime.prepare("p", pending.planned(), ownership)).isTrue();
        PipelineActuationOwnership.Execution continued = runtime.begin("p", pending.planned(), ownership);
        assertThat(continued.allowed()).isTrue();
        assertThat(continued.fence().executionGeneration()).isEqualTo(1);
        verify(capacity, times(1)).advanceExecution(any(), any(), anySet());
        assertNoNativeSubmission();
    }

    @Test void aPendingOrdinaryAllocationRefusesAChangedCohortBeforeOpeningOrAllocatingAgain() {
        PendingStart pending = ordinaryPendingStart();
        pending.held().set(atTopology(pending.held().get(), 8));
        when(gate.committed()).thenReturn(new ClusterMembership("cluster", 8, EXPANDED_NODES, 2));
        when(gate.visibleNodeIds()).thenReturn(EXPANDED_NODES);

        assertThatThrownBy(() -> runtime.prepare("p", plan(List.of(), EXPANDED_NODES), ownership))
                .isInstanceOfSatisfying(TapstateException.class, this::assertChangedCohort);

        assertThat(pending.held().get().executionGeneration()).isEqualTo(1);
        verify(capacity, times(1)).advanceExecution(any(), any(), anySet());
        assertNoNativeSubmission();
    }

    @Test void aPendingRecoveryAllocationContinuesAfterOnlyAcquisitionTopologyRefresh() {
        PendingStart pending = recoveryPendingStart();
        pending.held().set(atTopology(pending.held().get(), 8));
        when(gate.committed()).thenReturn(new ClusterMembership("cluster", 8, NODES, 2));

        assertThat(runtime.prepare("p", pending.planned(), ownership)).isTrue();
        PipelineActuationOwnership.Execution continued = runtime.begin("p", pending.planned(), ownership);
        assertThat(continued.allowed()).isTrue();
        assertThat(continued.fence().executionGeneration()).isEqualTo(9);
        verify(store.clusterRecovery(), times(1)).advanceExecution(any(), any(), anySet(), anySet());
        assertNoNativeSubmission();
    }

    @Test void aPendingRecoveryAllocationRefusesAChangedCohortInsteadOfWaitingBehindItsOldPermit() {
        PendingStart pending = recoveryPendingStart();
        pending.held().set(atTopology(pending.held().get(), 8));
        when(gate.committed()).thenReturn(new ClusterMembership("cluster", 8, EXPANDED_NODES, 2));
        when(gate.visibleNodeIds()).thenReturn(EXPANDED_NODES);

        assertThatThrownBy(() -> runtime.prepare("p", plan(List.of(), EXPANDED_NODES), ownership))
                .isInstanceOfSatisfying(TapstateException.class, this::assertChangedCohort);

        assertThat(pending.held().get().executionGeneration()).isEqualTo(9);
        verify(store.clusterRecovery(), times(1)).advanceExecution(any(), any(), anySet(), anySet());
        assertNoNativeSubmission();
    }

    @Test void aPendingOrdinaryAllocationRefusesAReincarnatedMemberWithTheSameStableId() {
        useLiveMembers(ORIGINAL_MEMBERS);
        when(gate.committed()).thenReturn(new ClusterMembership("cluster", 7, NODES, 2));
        PendingStart pending = ordinaryPendingStart(allocatedClaim(1, ORIGINAL_MEMBERS));
        assertThat(pending.held().get().originalMembersPresent(ORIGINAL_MEMBERS)).contains(true);

        useLiveMembers(REJOINED_MEMBERS);
        assertThat(pending.held().get().originalMembersPresent(REJOINED_MEMBERS)).contains(false);
        assertThat(runtime.admitsMissingJob("p")).isTrue();
        assertThatThrownBy(() -> {
            if (runtime.prepare("p", pending.planned(), ownership)) {
                runtime.begin("p", pending.planned(), ownership);
            }
        }).isInstanceOfSatisfying(TapstateException.class, this::assertReincarnatedCohort);

        assertThat(pending.held().get().executionGeneration()).isEqualTo(1);
        assertThat(pending.held().get().executionMembers()).isEqualTo(ORIGINAL_MEMBERS);
        verify(capacity, times(1)).advanceExecution(any(), any(), anySet());
        verify(ownership, times(1)).beginExecution(eq("p"), any());
        assertNoNativeSubmission();
    }

    @Test void aPendingRecoveryAllocationRefusesAReincarnatedMemberWithTheSameStableId() {
        useLiveMembers(ORIGINAL_MEMBERS);
        when(gate.committed()).thenReturn(new ClusterMembership("cluster", 7, NODES, 2));
        PendingStart pending = recoveryPendingStart(allocatedClaim(8, ORIGINAL_MEMBERS), allocatedClaim(9, ORIGINAL_MEMBERS));
        assertThat(pending.held().get().originalMembersPresent(ORIGINAL_MEMBERS)).contains(true);

        useLiveMembers(REJOINED_MEMBERS);
        assertThat(pending.held().get().originalMembersPresent(REJOINED_MEMBERS)).contains(false);
        assertThatThrownBy(() -> {
            if (runtime.prepare("p", pending.planned(), ownership)) {
                runtime.begin("p", pending.planned(), ownership);
            }
        }).isInstanceOfSatisfying(TapstateException.class, this::assertReincarnatedCohort);

        assertThat(pending.held().get().executionGeneration()).isEqualTo(9);
        assertThat(pending.held().get().executionMembers()).isEqualTo(ORIGINAL_MEMBERS);
        verify(store.clusterRecovery(), times(1)).advanceExecution(any(), any(), anySet(), anySet());
        verify(ownership, times(1)).beginExecution(eq("p"), any());
        assertNoNativeSubmission();
    }

    @Test void aPendingOrdinaryAllocationRefusesAnUnknownOriginalCohort() {
        PendingStart pending = ordinaryPendingStart(allocatedClaim(1, Map.of()));
        WorkloadClaim allocated = pending.held().get();
        assertThat(allocated.contextExecutionGeneration()).isEqualTo(allocated.executionGeneration());
        assertThat(allocated.executionProfile()).isEqualTo(profile);
        assertThat(allocated.executionMembers()).isEmpty();
        assertThat(allocated.originalMembersPresent(ORIGINAL_MEMBERS)).isEmpty();

        assertThatThrownBy(() -> runtime.prepare("p", pending.planned(), ownership))
                .isInstanceOfSatisfying(TapstateException.class, failure -> {
                    assertThat(failure.code()).isEqualTo(LifecycleError.CLUSTER_CAPACITY_UNPROVEN);
                    assertThat(failure.args()).containsEntry("pipeline", "p");
                });

        assertThat(pending.held().get().executionGeneration()).isEqualTo(1);
        verify(capacity, times(1)).reserve(any(), any(), anyString(), anyString(), anyMap(), any(), any());
        verify(capacity, times(1)).advanceExecution(any(), any(), anySet());
        verify(ownership, times(1)).beginExecution(eq("p"), any());
        assertNoNativeSubmission();
    }

    @Test void aPendingAllocatedCohortRefusalUsesItsAllocatedFailureStageWithoutANativeJob() {
        PendingStart pending = recoveryPendingStart();
        pending.held().set(atTopology(pending.held().get(), 8));
        WorkloadClaim current = pending.held().get();
        doAnswer(call -> { pending.held().set(failed(pending.held().get(), true)); return null; })
                .when(ownership).recordClassifiedFailure("p", true);
        var failure = new TapstateException(EngineError.EXECUTION_COHORT_CHANGED_BEFORE_START,
                Map.of("pipeline", "p", "reason", "live-membership", "planned", NODES.stream().sorted().toList().toString(),
                        "actual", EXPANDED_NODES.stream().sorted().toList().toString()), null);
        when(store.clusterRecovery().recordFailureNote(any(), any(), any(), isNull())).thenAnswer(call ->
                new ClusterRecoveryStore.Result(ClusterRecoveryMutation.APPLIED,
                        store.clusterRecovery().read(new ClusterRecoveryKey("cluster", "p", "inc")).orElseThrow(), null));

        runtime.failedAfterAllocation("p", new PipelineActuationOwnership.Execution(true,
                new ExecutionFence("p", current.claimGeneration(), current.executionGeneration(), current.profileGeneration()),
                current.topologyRevision()), failure);

        verify(ownership).recordClassifiedFailure("p", true);
        verify(ownership, never()).startRefusedBeforeItsRun(anyString());
        verify(store.clusterRecovery()).recordFailureNote(argThat(expected -> expected.pipelineClaim().equals(WorkloadClaimFence.from(current))),
                argThat(diagnostic -> diagnostic.code().equals(failure.code().code()) && diagnostic.params().equals(failure.args())),
                eq(ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION), isNull());
        verify(store.clusterRecovery(), times(1)).advanceExecution(any(), any(), anySet(), anySet());
        assertNoNativeSubmission();
    }

    @Test void anExpiredRetiredPermitIsReleasedBeforeItCanBeRenewedForever() {
        ClusterRecoveryStore queue = store.clusterRecovery();
        when(gate.committed()).thenReturn(new ClusterMembership("cluster", 7, NODES, 2));
        WorkloadClaim coordinator = new WorkloadClaim(new WorkloadClaimKey("cluster", WorkloadClaimType.CLUSTER_RECOVERY, "cluster"),
                owner, 1, 0, 7, NOW.plusSeconds(30), 0, 0, Set.of(), 0, false, 2);
        when(workloads.acquire(any(), eq(owner), eq(7L), any())).thenReturn(Optional.of(WorkloadClaimAttempt.acquired(coordinator)));
        StateStore states = mock(StateStore.class);
        when(store.state()).thenReturn(states);
        when(states.read("p")).thenReturn(Optional.empty());
        WorkloadClaimStore claimStore = mock(WorkloadClaimStore.class);
        when(store.workloadClaims()).thenReturn(claimStore);
        when(claimStore.read(any())).thenReturn(Optional.empty());
        DesiredState desired = store.desired().read("p").orElseThrow();
        var event = new ClusterRecoveryEvent(new ClusterRecoveryKey("cluster", "p", "inc"), ClusterRecoveryCause.FULL_CLUSTER_RESTART,
                8, "old", 6L, new ClusterExecutionProfile("cluster", 1, profile.profile()), profile, 7,
                io.tapstate.core.lifecycle.DesiredStateFingerprint.of(desired), Map.of());
        var waiting = ClusterRecoveryItem.enqueued(event, 1, NOW.minusSeconds(30), 3);
        var expired = waiting.permitted(new ClusterRecoveryPermit("expired", WorkloadClaimFence.from(coordinator),
                NOW.minusSeconds(20), NOW.minusSeconds(1), Map.of("a", demand, "b", demand, "c", demand), 0), NOW.minusSeconds(20));
        var renewed = expired.permitted(new ClusterRecoveryPermit("expired", WorkloadClaimFence.from(coordinator),
                NOW.minusSeconds(20), NOW.plusSeconds(30), expired.permit().demandByNode(), 0), NOW);
        when(queue.list("cluster", 0, 100)).thenReturn(List.of(expired));
        when(queue.resumePermit(any(), eq("expired"), any())).thenReturn(new ClusterRecoveryStore.Result(
                ClusterRecoveryMutation.APPLIED, renewed, null));
        when(queue.releaseExpiredPermit(any())).thenAnswer(call -> {
            ClusterRecoveryFence expected = call.getArgument(0);
            return expected.itemRevision() == expired.itemRevision()
                    ? new ClusterRecoveryStore.Result(ClusterRecoveryMutation.APPLIED, waiting, null)
                    : new ClusterRecoveryStore.Result(ClusterRecoveryMutation.WAITING_PERMIT, renewed, null);
        });

        runtime.retain(List.of("p"));

        verify(queue).releaseExpiredPermit(argThat(expected -> expected.itemRevision() == expired.itemRevision()));
        verify(queue, never()).resumePermit(any(), anyString(), any());
    }

    @Test void aFiniteSuccessorCanRecordItsProofWithoutHoldingTheRecoveryClaimLocally() {
        var completion = finiteSuccessor();
        var finished = new java.util.concurrent.atomic.AtomicBoolean();
        org.assertj.core.api.Assertions.assertThatCode(() -> finished.set(runtime.mayComplete("p")))
                .doesNotThrowAnyException();
        assertThat(finished.get()).isTrue();
        verify(store.clusterRecovery()).complete(argThat(expected -> expected.recoveryClaim().equals(
                WorkloadClaimFence.from(completion.coordinator()))));
        verifyNoInteractions(workloads);
    }

    @Test void startupEvidenceUsesTheRefreshedLivePipelineFence() {
        finiteSuccessor();
        WorkloadClaim refreshed = atTopology(claim(9), 8);
        WorkloadClaimFence expected = WorkloadClaimFence.from(refreshed);
        when(ownership.currentClaim("p")).thenReturn(Optional.of(refreshed));

        assertThat(runtime.mayComplete("p")).isTrue();

        verify(store.clusterRecovery()).recordSubmission(any(), eq(expected), eq("42"));
        verify(captures).startupProofs("p", expected);
        verify(captures).requiredSources("p", expected);
        verify(store.clusterRecovery()).recordStartup(any(), eq(expected), argThat(receipt -> receipt.pipelineClaim().equals(expected)));
    }

    @Test void anExpiredPipelineClaimCannotSupplyStartupEvidence() {
        finiteSuccessor();
        when(store.workloadClaims().read(any())).thenReturn(Optional.of(new WorkloadClaimReading(claim(9), Duration.ZERO)));

        assertThat(runtime.mayComplete("p")).isFalse();

        verify(store.clusterRecovery(), never()).recordSubmission(any(), any(), anyString());
        verify(store.clusterRecovery(), never()).recordStartup(any(), any(), any());
        verify(store.clusterRecovery(), never()).complete(any());
    }

    @Test void aDifferentPipelineHolderCannotSupplyThePreviousHoldersStartupEvidence() {
        finiteSuccessor();
        WorkloadClaim previous = claim(9);
        var next = new WorkloadClaim(previous.key(), new WorkloadOwner("b", "new-boot"), 2, previous.executionGeneration(), 8,
                previous.leaseUntil(), previous.contextExecutionGeneration(), previous.executionClaimGeneration(), previous.executionNodeIds(),
                0, false, previous.profileGeneration(), previous.executionProfile(), previous.executionTopologyRevision(),
                previous.executionIncarnation(), previous.executionRevision(), previous.executionMembers());
        when(store.workloadClaims().read(any())).thenReturn(Optional.of(new WorkloadClaimReading(next, Duration.ofSeconds(30))));

        assertThat(runtime.mayComplete("p")).isFalse();

        verify(store.clusterRecovery(), never()).recordSubmission(any(), any(), anyString());
        verify(store.clusterRecovery(), never()).recordStartup(any(), any(), any());
        verify(store.clusterRecovery(), never()).complete(any());
    }

    @Test void aCompatibleMemberJoinDoesNotRetargetAProvenSuccessfulSuccessor() {
        var completion = finiteSuccessor();
        when(gate.committed()).thenReturn(new ClusterMembership("cluster", 8, EXPANDED_NODES, 2));
        when(gate.visibleNodeIds()).thenReturn(EXPANDED_NODES);
        WorkloadClaim coordinator = new WorkloadClaim(completion.coordinator().key(), owner, 5, 0, 8,
                NOW.plusSeconds(30), 0, 0, Set.of(), 0, false, 2);
        when(workloads.acquire(any(), eq(owner), eq(8L), any())).thenReturn(Optional.of(WorkloadClaimAttempt.acquired(coordinator)));
        var states = mock(StateStore.class);
        when(store.state()).thenReturn(states);
        when(states.read("p")).thenReturn(Optional.empty());
        var claimStore = mock(WorkloadClaimStore.class);
        when(store.workloadClaims()).thenReturn(claimStore);
        when(claimStore.read(any())).thenAnswer(call -> ownership.currentClaim("p")
                .map(claim -> new WorkloadClaimReading(claim, Duration.ofSeconds(30))));
        when(store.clusterRecovery().list("cluster", 0, 100)).thenReturn(List.of(completion.item()));
        when(store.clusterRecovery().retarget(any(), any(), anyLong())).thenReturn(new ClusterRecoveryStore.Result(
                ClusterRecoveryMutation.STALE_ITEM, completion.item(), null));

        runtime.retain(List.of("p"));

        verify(store.clusterRecovery()).complete(any());
        verify(store.clusterRecovery(), never()).retarget(any(), any(), anyLong());
        verify(store.clusterRecovery(), never()).advanceExecution(any(), any(), anySet(), anySet());
        assertNoNativeSubmission();
    }

    @Test void aCodedOperatorFailureIsClassifiedBeforeAnUnrelatedMembershipObservation() {
        WorkloadClaim current = claim(8);
        when(ownership.currentClaim("p")).thenReturn(Optional.of(current));
        var error = new TapstateException(io.tapstate.runtime.engine.EngineError.ROUTING_KEY_MISSING,
                Map.of("node", "join", "stream", "s.orders", "columns", "id"), null);
        when(engine.nativeRun("p")).thenReturn(Optional.of(new Engine.NativeRun("42", 1, 8, 2, JobStatus.FAILED, Optional.empty())));
        when(engine.failureOf("p")).thenReturn(Optional.of(error));

        runtime.recordFailure("p");

        verify(ownership).recordClassifiedFailure("p", false);
        verify(ownership, never()).recordClassifiedFailure("p", true);
    }

    @Test void aStoppedUnclassifiedExecutionKeepsItsClaimUntilTheExistingDetectionVerdictIsRecorded() {
        WorkloadClaim stopped = claim(8);
        var held = new java.util.concurrent.atomic.AtomicReference<>(stopped);
        when(ownership.currentClaim("p")).thenAnswer(call -> Optional.of(held.get()));
        StateStore states = mock(StateStore.class);
        when(store.state()).thenReturn(states);
        when(states.read("p")).thenReturn(Optional.of(new io.tapstate.core.lifecycle.CheckpointDoc("p", "FAILED", 2, NOW)));

        runtime.stopped("p", stopped, true);

        verify(ownership, never()).retireStoppedExecution(any());
        WorkloadClaim classified = failed(stopped);
        held.set(classified);
        when(ownership.retireStoppedExecution(stopped)).thenReturn(true);
        runtime.recordFailure("p");
        verify(ownership).retireStoppedExecution(stopped);
    }

    @Test void thePipelineHolderPersistsItsFailureBeforeTheFailedStopRetiresAuthority() {
        CompletionFixture recovery = finiteSuccessor();
        WorkloadClaim allocated = claim(9);
        var held = new java.util.concurrent.atomic.AtomicReference<>(allocated);
        when(ownership.currentClaim("p")).thenAnswer(call -> Optional.of(held.get()));
        doAnswer(call -> { held.set(failed(held.get())); return null; }).when(ownership).recordClassifiedFailure("p", false);
        var error = new TapstateException(io.tapstate.runtime.engine.EngineError.ROUTING_KEY_MISSING,
                Map.of("node", "join", "stream", "s.orders", "columns", "id"), null);
        when(engine.nativeRun("p")).thenReturn(Optional.of(new Engine.NativeRun("42", 1, 9, 2, JobStatus.FAILED, Optional.empty())));
        when(engine.failureOf("p")).thenReturn(Optional.of(error));
        when(store.clusterRecovery().recordFailureNote(any(), any(), any(), isNull())).thenReturn(
                new ClusterRecoveryStore.Result(ClusterRecoveryMutation.APPLIED, recovery.item(), null));
        when(store.state().read("p")).thenReturn(Optional.of(new io.tapstate.core.lifecycle.CheckpointDoc("p", "FAILED", 2, NOW)));

        runtime.recordFailure("p");
        runtime.stopped("p", held.get(), true);

        var order = inOrder(store.clusterRecovery(), ownership);
        order.verify(store.clusterRecovery()).recordFailureNote(any(), argThat(diagnostic -> diagnostic.code().equals(error.code().code())),
                eq(ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION), isNull());
        order.verify(ownership).retireStoppedExecution(held.get());
        verifyNoInteractions(workloads);
    }

    private static WorkloadClaim failed(WorkloadClaim current) {
        return failed(current, false);
    }

    private static WorkloadClaim failed(WorkloadClaim current, boolean topologyFailure) {
        return new WorkloadClaim(current.key(), current.owner(), current.claimGeneration(), current.executionGeneration(),
                current.topologyRevision(), current.leaseUntil(), current.contextExecutionGeneration(), current.executionClaimGeneration(),
                current.executionNodeIds(), current.claimGeneration(), topologyFailure, current.profileGeneration(), current.executionProfile(),
                current.executionTopologyRevision(), current.executionIncarnation(), current.executionRevision(), current.executionMembers());
    }

    @Test void aNewRecoveryCoordinatorConsumesTheStoredCauseBeforeConsideringNativeSuccess() {
        CompletionFixture recovery = finiteSuccessor();
        var error = new TapstateException(EngineError.ROUTING_KEY_MISSING,
                Map.of("node", "join", "stream", "s.orders", "columns", "id"), null);
        var diagnostic = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                error, Map.of(), "Correct the missing routing key.");
        var note = new ClusterRecoveryFailureNote(recovery.item().successor().pipelineClaim(),
                ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, diagnostic, NOW);
        ClusterRecoveryItem noted = recovery.item().failureNoted(note, NOW);
        recoveryPass(recovery.coordinator(), noted);
        when(store.clusterRecovery().fail(any(), any(), any(), any(), any())).thenReturn(
                new ClusterRecoveryStore.Result(ClusterRecoveryMutation.SUCCESSOR_STILL_AUTHORIZED, noted, null));

        runtime.retain(List.of("p"));

        verify(store.clusterRecovery()).fail(any(), eq(note.pipelineClaim()), eq(diagnostic), eq(note.stage()), any());
        verify(store.clusterRecovery(), never()).recordStartup(any(), any(), any());
        verify(store.clusterRecovery(), never()).complete(any());
        verify(store.clusterRecovery(), never()).resumePermit(any(), anyString(), any());
    }

    @Test void aNewUnallocatedPermitDoesNotConsumeThePreviousSuccessorsFailureNote() {
        CompletionFixture recovery = finiteSuccessor();
        var error = new TapstateException(EngineError.ROUTING_KEY_MISSING,
                Map.of("node", "join", "stream", "s.orders", "columns", "id"), null);
        var diagnostic = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                error, Map.of(), "Correct the missing routing key.");
        var note = new ClusterRecoveryFailureNote(recovery.item().successor().pipelineClaim(),
                ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION, diagnostic, NOW);
        ClusterRecoveryItem next = recovery.item().failureNoted(note, NOW)
                .executionFailed(diagnostic, Duration.ofSeconds(1), NOW)
                .permitted(new ClusterRecoveryPermit("next-reservation", WorkloadClaimFence.from(recovery.coordinator()),
                        NOW.plusSeconds(1), NOW.plusSeconds(31), Map.of("a", demand, "b", demand, "c", demand), 0), NOW.plusSeconds(1));
        recoveryPass(recovery.coordinator(), next);
        when(store.clusterRecovery().releaseExpiredPermit(any())).thenReturn(
                new ClusterRecoveryStore.Result(ClusterRecoveryMutation.WAITING_PERMIT, next, null));
        when(store.clusterRecovery().resumePermit(any(), eq("next-reservation"), any())).thenReturn(
                new ClusterRecoveryStore.Result(ClusterRecoveryMutation.APPLIED, next, null));

        runtime.retain(List.of("p"));

        verify(store.clusterRecovery(), never()).fail(any(), any(), any(), any(), any());
        verify(store.clusterRecovery(), never()).complete(any());
        verify(store.clusterRecovery(), never()).recordStartup(any(), any(), any());
        verify(store.clusterRecovery()).resumePermit(any(), eq("next-reservation"), any());
    }

    @Test void aNewPermitAllocatesItsOwnExecutionRatherThanReusingItsHistoricalSuccessor() {
        CompletionFixture recovery = finiteSuccessor();
        var diagnostic = ClusterRecoveryDiagnostic.from(ClusterRecoveryDiagnostic.Reason.EXECUTION_FAILED,
                new TapstateException(EngineError.ROUTING_KEY_MISSING,
                        Map.of("node", "join", "stream", "s.orders", "columns", "id"), null),
                Map.of(), "Correct the missing routing key.");
        ClusterRecoveryItem next = recovery.item().executionFailed(diagnostic, Duration.ofSeconds(1), NOW)
                .permitted(new ClusterRecoveryPermit("next-reservation", WorkloadClaimFence.from(recovery.coordinator()),
                        NOW.plusSeconds(1), NOW.plusSeconds(31), Map.of("a", demand, "b", demand, "c", demand), 0), NOW.plusSeconds(1));
        WorkloadClaim previous = claim(9);
        WorkloadClaim advanced = claim(10);
        ClusterRecoveryItem issued = next.advanced(new ClusterRecoverySuccessor(WorkloadClaimFence.from(advanced), profile,
                NODES, Set.of("s"), NOW.plusSeconds(1), null, null, Map.of(), null), NOW.plusSeconds(1));
        when(store.clusterRecovery().read(any())).thenReturn(Optional.of(next));
        when(store.clusterRecovery().advanceExecution(any(), eq(previous), eq(NODES), eq(Set.of("s"))))
                .thenReturn(new ClusterRecoveryStore.Result(ClusterRecoveryMutation.APPLIED, issued, advanced));
        when(ownership.beginExecution(eq("p"), any())).thenAnswer(call -> {
            var issuer = call.<PipelineActuationOwnership.ExecutionAdvance>getArgument(1);
            return issuer.advance(previous, 7, NODES).map(claim -> new PipelineActuationOwnership.Execution(true,
                    new ExecutionFence("p", claim.claimGeneration(), claim.executionGeneration(), claim.profileGeneration()),
                    claim.topologyRevision())).orElseGet(PipelineActuationOwnership.Execution::refused);
        });
        DagSource.PlannedStart planned = plan(List.of());

        assertThat(runtime.prepare("p", planned, ownership)).isTrue();
        assertThat(runtime.begin("p", planned, ownership).fence().executionGeneration()).isEqualTo(10);

        verify(store.clusterRecovery()).advanceExecution(any(), eq(previous), eq(NODES), eq(Set.of("s")));
    }

    @Test void aFailureFactRefusalKeepsTheStoppedExecutionAuthorityForARetry() {
        CompletionFixture recovery = finiteSuccessor();
        WorkloadClaim current = failed(claim(9));
        when(ownership.currentClaim("p")).thenReturn(Optional.of(current));
        when(store.state().read("p")).thenReturn(Optional.of(new io.tapstate.core.lifecycle.CheckpointDoc("p", "FAILED", 2, NOW)));
        var error = new TapstateException(EngineError.ROUTING_KEY_MISSING,
                Map.of("node", "join", "stream", "s.orders", "columns", "id"), null);
        when(engine.nativeRun("p")).thenReturn(Optional.of(new Engine.NativeRun("42", 1, 9, 2, JobStatus.FAILED, Optional.empty())));
        when(engine.failureOf("p")).thenReturn(Optional.of(error));
        when(store.clusterRecovery().recordFailureNote(any(), any(), any(), isNull())).thenReturn(
                new ClusterRecoveryStore.Result(ClusterRecoveryMutation.STALE_PIPELINE_CLAIM, recovery.item(), null));

        runtime.recordFailure("p");
        runtime.stopped("p", current, true);
        verify(ownership, never()).retireStoppedExecution(any());

        when(store.clusterRecovery().recordFailureNote(any(), any(), any(), isNull())).thenReturn(
                new ClusterRecoveryStore.Result(ClusterRecoveryMutation.APPLIED, recovery.item(), null));
        when(ownership.retireStoppedExecution(current)).thenReturn(true);
        runtime.afterFailedStop("p");
        verify(ownership).retireStoppedExecution(current);
    }

    @Test void aStoppedExecutionStillRetiresAfterItsOwningClaimRefreshesOnlyTopology() {
        WorkloadClaim stopped = claim(8);
        var held = new java.util.concurrent.atomic.AtomicReference<>(stopped);
        when(ownership.currentClaim("p")).thenAnswer(call -> Optional.of(held.get()));
        when(store.state().read("p")).thenReturn(Optional.of(new io.tapstate.core.lifecycle.CheckpointDoc("p", "FAILED", 2, NOW)));
        runtime.stopped("p", stopped, true);
        verify(ownership, never()).retireStoppedExecution(any());
        WorkloadClaim classified = failed(stopped);
        held.set(new WorkloadClaim(classified.key(), classified.owner(), classified.claimGeneration(), classified.executionGeneration(),
                8, classified.leaseUntil(), classified.contextExecutionGeneration(), classified.executionClaimGeneration(),
                classified.executionNodeIds(), classified.failureClaimGeneration(), classified.failureAfterMemberLoss(), classified.profileGeneration(),
                classified.executionProfile(), classified.executionTopologyRevision(), classified.executionIncarnation(),
                classified.executionRevision(), classified.executionMembers()));
        when(ownership.retireStoppedExecution(stopped)).thenReturn(true);

        runtime.afterFailedStop("p");

        verify(ownership).retireStoppedExecution(stopped);
    }

    @Test void aPreAllocationRefusalDoesNotKeepAnExecutionlessClaimWaitingForFailureDetection() {
        when(store.state().read("p")).thenReturn(Optional.of(new io.tapstate.core.lifecycle.CheckpointDoc("p", "FAILED", 2, NOW)));

        runtime.stopped("p", initial, true);

        verify(ownership).retireStoppedExecution(initial);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.MethodSource("positionRejections")
    void aQualifiedSourceFailureRetainsItsTypedParametersAndAttemptPosition(io.tapstate.core.common.TapstateErrorCode code) {
        CompletionFixture recovery = successor(Set.of("s"));
        WorkloadClaim current = claim(9);
        var held = new java.util.concurrent.atomic.AtomicReference<>(current);
        when(ownership.currentClaim("p")).thenAnswer(call -> Optional.of(held.get()));
        doAnswer(call -> { held.set(failed(held.get())); return null; }).when(ownership).recordClassifiedFailure("p", false);
        var capture = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.CAPTURE, "capture-actual"),
                owner, 3, 0, 7, 2);
        var attempt = new CaptureReadAttempt("mc-s", 1, 4, capture, List.of("orders"),
                CaptureReadAttempt.Kind.RESUME, "original-token", null, NOW);
        var witness = new CaptureResumeWitness("s", "fixture-source", "mc-s", "p/s", io.tapstate.core.model.ReadMode.CDC_ONLY,
                false, List.of("orders"), true, 1, null, false, false, List.of(), null, 0, null, null, Map.of());
        var point = new ClusterRecoveryPosition("s", "fixture-source", "capture-actual", ClusterRecoveryPosition.Kind.DURABLE_POSITION,
                new io.tapstate.core.event.ChainPosition(io.tapstate.core.event.SourceOrder.snapshotRow(1), "original-token"),
                "mongo-confirmed-consumer", "mc-s/p/s");
        Map<String, Object> params = code == io.tapstate.runtime.srs.CaptureError.START_FROM_OUTSIDE_WINDOW
                ? Map.of("requested", "original-token", "earliest", "head\n" + "x".repeat(300), "retention", 2L)
                : Map.of("connector", "fixture-source", "requested", "original-token", "pdkId", "fixture-source",
                        "pdkCode", "10003", "serverCode", "286", "pdkArgs", List.of("retained point is outside the source log"));
        String disposition = "Extend source retention before retrying the original point.";
        var fact = new CaptureStartupFailure(WorkloadClaimFence.from(current), witness, point, NOW,
                new CaptureReadState(attempt, null, null, null, true, code.code(),
                        params, disposition, NOW));
        when(captures.captureFailure("p", WorkloadClaimFence.from(current))).thenReturn(
                Optional.of(new io.tapstate.runtime.srs.CaptureStartupException(fact)));
        when(store.clusterRecovery().recordFailureNote(any(), any(), any(), eq(fact))).thenReturn(
                new ClusterRecoveryStore.Result(ClusterRecoveryMutation.APPLIED, recovery.item(), null));

        runtime.recordFailure("p");

        verify(store.clusterRecovery()).recordFailureNote(any(), argThat(diagnostic -> diagnostic.code().equals(fact.code())
                && diagnostic.params().equals(params) && diagnostic.positions().equals(Map.of("s", point))
                && diagnostic.disposition().equals(disposition)), eq(ClusterRecoveryStore.FailureStage.SOURCE_POSITION_REJECTION), eq(fact));
    }

    private static java.util.stream.Stream<io.tapstate.core.common.TapstateErrorCode> positionRejections() {
        return java.util.stream.Stream.of(io.tapstate.runtime.srs.CaptureError.START_FROM_OUTSIDE_WINDOW,
                io.tapstate.adapters.pdk.ConnectorError.RESUME_POSITION_REJECTED);
    }

    @Test void anAllocatedRefusalKeepsItsExactCodedCauseWithoutANativeResult() {
        CompletionFixture recovery = finiteSuccessor();
        var held = new java.util.concurrent.atomic.AtomicReference<>(claim(9));
        when(ownership.currentClaim("p")).thenAnswer(call -> Optional.of(held.get()));
        doAnswer(call -> { held.set(failed(held.get())); return null; }).when(ownership).recordClassifiedFailure("p", false);
        when(engine.nativeRun("p")).thenReturn(Optional.empty());
        var failure = new TapstateException(EngineError.ROUTING_KEY_MISSING,
                Map.of("node", "join", "stream", "s.orders", "columns", "id"), null);
        when(store.clusterRecovery().recordFailureNote(any(), any(), any(), isNull())).thenReturn(
                new ClusterRecoveryStore.Result(ClusterRecoveryMutation.APPLIED, recovery.item(), null));

        runtime.failedAfterAllocation("p", new PipelineActuationOwnership.Execution(true, new ExecutionFence("p", 1, 9, 2), 7L), failure);
        runtime.recordFailure("p");

        verify(store.clusterRecovery()).recordFailureNote(any(), argThat(diagnostic -> diagnostic.code().equals(failure.code().code())
                && diagnostic.params().equals(failure.args())), eq(ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION), isNull());
    }

    @Test void aCachedAllocatedCauseCannotClassifyAReacquiredClaimAtTheSameExecutionGeneration() {
        var held = new java.util.concurrent.atomic.AtomicReference<>(claim(9));
        when(ownership.currentClaim("p")).thenAnswer(call -> Optional.of(held.get()));
        var failure = new TapstateException(EngineError.ROUTING_KEY_MISSING,
                Map.of("node", "join", "stream", "s.orders", "columns", "id"), null);
        runtime.failedAfterAllocation("p", new PipelineActuationOwnership.Execution(true, new ExecutionFence("p", 1, 9, 2), 7L), failure);
        clearInvocations(ownership);
        WorkloadClaim previous = held.get();
        held.set(new WorkloadClaim(previous.key(), previous.owner(), 2, previous.executionGeneration(), 8,
                previous.leaseUntil(), previous.contextExecutionGeneration(), previous.executionClaimGeneration(),
                previous.executionNodeIds(), 0, false, previous.profileGeneration(), previous.executionProfile(),
                previous.executionTopologyRevision(), previous.executionIncarnation(), previous.executionRevision(), previous.executionMembers()));

        runtime.recordFailure("p");

        verify(ownership, never()).recordClassifiedFailure(anyString(), anyBoolean());
    }

    @Test void aFailureNoteIsWrittenWithTheRefreshedLivePipelineFence() {
        CompletionFixture recovery = finiteSuccessor();
        WorkloadClaim refreshed = failed(atTopology(claim(9), 8));
        WorkloadClaimFence expected = WorkloadClaimFence.from(refreshed);
        when(ownership.currentClaim("p")).thenReturn(Optional.of(refreshed));
        var failure = new TapstateException(EngineError.ROUTING_KEY_MISSING,
                Map.of("node", "join", "stream", "s.orders", "columns", "id"), null);
        when(engine.nativeRun("p")).thenReturn(Optional.of(new Engine.NativeRun("42", 1, 9, 2, JobStatus.FAILED, Optional.empty())));
        when(engine.failureOf("p")).thenReturn(Optional.of(failure));
        when(store.clusterRecovery().recordFailureNote(any(), any(), any(), isNull())).thenReturn(
                new ClusterRecoveryStore.Result(ClusterRecoveryMutation.APPLIED, recovery.item(), null));

        runtime.recordFailure("p");

        verify(store.clusterRecovery()).recordFailureNote(argThat(fence -> fence.pipelineClaim().equals(expected)),
                argThat(diagnostic -> diagnostic.code().equals(failure.code().code()) && diagnostic.params().equals(failure.args())),
                eq(ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION), isNull());
    }

    private static WorkloadClaim atTopology(WorkloadClaim claim, long topology) {
        return new WorkloadClaim(claim.key(), claim.owner(), claim.claimGeneration(), claim.executionGeneration(), topology,
                claim.leaseUntil(), claim.contextExecutionGeneration(), claim.executionClaimGeneration(), claim.executionNodeIds(),
                claim.failureClaimGeneration(), claim.failureAfterMemberLoss(), claim.profileGeneration(), claim.executionProfile(),
                claim.executionTopologyRevision(), claim.executionIncarnation(), claim.executionRevision(), claim.executionMembers());
    }

    private void recoveryPass(WorkloadClaim previousCoordinator, ClusterRecoveryItem item) {
        when(gate.committed()).thenReturn(new ClusterMembership("cluster", 7, NODES, 2));
        WorkloadClaim coordinator = new WorkloadClaim(previousCoordinator.key(), owner, 5, 0, 7,
                NOW.plusSeconds(30), 0, 0, Set.of(), 0, false, 2);
        when(workloads.acquire(any(), eq(owner), eq(7L), any())).thenReturn(Optional.of(WorkloadClaimAttempt.acquired(coordinator)));
        var claimStore = mock(WorkloadClaimStore.class);
        when(store.workloadClaims()).thenReturn(claimStore);
        when(store.clusterRecovery().list("cluster", 0, 100)).thenReturn(List.of(item));
    }

    private CompletionFixture finiteSuccessor() {
        return successor(Set.of());
    }

    private CompletionFixture successor(Set<String> sources) {
        WorkloadClaim successorClaim = claim(9);
        when(ownership.currentClaim("p")).thenReturn(Optional.of(successorClaim));
        var claimStore = mock(WorkloadClaimStore.class);
        when(store.workloadClaims()).thenReturn(claimStore);
        when(claimStore.read(any())).thenAnswer(call -> ownership.currentClaim("p")
                .map(claim -> new WorkloadClaimReading(claim, Duration.ofSeconds(30))));
        WorkloadClaim coordinator = new WorkloadClaim(new WorkloadClaimKey("cluster", WorkloadClaimType.CLUSTER_RECOVERY, "cluster"),
                new WorkloadOwner("b", "boot-b"), 4, 0, 7, NOW.plusSeconds(30), 0, 0, Set.of(), 0, false, 2);
        DesiredState desired = store.desired().read("p").orElseThrow();
        var event = new ClusterRecoveryEvent(new ClusterRecoveryKey("cluster", "p", "inc"), ClusterRecoveryCause.FULL_CLUSTER_RESTART,
                8, "old", 6L, new ClusterExecutionProfile("cluster", 1, profile.profile()), profile, 7,
                io.tapstate.core.lifecycle.DesiredStateFingerprint.of(desired), Map.of());
        var allocated = ClusterRecoveryItem.enqueued(event, 1, NOW.minusSeconds(30), 3)
                .permitted(new ClusterRecoveryPermit("reserved", WorkloadClaimFence.from(coordinator),
                        NOW.minusSeconds(20), NOW.plusSeconds(30), Map.of("a", demand, "b", demand, "c", demand), 0), NOW)
                .advanced(new ClusterRecoverySuccessor(WorkloadClaimFence.from(successorClaim), profile, NODES, sources,
                        NOW, null, null, Map.of(), null), NOW);
        when(store.clusterRecovery().read(any())).thenReturn(Optional.of(allocated));
        var context = new ProcessorRuntimeContext("p", "finite", "42", "43", 1, 9, 2, "a", "boot-a", "uuid-a",
                "127.0.0.1:5701", 0, 0, 0, 1, 1, 3, NOW);
        var evidence = new NativeExecutionStartup.Evidence(1, 9, 2, "42", "43", Set.of("finite"),
                Map.of("finite", 1), Map.of("finite:0", context));
        when(engine.nativeRun("p")).thenReturn(Optional.of(new Engine.NativeRun("42", 1, 9, 2, JobStatus.COMPLETED,
                Optional.of(evidence))));
        when(captures.requiredSources("p", WorkloadClaimFence.from(successorClaim))).thenReturn(sources);
        when(captures.startupProofs("p", WorkloadClaimFence.from(successorClaim))).thenReturn(Map.of());
        var submitted = allocated.submitted("42", NOW);
        when(store.clusterRecovery().recordSubmission(any(), any(), eq("42")))
                .thenReturn(new ClusterRecoveryStore.Result(ClusterRecoveryMutation.APPLIED, submitted, null));
        var current = new java.util.concurrent.atomic.AtomicReference<>(submitted);
        when(store.clusterRecovery().recordStartup(any(), any(), any())).thenAnswer(call -> {
            ClusterRecoveryStartupReceipt supplied = call.getArgument(2);
            var normalized = new ClusterRecoveryStartupReceipt(current.get().successor().pipelineClaim(), supplied.nativeJobId(),
                    supplied.nativeInitializedAt(), supplied.preparedWitnesses(), supplied.requestedPositions(), supplied.acceptedPositions(),
                    supplied.positionsAcceptedAt(), supplied.executionCompleted());
            var initialized = current.get().initialized(normalized, NOW);
            current.set(initialized);
            return new ClusterRecoveryStore.Result(ClusterRecoveryMutation.APPLIED, initialized, null);
        });
        when(store.clusterRecovery().complete(any())).thenAnswer(call -> new ClusterRecoveryStore.Result(
                ClusterRecoveryMutation.APPLIED, current.get().recovered(NOW), null));

        return new CompletionFixture(allocated, coordinator);
    }

    private record CompletionFixture(ClusterRecoveryItem item, WorkloadClaim coordinator) { }

    private PendingStart ordinaryPendingStart() {
        return ordinaryPendingStart(allocatedClaim(1));
    }

    private PendingStart ordinaryPendingStart(WorkloadClaim allocated) {
        var held = new java.util.concurrent.atomic.AtomicReference<>(initial);
        ClusterCapacityReservation reserved = reservation(null, initial);
        ClusterCapacityReservation pending = reservation(1L, allocated);
        when(ownership.currentClaim("p")).thenAnswer(call -> Optional.of(held.get()));
        bindIssuerToCurrentClaim(held);
        when(capacity.reserve(any(), any(), anyString(), anyString(), anyMap(), any(), any())).thenAnswer(call ->
                held.get().executionGeneration() == 0
                        ? new ClusterCapacityStore.Result(ClusterCapacityStore.Outcome.APPLIED, reserved, null, List.of())
                        : new ClusterCapacityStore.Result(ClusterCapacityStore.Outcome.ALREADY_RESERVED, pending, null, List.of()));
        when(capacity.advanceExecution(reserved, initial, NODES)).thenReturn(
                new ClusterCapacityStore.Result(ClusterCapacityStore.Outcome.APPLIED, pending, allocated, List.of()));
        DagSource.PlannedStart planned = plan(List.of());
        assertThat(runtime.prepare("p", planned, ownership)).isTrue();
        assertThat(runtime.begin("p", planned, ownership).fence().executionGeneration()).isEqualTo(1);
        return new PendingStart(planned, held);
    }

    private PendingStart recoveryPendingStart() {
        return recoveryPendingStart(allocatedClaim(8), allocatedClaim(9));
    }

    private PendingStart recoveryPendingStart(WorkloadClaim previous, WorkloadClaim allocated) {
        var held = new java.util.concurrent.atomic.AtomicReference<>(previous);
        when(ownership.currentClaim("p")).thenAnswer(call -> Optional.of(held.get()));
        bindIssuerToCurrentClaim(held);
        WorkloadClaim coordinator = new WorkloadClaim(new WorkloadClaimKey("cluster", WorkloadClaimType.CLUSTER_RECOVERY, "cluster"),
                new WorkloadOwner("b", "boot-b"), 4, 0, 7, NOW.plusSeconds(30), 0, 0, Set.of(), 0, false, 2);
        var event = new ClusterRecoveryEvent(new ClusterRecoveryKey("cluster", "p", "inc"), ClusterRecoveryCause.FULL_CLUSTER_RESTART,
                8, "old", 6L, new ClusterExecutionProfile("cluster", 1, profile.profile()), profile, 7,
                io.tapstate.core.lifecycle.DesiredStateFingerprint.of(store.desired().read("p").orElseThrow()), Map.of());
        var permitted = ClusterRecoveryItem.enqueued(event, 1, NOW.minusSeconds(30), 3)
                .permitted(new ClusterRecoveryPermit("reserved", WorkloadClaimFence.from(coordinator),
                        NOW.minusSeconds(20), NOW.plusSeconds(30), Map.of("a", demand, "b", demand, "c", demand), 0), NOW);
        var item = new java.util.concurrent.atomic.AtomicReference<>(permitted);
        when(store.clusterRecovery().read(any())).thenAnswer(call -> Optional.of(item.get()));
        when(store.clusterRecovery().advanceExecution(any(), eq(previous), eq(NODES), eq(Set.of("s")))).thenAnswer(call -> {
            ClusterRecoveryItem issued = item.get().advanced(new ClusterRecoverySuccessor(WorkloadClaimFence.from(allocated), profile,
                    NODES, Set.of("s"), NOW, null, null, Map.of(), null), NOW);
            item.set(issued);
            return new ClusterRecoveryStore.Result(ClusterRecoveryMutation.APPLIED, issued, allocated);
        });
        DagSource.PlannedStart planned = plan(List.of());
        assertThat(runtime.prepare("p", planned, ownership)).isTrue();
        assertThat(runtime.begin("p", planned, ownership).fence().executionGeneration()).isEqualTo(9);
        return new PendingStart(planned, held);
    }

    private void bindIssuerToCurrentClaim(java.util.concurrent.atomic.AtomicReference<WorkloadClaim> held) {
        when(ownership.beginExecution(eq("p"), any())).thenAnswer(call -> {
            var issuer = call.<PipelineActuationOwnership.ExecutionAdvance>getArgument(1);
            WorkloadClaim current = held.get();
            return issuer.advance(current, current.topologyRevision(), gate.visibleNodeIds()).map(advanced -> {
                held.set(advanced);
                return new PipelineActuationOwnership.Execution(true,
                        new ExecutionFence("p", advanced.claimGeneration(), advanced.executionGeneration(), advanced.profileGeneration()),
                        advanced.topologyRevision());
            }).orElseGet(PipelineActuationOwnership.Execution::refused);
        });
    }

    private WorkloadClaim allocatedClaim(long execution) {
        return allocatedClaim(execution, ORIGINAL_MEMBERS);
    }

    private WorkloadClaim allocatedClaim(long execution, Map<String, ClusterExecutionMember> members) {
        WorkloadClaim base = claim(execution);
        return new WorkloadClaim(base.key(), base.owner(), base.claimGeneration(), base.executionGeneration(), base.topologyRevision(),
                base.leaseUntil(), base.contextExecutionGeneration(), base.executionClaimGeneration(), base.executionNodeIds(),
                0, false, base.profileGeneration(), profile, 7L, "inc", "a".repeat(64), members);
    }

    private void useLiveMembers(Map<String, ClusterExecutionMember> identities) {
        Set<Member> peers = identities.values().stream().map(identity -> {
            Member peer = mock(Member.class);
            when(peer.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn(identity.nodeId());
            when(peer.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn(identity.bootId());
            when(peer.getAttribute(ClusterMembershipGate.PROFILE_GENERATION_ATTRIBUTE)).thenReturn("2");
            when(peer.getAttribute(ClusterMembershipGate.PROFILE_HASH_ATTRIBUTE)).thenReturn(profile.profile().hash());
            when(peer.getUuid()).thenReturn(UUID.fromString(identity.memberUuid()));
            return peer;
        }).collect(java.util.stream.Collectors.toSet());
        when(cluster.getMembers()).thenReturn(peers);
    }

    private void assertReincarnatedCohort(TapstateException failure) {
        assertThat(failure.code()).isEqualTo(EngineError.EXECUTION_COHORT_CHANGED_BEFORE_START);
        assertThat(failure.args()).containsEntry("pipeline", "p");
        assertThat(String.valueOf(failure.args().get("planned")))
                .contains(ORIGINAL_MEMBERS.get("b").bootId(), ORIGINAL_MEMBERS.get("b").memberUuid());
        assertThat(String.valueOf(failure.args().get("actual")))
                .contains(REJOINED_MEMBERS.get("b").bootId(), REJOINED_MEMBERS.get("b").memberUuid());
    }

    private void assertChangedCohort(TapstateException failure) {
        assertThat(failure.code()).isEqualTo(EngineError.EXECUTION_COHORT_CHANGED_BEFORE_START);
        assertThat(failure.args()).containsEntry("pipeline", "p")
                .containsEntry("planned", NODES.stream().sorted().toList().toString())
                .containsEntry("actual", EXPANDED_NODES.stream().sorted().toList().toString());
    }

    private void assertNoNativeSubmission() {
        verify(engine, never()).submit(anyString(), any(DAG.class), anyMap(), any());
        verify(engine, never()).submitFenced(anyString(), any(DAG.class), anyMap(), any(), anyLong(), anyLong(), anyLong());
    }

    private record PendingStart(DagSource.PlannedStart planned, java.util.concurrent.atomic.AtomicReference<WorkloadClaim> held) { }

    private DagSource.PlannedStart plan(List<String> unknown) {
        return plan(unknown, NODES);
    }

    private DagSource.PlannedStart plan(List<String> unknown, Set<String> nodes) {
        Map<String, ClusterCapacityDemand> demands = nodes.stream().collect(java.util.stream.Collectors.toMap(node -> node, node -> demand));
        ClusterCapacityDemand total = demands.values().stream().reduce(ClusterCapacityDemand.ZERO, ClusterCapacityDemand::plus);
        var facts = new DagSource.PlanningFacts(nodes.stream().sorted().toList(), new ExecutionShape(nodes.size(), Map.of(), Map.of()),
                demands, total,
                Map.of(), List.of(), unknown, List.of(), Set.of("s"));
        DagSource.FactBearingBuilder builder = new DagSource.FactBearingBuilder() {
            @Override public Optional<DagSource.PlanningFacts> planningFacts() { return Optional.of(facts); }
            @Override public DagSource.PlannedDag apply(ExecutionFence fence) { throw new AssertionError("no physical build during admission"); }
        };
        var preparation = new DagSource.StartPreparation(DagSource.NestCapacity.none(), Set.of(), Optional.empty(),
                () -> builder, Map.of());
        return preparation.plan();
    }

    private WorkloadClaim claim(long execution) {
        return new WorkloadClaim(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "p"),
                owner, 1, execution, 7, NOW.plusSeconds(60), execution, execution == 0 ? 0 : 1,
                execution == 0 ? Set.of() : NODES, 0, false, 2);
    }

    private ClusterCapacityReservation reservation(Long execution, WorkloadClaim claim) {
        return new ClusterCapacityReservation("reserved", "cluster", "p", "inc", "intent", profile,
                WorkloadClaimFence.from(claim), Map.of("a", demand, "b", demand, "c", demand),
                NOW, NOW.plusSeconds(30), execution, null);
    }
}
