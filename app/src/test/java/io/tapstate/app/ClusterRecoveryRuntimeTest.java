package io.tapstate.app;

import com.hazelcast.cluster.Cluster;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ClusterRecoveryRuntimeTest {
    private static final Set<String> NODES = Set.of("a", "b", "c");
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
    private ClusterRecoveryRuntime runtime;

    @BeforeEach void setup() {
        var member = mock(HazelcastInstance.class);
        var cluster = mock(Cluster.class);
        when(member.getCluster()).thenReturn(cluster);
        when(cluster.getMembers()).thenReturn(Set.of());
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
            return issuer.advance(initial, 7, NODES).map(advanced -> new PipelineActuationOwnership.Execution(true,
                    new ExecutionFence("p", advanced.claimGeneration(), advanced.executionGeneration(), advanced.profileGeneration()),
                    advanced.topologyRevision())).orElseGet(PipelineActuationOwnership.Execution::refused);
        });
    }

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

    @Test void aCompatibleMemberJoinDoesNotRetargetAProvenSuccessfulSuccessor() {
        var completion = finiteSuccessor();
        when(gate.committed()).thenReturn(new ClusterMembership("cluster", 8, NODES, 2));
        WorkloadClaim coordinator = new WorkloadClaim(completion.coordinator().key(), owner, 5, 0, 8,
                NOW.plusSeconds(30), 0, 0, Set.of(), 0, false, 2);
        when(workloads.acquire(any(), eq(owner), eq(8L), any())).thenReturn(Optional.of(WorkloadClaimAttempt.acquired(coordinator)));
        var states = mock(StateStore.class);
        when(store.state()).thenReturn(states);
        when(states.read("p")).thenReturn(Optional.empty());
        var claimStore = mock(WorkloadClaimStore.class);
        when(store.workloadClaims()).thenReturn(claimStore);
        when(claimStore.read(any())).thenReturn(Optional.empty());
        when(store.clusterRecovery().list("cluster", 0, 100)).thenReturn(List.of(completion.item()));
        when(store.clusterRecovery().retarget(any(), any(), anyLong())).thenReturn(new ClusterRecoveryStore.Result(
                ClusterRecoveryMutation.STALE_ITEM, completion.item(), null));

        runtime.retain(List.of("p"));

        verify(store.clusterRecovery()).complete(any());
        verify(store.clusterRecovery(), never()).retarget(any(), any(), anyLong());
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

        runtime.recordFailure("p");
        runtime.stopped("p", held.get(), true);

        var order = inOrder(store.clusterRecovery(), ownership);
        order.verify(store.clusterRecovery()).recordFailureNote(any(), argThat(diagnostic -> diagnostic.code().equals(error.code().code())),
                eq(ClusterRecoveryStore.FailureStage.ALLOCATED_EXECUTION), isNull());
        order.verify(ownership).retireStoppedExecution(held.get());
        verifyNoInteractions(workloads);
    }

    private static WorkloadClaim failed(WorkloadClaim current) {
        return new WorkloadClaim(current.key(), current.owner(), current.claimGeneration(), current.executionGeneration(),
                current.topologyRevision(), current.leaseUntil(), current.contextExecutionGeneration(), current.executionClaimGeneration(),
                current.executionNodeIds(), current.claimGeneration(), false, current.profileGeneration(), current.executionProfile(),
                current.executionTopologyRevision(), current.executionIncarnation(), current.executionRevision(), current.executionMembers());
    }

    private CompletionFixture finiteSuccessor() {
        WorkloadClaim successorClaim = claim(9);
        when(ownership.currentClaim("p")).thenReturn(Optional.of(successorClaim));
        WorkloadClaim coordinator = new WorkloadClaim(new WorkloadClaimKey("cluster", WorkloadClaimType.CLUSTER_RECOVERY, "cluster"),
                new WorkloadOwner("b", "boot-b"), 4, 0, 7, NOW.plusSeconds(30), 0, 0, Set.of(), 0, false, 2);
        DesiredState desired = store.desired().read("p").orElseThrow();
        var event = new ClusterRecoveryEvent(new ClusterRecoveryKey("cluster", "p", "inc"), ClusterRecoveryCause.FULL_CLUSTER_RESTART,
                8, "old", 6L, new ClusterExecutionProfile("cluster", 1, profile.profile()), profile, 7,
                io.tapstate.core.lifecycle.DesiredStateFingerprint.of(desired), Map.of());
        var allocated = ClusterRecoveryItem.enqueued(event, 1, NOW.minusSeconds(30), 3)
                .permitted(new ClusterRecoveryPermit("reserved", WorkloadClaimFence.from(coordinator),
                        NOW.minusSeconds(20), NOW.plusSeconds(30), Map.of("a", demand, "b", demand, "c", demand), 0), NOW)
                .advanced(new ClusterRecoverySuccessor(WorkloadClaimFence.from(successorClaim), profile, NODES, Set.of(),
                        NOW, null, null, Map.of(), null), NOW);
        when(store.clusterRecovery().read(any())).thenReturn(Optional.of(allocated));
        var context = new ProcessorRuntimeContext("p", "finite", "42", "43", 1, 9, 2, "a", "boot-a", "uuid-a",
                "127.0.0.1:5701", 0, 0, 0, 1, 1, 3, NOW);
        var evidence = new NativeExecutionStartup.Evidence(1, 9, 2, "42", "43", Set.of("finite"),
                Map.of("finite", 1), Map.of("finite:0", context));
        when(engine.nativeRun("p")).thenReturn(Optional.of(new Engine.NativeRun("42", 1, 9, 2, JobStatus.COMPLETED,
                Optional.of(evidence))));
        when(captures.requiredSources("p", WorkloadClaimFence.from(successorClaim))).thenReturn(Set.of());
        when(captures.startupProofs("p", WorkloadClaimFence.from(successorClaim))).thenReturn(Map.of());
        var submitted = allocated.submitted("42", NOW);
        when(store.clusterRecovery().recordSubmission(any(), any(), eq("42")))
                .thenReturn(new ClusterRecoveryStore.Result(ClusterRecoveryMutation.APPLIED, submitted, null));
        var current = new java.util.concurrent.atomic.AtomicReference<>(submitted);
        when(store.clusterRecovery().recordStartup(any(), any(), any())).thenAnswer(call -> {
            var initialized = current.get().initialized(call.getArgument(2), NOW);
            current.set(initialized);
            return new ClusterRecoveryStore.Result(ClusterRecoveryMutation.APPLIED, initialized, null);
        });
        when(store.clusterRecovery().complete(any())).thenAnswer(call -> new ClusterRecoveryStore.Result(
                ClusterRecoveryMutation.APPLIED, current.get().recovered(NOW), null));

        return new CompletionFixture(allocated, coordinator);
    }

    private record CompletionFixture(ClusterRecoveryItem item, WorkloadClaim coordinator) { }

    private DagSource.PlannedStart plan(List<String> unknown) {
        var facts = new DagSource.PlanningFacts(List.of("a", "b", "c"), new ExecutionShape(3, Map.of(), Map.of()),
                Map.of("a", demand, "b", demand, "c", demand), new ClusterCapacityDemand(3, 0, 3, 6, 12, 15),
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
