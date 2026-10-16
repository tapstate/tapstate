package io.tapstate.app;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.DesiredStateFingerprint;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.spi.store.ArtifactIdentity;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ClusterExecutionMember;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ClusterProfileStore;
import io.tapstate.spi.store.ClusterRecoveryCause;
import io.tapstate.spi.store.ClusterRecoveryEvent;
import io.tapstate.spi.store.ClusterRecoveryFence;
import io.tapstate.spi.store.ClusterRecoveryItem;
import io.tapstate.spi.store.ClusterRecoveryKey;
import io.tapstate.spi.store.ClusterRecoveryMutation;
import io.tapstate.spi.store.ClusterRecoveryStore;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class ClusterRecoveryDiscoveryOrderTest {
    private static final String HEAD = "poison_a_head";
    private static final String FIRST_FOLLOWER = "poison_b_follow";
    private static final String SECOND_FOLLOWER = "poison_c_follow";
    private static final List<String> REVERSED = List.of(SECOND_FOLLOWER, FIRST_FOLLOWER, HEAD);
    private static final Set<String> NODES = Set.of("node-a", "node-b", "node-c");
    private static final Instant NOW = Instant.parse("2026-10-11T00:00:00Z");
    private static final String REVISION = "a".repeat(64);

    @Test
    void aColdDriverPassDiscoversTheHeadFirstDespiteReversedDesiredEnumeration() {
        Fixture fixture = new Fixture(REVERSED);
        LifecycleActuator actuator = mock(LifecycleActuator.class);
        when(actuator.failure(anyString())).thenReturn(Optional.empty());
        when(actuator.isCarryingAJob(anyString())).thenAnswer(call -> {
            fixture.operations.add("missing:" + call.getArgument(0));
            return false;
        });
        PipelineConverger converger = new PipelineConverger(fixture.desired, fixture.state, actuator,
                Clock.fixed(NOW, ZoneOffset.UTC), fixture.runtime);
        ConvergenceDriver driver = new ConvergenceDriver(converger, fixture.desired,
                new ObservationPublisher(fixture.state, new InMemoryObservationStore()), () -> true, fixture.ownership);

        driver.reconcile();

        assertThat(fixture.operations).containsExactly(
                "missing:" + SECOND_FOLLOWER, "missing:" + FIRST_FOLLOWER, "missing:" + HEAD,
                "enqueue:" + HEAD, "enqueue:" + FIRST_FOLLOWER, "enqueue:" + SECOND_FOLLOWER);
        assertThat(fixture.orderedQueue()).extracting(item -> item.event().key().pipelineId())
                .containsExactly(HEAD, FIRST_FOLLOWER, SECOND_FOLLOWER);
        assertThat(fixture.orderedQueue()).extracting(ClusterRecoveryItem::enqueueSequence)
                .containsExactly(1L, 2L, 3L);
        assertThat(fixture.orderedQueue()).allSatisfy(item ->
                assertThat(item.event().cause()).isEqualTo(ClusterRecoveryCause.FULL_CLUSTER_RESTART));
        verify(actuator, never()).start(anyString());
        verify(fixture.workloads, times(1)).acquire(eq(fixture.coordinator.key()), eq(fixture.owner), eq(7L), any());
        REVERSED.forEach(pipeline -> assertThat(fixture.state.read(pipeline).orElseThrow().stateJson()).isEqualTo("RUNNING"));
    }

    @Test
    void stableDiscoveryDoesNotReorderOrReplaceAlreadyAssignedQueueItems() {
        String existingHead = "zz_existing_head";
        String existingLater = "aa_existing_later";
        Fixture fixture = new Fixture(List.of(existingHead, SECOND_FOLLOWER, FIRST_FOLLOWER, HEAD, existingLater));
        ClusterRecoveryItem first = fixture.append(fixture.event(existingHead));
        ClusterRecoveryItem second = fixture.append(fixture.event(existingLater));

        fixture.runtime.retain(fixture.desired.pipelineIds());

        assertThat(fixture.items.get(first.event().key())).isSameAs(first);
        assertThat(fixture.items.get(second.event().key())).isSameAs(second);
        assertThat(fixture.orderedQueue()).extracting(item -> item.event().key().pipelineId())
                .containsExactly(existingHead, existingLater, HEAD, FIRST_FOLLOWER, SECOND_FOLLOWER);
        assertThat(fixture.permitVisits).containsExactly(existingHead, existingLater, HEAD, FIRST_FOLLOWER, SECOND_FOLLOWER);
        verify(fixture.queue, never()).retarget(any(), any(), anyLong());
        verify(fixture.queue, never()).cancel(any());
        verify(fixture.queue, never()).advanceExecution(any(), any(), anySet(), anySet());
    }

    @Test
    void anUnacquiredRecoveryClaimCannotDiscoverOrReconcileTheDesiredSet() {
        Fixture fixture = new Fixture(REVERSED);
        when(fixture.workloads.acquire(any(), any(), anyLong(), any())).thenReturn(Optional.empty());

        fixture.runtime.retain(REVERSED);

        assertThat(fixture.items).isEmpty();
        assertThat(fixture.operations).isEmpty();
        verifyNoInteractions(fixture.queue);
        verify(fixture.workloads, times(1)).acquire(eq(fixture.coordinator.key()), eq(fixture.owner), eq(7L), any());
    }

    private static final class Fixture {
        private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
        private final InMemoryStateStore state = new InMemoryStateStore();
        private final StorePort stores = mock(StorePort.class);
        private final ClusterRecoveryStore queue = mock(ClusterRecoveryStore.class);
        private final ClusterWorkloadClaims workloads = mock(ClusterWorkloadClaims.class);
        private final PipelineActuationOwnership ownership = mock(PipelineActuationOwnership.class);
        private final WorkloadOwner owner = new WorkloadOwner("node-a", "new-boot-node-a");
        private final ClusterExecutionProfile previous = new ClusterExecutionProfile("cluster", 1,
                new ExecutionProfile(1, Map.of("capacityWriters", "8")));
        private final ClusterExecutionProfile current = new ClusterExecutionProfile("cluster", 2,
                new ExecutionProfile(1, Map.of("capacityWriters", "5")));
        private final WorkloadClaim coordinator = new WorkloadClaim(
                new WorkloadClaimKey("cluster", WorkloadClaimType.CLUSTER_RECOVERY, "cluster"), owner,
                2, 0, 7, NOW.plusSeconds(30), 0, 0, Set.of(), 0, false, 2);
        private final Map<ClusterRecoveryKey, ClusterRecoveryItem> items = new LinkedHashMap<>();
        private final List<String> operations = new ArrayList<>();
        private final List<String> permitVisits = new ArrayList<>();
        private final ClusterRecoveryRuntime runtime;

        private Fixture(Collection<String> pipelines) {
            Map<String, WorkloadClaim> executions = new LinkedHashMap<>();
            Map<String, ClusterExecutionMember> oldMembers = Map.of(
                    "node-a", new ClusterExecutionMember("node-a", "old-boot-a", "00000000-0000-4000-8000-000000000001"),
                    "node-b", new ClusterExecutionMember("node-b", "old-boot-b", "00000000-0000-4000-8000-000000000002"),
                    "node-c", new ClusterExecutionMember("node-c", "old-boot-c", "00000000-0000-4000-8000-000000000003"));
            ArtifactStore artifacts = mock(ArtifactStore.class);
            for (String pipeline : pipelines) {
                desired.save(new DesiredState(pipeline, PipelineState.RUNNING, REVISION));
                state.create(pipeline, StateJson.of(PipelineState.RUNNING), NOW);
                WorkloadClaim execution = new WorkloadClaim(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, pipeline),
                        owner, 2, 1, 7, NOW.plusSeconds(30), 1, 1, NODES, 0, false, 2, previous, 6L,
                        "inc-" + pipeline, REVISION, oldMembers);
                executions.put(pipeline, execution);
                when(artifacts.identity(pipeline)).thenReturn(Optional.of(new ArtifactIdentity(pipeline, "inc-" + pipeline, REVISION)));
                when(ownership.currentClaim(pipeline)).thenAnswer(call -> Optional.of(executions.get(pipeline)));
                when(ownership.permit(pipeline)).thenAnswer(call -> new PipelineActuationOwnership.Permit(true, executions.get(pipeline)));
            }
            WorkloadClaimStore claimStore = mock(WorkloadClaimStore.class);
            when(claimStore.read(any())).thenAnswer(call -> Optional.ofNullable(executions.get(call.<WorkloadClaimKey>getArgument(0).resourceId()))
                    .map(claim -> new WorkloadClaimReading(claim, Duration.ofSeconds(30))));
            ClusterProfileStore profiles = mock(ClusterProfileStore.class);
            when(profiles.profile("cluster")).thenReturn(Optional.of(current));
            when(stores.desired()).thenReturn(desired);
            when(stores.state()).thenReturn(state);
            when(stores.artifacts()).thenReturn(artifacts);
            when(stores.clusterProfiles()).thenReturn(profiles);
            when(stores.workloadClaims()).thenReturn(claimStore);
            when(stores.clusterRecovery()).thenReturn(queue);
            when(queue.read(any())).thenAnswer(call -> Optional.ofNullable(items.get(call.<ClusterRecoveryKey>getArgument(0))));
            when(queue.enqueue(any(), any())).thenAnswer(call -> {
                ClusterRecoveryEvent event = call.getArgument(0);
                ClusterRecoveryFence fence = call.getArgument(1);
                assertThat(fence.recoveryClaim().key()).isEqualTo(coordinator.key());
                operations.add("enqueue:" + event.key().pipelineId());
                ClusterRecoveryItem existing = items.get(event.key());
                return new ClusterRecoveryStore.Result(existing == null ? ClusterRecoveryMutation.APPLIED : ClusterRecoveryMutation.DUPLICATE,
                        existing == null ? append(event) : existing, null);
            });
            when(queue.list(eq("cluster"), anyInt(), eq(100))).thenAnswer(call -> orderedQueue().stream()
                    .skip(call.<Integer>getArgument(1)).limit(100).toList());
            when(queue.acquirePermit(any(), anyMap(), any(), any(), any(), anyInt())).thenAnswer(call -> {
                ClusterRecoveryFence fence = call.getArgument(0);
                permitVisits.add(fence.key().pipelineId());
                return new ClusterRecoveryStore.Result(ClusterRecoveryMutation.WAITING_PERMIT, items.get(fence.key()), null);
            });
            when(workloads.acquire(any(), eq(owner), eq(7L), any())).thenReturn(Optional.of(WorkloadClaimAttempt.acquired(coordinator)));
            HazelcastInstance member = mock(HazelcastInstance.class);
            Cluster nativeCluster = mock(Cluster.class);
            when(member.getCluster()).thenReturn(nativeCluster);
            Set<Member> peers = NODES.stream().map(node -> {
                Member peer = mock(Member.class);
                when(peer.getAttribute(ClusterMembershipGate.NODE_ID_ATTRIBUTE)).thenReturn(node);
                when(peer.getAttribute(ClusterMembershipGate.BOOT_ID_ATTRIBUTE)).thenReturn("new-boot-" + node);
                when(peer.getAttribute(ClusterMembershipGate.PROFILE_GENERATION_ATTRIBUTE)).thenReturn("2");
                when(peer.getAttribute(ClusterMembershipGate.PROFILE_HASH_ATTRIBUTE)).thenReturn(current.profile().hash());
                when(peer.getUuid()).thenReturn(UUID.nameUUIDFromBytes(node.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
                return peer;
            }).collect(Collectors.toSet());
            when(nativeCluster.getMembers()).thenReturn(peers);
            ConcurrentHashMap<String, Object> context = new ConcurrentHashMap<>();
            context.put(HazelcastConfiguration.NODE_SESSION_CONTEXT_KEY, new WorkloadClaim(
                    new WorkloadClaimKey("cluster", WorkloadClaimType.NODE_SESSION, "node-a"), owner, 2, 0, 0,
                    NOW.plusSeconds(30), 0, 0, Set.of(), 0, false, 2));
            when(member.getUserContext()).thenReturn(context);
            ClusterMembershipGate membership = mock(ClusterMembershipGate.class);
            when(membership.businessEligible()).thenReturn(true);
            when(membership.visibleNodeIds()).thenReturn(NODES);
            when(membership.committed()).thenReturn(new ClusterMembership("cluster", 7, NODES, 2));
            PipelineCaptureCoordinator captures = mock(PipelineCaptureCoordinator.class);
            when(captures.resumePositions(anyString(), eq(artifacts))).thenReturn(Map.of());
            DagSource dags = mock(DagSource.class);
            when(dags.prepareStart(anyString(), anyString())).thenReturn(new DagSource.StartPreparation(
                    DagSource.NestCapacity.none(), Set.of(), Optional.empty(), (ExecutionFence fence) -> {
                        throw new AssertionError("discovery must not physically build a DAG");
                    }));
            ClusterProperties properties = new ClusterProperties();
            properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
            properties.setId("cluster");
            runtime = new ClusterRecoveryRuntime(stores, dags, captures, mock(Engine.class), ownership, workloads,
                    membership, member, properties, new ClusterCapacityProperties().limits(), "default", Duration.ofSeconds(30));
        }

        private ClusterRecoveryEvent event(String pipeline) {
            return new ClusterRecoveryEvent(new ClusterRecoveryKey("cluster", pipeline, "inc-" + pipeline),
                    ClusterRecoveryCause.FULL_CLUSTER_RESTART, 1, REVISION, 6L, previous, current, 7,
                    DesiredStateFingerprint.of(desired.read(pipeline).orElseThrow()), Map.of());
        }

        private ClusterRecoveryItem append(ClusterRecoveryEvent event) {
            ClusterRecoveryItem item = ClusterRecoveryItem.enqueued(event, items.size() + 1L, NOW, 3);
            items.put(event.key(), item);
            return item;
        }

        private List<ClusterRecoveryItem> orderedQueue() {
            return items.values().stream().sorted(Comparator.comparingLong(ClusterRecoveryItem::enqueueSequence)).toList();
        }
    }
}
