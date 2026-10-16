package io.tapstate.control.core;

import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ClusterMembershipStore;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterNodeReading;
import io.tapstate.spi.store.ClusterNodeRegistration;
import io.tapstate.spi.store.ClusterNodeReservation;
import io.tapstate.spi.store.ClusterProfileStore;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * The topology is neither of its two halves. What the engine sees includes a node that has just arrived
 * and that nobody has committed; what the cluster has committed includes a node that has gone and whose
 * removal nobody has committed yet. The answer has to say which is which, because the gap between them is
 * exactly what a reader is looking at when something is wrong.
 */
class ClusterTopologyServiceTest {

    private static final Instant STORE_TIME = Instant.parse("2026-09-19T08:30:00Z");
    private static final ClusterExecutionProfile PROFILE = new ClusterExecutionProfile(
            "cluster-a", 2, new ExecutionProfile(1, Map.of("build", "fixture")));

    private static final LiveClusterMember NODE_A = new LiveClusterMember(
            "node-a", "uuid-a", "boot-a", "[127.0.0.1]:5701", "https://a.example:8443");
    private static final LiveClusterMember NODE_B = new LiveClusterMember(
            "node-b", "uuid-b", "boot-b", "[127.0.0.1]:5702", "https://b.example:8443");

    @Test
    void aMemberTheClusterHasCommittedIsActive() {
        ClusterTopologyService topology = new ClusterTopologyService(
                () -> List.of(NODE_A, NODE_B), committed(4, "node-a", "node-b"), noPipelines(),
                "cluster-a");

        ClusterTopologyView view = topology.topology();

        assertThat(view.topologyRevision())
                .as("the revision these members were judged against, so a reader knows what the "
                        + "judgement was made from")
                .isEqualTo(4L);
        assertThat(view.members())
                .extracting(ClusterMemberView::nodeId, ClusterMemberView::state)
                .containsExactly(
                        tuple("node-a", ClusterMemberState.ACTIVE),
                        tuple("node-b", ClusterMemberState.ACTIVE));
    }

    @Test
    void aMemberTheEngineSeesButNobodyHasCommittedIsStillJoining() {
        ClusterTopologyService topology = new ClusterTopologyService(
                () -> List.of(NODE_A, NODE_B), committed(4, "node-a"), noPipelines(), "cluster-a");

        assertThat(topology.topology().members())
                .extracting(ClusterMemberView::nodeId, ClusterMemberView::state)
                .as("reachable is not the same as entitled to work, and reporting the second as the "
                        + "first is how a member that never gets committed goes unnoticed")
                .containsExactly(
                        tuple("node-a", ClusterMemberState.ACTIVE),
                        tuple("node-b", ClusterMemberState.JOINING));
    }

    @Test
    void aMemberTheClusterCommittedButTheEngineCannotSeeIsNotListed() {
        ClusterTopologyService topology = new ClusterTopologyService(
                () -> List.of(NODE_A), committed(4, "node-a", "node-b"), noPipelines(), "cluster-a");

        assertThat(topology.topology().members())
                .extracting(ClusterMemberView::nodeId)
                .as("this answers what is here; a member that is gone is absent from it, and that it "
                        + "was committed is readable from the claims that outlive it")
                .containsExactly("node-a");
    }

    @Test
    void withNothingCommittedNobodyIsReportedAsJoiningSomething() {
        ClusterTopologyService topology =
                new ClusterTopologyService(() -> List.of(NODE_A), null, noPipelines(), "cluster-a");

        ClusterTopologyView view = topology.topology();

        assertThat(view.topologyRevision())
                .as("null is 'cannot say'; printed as zero it would read as a cluster at revision zero")
                .isNull();
        assertThat(view.members())
                .extracting(ClusterMemberView::state)
                .as("with no committed set there is nothing to be outside of")
                .containsExactly(ClusterMemberState.ACTIVE);
    }

    @Test
    void membersComeBackInAStableOrder() {
        ClusterTopologyService topology = new ClusterTopologyService(
                () -> List.of(NODE_B, NODE_A), committed(4, "node-a", "node-b"), noPipelines(),
                "cluster-a");

        assertThat(topology.topology().members())
                .extracting(ClusterMemberView::nodeId)
                .as("the engine's own order is not one; a list that reshuffles between reads cannot be "
                        + "diffed by anybody")
                .containsExactly("node-a", "node-b");
    }

    @Test
    void theRegistryDistinguishesADepartedBootFromOneReservedButNeverJoined() {
        ClusterNodeReading departed = registered(NODE_A, PROFILE, true, Duration.ofSeconds(12));
        ClusterNodeReading expired = registered(NODE_B, PROFILE, true, Duration.ofSeconds(-7));
        LiveClusterMember reserved = new LiveClusterMember(
                "node-c", null, "boot-c", null, "https://c.example:8443");
        ClusterTopologyView view = new ClusterTopologyService(() -> List.of(),
                committed(4, "node-a", "node-b"), noPipelines(), "cluster-a",
                profiles(PROFILE, departed, expired, registered(reserved, PROFILE, false, Duration.ofSeconds(15))))
                .topology();

        assertThat(view.members()).extracting(ClusterMemberView::nodeId, ClusterMemberView::state,
                ClusterMemberView::live, ClusterMemberView::joined, ClusterMemberView::sessionLeaseRemainingMillis)
                .containsExactly(tuple("node-a", ClusterMemberState.LOST, false, true, 12_000L),
                        tuple("node-b", ClusterMemberState.LOST, false, true, -7_000L),
                        tuple("node-c", ClusterMemberState.JOINING, false, false, 15_000L));
        assertThat(view.members().get(0).memberUuid()).isEqualTo(NODE_A.memberUuid());
        assertThat(view.members().get(2).memberUuid()).isNull();
        assertThat(view.members().get(2).joinedAt()).isNull();
        assertThat(view.profileGeneration()).isEqualTo(2L);
        assertThat(view.profileHash()).isEqualTo(PROFILE.profile().hash());
    }

    @Test
    void aLiveMemberCannotBorrowTheSessionOfAnotherBoot() {
        LiveClusterMember nextBoot = new LiveClusterMember("node-a", null, "boot-a2", null, NODE_A.controlUrl());
        ClusterTopologyView view = new ClusterTopologyService(() -> List.of(NODE_A),
                committed(4, "node-a"), noPipelines(), "cluster-a",
                profiles(PROFILE, registered(nextBoot, PROFILE, false, Duration.ofSeconds(30)))).topology();

        assertThat(view.members()).extracting(ClusterMemberView::state, ClusterMemberView::bootId,
                ClusterMemberView::sessionBootId, ClusterMemberView::joined, ClusterMemberView::live)
                .containsExactly(tuple(ClusterMemberState.INCOMPATIBLE, "boot-a", "boot-a2", false, true));
    }

    @Test
    void aCommittedLiveMemberIsActiveOnlyWithTheCurrentProfileAndStoreProvenLease() {
        ClusterTopologyService active = new ClusterTopologyService(() -> List.of(NODE_A),
                committed(4, "node-a"), noPipelines(), "cluster-a",
                profiles(PROFILE, registered(NODE_A, PROFILE, true, Duration.ofSeconds(30))));
        assertThat(active.topology().members()).extracting(ClusterMemberView::state)
                .containsExactly(ClusterMemberState.ACTIVE);

        for (long generation : List.of(1L, 2L)) {
            ClusterExecutionProfile other = new ClusterExecutionProfile(
                    "cluster-a", generation, new ExecutionProfile(1, Map.of("build", "other")));
            ClusterTopologyService incompatible = new ClusterTopologyService(() -> List.of(NODE_A),
                    committed(4, "node-a"), noPipelines(), "cluster-a",
                    profiles(PROFILE, registered(NODE_A, other, true, Duration.ofSeconds(30))));
            assertThat(incompatible.topology().members()).extracting(ClusterMemberView::state)
                    .containsExactly(ClusterMemberState.INCOMPATIBLE);
        }

        ClusterTopologyService expired = new ClusterTopologyService(() -> List.of(NODE_A),
                committed(4, "node-a"), noPipelines(), "cluster-a",
                profiles(PROFILE, registered(NODE_A, PROFILE, true, Duration.ofMillis(-1))));
        assertThat(expired.topology().members()).extracting(ClusterMemberView::state,
                ClusterMemberView::sessionLeased, ClusterMemberView::sessionLeaseRemainingMillis)
                .containsExactly(tuple(ClusterMemberState.LOST, false, -1L));
    }

    @Test
    void aLiveRuntimeUuidDoesNotReplaceTheConflictingJoinReceipt() {
        LiveClusterMember recorded = new LiveClusterMember(
                NODE_A.nodeId(), "older-uuid", NODE_A.bootId(), NODE_A.hzAddress(), NODE_A.controlUrl());
        ClusterMemberView member = new ClusterTopologyService(() -> List.of(NODE_A),
                committed(4, "node-a"), noPipelines(), "cluster-a",
                profiles(PROFILE, registered(recorded, PROFILE, true, Duration.ofSeconds(30))))
                .topology().members().getFirst();

        assertThat(member.state()).isEqualTo(ClusterMemberState.INCOMPATIBLE);
        assertThat(member.memberUuid()).isEqualTo("uuid-a");
        assertThat(member.joinedMemberUuid()).isEqualTo("older-uuid");
    }

    @Test
    void aProfileBackedClusterCannotCallAnUncommittedOrUnregisteredMemberActive() {
        ClusterTopologyService uncommitted = new ClusterTopologyService(() -> List.of(NODE_A), null,
                noPipelines(), "cluster-a",
                profiles(PROFILE, registered(NODE_A, PROFILE, true, Duration.ofSeconds(30))));
        assertThat(uncommitted.topology().members()).extracting(ClusterMemberView::state)
                .containsExactly(ClusterMemberState.JOINING);
        ClusterTopologyService unregistered = new ClusterTopologyService(() -> List.of(NODE_A),
                committed(4, "node-a"), noPipelines(), "cluster-a", profiles(PROFILE));
        ClusterMemberView member = unregistered.topology().members().get(0);
        assertThat(member.state()).isEqualTo(ClusterMemberState.INCOMPATIBLE);
        assertThat(member.sessionLeaseRemainingMillis()).isNull();
        assertThat(member.profileGeneration()).isNull();
    }

    private static ClusterNodeReading registered(LiveClusterMember member, ClusterExecutionProfile profile,
            boolean joined, Duration remaining) {
        WorkloadClaim session = new WorkloadClaim(
                new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, member.nodeId()),
                new WorkloadOwner(member.nodeId(), member.bootId()), 1, 0, 0, STORE_TIME.plus(remaining),
                0, 0, Set.of(), 0, false, profile.generation());
        return new ClusterNodeReading(new ClusterNodeRegistration(session, profile, URI.create(member.controlUrl()),
                joined, joined ? member.memberUuid() : null, joined ? member.hzAddress() : null,
                joined ? STORE_TIME.minusSeconds(60) : null), remaining);
    }

    private static ClusterProfileStore profiles(ClusterExecutionProfile profile, ClusterNodeReading... nodes) {
        return new ClusterProfileStore() {
            @Override
            public ClusterNodeReservation reserve(String clusterId, WorkloadOwner owner, URI controlUrl,
                    ExecutionProfile proposed, Duration ttl) {
                throw new UnsupportedOperationException("a read face reserves nothing");
            }

            @Override
            public Optional<ClusterExecutionProfile> profile(String clusterId) {
                return Optional.of(profile);
            }

            @Override
            public List<ClusterNodeReading> nodes(String clusterId) {
                return List.of(nodes);
            }

            @Override
            public boolean markJoined(WorkloadClaim expectedSession, String memberUuid, String memberAddress) {
                throw new UnsupportedOperationException("a read face records nothing");
            }
        };
    }

    /** This case is about the member half; the pipeline half has nothing to enumerate from. */
    private static ClusterPipelineTopologyService noPipelines() {
        return new ClusterPipelineTopologyService(
                LivePipelineRuns.none(), PipelineCaptures.none(), null, null, "cluster-a");
    }

    private static ClusterMembershipStore committed(long revision, String... nodeIds) {
        ClusterMembership membership = new ClusterMembership("cluster-a", revision, Set.of(nodeIds));
        return new ClusterMembershipStore() {
            @Override
            public Optional<ClusterMembership> read(String clusterId) {
                return Optional.of(membership);
            }

            @Override
            public ClusterMembership createIfAbsent(String clusterId, Set<String> activeNodeIds) {
                throw new UnsupportedOperationException("a read face commits nothing");
            }

            @Override
            public Optional<ClusterMembership> compareAndSet(
                    String clusterId, long expectedRevision, Set<String> activeNodeIds) {
                throw new UnsupportedOperationException("a read face commits nothing");
            }
        };
    }
}
