package io.tapstate.control.core;

import io.tapstate.spi.store.ClusterMembership;
import io.tapstate.spi.store.ClusterMembershipStore;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * The topology is neither of its two halves. What the engine sees includes a node that has just arrived
 * and that nobody has committed; what the cluster has committed includes a node that has gone and whose
 * removal nobody has committed yet. The answer has to say which is which, because the gap between them is
 * exactly what a reader is looking at when something is wrong.
 */
class ClusterTopologyServiceTest {

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
