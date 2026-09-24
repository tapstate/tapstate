package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.testsupport.DockerGate;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Two server processes told about each other become one cluster, and each of them says so.
 *
 * <p>Everything else about clustering rests on this and none of it says whether it happened. Two
 * processes that never find each other are two clusters of one: both answer their health probe, both
 * serve every verb, both report a membership - of themselves. Nothing fails, and every later property
 * built on "the cluster" is then being asserted twice over two separate installations.
 *
 * <p>So what is asserted is the membership each of them reports, by node id, and that the two answers
 * are the same. A case that asked only whether the topology verb answers would pass on two
 * installations that had never heard of one another, which is exactly the shape that has to fail here.
 *
 * <p>How the pair is wired - where each listens, what it advertises, and why one bring-up gets a
 * cluster id of its own - is {@link TwoMemberCluster}, which every two-member case shares.
 *
 * <p><b>Bespoke rather than declarative, and that is registered rather than assumed.</b> The
 * specification envelope has no word for a second member - its setup speaks of connectors, resources,
 * discovery and databases, and a member is none of those - so this is written in Java against the
 * real-process tier. The gap is written down where gaps are written down rather than left as a habit.
 */
class TwoMembersFormOneClusterIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void twoProcessesFindEachOtherAndBothAnswerTheSameMembership() {
        String store = SharedMongo.replicaSetUrl("e2e_two_members");

        try (TwoMemberCluster cluster = TwoMemberCluster.start(store, "e2e-two-members")) {
            List<String> asAsees = cluster.awaitBothMembers();

            assertThat(asAsees)
                    .describedAs("the two processes joined. Told about each other and unable to reach "
                            + "one another, each would answer with itself alone")
                    .containsExactly(TwoMemberCluster.NODE_A, TwoMemberCluster.NODE_B);
            assertThat(cluster.second().clusterMemberNodeIds())
                    .describedAs("and the other one answers the same list rather than its own view of "
                            + "who it has met - which is the whole promise of reading this from any member")
                    .isEqualTo(asAsees);
            assertThat(cluster.second().clusterId())
                    .describedAs("one cluster, not two that happen to hold the same members")
                    .isEqualTo(cluster.first().clusterId());
        }
    }
}
