package io.tapstate.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.testsupport.DockerGate;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Three processes join through links that can be cut, and every byte between them goes through one.
 *
 * <p>This exists because everything a partition case asserts rests on an arrangement that is easy to
 * get silently wrong. Members exchange the address each reports for itself and dial that from then on,
 * so members that report their own addresses reach each other directly — and a case that cut the links
 * would then be cutting nothing at all, while every process carried on talking and the case passed by
 * asserting that nothing broke. The cut has to be reaching something before any conclusion drawn from
 * it means anything.
 *
 * <p>So the reading is in two halves and both are needed. They became one cluster of three — the same
 * thing a two-member case asserts, and the half that fails if reporting a link address stops the
 * cluster forming at all. And every link carried connections — the half that fails if the members
 * found each other directly, which looks identical from the membership alone.
 */
class ThreeMembersJoinThroughCuttableLinksIT {

    @BeforeAll
    static void requireDocker() {
        DockerGate.require();
    }

    @Test
    void threeProcessesJoinThroughTheirLinksAndEveryLinkCarriesTraffic() {
        String store = SharedMongo.replicaSetUrl("e2e_three_members");
        List<String> nodes = List.of("node-a", "node-b", "node-c");

        try (PartitionableCluster cluster = PartitionableCluster.start(store, "e2e-three-members", nodes)) {
            List<String> membership = cluster.awaitMembers("node-a", 3);

            assertThat(membership)
                    .describedAs("the three joined. Reporting an address the others cannot reach, each "
                            + "would answer with itself alone")
                    .containsExactlyInAnyOrderElementsOf(nodes);
            for (String node : nodes) {
                assertThat(cluster.member(node).clusterMemberNodeIds())
                        .describedAs("%s answers the same membership rather than its own view of who it "
                                + "has met", node)
                        .containsExactlyInAnyOrderElementsOf(nodes);
            }
            assertThat(cluster.linkTo("node-a").carried())
                    .describedAs("connections the first member's link carried. It is dialled by both of "
                            + "the others, so nought here means they reached it directly and a cut would "
                            + "sever nothing — the failure this case exists to catch")
                    .isGreaterThan(0);
            assertThat(cluster.linkTo("node-b").carried() + cluster.linkTo("node-c").carried())
                    .describedAs("and the pair that does not involve the first member also went through "
                            + "a link, which is what a cut has to leave alone")
                    .isGreaterThan(0);
        }
    }
}
