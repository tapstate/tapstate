package io.tapstate.app;

import io.tapstate.spi.store.ClusterMembership;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class ClusterMembershipGateTest {

    @Test
    void threeMembersRequireTwoOfTheCommittedSet() {
        ClusterMembershipGate gate = productionGate();
        gate.install(new ClusterMembership("cluster-a", 1, Set.of("a", "b", "c")));

        assertThat(gate.canCommit(Set.of("a", "b"))).isTrue();
        assertThat(gate.businessEligible()).isTrue();
        assertThat(gate.canCommit(Set.of("a"))).isFalse();
        assertThat(gate.businessEligible()).isFalse();
    }

    @Test
    void fourMembersSplitTwoAndTwoFailsClosedOnBothSides() {
        ClusterMembershipGate left = productionGate();
        ClusterMembershipGate right = productionGate();
        ClusterMembership committed =
                new ClusterMembership("cluster-a", 7, Set.of("a", "b", "c", "d"));
        left.install(committed);
        right.install(committed);

        assertThat(left.canCommit(Set.of("a", "b"))).isFalse();
        assertThat(right.canCommit(Set.of("c", "d"))).isFalse();
        assertThat(left.businessEligible()).isFalse();
        assertThat(right.businessEligible()).isFalse();
    }

    @Test
    void aNewMemberDoesNotCountUntilTheOldCommittedMajorityCanCommitIt() {
        ClusterMembershipGate gate = productionGate();
        gate.install(new ClusterMembership("cluster-a", 3, Set.of("a", "b", "c")));

        assertThat(gate.canCommit(Set.of("c", "d")))
                .describedAs("one old member plus one uncommitted member is not an old-set majority")
                .isFalse();
        assertThat(gate.canCommit(Set.of("a", "b", "c", "d"))).isTrue();
    }

    @Test
    void thePredicateIsDerivedFromTheCommittedSetRatherThanAFixedMinimumOfTwo() {
        assertThat(ClusterMembershipGate.strictMajority(3, 2)).isTrue();
        assertThat(ClusterMembershipGate.strictMajority(3, 1)).isFalse();
        assertThat(ClusterMembershipGate.strictMajority(4, 2)).isFalse();
        assertThat(ClusterMembershipGate.strictMajority(4, 3)).isTrue();
    }

    private static ClusterMembershipGate productionGate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        properties.setBootstrapMinMembers(3);
        return new ClusterMembershipGate(properties);
    }
}
