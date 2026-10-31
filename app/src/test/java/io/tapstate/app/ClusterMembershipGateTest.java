package io.tapstate.app;

import io.tapstate.spi.store.ClusterMembership;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.List;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicBoolean;
import com.hazelcast.cluster.Member;

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

    /**
     * Two readings of one predicate, and the one that admits work waits for the one that serves it.
     *
     * <p>This is not symmetry for its own sake. The cluster library caches its reading and recomputes
     * it when the membership changes and on a timer of its own, so right after a cluster forms this
     * side says yes for up to a heartbeat interval while every ring and map still refuses. A run
     * admitted then reaches RUNNING and dies on its first write - and it dies permanently, for a
     * condition that clears itself seconds later. Measured on a two-member cluster: committed at
     * 23:14:18, admitted immediately, first ring write refused at 23:14:19, pipeline FAILED at 23:14:20.
     *
     * <p>Asserted in both directions, because only one of them is the defect: refusing while the data
     * plane refuses is the fix, and refusing after it agrees would be a gate that never opens.
     */
    @Test
    void workIsNotAdmittedWhileThisMembersOwnDataPlaneWouldRefuseIt() {
        ClusterMembershipGate gate = productionGate();
        gate.install(new ClusterMembership("cluster-a", 1, Set.of("a", "b", "c")));
        assertThat(gate.canCommit(Set.of("a", "b")))
                .describedAs("this side's own answer, which is yes")
                .isTrue();

        gate.observeDataPlane(() -> false);
        assertThat(gate.businessEligible())
                .describedAs("the data plane has not caught up, so nothing may be taken on yet")
                .isFalse();

        gate.observeDataPlane(() -> true);
        assertThat(gate.businessEligible())
                .describedAs("and once it agrees, this member takes work on again without anything "
                        + "else having to change")
                .isTrue();
    }

    private static ClusterMembershipGate productionGate() {
        ClusterMembershipGate gate = unboundProductionGate();
        gate.observeNodeSession(() -> true);
        return gate;
    }

    private static ClusterMembershipGate unboundProductionGate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        properties.setBootstrapMinMembers(3);
        return new ClusterMembershipGate(properties);
    }

    @Test
    void aCommittedMajorityCannotAdmitWorkBeforeItsLocalSessionIsBound() {
        ClusterMembershipGate gate = unboundProductionGate();
        gate.install(new ClusterMembership("cluster-a", 1, Set.of("a", "b", "c")));
        assertThat(gate.apply(List.of(member("a"), member("b")))).isFalse();
        assertThat(gate.businessEligible()).isFalse();
        assertThat(gate.canCommit(Set.of("a", "b"))).isFalse();

        gate.observeNodeSession(() -> true);
        assertThat(gate.apply(List.of(member("a"), member("b")))).isTrue();
        assertThat(gate.businessEligible()).isTrue();
        assertThat(gate.canCommit(Set.of("a", "b"))).isTrue();
    }

    @Test
    void anUnprovedLocalNodeSessionRefusesWorkAndMembershipWritesBeforeTheMemberStops() {
        ClusterMembershipGate gate = productionGate();
        AtomicBoolean current = new AtomicBoolean(true);
        gate.observeNodeSession(current::get);
        gate.install(new ClusterMembership("cluster-a", 1, Set.of("a", "b", "c")));
        List<Member> members = List.of(member("a"), member("b"));
        assertThat(gate.apply(members)).isTrue();
        assertThat(gate.businessEligible()).isTrue();
        assertThat(gate.canCommit(Set.of("a", "b"))).isTrue();

        current.set(false);

        assertThat(gate.apply(members)).isFalse();
        assertThat(gate.businessEligible()).isFalse();
        assertThat(gate.canCommit(Set.of("a", "b"))).isFalse();
    }

    private static Member member(String nodeId) {
        return (Member) Proxy.newProxyInstance(Member.class.getClassLoader(), new Class<?>[]{Member.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getAttribute")) return nodeId;
                    throw new AssertionError("unexpected member metadata call: " + method.getName());
                });
    }
}
