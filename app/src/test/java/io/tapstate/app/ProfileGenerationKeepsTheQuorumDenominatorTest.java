package io.tapstate.app;

import com.hazelcast.cluster.Member;
import io.tapstate.spi.store.ClusterMembership;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class ProfileGenerationKeepsTheQuorumDenominatorTest {

    @Test
    void anOldProfilesMembershipCannotAuthorizeTheNewBoot() {
        ClusterMembershipGate gate = gate();
        gate.install(new ClusterMembership("east", 8, Set.of("a", "b", "c"), 6));
        gate.canCommit(Set.of("a", "b", "c"));
        assertThat(gate.businessEligible()).isFalse();
        assertThat(gate.committed()).isNull();

        gate.install(new ClusterMembership("east", 9, Set.of("a", "b", "c"), 7));
        assertThat(gate.businessEligible()).isTrue();
    }

    @Test
    void aJoiningMemberDelaysNewSubmissionsWithoutRevokingTheOldJobsQuorum() {
        ClusterMembershipGate gate = active(Set.of("a", "b", "c"));
        gate.canCommit(Set.of("a", "b", "c", "d"));
        assertThat(gate.businessEligible()).isTrue();
        assertThat(gate.submissionEligible(List.of(member("a", "H"), member("b", "H"),
                member("c", "H"), member("d", "H")))).isFalse();
    }

    @Test
    void aLostMemberRemainsInTheDenominatorButDoesNotPreventAMajorityRebuild() {
        ClusterMembershipGate gate = active(Set.of("a", "b", "c"));
        gate.canCommit(Set.of("a", "b"));
        assertThat(gate.submissionEligible(List.of(member("a", "H"), member("b", "H")))).isTrue();
        assertThat(gate.committed().activeNodeIds()).containsExactlyInAnyOrder("a", "b", "c");

        gate.canCommit(Set.of("a"));
        assertThat(gate.submissionEligible(List.of(member("a", "H")))).isFalse();
    }

    @Test
    void compatibleCountDoesNotHideAnIncompatibleMembersIdentity() {
        ClusterMembershipGate gate = active(Set.of("a", "b", "c"));
        gate.canCommit(Set.of("a", "b", "c"));
        assertThat(gate.submissionEligible(List.of(member("a", "H"), member("b", "H"),
                member("c", "another-profile")))).isFalse();
    }

    private static ClusterMembershipGate active(Set<String> nodes) {
        ClusterMembershipGate gate = gate();
        gate.install(new ClusterMembership("east", 9, nodes, 7));
        return gate;
    }

    private static ClusterMembershipGate gate() {
        ClusterProperties properties = new ClusterProperties();
        properties.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        properties.setBootstrapMinMembers(3);
        ClusterMembershipGate gate = new ClusterMembershipGate(properties);
        gate.bindProfile(7, "H");
        return gate;
    }

    private static Member member(String id, String hash) {
        UUID uuid = UUID.nameUUIDFromBytes(id.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return (Member) Proxy.newProxyInstance(Member.class.getClassLoader(), new Class<?>[] {Member.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getUuid" -> uuid;
                    case "isLiteMember" -> false;
                    case "getAttribute" -> switch ((String) args[0]) {
                        case ClusterMembershipGate.NODE_ID_ATTRIBUTE -> id;
                        case ClusterMembershipGate.PROFILE_GENERATION_ATTRIBUTE -> "7";
                        case ClusterMembershipGate.PROFILE_HASH_ATTRIBUTE -> hash;
                        default -> null;
                    };
                    case "hashCode" -> uuid.hashCode();
                    case "equals" -> proxy == args[0];
                    case "toString" -> id;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
