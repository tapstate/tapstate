package io.tapstate.app;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.control.core.LiveClusterMember;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

/**
 * A node's identity travels with membership, so every member holds every other's and the topology reads
 * the same from either of them. That is the property the read face is built on: a reader who has to know
 * which node they reached in order to trust the answer has been given a node, not a cluster.
 *
 * <p>Two members, really started, because one member cannot show it -- the question is what a member
 * knows about somebody else, and a single member has nobody else to know anything about.
 */
class EveryMemberCanSayWhoTheOthersAreTest {

    @Test
    void eitherMemberAnswersBothIdentitiesAndTheyAgree() throws Exception {
        int[] ports = twoFreePorts();
        HazelcastInstance first = start(ports[0], ports[0], "node-a", "boot-a1", "https://a.example:8443");
        HazelcastInstance second = null;
        try {
            second = start(ports[1], ports[0], "node-b", "boot-b1", "https://b.example:8443");
            awaitMembers(first, 2);
            awaitMembers(second, 2);

            List<LiveClusterMember> fromFirst = new HazelcastLiveClusterMembers(first).members();
            List<LiveClusterMember> fromSecond = new HazelcastLiveClusterMembers(second).members();

            assertThat(fromFirst)
                    .as("the same answer from either member, which is what makes it the cluster's answer")
                    .containsExactlyInAnyOrderElementsOf(fromSecond);
            assertThat(fromFirst)
                    .extracting(LiveClusterMember::nodeId, LiveClusterMember::bootId,
                            LiveClusterMember::controlUrl)
                    .as("each member's stable id, this boot of it, and where a client reaches its control "
                            + "face -- carried on the member itself, so nobody had to be asked")
                    .containsExactlyInAnyOrder(
                            tuple("node-a", "boot-a1", "https://a.example:8443"),
                            tuple("node-b", "boot-b1", "https://b.example:8443"));
            assertThat(fromFirst).extracting(LiveClusterMember::memberUuid)
                    .as("the runtime identity is each member's own; two nodes sharing one would make a "
                            + "restarted node indistinguishable from the one it replaced")
                    .doesNotHaveDuplicates()
                    .doesNotContainNull();
            assertThat(fromFirst).extracting(LiveClusterMember::hzAddress)
                    .as("and the engine address is separate from the control URL, because they are "
                            + "different addresses and a reader must never dial one for the other")
                    .doesNotContainNull();
        } finally {
            if (second != null) {
                second.shutdown();
            }
            first.shutdown();
        }
    }

    /** A member started the way the assembly starts one, identity written on before it joins. */
    private static HazelcastInstance start(
            int memberPort, int seedPort, String nodeId, String bootId, String controlUrl) {
        HazelcastProperties properties = new HazelcastProperties();
        properties.setClusterName("member-identity-test");
        properties.setMemberPort(memberPort);
        properties.getDiscovery().setMode(HazelcastProperties.DiscoveryMode.TCP_IP);
        properties.getDiscovery().getTcpIp().setSeeds(List.of("127.0.0.1:" + seedPort));
        ClusterMemberPreflight.Identity identity = new ClusterMemberPreflight.Identity(
                "member-identity-test", nodeId, URI.create(controlUrl), nodeSession(nodeId, bootId));
        return HazelcastConfiguration.startMember(() -> Hazelcast.newHazelcastInstance(
                HazelcastConfiguration.identify(
                        HazelcastConfiguration.memberConfig(properties), identity)));
    }

    private static WorkloadClaim nodeSession(String nodeId, String bootId) {
        return new WorkloadClaim(
                new WorkloadClaimKey("member-identity-test", WorkloadClaimType.NODE_SESSION, nodeId),
                new WorkloadOwner(nodeId, bootId), 1, 0, 1, Instant.now().plusSeconds(30));
    }

    private static int[] twoFreePorts() throws Exception {
        try (ServerSocket first = new ServerSocket(0); ServerSocket second = new ServerSocket(0)) {
            return new int[] {first.getLocalPort(), second.getLocalPort()};
        }
    }

    private static void awaitMembers(HazelcastInstance member, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
        while (member.getCluster().getMembers().size() != expected && System.nanoTime() < deadline) {
            Thread.sleep(25);
        }
        assertThat(member.getCluster().getMembers())
                .as("both members have to be in before either can be asked about the other")
                .hasSize(expected);
    }
}
