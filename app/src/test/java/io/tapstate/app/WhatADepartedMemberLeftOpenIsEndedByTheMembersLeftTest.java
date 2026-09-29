package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.lang.reflect.Proxy;
import java.net.ServerSocket;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;

/**
 * A member that leaves the cluster has what it left open at the store ended by the members left, as soon as they
 * see it go - asked of the store by the start of the process it was, which is what its store client was named for.
 *
 * <p>Two members, really started the way the assembly starts one, and one of them killed: what is at stake is what
 * a member does when another goes, and nothing about it can be seen with one.
 */
class WhatADepartedMemberLeftOpenIsEndedByTheMembersLeftTest {

    @Test
    void theMembersLeftAskTheStoreToEndWhatTheDepartedStartOfTheProcessLeftOpen() throws Exception {
        int[] ports = twoFreePorts();
        List<String> asked = new CopyOnWriteArrayList<>();
        HazelcastInstance first = start(ports[0], ports[0], "node-a", "boot-a1");
        HazelcastInstance second = null;
        try {
            HazelcastConfiguration.endWhatDepartedMembersLeaveOpen(first, storeRecording(asked));
            second = start(ports[1], ports[0], "node-b", "boot-b1");
            awaitMembers(first, 2);
            assertThat(asked).as("a member joining leaves nothing open that anybody waits on").isEmpty();

            second.getLifecycleService().terminate();

            long deadline = System.nanoTime() + Duration.ofSeconds(30).toNanos();
            while (asked.isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(25);
            }
            assertThat(asked)
                    .as("the store is asked about the start of the process that went, and about nobody else")
                    .containsExactly("boot-b1");
        } finally {
            if (second != null && second.getLifecycleService().isRunning()) {
                second.getLifecycleService().terminate();
            }
            first.shutdown();
        }
    }

    /** A member started the way the assembly starts one, identity written on before it joins. */
    private static HazelcastInstance start(int memberPort, int seedPort, String nodeId, String bootId) {
        HazelcastProperties properties = new HazelcastProperties();
        properties.setClusterName("departed-member-test");
        properties.setMemberPort(memberPort);
        properties.getDiscovery().setMode(HazelcastProperties.DiscoveryMode.TCP_IP);
        properties.getDiscovery().getTcpIp().setSeeds(List.of("127.0.0.1:" + seedPort));
        ClusterMemberPreflight.Identity identity = new ClusterMemberPreflight.Identity("departed-member-test",
                nodeId, URI.create("https://" + nodeId + ".example:8443"), nodeSession(nodeId, bootId));
        return HazelcastConfiguration.startMember(() -> Hazelcast.newHazelcastInstance(
                HazelcastConfiguration.identify(HazelcastConfiguration.memberConfig(properties), identity)));
    }

    private static WorkloadClaim nodeSession(String nodeId, String bootId) {
        return new WorkloadClaim(
                new WorkloadClaimKey("departed-member-test", WorkloadClaimType.NODE_SESSION, nodeId),
                new WorkloadOwner(nodeId, bootId), 1, 0, 1, Instant.now().plusSeconds(30));
    }

    /** A store that only records whom it was asked to end transactions for; asked anything else, it refuses. */
    private static StorePort storeRecording(List<String> asked) {
        return (StorePort) Proxy.newProxyInstance(StorePort.class.getClassLoader(), new Class<?>[] {StorePort.class},
                (proxy, method, args) -> {
                    if (method.getDeclaringClass() == Object.class) {
                        return switch (method.getName()) {
                            case "equals" -> proxy == args[0];
                            case "hashCode" -> System.identityHashCode(proxy);
                            default -> "a store recording whom it is asked to end transactions for";
                        };
                    }
                    if (method.getName().equals("endTransactionsLeftOpenBy")) {
                        asked.add((String) args[0]);
                        return 0;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
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
        assertThat(member.getCluster().getMembers()).hasSize(expected);
    }
}
