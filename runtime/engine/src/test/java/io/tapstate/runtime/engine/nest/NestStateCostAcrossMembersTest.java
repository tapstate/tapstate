package io.tapstate.runtime.engine.nest;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.tapstate.runtime.engine.StateStoreCostProbe;
import io.tapstate.runtime.engine.StateStoreCostStats;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** A cold-store operation is measured where its partition executes, including a remote member. */
class NestStateCostAcrossMembersTest {

    private static final String NAMESPACE = "nest.state-cost.member.root";
    private static final AtomicInteger NEXT_PORT = new AtomicInteger(
            20_000 + 2 * new java.util.Random().nextInt(1_000));

    @Test
    void remotePartitionWriteAndLoadAreMeasuredOnlyOnItsMember() {
        int base = NEXT_PORT.getAndAdd(2);
        String cluster = "nest-state-cost-" + UUID.randomUUID();
        HeapKeyedStateStore cold = new HeapKeyedStateStore();
        HazelcastInstance one = null;
        HazelcastInstance two = null;
        try {
            one = member(cluster, base, base, cold);
            two = member(cluster, base + 1, base, cold);
            awaitMembers(one, 2);
            String remoteMember = two.getCluster().getLocalMember().getUuid().toString();
            List<String> key = null;
            for (int index = 0; index < 10_000; index++) {
                List<String> candidate = List.of("remote-" + index);
                if (one.getPartitionService().getPartition(candidate).getOwner().getUuid().toString()
                        .equals(remoteMember)) {
                    key = candidate;
                    break;
                }
            }
            assertThat(key).as("a nest key assigned to the remote member").isNotNull();
            IMap<List<String>, String> map = one.getMap(NAMESPACE);
            map.set(key, "payload");

            assertThat(StateStoreCostStats.of(one).reading(NAMESPACE)).isEmpty();
            StateStoreCostStats.Reading written = StateStoreCostStats.of(two)
                    .reading(NAMESPACE).orElseThrow();
            assertThat(written.operations().get(StateStoreCostProbe.Operation.SAVE).completed()).isOne();
            assertThat(written.operations().get(StateStoreCostProbe.Operation.SAVE).payloadBytes()).isPositive();
            assertThat(written.codecs().get(StateStoreCostProbe.Codec.ENCODE).completed()).isOne();

            map.evict(key);
            assertThat(map.get(key)).isEqualTo("payload");
            StateStoreCostStats.Reading loaded = StateStoreCostStats.of(two)
                    .reading(NAMESPACE).orElseThrow();
            assertThat(loaded.operations().get(StateStoreCostProbe.Operation.LOAD).completed()).isOne();
            assertThat(loaded.codecs().get(StateStoreCostProbe.Codec.DECODE).completed()).isOne();
            assertThat(loaded.operations().get(StateStoreCostProbe.Operation.LOAD).payloadBytes())
                    .isEqualTo(written.operations().get(StateStoreCostProbe.Operation.SAVE).payloadBytes());

            map.destroy();
            awaitForgotten(two);
        } finally {
            if (one != null && one.getLifecycleService().isRunning()) {
                one.shutdown();
            }
            if (two != null && two.getLifecycleService().isRunning()) {
                two.shutdown();
            }
        }
    }

    private static HazelcastInstance member(String cluster, int port, int base,
            HeapKeyedStateStore cold) {
        Config config = new Config();
        config.setClusterName(cluster);
        config.getJetConfig().setEnabled(false);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().setPort(port).setPortAutoIncrement(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                .addMember("127.0.0.1:" + base).addMember("127.0.0.1:" + (base + 1));
        config.addMapConfig(NestMaps.backedStateMaps(271));
        HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
        NestStateMapStoreFactory.bindTo(member, cold);
        return member;
    }

    private static void awaitMembers(HazelcastInstance member, int expected) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(45);
        while (System.nanoTime() < deadline) {
            if (member.getCluster().getMembers().size() == expected
                    && member.getPartitionService().isClusterSafe()) {
                return;
            }
            pause();
        }
        throw new AssertionError("the two nest members did not form a safe cluster");
    }

    private static void awaitForgotten(HazelcastInstance member) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (StateStoreCostStats.of(member).reading(NAMESPACE).isEmpty()) {
                return;
            }
            pause();
        }
        throw new AssertionError("destroyed nest map retained its cost account");
    }

    private static void pause() {
        try {
            Thread.sleep(50);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for nest member state", interrupted);
        }
    }
}
