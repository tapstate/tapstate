package io.tapstate.runtime.engine.nest;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A nest's state asked for while the cluster refuses it, as a member joining makes it do - the same window a
 * join's state meets, reached through the nest's own store. Reproduced the same way: a two-member cluster whose
 * protection needs both, one member taken away while the state is asked for and brought back while the asking
 * waits.
 */
class ANestWaitsOutTheClusterRefusingItsStateTest {

    private static final String PROTECTION = "needs-both-members";
    private static final String STATE = "nest-refused.p1.orders.state";
    private static final long SETTLE_BUDGET_MS = 60_000;
    private static final AtomicInteger BASE_PORT = new AtomicInteger(26_000 + 2 * new Random().nextInt(1_000));

    private final String cluster = "nest-refused-state-" + UUID.randomUUID();
    private final ExecutorService asking = Executors.newSingleThreadExecutor();
    private HazelcastInstance one;
    private HazelcastInstance two;

    @AfterEach
    void stopMembers() {
        asking.shutdownNow();
        for (HazelcastInstance member : new HazelcastInstance[] {one, two}) {
            if (member != null && member.getLifecycleService().isRunning()) {
                member.shutdown();
            }
        }
    }

    @Test
    void aStateOperationTheClusterRefusesWaitsForTheClusterInsteadOfEndingTheRun() throws Exception {
        int base = BASE_PORT.getAndAdd(2);
        one = member(base, base);
        two = member(base + 1, base);
        awaitMembers(2);
        MapNestStore<String> store = new MapNestStore<>(one.getMap(STATE));
        store.save("k1", "root one");
        awaitClusterSafe();

        two.shutdown();
        awaitMembers(1);
        Future<String> asked = asking.submit(() -> {
            store.save("k2", "root two");
            return store.load("k1");
        });

        two = member(base + 1, base);

        assertThat(asked.get(SETTLE_BUDGET_MS, TimeUnit.MILLISECONDS))
                .as("the operations the cluster refused waited for it and were then carried out")
                .isEqualTo("root one");
        assertThat(new MapNestStore<String>(two.getMap(STATE)).load("k2"))
                .as("and the write refused along the way landed when the cluster let it through")
                .isEqualTo("root two");
    }

    private HazelcastInstance member(int port, int base) {
        Config config = new Config();
        config.setClusterName(cluster);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getJetConfig().setEnabled(false);
        config.getNetworkConfig().setPort(port).setPortAutoIncrement(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                .setMembers(List.of("127.0.0.1:" + base, "127.0.0.1:" + (base + 1)));
        config.addSplitBrainProtectionConfig(new SplitBrainProtectionConfig(PROTECTION, true)
                .setProtectOn(SplitBrainProtectionOn.READ_WRITE)
                .setMinimumClusterSize(2));
        config.addMapConfig(new MapConfig("nest-refused.*")
                .setBackupCount(1)
                .setSplitBrainProtectionName(PROTECTION));
        return Hazelcast.newHazelcastInstance(config);
    }

    private void awaitMembers(int size) {
        long deadline = System.currentTimeMillis() + SETTLE_BUDGET_MS;
        while (System.currentTimeMillis() < deadline) {
            if (one.getCluster().getMembers().size() == size) {
                return;
            }
            pause();
        }
        throw new AssertionError("the cluster never reached " + size + " member(s); it has "
                + one.getCluster().getMembers().size());
    }

    private void awaitClusterSafe() {
        long deadline = System.currentTimeMillis() + SETTLE_BUDGET_MS;
        while (System.currentTimeMillis() < deadline) {
            if (one.getPartitionService().isClusterSafe()) {
                return;
            }
            pause();
        }
        throw new AssertionError("the cluster never reported itself safe");
    }

    private static void pause() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while waiting for the cluster", interrupted);
        }
    }
}
