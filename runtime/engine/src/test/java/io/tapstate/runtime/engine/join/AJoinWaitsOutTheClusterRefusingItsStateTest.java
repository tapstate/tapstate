package io.tapstate.runtime.engine.join;

import com.hazelcast.config.Config;
import com.hazelcast.config.SplitBrainProtectionConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionOn;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A join's state asked for while the cluster refuses it, which is what a member joining makes it do.
 *
 * <p>Operator state is guarded by the cluster's split brain protection. A member that has just joined takes
 * parts of the state over before its own protection verdict has caught up, and until it does every operation on
 * those parts is refused. On three hosts that refusal ended a running join every time a killed member came back:
 * the run that was meant to go on, with the returning member waiting for a rebalance, died instead - and was left
 * failed for a person when the member came back after the departure had stopped answering for it.
 *
 * <p>The window is reproduced the way it happens: a two-member cluster whose protection needs both, with one
 * member taken away while the state is asked for, and brought back while the asking waits. Nothing simulates the
 * refusal: the real protection refuses the real maps.
 */
class AJoinWaitsOutTheClusterRefusingItsStateTest {

    private static final String PROTECTION = "needs-both-members";
    private static final String PIPELINE = "p1";
    private static final String STEP = "widen";

    /** Bounded, and generous: a loaded machine forms a cluster far slower than an idle one. */
    private static final long SETTLE_BUDGET_MS = 60_000;

    /** A pair of ports for this run, below the ephemeral ranges, clear of the other two-member cases. */
    private static final AtomicInteger BASE_PORT = new AtomicInteger(24_000 + 2 * new Random().nextInt(1_000));

    private final String cluster = "join-refused-state-" + UUID.randomUUID();
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
        ImapJoinStores stores = new ImapJoinStores(one, PIPELINE, STEP, 4);
        stores.putFact("f1", row("id", 1L));
        awaitClusterSafe();

        // The cluster no longer qualifies, so every operation on the state is refused: the state a member is in
        // between joining and its own verdict agreeing, held still for as long as the case needs.
        two.shutdown();
        awaitMembers(1);
        Future<Map<String, Object>> asked = asking.submit(() -> {
            stores.putFact("f2", row("id", 2L));
            return stores.fact("f1");
        });

        two = member(base + 1, base);

        assertThat(asked.get(SETTLE_BUDGET_MS, TimeUnit.MILLISECONDS))
                .as("the operations the cluster refused waited for it and were then carried out, rather than "
                        + "ending a run that a member joining has to leave alone")
                .isEqualTo(row("id", 1L));
        assertThat(new ImapJoinStores(two, PIPELINE, STEP, 4).fact("f2"))
                .as("and the write refused along the way landed when the cluster let it through")
                .isEqualTo(row("id", 2L));
    }

    /** A member of a cluster whose join state is only reachable while both members are present. */
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
        config.addMapConfig(JoinMaps.stateMaps().setSplitBrainProtectionName(PROTECTION));
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

    /** The state written above has a backup before a member is taken away, so nothing is lost with it. */
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

    private static Map<String, Object> row(String key, Object value) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(key, value);
        return row;
    }
}
