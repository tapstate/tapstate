package io.tapstate.runtime.engine.join;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.spi.store.KeyedStateStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The join's state read from a member other than the one that wrote it, and read again after that
 * member has gone.
 *
 * <p>Every other case here runs one member, and one member cannot tell shared state from state that
 * merely lives where it was written. An implementation holding the mirrors and the index in ordinary
 * local maps answers every single-member case correctly and loses half the rows the moment the work
 * is spread out -- and it loses them silently, because a fact row whose dimension lookup found
 * nothing is indistinguishable from one whose dimension genuinely is not there.
 *
 * <p>Two members are also the smallest arrangement in which the two sides of a join can sit in
 * different partitions, which is the arrangement the join actually runs in: nothing pins the fact
 * mirror, the dimension mirror and the reverse index for one key to one member.
 *
 * <p>The control matters as much as the reading. A store that answered every namespace with
 * everything it had would satisfy "member two can see it" perfectly, so a second namespace is
 * written and its separateness asserted at the same time.
 */
class JoinStateIsSharedAcrossMembersTest {

    private static final String PIPELINE = "p1";
    private static final String STEP = "widen";
    private static final String OTHER_STEP = "narrow";
    private static final String DIMENSION = "c";

    /** Bounded, and generous: a loaded machine forms a cluster far slower than an idle one. */
    private static final long SETTLE_BUDGET_MS = 60_000;

    /**
     * A pair of ports for this run, well clear of the range a stray cluster occupies. Each run of
     * this class takes the next pair, so two of them overlapping in one build do not collide.
     */
    private static final java.util.concurrent.atomic.AtomicInteger BASE_PORT =
            new java.util.concurrent.atomic.AtomicInteger(
                    34_000 + 2 * new java.util.Random().nextInt(1_000));

    private HazelcastInstance one;
    private HazelcastInstance two;
    private SharedColdLayer cold;

    @BeforeEach
    void startTwoMembers() {
        cold = new SharedColdLayer();
        // A name nothing else answers to. This machine runs other clusters, and a member that joined
        // one of those would read and write somebody else's maps while every assertion here passed.
        String cluster = "join-two-member-" + UUID.randomUUID();

        // Explicit ports, and the member list names exactly these two.
        //
        // Not the default range, and not a scan. Left to discover for itself a member probes 5701
        // upward, and on any machine also running another cluster it meets a stranger there, refuses
        // it -- rightly, the cluster name differs -- and then PERMANENTLY BLACKLISTS that address.
        // The two members here would each sit alone in a cluster of one, which is the arrangement
        // this whole case exists to stop being the only one that ever gets tested. Measured on a
        // development machine with another server running: both members formed singleton clusters
        // and every reading below still had to fail on a timeout to say so.
        int base = BASE_PORT.getAndAdd(2);
        one = member(cluster, base, base);
        two = member(cluster, base + 1, base);
        awaitClusterOf(2);
    }

    private HazelcastInstance member(String cluster, int port, int base) {
        Config config = new Config();
        config.setClusterName(cluster);
        config.getJetConfig().setEnabled(false);
        config.getNetworkConfig().setPort(port).setPortAutoIncrement(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true)
                .addMember("127.0.0.1:" + base)
                .addMember("127.0.0.1:" + (base + 1));
        config.addMapConfig(JoinMaps.backedStateMaps(JoinMaps.DEFAULT_ENTRIES_HELD_IN_MEMORY));
        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);
        JoinStateMapStoreFactory.bindTo(instance, cold);
        return instance;
    }

    @AfterEach
    void stopBoth() {
        for (HazelcastInstance instance : new HazelcastInstance[] {one, two}) {
            if (instance != null && instance.getLifecycleService().isRunning()) {
                instance.shutdown();
            }
        }
    }

    @Test
    @DisplayName("what one member writes, the other member reads -- and only under the same namespace")
    void theOtherMemberSeesIt() {
        ImapJoinStores wrote = new ImapJoinStores(one, PIPELINE, STEP, 4);
        ImapJoinStores reads = new ImapJoinStores(two, PIPELINE, STEP, 4);
        ImapJoinStores elsewhere = new ImapJoinStores(two, PIPELINE, OTHER_STEP, 4);

        wrote.putFact("f1", row("id", 1L));
        wrote.putDimensionRow(DIMENSION, "d1", row("name", "Ada"));
        wrote.indexAdd(DIMENSION, "d1", "f1");

        assertThat(reads.fact("f1"))
                .as("the fact mirror is not the writing member's own map")
                .isEqualTo(row("id", 1L));
        assertThat(reads.dimensionRow(DIMENSION, "d1"))
                .as("nor is the dimension mirror -- this is the lookup an arriving fact row makes")
                .isEqualTo(row("name", "Ada"));
        assertThat(reads.indexPage(DIMENSION, "d1", 0))
                .as("nor the reverse index, which is how a changed dimension row finds its fact rows")
                .containsExactly("f1");

        // The control. Without it, a store answering every namespace with everything satisfies
        // all three readings above.
        assertThat(elsewhere.fact("f1"))
                .as("a different step is a different namespace and must not see it")
                .isNull();
    }

    @Test
    @DisplayName("and it is still readable once the member that wrote it has left")
    void itSurvivesTheWriterLeaving() {
        ImapJoinStores wrote = new ImapJoinStores(one, PIPELINE, STEP, 4);
        wrote.putFact("f1", row("id", 1L));
        wrote.putDimensionRow(DIMENSION, "d1", row("name", "Ada"));
        wrote.indexAdd(DIMENSION, "d1", "f1");

        // Before killing anything: the cluster has to have finished moving data around, or this
        // asserts nothing about durability and everything about timing.
        awaitClusterSafe();
        one.shutdown();
        awaitClusterOf(1);

        ImapJoinStores survivor = new ImapJoinStores(two, PIPELINE, STEP, 4);
        assertThat(survivor.fact("f1")).isEqualTo(row("id", 1L));
        assertThat(survivor.dimensionRow(DIMENSION, "d1")).isEqualTo(row("name", "Ada"));
        assertThat(survivor.indexPage(DIMENSION, "d1", 0)).containsExactly("f1");
    }

    private void awaitClusterOf(int size) {
        long deadline = System.currentTimeMillis() + SETTLE_BUDGET_MS;
        HazelcastInstance alive = (two != null && two.getLifecycleService().isRunning()) ? two : one;
        while (System.currentTimeMillis() < deadline) {
            if (alive.getCluster().getMembers().size() == size) {
                return;
            }
            sleep();
        }
        throw new AssertionError("the cluster never reached " + size
                + " member(s); it has " + alive.getCluster().getMembers().size());
    }

    private void awaitClusterSafe() {
        long deadline = System.currentTimeMillis() + SETTLE_BUDGET_MS;
        while (System.currentTimeMillis() < deadline) {
            if (one.getPartitionService().isClusterSafe()) {
                return;
            }
            sleep();
        }
        throw new AssertionError("the cluster never reported itself safe, so nothing here would be "
                + "a statement about surviving a member leaving");
    }

    private static void sleep() {
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

    /**
     * One cold layer behind both members, which is what a real deployment has.
     *
     * <p>Concurrent by construction: two members write it from their own threads, and a plain map
     * here loses writes that nothing reports -- the symptom is a missing row, which reads exactly
     * like the product losing it.
     */
    private static final class SharedColdLayer implements KeyedStateStore {

        private final Map<String, byte[]> entries = new ConcurrentHashMap<>();

        private static String at(String namespace, String key) {
            return namespace + " " + key;
        }

        @Override
        public Optional<byte[]> load(String namespace, String key) {
            return Optional.ofNullable(entries.get(at(namespace, key)));
        }

        @Override
        public void save(String namespace, String key, byte[] state) {
            entries.put(at(namespace, key), state);
        }

        @Override
        public void delete(String namespace, String key) {
            entries.remove(at(namespace, key));
        }

        @Override
        public void dropNamespace(String namespace) {
            entries.keySet().removeIf(at -> at.startsWith(namespace + " "));
        }

        @Override
        public long count(String namespace) {
            return entries.keySet().stream().filter(at -> at.startsWith(namespace + " ")).count();
        }

        @Override
        public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
            Map<String, byte[]> loaded = new LinkedHashMap<>();
            for (String key : keys) {
                byte[] state = entries.get(at(namespace, key));
                if (state != null) {
                    loaded.put(key, state);
                }
            }
            return loaded;
        }
    }
}
