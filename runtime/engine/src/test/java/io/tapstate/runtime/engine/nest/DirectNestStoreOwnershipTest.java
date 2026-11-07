package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The two rules that make a per-member resident copy safe without any invalidation protocol: residency is
 * dropped when the execution generation changes, and a namespace another vertex owns is read through to
 * the store and never kept.
 *
 * <p>Two members that never join each other stand for "the old owner" and "the new owner": they share the
 * store (one per cluster name for a namespace with nothing behind it), which is all either rule is about.
 */
class DirectNestStoreOwnershipTest {

    private static final String NAMESPACE = "nest.p.doc.customer";
    private static final Object KEY = List.of("C1");

    private HazelcastInstance oldOwner;
    private HazelcastInstance newOwner;

    @BeforeEach
    void start() {
        String cluster = "direct-ownership-" + System.nanoTime();
        oldOwner = Hazelcast.newHazelcastInstance(alone(cluster));
        newOwner = Hazelcast.newHazelcastInstance(alone(cluster));
    }

    private static Config alone(String cluster) {
        Config config = new Config();
        config.setClusterName(cluster);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        return config;
    }

    @AfterEach
    void stop() {
        oldOwner.shutdown();
        newOwner.shutdown();
        DirectNestStore.forgetUnbacked();
    }

    @Test
    @DisplayName("a resident copy is dropped when the generation changes, and only then")
    void residencyIsDroppedOnGenerationChange() {
        NestStore<Map<String, Object>> before = DirectNestStore.on(oldOwner, NAMESPACE, 1L);
        assertThat(before.load(KEY)).describedAs("nothing filed yet").isNull();

        // Ownership moved: another member, in the next generation, files the row.
        NestStore<Map<String, Object>> moved = DirectNestStore.on(newOwner, NAMESPACE, 2L);
        moved.save(KEY, Map.of("name", "Grace"));
        moved.commit();

        assertThat(DirectNestStore.<Map<String, Object>>on(oldOwner, NAMESPACE, 1L).load(KEY))
                .describedAs("within one generation the resident copy stands; this is the hazard the "
                        + "generation rule exists for, since ownership cannot move without a new generation")
                .isNull();
        assertThat(DirectNestStore.<Map<String, Object>>on(oldOwner, NAMESPACE, 3L).load(KEY))
                .describedAs("a new generation starts with empty residency and reads the store")
                .containsEntry("name", "Grace");
    }

    @Test
    @DisplayName("a namespace read from a member that does not own it always reads the store and keeps nothing")
    void readThroughSeesEveryCommitAndRefusesWrites() {
        NestStore<Map<String, Object>> owner = DirectNestStore.on(newOwner, NAMESPACE, 1L);
        NestStore<Map<String, Object>> reader = DirectNestStore.readThrough(oldOwner, NAMESPACE);

        owner.save(KEY, Map.of("name", "Ada"));
        assertThat(reader.load(KEY)).describedAs("uncommitted changes are the owner's alone").isNull();
        owner.commit();
        assertThat(reader.load(KEY)).containsEntry("name", "Ada");

        owner.save(KEY, Map.of("name", "Grace"));
        owner.commit();
        assertThat(reader.loadAll(List.of(KEY))).containsEntry(KEY, Map.of("name", "Grace"));
        assertThat(DirectNestStore.keysResidentOn(oldOwner, NAMESPACE)).isEmpty();

        assertThatThrownBy(() -> reader.save(KEY, Map.of("name", "Eve")))
                .isInstanceOf(IllegalStateException.class);
    }
}
