package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.tapstate.spi.store.KeyedStateStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A map's configuration has to exist before the map does, and until now that meant before the member
 * did: the shape of every nest state map was declared at start-up under a pattern covering all of them.
 * A number that belongs to one pipeline cannot be declared then, because no pipeline exists yet.
 *
 * <p>Adding the configuration later is the way out, and it was measured to fail for a reason that is not
 * the obvious one. A configuration added to a running member is broadcast to the cluster, so everything
 * in it has to survive being written down - and the store was being carried as <em>the object itself</em>.
 * The configuration was fine; the passenger was not.
 *
 * <p>So the store travels as a class name and is resolved where it lands. What that buys beyond this is a
 * live instance never had a way to reach a second member either: the same passenger would have been
 * missing there, and only once a cluster had more than one member to notice.
 */
class AStateMapMayBeConfiguredAfterTheMemberIsUpTest {

    private static final String NAMESPACE = "nest.p1.n1.orders";

    private HazelcastInstance member;

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    /** A member configured the way the assembly root configures one, with {@code store} behind its nests. */
    private HazelcastInstance memberWith(KeyedStateStore store) {
        Config config = new Config();
        config.setClusterName("nest-dynamic-config-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        member = Hazelcast.newHazelcastInstance(config);
        if (store != null) {
            NestStateMapStoreFactory.bindTo(member, store);
        }
        return member;
    }

    @Test
    void aStoreBackedConfigMayBeAddedOnceTheMemberIsRunning() {
        HazelcastInstance running = memberWith(new HeapKeyedStateStore());

        // The whole point: this is what threw before, and it threw while writing the config down rather
        // than while applying it - so nothing about the map was wrong, only what was riding along in it.
        assertThatCode(() -> running.getConfig()
                .addMapConfig(NestSettings.defaults().withEntriesHeldInMemory(271L).backedStateMaps(NAMESPACE)))
                .doesNotThrowAnyException();
    }

    @Test
    void aMapConfiguredThatWayStillReadsThroughToItsStore() {
        HeapKeyedStateStore store = new HeapKeyedStateStore();
        HazelcastInstance running = memberWith(store);
        running.getConfig().addMapConfig(NestSettings.defaults().withEntriesHeldInMemory(271L).backedStateMaps(NAMESPACE));

        IMap<Object, Object> map = running.getMap(NAMESPACE);
        map.put("k", "v");

        // The discriminating half. A config that was accepted but whose factory resolved no store would
        // pass the case above and leave the cold layer silently absent - writes going nowhere and reads
        // answering empty, with the configuration reading back as though a store were behind it.
        // Counted rather than looked up by name: what a key is called in the store is that layer's own
        // business, and an assertion spelling it out here would break on a change that broke nothing.
        assertThat(store.count(NAMESPACE)).isEqualTo(1L);
        map.evictAll();
        assertThat(map.get("k")).isEqualTo("v");
    }

    @Test
    void aMapThatNamesAStoreOnAMemberThatHasNoneFailsLoudly() {
        HazelcastInstance running = memberWith(null);
        running.getConfig().addMapConfig(NestSettings.defaults().withEntriesHeldInMemory(271L).backedStateMaps(NAMESPACE));

        // A wiring mistake rather than anything a user did, so it crashes bare rather than being laundered
        // into a code. Degrading to a map that quietly keeps nothing is the outcome worth refusing: the
        // state would be gone at the first eviction and nothing would have said so.
        assertThatThrownBy(() -> running.getMap(NAMESPACE).put("k", "v"))
                .hasStackTraceContaining("no nest state store");
    }
}
