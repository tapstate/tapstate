package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.spi.store.KeyedStateStore;
import java.io.Serializable;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * That a trip to the layer behind a map is counted where it happens.
 *
 * <p>It is asserted through a real map with a real store behind it, rather than by calling the store
 * directly, because what is actually in question is whether the substrate ever hands the store the member
 * it needs to count against. It does so at one specific moment and by one specific route: a store reached
 * through a factory is <b>not</b> given the member by the awareness interface (measured - the call is never
 * made), only by the loader's own lifecycle hook. Getting that wrong costs nothing that looks like a
 * failure: every read still works, every trip is still made, and the counters that were supposed to make
 * those trips visible simply stay at zero - which reads as a state layer serving everything from memory.
 */
class TheColdLayerCountsTheTripsItMakesTest {

    private static final String NAMESPACE = "nest.p.step.$root";

    private HazelcastInstance member;

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("a read that goes behind the map is counted, and its cost measured")
    void aReadThatMissesInMemoryIsCountedAgainstItsNamespace() {
        member = memberWith(new EmptyColdLayer());

        member.getMap(NAMESPACE).get("a-key-nothing-has-written");

        NestStateStats.Counted counted = NestStateStats.of(member).counted(NAMESPACE);
        assertThat(counted.backfills())
                .describedAs("the store made the trip; if this is zero the store never got the member and "
                        + "no trip will ever be counted, however many are made")
                .isOne();
        assertThat(counted.backfillNanos())
                .describedAs("what the trip cost is measured rather than assumed")
                .isNotNegative();
    }

    @Test
    void aMapWithNothingBehindItCountsNoTrips() {
        member = memberWith(null);

        member.getMap(NAMESPACE).get("a-key-nothing-has-written");

        // Nothing to go to, so nothing to count. The namespace is absent rather than present at zero: a map
        // with no cold layer serves every read from memory by construction, and has no ratio to report.
        assertThat(NestStateStats.of(member).knows(NAMESPACE)).isFalse();
    }

    private static HazelcastInstance memberWith(KeyedStateStore store) {
        Config config = new Config();
        config.setClusterName("cold-layer-trips-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        NestSettings settings = NestSettings.defaults();
        config.addMapConfig(store == null ? settings.stateMaps() : settings.backedStateMaps());
        HazelcastInstance started = Hazelcast.newHazelcastInstance(config);
        if (store != null) {
            NestStateMapStoreFactory.bindTo(started, store);
        }
        return started;
    }

    /** A cold layer holding nothing, so every read of it is a trip that comes back empty. */
    private static final class EmptyColdLayer implements KeyedStateStore, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public Optional<byte[]> load(String namespace, String key) {
            return Optional.empty();
        }

        @Override
        public void save(String namespace, String key, byte[] state) {
        }

        @Override
        public void delete(String namespace, String key) {
        }

        @Override
        public void dropNamespace(String namespace) {
        }

        @Override
        public long count(String namespace) {
            return 0L;
        }
    }
}
