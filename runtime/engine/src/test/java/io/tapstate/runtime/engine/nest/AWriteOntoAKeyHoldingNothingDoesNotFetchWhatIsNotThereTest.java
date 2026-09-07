package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.spi.store.KeyedStateStore;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * What the two write forms cost, and why each is used where it is.
 *
 * <p>A carried write is handed the current entry, so on a key that is not resident the substrate fetches it
 * from the layer behind the map before the processor runs - and that processor overwrites what it is handed
 * without reading it. Told that a read has just found nothing under the key, the store puts the state across
 * the map instead, which fetches nothing.
 *
 * <p><b>Both halves are asserted, because either one alone reads as the whole answer.</b> The trip removed
 * is worth a copy of the state only where the state is what a drain just brought; on a key already holding a
 * document, the same put would copy everything that key has ever absorbed and save no trip at all. So the
 * case that says the trip is gone stands next to the case that says the ordinary write still carries - and
 * the ordinary write is measured on the same map, against the same store, one call apart.
 */
class AWriteOntoAKeyHoldingNothingDoesNotFetchWhatIsNotThereTest {

    private static final String NAMESPACE = "nest.p.step.$root";

    private static final AtomicLong LOADS = new AtomicLong();
    private static final AtomicLong SAVES = new AtomicLong();
    private static final AtomicLong COPIES = new AtomicLong();

    private HazelcastInstance member;
    private MapNestStore<CountedState> store;

    @BeforeEach
    void startMember() {
        Config config = new Config();
        config.setClusterName("write-onto-nothing-test-" + System.nanoTime());
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.addMapConfig(NestSettings.defaults().backedStateMaps());
        member = Hazelcast.newHazelcastInstance(config);
        NestStateMapStoreFactory.bindTo(member, new CountingColdLayer());
        store = new MapNestStore<>(member.getMap(NAMESPACE));
        LOADS.set(0);
        SAVES.set(0);
        COPIES.set(0);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("a write told the key holds nothing makes no trip behind the map")
    void aWriteToldTheKeyHoldsNothingFetchesNothing() {
        store.save("told", new CountedState(), true);

        assertThat(LOADS.get())
                .describedAs("the write fetched what a read had already established is not there, which is "
                        + "the whole of what the word it was given exists to stop. It is invisible from "
                        + "every other angle: the state is stored, the value is right, and the only trace "
                        + "is a round trip to another process per key per residency")
                .isZero();
        assertThat(SAVES.get())
                .describedAs("the state has to reach the layer behind the map like any other write - a "
                        + "form that skipped that would lose it on the first eviction")
                .isOne();
    }

    @Test
    @DisplayName("a write not told that makes the trip, which is what the word is worth")
    void aWriteNotToldMakesTheTripThatIsBeingRemoved() {
        store.save("untold", new CountedState());

        assertThat(LOADS.get())
                .describedAs("the carried write onto a key that is not resident no longer fetches the "
                        + "entry, so the trip the other case says is gone was never there to remove and "
                        + "that case is passing for free")
                .isOne();
    }

    @Test
    @DisplayName("what each write form copies, and why the put stays where it is")
    void theOrdinaryWriteCarriesTheStateWhereThePutWouldCopyItTwiceOver() {
        store.save("onto-nothing", new CountedState(), true);
        long ontoNothing = COPIES.getAndSet(0);

        store.save("resident", new CountedState(), true);
        COPIES.set(0);
        store.save("resident", new CountedState());
        long carried = COPIES.getAndSet(0);

        store.save("resident", new CountedState(), true);
        long putOverAState = COPIES.get();

        assertThat(ontoNothing)
                .describedAs("a put onto a key holding nothing copied the state %d times: one handed to "
                        + "the map, one written to the layer behind it. The second is what both forms pay, "
                        + "so the first is the whole price of the trip it removes", ontoNothing)
                .isEqualTo(2);
        assertThat(carried)
                .describedAs("a carried write copied the state %d times. One is the write to the layer "
                        + "behind the map; the map itself is handed no copy at all, which is what makes "
                        + "this the form every write after the first uses", carried)
                .isOne();
        assertThat(putOverAState)
                .describedAs("a put onto a key already holding a document copied the state %d times, "
                        + "against the carried write's %d on the same key. It is three because the "
                        + "operation renders the value being replaced as well as the one replacing it - so "
                        + "the extra copies grow with everything that key has ever absorbed, and grow on "
                        + "every event rather than once per residency. That is the arithmetic keeping this "
                        + "form to the key holding nothing", putOverAState, carried)
                .isEqualTo(3);
    }

    /** A state that says how many times it has been copied. */
    private static final class CountedState implements Serializable {

        private static final long serialVersionUID = 1L;

        private final long[] payload = new long[64];

        private void writeObject(ObjectOutputStream out) throws IOException {
            COPIES.incrementAndGet();
            out.defaultWriteObject();
        }
    }

    /** A cold layer that answers nothing and counts what it was asked. */
    private static final class CountingColdLayer implements KeyedStateStore, Serializable {

        private static final long serialVersionUID = 1L;

        private final Map<String, byte[]> held = new LinkedHashMap<>();

        @Override
        public synchronized Optional<byte[]> load(String namespace, String key) {
            LOADS.incrementAndGet();
            return Optional.ofNullable(held.get(key));
        }

        @Override
        public synchronized void save(String namespace, String key, byte[] state) {
            SAVES.incrementAndGet();
            held.put(key, state);
        }

        @Override
        public synchronized void delete(String namespace, String key) {
            held.remove(key);
        }

        @Override
        public synchronized void dropNamespace(String namespace) {
            held.clear();
        }

        @Override
        public synchronized long count(String namespace) {
            return held.size();
        }
    }
}
