package io.tapstate.runtime.engine.join;

import io.tapstate.spi.store.KeyedStateStore;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * What the bridge under a join map has to do, and the two ways it can look right while doing nothing.
 *
 * <ul>
 *   <li><b>It can answer a batch one key at a time.</b> Every value comes back, so nothing about the
 *       result differs; a large recompute takes three orders of magnitude longer and the run reports
 *       only its own slowness. Witnessed by counting what the layer beneath was asked, not by timing.
 *   <li><b>It can count a batch as one reach.</b> The pressure ratio is a share of reaching that went
 *       to storage, and reaching is per key - counting a thousand-key batch as one loosens the ratio by
 *       the batch size, and the alarm goes on reporting a healthy namespace for ever.
 * </ul>
 */
class JoinStateMapStoreTest {

    @Test
    void aBatchIsAskedOfTheLayerBeneathAsOneBatch() {
        RecordingStore store = new RecordingStore();
        store.put("ns", "kO1", "state of O1");
        store.put("ns", "kO2", "state of O2");
        JoinStateMapStore bridge = bridge("ns", store, new JoinStateStats());

        Map<Object, Object> loaded = bridge.loadAll(List.of("O1", "O2"));

        assertThat(loaded).containsEntry("O1", "state of O1").containsEntry("O2", "state of O2");
        assertThat(store.batches).hasSize(1);
        assertThat(store.singles).as("nothing fell back to the key-at-a-time read").isEmpty();
    }

    @Test
    void aKeyWithNoStateIsLeftOutOfTheAnswer() {
        RecordingStore store = new RecordingStore();
        store.put("ns", "kO1", "state of O1");
        JoinStateMapStore bridge = bridge("ns", store, new JoinStateStats());

        assertThat(bridge.loadAll(List.of("O1", "never-saved"))).containsOnlyKeys("O1");
    }

    /**
     * What comes back from the layer beneath is filed under rendered names; what the map wants back is
     * its own key objects. A bridge that answered under the names would hand the map keys it does not
     * have, and the map would treat every one of them as a miss it had just filled.
     */
    @Test
    void theAnswerComesBackUnderTheMapsOwnKeysNotTheRenderedNames() {
        RecordingStore store = new RecordingStore();
        ReverseBucket.At page = new ReverseBucket.At("C1", 2);
        store.put("ns", JoinStateKeys.nameOf(page), "the page");
        JoinStateMapStore bridge = bridge("ns", store, new JoinStateStats());

        assertThat(bridge.loadAll(List.of(page))).containsEntry(page, "the page");
    }

    @Test
    void theKeysOfANamespaceAreNeverListed() {
        JoinStateMapStore bridge = bridge("ns", new RecordingStore(), new JoinStateStats());

        assertThat(bridge.loadAllKeys()).isNotNull().isEmpty();
    }

    @Test
    void whatWasWrittenComesBackAsItWasWritten() {
        RecordingStore store = new RecordingStore();
        JoinStateMapStore bridge = bridge("ns", store, new JoinStateStats());

        bridge.store("O1", new ReverseBucket(List.of("a", "b"), 3));

        assertThat(bridge.load("O1")).isEqualTo(new ReverseBucket(List.of("a", "b"), 3));
    }

    /**
     * The item the batch read exists for, stated as a number. Keys and trips are separate counters, and
     * the pressure ratio is computed on keys - so a batch of five that went to storage is five reaches
     * served from storage, in one trip.
     */
    @Test
    void aBatchCountsItsKeysAsReachesAndItselfAsOneTrip() {
        RecordingStore store = new RecordingStore();
        JoinStateStats stats = new JoinStateStats();
        JoinStateMapStore bridge = bridge("ns", store, stats);

        bridge.loadAll(List.of("O1", "O2", "O3", "O4", "O5"));

        assertThat(stats.keysFromCold("ns")).isEqualTo(5);
        assertThat(stats.trips("ns")).isEqualTo(1);
    }

    @Test
    void aSingleReadIsOneKeyAndOneTrip() {
        RecordingStore store = new RecordingStore();
        JoinStateStats stats = new JoinStateStats();
        JoinStateMapStore bridge = bridge("ns", store, stats);

        bridge.load("O1");

        assertThat(stats.keysFromCold("ns")).isEqualTo(1);
        assertThat(stats.trips("ns")).isEqualTo(1);
    }

    /**
     * A mirror key and an index page key share one namespace's worth of names in the layer beneath, and
     * a mirror key is a rendering that can hold anything - including something shaped like a page name.
     * Two keys sharing a name is one reading and overwriting the other's state, with the right shape.
     */
    @Test
    void aMirrorKeyCannotBeReadAsAnIndexPageOrTheOtherWayAround() {
        String looksLikeAPage = "0/C1";

        assertThat(JoinStateKeys.nameOf(looksLikeAPage))
                .isNotEqualTo(JoinStateKeys.nameOf(new ReverseBucket.At("C1", 0)));
        assertThat(JoinStateKeys.nameOf(new ReverseBucket.At("C1", 10)))
                .isNotEqualTo(JoinStateKeys.nameOf(new ReverseBucket.At("C1", 1)));
        assertThat(JoinStateKeys.nameOf(new ReverseBucket.At("1/C1", 0)))
                .as("the first slash separates, so a key holding one still cannot be read across")
                .isNotEqualTo(JoinStateKeys.nameOf(new ReverseBucket.At("C1", 1)));
    }

    @Test
    void aKeyOfAKindThisLayerCannotNameCrashesRatherThanBeingGivenOne() {
        assertThatThrownBy(() -> JoinStateKeys.nameOf(42))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("no name in the state layer");
    }

    private static JoinStateMapStore bridge(String namespace, KeyedStateStore store,
            JoinStateStats stats) {
        return new JoinStateMapStore(namespace, store, stats);
    }

    /** A cold layer that says what it was asked, and how. */
    private static final class RecordingStore implements KeyedStateStore {

        private final Map<String, byte[]> entries = new HashMap<>();
        private final List<String> singles = new ArrayList<>();
        private final List<Collection<String>> batches = new ArrayList<>();

        void put(String namespace, String key, Object state) {
            entries.put(namespace + " " + key, JoinStateMapStoreTest.bytes(state));
        }

        @Override
        public Optional<byte[]> load(String namespace, String key) {
            singles.add(key);
            return Optional.ofNullable(entries.get(namespace + " " + key));
        }

        @Override
        public Map<String, byte[]> loadAll(String namespace, Collection<String> keys) {
            batches.add(List.copyOf(keys));
            Map<String, byte[]> loaded = new LinkedHashMap<>();
            for (String key : keys) {
                byte[] state = entries.get(namespace + " " + key);
                if (state != null) {
                    loaded.put(key, state);
                }
            }
            return loaded;
        }

        @Override
        public void save(String namespace, String key, byte[] state) {
            entries.put(namespace + " " + key, state);
        }

        @Override
        public void delete(String namespace, String key) {
            entries.remove(namespace + " " + key);
        }

        @Override
        public void dropNamespace(String namespace) {
            entries.keySet().removeIf(id -> id.startsWith(namespace + " "));
        }

        @Override
        public long count(String namespace) {
            return entries.keySet().stream().filter(id -> id.startsWith(namespace + " ")).count();
        }
    }

    private static byte[] bytes(Object state) {
        java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream stream = new java.io.ObjectOutputStream(out)) {
            stream.writeObject(state);
        } catch (java.io.IOException cause) {
            throw new IllegalStateException(cause);
        }
        return out.toByteArray();
    }
}
