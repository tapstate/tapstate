package io.tapstate.runtime.engine.join;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.spi.store.KeyedStateStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A join's state on a real member, and the one thing about it that nothing else can show: that the
 * batch read is a batch all the way down.
 *
 * <p>The layer under these maps offers a read for many keys at once, and the bridge over it passes one
 * through. Neither is worth anything until something actually calls it: a run that asks key by key
 * answers identically and takes three orders of magnitude longer on a large recompute, and the only
 * signal is that it is slow. So the case below evicts the entries out of memory and counts what the
 * cold layer was asked - one call for a page, not one per key.
 */
class ImapJoinStoresTest {

    private static final String PIPELINE = "p1";
    private static final String STEP = "widen";
    private static final String DIMENSION = "c";

    private HazelcastInstance member;
    private RecordingColdLayer cold;
    private ImapJoinStores stores;

    @BeforeEach
    void startMember() {
        cold = new RecordingColdLayer();
        Config config = new Config();
        config.getJetConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config.addMapConfig(JoinMaps.backedStateMaps(JoinMaps.DEFAULT_ENTRIES_HELD_IN_MEMORY));
        member = Hazelcast.newHazelcastInstance(config);
        JoinStateMapStoreFactory.bindTo(member, cold);
        stores = new ImapJoinStores(member, PIPELINE, STEP, 4);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    @DisplayName("a fact row and a dimension row come back as they were written")
    void whatWasWrittenComesBack() {
        stores.putFact("f1", row("id", 1L));
        stores.putDimensionRow(DIMENSION, "d1", row("name", "Ada"));

        assertThat(stores.fact("f1")).isEqualTo(row("id", 1L));
        assertThat(stores.dimensionRow(DIMENSION, "d1")).isEqualTo(row("name", "Ada"));
        stores.removeFact("f1");
        assertThat(stores.fact("f1")).isNull();
    }

    /**
     * The one C13 asks for and the one nothing else can answer. The bridge and the port both offer a
     * batch; this is whether anything reaches them. Entries are pushed out of memory first, because a
     * read served from memory never reaches the layer at all and would pass whatever the caller did.
     *
     * <p><b>What it measured, which is not what a page-sized batch was expected to be.</b> A read of
     * many keys is split by the partition each key belongs to, and each partition asks the layer for
     * <em>its</em> keys alone. So the number of calls is bounded by the partition count rather than by
     * one: 8 keys across 271 partitions reach the layer as 8 calls of one key each - which is the
     * key-at-a-time read wearing the batch's name, and it passes any assertion written as "fewer calls
     * than keys" on a small enough sample.
     *
     * <p>The saving is therefore real only once a read carries many more keys than there are
     * partitions, and this case is sized to show that rather than to hide it.
     */
    @Test
    @DisplayName("a large read reaches the cold layer once per partition, never once per key")
    void aLargeReadReachesTheColdLayerOncePerPartitionNotOncePerKey() {
        int keyCount = 2_000;
        List<String> keys = new ArrayList<>(keyCount);
        for (int i = 0; i < keyCount; i++) {
            keys.add("f" + i);
            stores.putFact("f" + i, row("id", (long) i));
        }
        member.<String, Map<String, Object>>getMap(JoinMaps.factMirror(PIPELINE, STEP)).evictAll();
        cold.batches.clear();
        cold.singles.clear();

        Map<String, Map<String, Object>> found = stores.factsUnder(keys);

        assertThat(found).hasSize(keyCount);
        assertThat(cold.singles).as("nothing fell back to the key-at-a-time read").isEmpty();
        int keysAsked = cold.batches.stream().mapToInt(Collection::size).sum();
        // Not "exactly every key": pushing entries out of memory is best effort, and a handful measured
        // as still resident. What matters is that the bulk of them went to the layer and went in groups.
        assertThat(keysAsked).as("the layer really was reached, for nearly all of them")
                .isGreaterThan(keyCount / 2);
        // The partition count is the floor a read like this can reach, so this is "the batching is
        // working" rather than an arbitrary number. Written as a fraction of the keys, so a run that
        // quietly went back to one call per key reddens here.
        assertThat(cold.batches.size())
                .as("bounded by the partitions, not by the keys")
                .isLessThan(keyCount / 4);
        assertThat(keysAsked / cold.batches.size())
                .as("and a call carries several keys rather than one")
                .isGreaterThanOrEqualTo(4);
    }

    @Test
    @DisplayName("asking for no fact rows reaches neither the map nor the layer under it")
    void askingForNothingCostsNothing() {
        cold.batches.clear();

        assertThat(stores.factsUnder(List.of())).isEmpty();

        assertThat(cold.batches).isEmpty();
        assertThat(cold.singles).isEmpty();
    }

    /**
     * What the page walk costs, which is the one thing about it no other case here says. The walk ends
     * on the page after the last one, that page is by definition absent, and asking a read-through map
     * whether a key is there is a read: the answer for a key that is not in memory comes from the layer
     * under it. So every operation on a bucket pays one trip that is certain to find nothing.
     *
     * <p><b>The control arm is the head page, and it is what makes this a measurement.</b> It is asked
     * for in the same call and it is in memory, so it reaches the layer not at all - without that, a
     * layer reached once would be indistinguishable from a layer reached for everything.
     */
    @Test
    @DisplayName("counting a bucket's pages costs one trip to the layer, for the page that is not there")
    void theWalkPastTheLastPageIsATripToTheLayer() {
        stores.indexAdd(DIMENSION, "d1", "f0");
        cold.singles.clear();

        assertThat(stores.indexPageCount(DIMENSION, "d1")).isEqualTo(1);

        assertThat(cold.singles)
                .as("the page after the last, asked of the layer because it is nowhere in memory")
                .containsExactly("1/d1");
    }

    /**
     * The walk is what {@link ImapJoinStores#indexPageCount} and a removal need, and an append does
     * not: an append that starts below the end is told so by the page it tries, and moves on. Paying
     * the walk here bought nothing and cost a trip on every fact row that ever joined a bucket, which
     * on a first load is every fact row there is.
     */
    @Test
    @DisplayName("appending to a bucket reaches the layer not at all")
    void anAppendDoesNotWalkPastTheEnd() {
        stores.indexAdd(DIMENSION, "d1", "f0");
        cold.singles.clear();

        stores.indexAdd(DIMENSION, "d1", "f1");

        assertThat(cold.singles)
                .as("the head is in memory and nothing else has to be asked about")
                .isEmpty();
    }

    /**
     * A removal walks to the end to find the page holding the key, and the trailing-page trim that
     * follows it walks to the same end for the same reason. One walk answers both: nothing between
     * them opens a page, and a walk that starts low trims less rather than wrongly.
     */
    @Test
    @DisplayName("a removal walks past the end once, not once again for the trim behind it")
    void aRemovalWalksPastTheEndOnce() {
        stores.indexAdd(DIMENSION, "d1", "f0");
        stores.indexAdd(DIMENSION, "d1", "f1");
        cold.singles.clear();

        stores.indexRemove(DIMENSION, "d1", "f0");

        assertThat(cold.singles)
                .as("the page after the last, asked once for the whole removal")
                .containsExactly("1/d1");
    }

    @Test
    @DisplayName("a bucket fills a page and opens the next")
    void aBucketPagesAsItGrows() {
        for (int i = 0; i < 6; i++) {
            stores.indexAdd(DIMENSION, "d1", "f" + i);
        }

        assertThat(stores.indexPageCount(DIMENSION, "d1")).isEqualTo(2);
        assertThat(stores.indexPage(DIMENSION, "d1", 0)).containsExactly("f0", "f1", "f2", "f3");
        assertThat(stores.indexPage(DIMENSION, "d1", 1)).containsExactly("f4", "f5");
    }

    /**
     * The head's count is bumped after the append it describes, so a lost bump leaves it low. Reading
     * it as the truth would hide whole pages of fact rows - a fan-out that stops part way with nothing
     * saying so - which is why the count is probed rather than trusted.
     */
    @Test
    @DisplayName("a page-count hint left behind still finds every page")
    void aStaleHintDoesNotHideAPage() {
        for (int i = 0; i < 10; i++) {
            stores.indexAdd(DIMENSION, "d1", "f" + i);
        }
        assertThat(stores.indexPageCount(DIMENSION, "d1")).isEqualTo(3);

        // What a bump that never landed leaves: the head saying there is one page while there are three.
        member.<ReverseBucket.At, ReverseBucket>getMap(JoinMaps.reverseIndex(PIPELINE, STEP, DIMENSION))
                .set(new ReverseBucket.At("d1", 0),
                        new ReverseBucket(stores.indexPage(DIMENSION, "d1", 0), 0));

        assertThat(stores.indexPageCount(DIMENSION, "d1"))
                .as("probed upwards from the hint, so the pages past it are still there")
                .isEqualTo(3);
        assertThat(stores.indexPage(DIMENSION, "d1", 2)).containsExactly("f8", "f9");
    }

    @Test
    @DisplayName("an emptied trailing page is closed and an emptied bucket leaves no entry at all")
    void emptiedPagesAreClosed() {
        for (int i = 0; i < 6; i++) {
            stores.indexAdd(DIMENSION, "d1", "f" + i);
        }

        stores.indexRemove(DIMENSION, "d1", "f4");
        stores.indexRemove(DIMENSION, "d1", "f5");

        assertThat(stores.indexPageCount(DIMENSION, "d1")).isEqualTo(1);
        for (int i = 0; i < 4; i++) {
            stores.indexRemove(DIMENSION, "d1", "f" + i);
        }
        assertThat(stores.indexPageCount(DIMENSION, "d1")).isZero();
        assertThat(member.getMap(JoinMaps.reverseIndex(PIPELINE, STEP, DIMENSION)).size())
                .as("a bucket that outlived its rows is an entry spent on nothing").isZero();
    }

    /**
     * The head has to survive its own page emptying while pages follow it: it is the only thing naming
     * them, and losing it answers that this dimension key has no fact rows while page 1 still holds
     * some.
     */
    @Test
    @DisplayName("the head outlives its own keys while pages still follow it")
    void theHeadOutlivesItsOwnKeys() {
        for (int i = 0; i < 6; i++) {
            stores.indexAdd(DIMENSION, "d1", "f" + i);
        }

        for (int i = 0; i < 4; i++) {
            stores.indexRemove(DIMENSION, "d1", "f" + i);
        }

        assertThat(stores.indexPageCount(DIMENSION, "d1")).isEqualTo(2);
        assertThat(stores.indexPage(DIMENSION, "d1", 0)).isEmpty();
        assertThat(stores.indexPage(DIMENSION, "d1", 1)).containsExactly("f4", "f5");
    }

    /**
     * The reference the distributed one has to agree with. Two implementations of one contract drift in
     * ways no case aimed at either of them alone would catch, and the one that drifts is the one nothing
     * runs in a unit test.
     */
    @Test
    @DisplayName("the distributed state answers what the plain-map state answers")
    void itAgreesWithThePlainMapReference() {
        MapJoinStores reference = new MapJoinStores(4);
        List<String> script = List.of("f0", "f1", "f2", "f3", "f4", "f5", "f6");
        for (String key : script) {
            stores.indexAdd(DIMENSION, "d1", key);
            reference.indexAdd(DIMENSION, "d1", key);
        }
        for (String key : List.of("f6", "f5", "f4", "f0")) {
            stores.indexRemove(DIMENSION, "d1", key);
            reference.indexRemove(DIMENSION, "d1", key);
        }

        assertThat(stores.indexPageCount(DIMENSION, "d1"))
                .isEqualTo(reference.indexPageCount(DIMENSION, "d1"));
        for (int page = 0; page < reference.indexPageCount(DIMENSION, "d1"); page++) {
            assertThat(stores.indexPage(DIMENSION, "d1", page))
                    .as("page %d", page)
                    .isEqualTo(reference.indexPage(DIMENSION, "d1", page));
        }
    }

    private static Map<String, Object> row(String field, Object value) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put(field, value);
        return row;
    }

    /** A cold layer that says what it was asked, and how. */
    private static final class RecordingColdLayer implements KeyedStateStore {

        private final Map<String, byte[]> entries = new HashMap<>();
        private final List<String> singles = new ArrayList<>();
        private final List<Collection<String>> batches = new ArrayList<>();

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
}
