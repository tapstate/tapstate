package io.tapstate.runtime.engine.nest;

import static org.assertj.core.api.Assertions.assertThat;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapStoreFactory;
import io.tapstate.core.event.SourceOrder;
import io.tapstate.spi.store.KeyedStateStore;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * What a nest vertex holds outlives the memory holding it, and is fetched back one key at a time.
 *
 * <p>This is the whole reason the state moved off the heap. A heap store begins empty after a restart,
 * so every document has to be rebuilt by re-reading the sources - which is the cost the cold layer
 * exists to avoid, and which cannot even be paid where a source no longer has the history. With a store
 * behind the map, a key that is no longer in memory is loaded back the moment it is asked for, and a
 * process that died mid-assembly picks up what it had.
 *
 * <p>Three properties are what make that true rather than approximately true:
 *
 * <ul>
 *   <li><b>The write is through, not behind.</b> When a write returns, the store already has it. A
 *       queued write is redundancy-free here: the queue is memory, the only copy of it would be a backup
 *       replica, and these maps deliberately keep none - so a crash with a queue is a crash that loses
 *       the tail with nothing reporting the loss.</li>
 *   <li><b>The read is through, per key.</b> The map asks the store only for the key being asked for.</li>
 *   <li><b>Nothing ever asks the store for everything it holds.</b> A store that could answer that would
 *       be asked it on the way up from a restart, and the whole keyspace would be loaded to serve the
 *       first event - which is exactly the warm-up the cold layer exists to avoid. The refusal has a
 *       shape that matters: an empty answer, never a null one, because null is what an implementation
 *       reaches for to mean "nothing" and it is a crash rather than an empty load.</li>
 * </ul>
 */
class NestStateSurvivesWhatTheMemoryDoesNotTest {

    private static final String NAMESPACE = "nest.p1.n1.orders";
    private static final String SIBLING = "nest.p1.n1.payouts";

    private RecordingKeyedStateStore store;
    private HazelcastInstance member;

    @BeforeEach
    void startMember() {
        store = new RecordingKeyedStateStore();
        Config config = new Config();
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.shutdownhook.enabled", "false");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        join.getAutoDetectionConfig().setEnabled(false);
        config.getNetworkConfig().getInterfaces().setEnabled(true).addInterface("127.0.0.1");
        config.addMapConfig(NestSettings.defaults().backedStateMaps());
        member = Hazelcast.newHazelcastInstance(config);
        NestStateMapStoreFactory.bindTo(member, store);
    }

    @AfterEach
    void stopMember() {
        if (member != null) {
            member.shutdown();
        }
    }

    @Test
    void aWriteHasReachedTheStoreByTheTimeItReturns() {
        stores(NAMESPACE).forResolver(vertex(NAMESPACE)).save(List.of("C1"), declaring("parent-1"));

        assertThat(store.namespaces()).containsExactly(NAMESPACE);
        assertThat(store.keysIn(NAMESPACE)).hasSize(1);
    }

    @Test
    void aKeyNoLongerInMemoryComesBackFromTheStore() {
        NestStore<ResolverState> state = stores(NAMESPACE).forResolver(vertex(NAMESPACE));
        state.save(List.of("C1"), declaring("parent-1"));

        // Evicting takes the entry out of memory without taking it out of the store, which is what
        // happens under capacity pressure and, in the large, what a restart looks like to a key.
        member.<Object, Object>getMap(NAMESPACE).evict(List.of("C1"));
        assertThat(member.getMap(NAMESPACE).getLocalMapStats().getOwnedEntryCount())
                .describedAs("the entry really left memory, so the read below has to go to the store")
                .isZero();

        ResolverState reread = state.load(List.of("C1"));
        assertThat(reread).isNotNull();
        assertThat(reread.parentKey()).isEqualTo("parent-1");
    }

    /**
     * A write hands the state itself over rather than a copy of it, which is what makes the write cost
     * no serialization of the document - and would make a state changed after it was written change what
     * is held, without the store behind the map hearing about it. What stops that is the read: it answers
     * with something of its own, so what a later event is given is never the thing the map is holding.
     *
     * <p>Under a format that keeps values as bytes this was true by accident, since writing them made a
     * copy anyway. It is now load-bearing, so it is asked here rather than left to be read off two
     * classes at once.
     */
    @Test
    void whatAReadHandsBackIsNotTheStateTheMapIsHolding() {
        NestStore<ResolverState> state = stores(NAMESPACE).forResolver(vertex(NAMESPACE));
        ResolverState written = declaring("parent-1");
        state.save(List.of("C1"), written);

        ResolverState read = state.load(List.of("C1"));

        assertThat(read)
                .describedAs("a read that handed back what is held would let the next event change the "
                        + "held state directly, and the store behind the map would never be told")
                .isNotSameAs(written);
        assertThat(read.parentKey()).isEqualTo("parent-1");
    }

    @Test
    void aMapBuiltAgainOverTheSameNameReadsBackWhatTheLastOneWrote() {
        stores(NAMESPACE).forResolver(vertex(NAMESPACE)).save(List.of("C1"), declaring("parent-1"));

        // Destroying and asking for the name again is what a restart is to the map: a new empty one over
        // the same name, whose entries can only come back from the store.
        member.getMap(NAMESPACE).destroy();

        ResolverState reread = stores(NAMESPACE).forResolver(vertex(NAMESPACE)).load(List.of("C1"));
        assertThat(reread).isNotNull();
        assertThat(reread.parentKey()).isEqualTo("parent-1");
    }

    /**
     * The refusal to list a keyspace, in the shape the substrate reads it. An implementation that meant
     * "nothing" by returning null crashes the map on the way up instead of loading nothing, and one that
     * answered with its keys would have the whole keyspace loaded to serve the first event. The port
     * behind it offers no way to enumerate at all, which is what makes this answer the only one available
     * rather than a choice that has to be remembered.
     */
    @Test
    void theStoreBehindAMapWillNotListItsKeysAndSaysSoWithAnEmptyAnswer() {
        MapStoreFactory<Object, Object> factory = factoryNamedBy(NestSettings.defaults().backedStateMaps());

        MapLoader<Object, Object> loader = factory.newMapStore(NAMESPACE, new Properties());

        assertThat(loader.loadAllKeys())
                .describedAs("null here is not an empty load - it is a crash while the map starts")
                .isNotNull()
                .isEmpty();
    }

    @Test
    void twoNamespacesDoNotAnswerEachOthersKeys() {
        stores(NAMESPACE).forResolver(vertex(NAMESPACE)).save(List.of("C1"), declaring("parent-1"));

        assertThat(stores(SIBLING).forResolver(vertex(SIBLING)).load(List.of("C1"))).isNull();
        assertThat(store.keysIn(SIBLING)).isEmpty();
    }

    @Test
    void removingAnEntryRemovesItFromTheStoreAsWell() {
        NestStore<ResolverState> state = stores(NAMESPACE).forResolver(vertex(NAMESPACE));
        state.save(List.of("C1"), declaring("parent-1"));

        state.remove(List.of("C1"));

        assertThat(store.keysIn(NAMESPACE)).isEmpty();
        assertThat(state.load(List.of("C1"))).isNull();
    }

    private NestBinding.NestStores stores(String mapName) {
        return NestBinding.onMap().bind(member);
    }

    /**
     * The factory the configuration names, built the way the substrate builds it. Named rather than
     * placed, so reading it back means constructing one from the name - which is also the thing that has
     * to keep working for a configuration added to a running member to be usable at all.
     */
    @SuppressWarnings("unchecked")
    private static MapStoreFactory<Object, Object> factoryNamedBy(MapConfig config) {
        try {
            return (MapStoreFactory<Object, Object>) Class
                    .forName(config.getMapStoreConfig().getFactoryClassName())
                    .getDeclaredConstructor().newInstance();
        } catch (ReflectiveOperationException cause) {
            throw new IllegalStateException("the named map store factory could not be built", cause);
        }
    }

    private static NestVertex vertex(String mapName) {
        return new NestVertex(List.of("orders"), "nest:n1:orders", mapName,
                List.of("id"), List.of("customer_id"), List.of());
    }

    private static ResolverState declaring(Object parentKey) {
        ResolverState state = new ResolverState();
        state.declare(parentKey, new SourceOrder(1L, 1L));
        return state;
    }

    /** A store that keeps what it is given in memory and remembers whether it was ever asked for all of it. */
    private static final class RecordingKeyedStateStore implements KeyedStateStore {

        private final Map<String, Map<String, byte[]>> byNamespace = new LinkedHashMap<>();

        @Override
        public Optional<byte[]> load(String namespace, String key) {
            Map<String, byte[]> entries = byNamespace.get(namespace);
            return entries == null ? Optional.empty() : Optional.ofNullable(entries.get(key));
        }

        @Override
        public void save(String namespace, String key, byte[] state) {
            byNamespace.computeIfAbsent(namespace, ignored -> new LinkedHashMap<>()).put(key, state);
        }

        @Override
        public void delete(String namespace, String key) {
            Map<String, byte[]> entries = byNamespace.get(namespace);
            if (entries != null) {
                entries.remove(key);
            }
        }

        @Override
        public void dropNamespace(String namespace) {
            byNamespace.remove(namespace);
        }

        @Override
        public long count(String namespace) {
            return byNamespace.getOrDefault(namespace, Map.of()).size();
        }

        List<String> namespaces() {
            return new ArrayList<>(byNamespace.keySet());
        }

        List<String> keysIn(String namespace) {
            Map<String, byte[]> entries = byNamespace.get(namespace);
            return entries == null ? List.of() : new ArrayList<>(entries.keySet());
        }
    }
}
