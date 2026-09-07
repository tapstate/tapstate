package io.tapstate.runtime.engine.join;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import io.tapstate.spi.store.KeyedStateStore;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * The cold layer under one join map: what the map reads through to when a key it is asked for is not in
 * memory, and writes through to as that key is handled.
 *
 * <p>One of these serves one map, and its map name is the namespace it files under - so two maps cannot
 * answer each other's keys in the store any more than they can in memory, and dropping one pipeline's
 * state does not touch another's.
 *
 * <p><b>It never lists its keys.</b> {@link #loadAllKeys()} answers with nothing, and the answer is an
 * empty collection rather than null: the substrate reads null as a fault while the map is starting, not
 * as an empty load, so the two differ by a crash. Listing them would be worse than useless - the map
 * would load the whole keyspace on the way up in order to serve the first event.
 *
 * <p><b>{@link #loadAll} makes one trip for the whole batch, and that is the reason this class exists
 * rather than the join reusing a bridge that is already here.</b> A recompute reads one key per row it
 * is about to rebuild, and a large fan-out is a million of them: batched, that is a few thousand round
 * trips; one at a time, it is a million, taking three orders of magnitude longer with the run reporting
 * nothing but its own slowness. The layer beneath offers the batch; a bridge that looped over the
 * single-key read would leave the whole gain on the floor while every test stayed green.
 *
 * <p><b>A batch by named keys is not the enumeration the layer beneath forbids.</b> The keys arrive from
 * the caller - off a reverse-index page - and what is asked is their states. Nothing here can ask what a
 * namespace contains, which is why {@link #loadAllKeys()} can be empty and honest at the same time.
 */
final class JoinStateMapStore implements MapStore<Object, Object>, MapLoaderLifecycleSupport {

    private final String namespace;

    /**
     * The layer this files under, and where a trip made here is counted. Neither can be handed in: the
     * substrate builds this rather than the code that wires the map, so both arrive at {@link #init},
     * which is the one thing the substrate offers a store built this way.
     */
    private KeyedStateStore store;

    private JoinStateStats stats;

    JoinStateMapStore(String namespace) {
        this.namespace = Objects.requireNonNull(namespace, "namespace");
    }

    /**
     * The same store with its two collaborators handed in rather than resolved off a member. What
     * {@link #init} does is find these; what the cases below it are about is what the store does with
     * them, and standing a whole cluster member up to reach a two-line lookup would make the cheap
     * question cost the expensive one. The wiring itself is witnessed where a member exists anyway.
     */
    JoinStateMapStore(String namespace, KeyedStateStore store, JoinStateStats stats) {
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.store = Objects.requireNonNull(store, "store");
        this.stats = Objects.requireNonNull(stats, "stats");
    }

    @Override
    public void init(HazelcastInstance member, Properties properties, String mapName) {
        this.store = JoinStateMapStoreFactory.boundTo(member);
        this.stats = JoinStateStats.of(member);
    }

    @Override
    public void destroy() {
    }

    /**
     * The state under {@code key}, from the layer behind the map. Every call is by definition a key that
     * was not in memory, which is what makes this the one place a miss can be counted at all: the map
     * serves a hit and a filled miss identically, so nothing above here can tell them apart.
     */
    @Override
    public Object load(Object key) {
        long began = System.nanoTime();
        Object state = store.load(namespace, JoinStateKeys.nameOf(key))
                .map(JoinStateMapStore::fromBytes).orElse(null);
        count(1, began);
        return state;
    }

    /**
     * The states of {@code keys}, in one trip. Only ever called with keys the map already named, so it
     * stays per-key work and never becomes a scan.
     *
     * <p>The names are kept beside the keys they were rendered from, because what comes back is filed
     * under names and what the map wants back is its own keys. The rendering is injective, so the two
     * cannot cross.
     */
    @Override
    public Map<Object, Object> loadAll(Collection<Object> keys) {
        Map<String, Object> byName = new LinkedHashMap<>();
        for (Object key : keys) {
            byName.put(JoinStateKeys.nameOf(key), key);
        }
        long began = System.nanoTime();
        Map<String, byte[]> states = store.loadAll(namespace, byName.keySet());
        count(byName.size(), began);
        Map<Object, Object> loaded = new LinkedHashMap<>();
        states.forEach((name, bytes) -> loaded.put(byName.get(name), fromBytes(bytes)));
        return loaded;
    }

    /** Nothing, always - and an empty answer rather than a null one. See the class note. */
    @Override
    public Iterable<Object> loadAllKeys() {
        return List.of();
    }

    @Override
    public void store(Object key, Object value) {
        store.save(namespace, JoinStateKeys.nameOf(key), toBytes(value));
    }

    @Override
    public void storeAll(Map<Object, Object> map) {
        map.forEach(this::store);
    }

    @Override
    public void delete(Object key) {
        store.delete(namespace, JoinStateKeys.nameOf(key));
    }

    @Override
    public void deleteAll(Collection<Object> keys) {
        keys.forEach(this::delete);
    }

    /**
     * Counts one trip that fetched {@code keys} keys. Keys and trips go to separate counters: after
     * batching they differ by up to the batch size, and reading one for the other is how a pressure
     * ratio loosens without anything saying so.
     */
    private void count(int keys, long began) {
        if (stats != null) {
            stats.backfill(namespace, keys, System.nanoTime() - began);
        }
    }

    /**
     * The state as bytes. What is inside belongs to the operator that built it and is written the same
     * way it already travels between members, so the cold layer introduces no second shape that has to
     * be kept in step with the first. A state that cannot be written is a defect in the state class,
     * not something a user did, so it crashes bare.
     */
    private static byte[] toBytes(Object value) {
        if (!(value instanceof Serializable)) {
            throw new IllegalArgumentException("join state of type " + value.getClass().getName()
                    + " cannot be written to the state layer");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        } catch (IOException cause) {
            throw new IllegalStateException("could not write join state", cause);
        }
        return bytes.toByteArray();
    }

    private static Object fromBytes(byte[] bytes) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            return in.readObject();
        } catch (IOException | ClassNotFoundException cause) {
            throw new IllegalStateException("could not read back join state", cause);
        }
    }
}
