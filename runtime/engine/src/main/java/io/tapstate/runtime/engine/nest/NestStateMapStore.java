package io.tapstate.runtime.engine.nest;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.runtime.engine.StateStoreCostProbe;
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
 * The cold layer under one vertex's state map: what the map reads through to when a key it is asked for
 * is not in memory, and writes through to as that key is handled.
 *
 * <p>One of these serves one map, and its map name is the namespace it files under - so two vertices
 * cannot answer each other's keys in the store any more than they can in memory, and dropping one
 * pipeline's state does not touch another's.
 *
 * <p><b>It never lists its keys.</b> {@link #loadAllKeys()} answers with nothing, and the answer is an
 * empty collection rather than null: the substrate reads null as a fault while the map is starting, not
 * as an empty load, so the two differ by a crash. Listing them would be worse than useless - the map
 * would load the whole keyspace on the way up in order to serve the first event, which is the warm-up
 * this layer exists to remove. The port beneath offers no way to enumerate either, so this is the only
 * answer available rather than one that has to be remembered.
 */
final class NestStateMapStore implements MapStore<Object, Object>, MapLoaderLifecycleSupport {

    private final String namespace;
    private final String database;

    /**
     * The layer this files under, and where a trip made here is counted. Neither can be handed in: the
     * substrate builds this rather than the code that wires the map, so both arrive at {@link #init},
     * which is the one thing the substrate offers a store built this way. Measured: a store reached
     * through a factory is <em>not</em> given the member by the awareness interface, only by this one,
     * and the difference is silent - counters that are simply never written.
     */
    private KeyedStateStore store;

    private NestStateStats stats;
    private StateStoreCostProbe costs;

    NestStateMapStore(String namespace) {
        this(namespace, null);
    }

    NestStateMapStore(String namespace, String database) {
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.database = database;
    }

    NestStateMapStore(String namespace, KeyedStateStore store, StateStoreCostProbe costs) {
        this(namespace, null);
        this.store = Objects.requireNonNull(store, "store");
        this.costs = Objects.requireNonNull(costs, "costs");
    }

    @Override
    public void init(HazelcastInstance member, Properties properties, String mapName) {
        var stores = NestStateMapStoreFactory.boundTo(member);
        String resolved = database == null ? stores.defaultDatabase() : database;
        this.store = stores.inDatabase(resolved).state();
        this.stats = NestStateStats.of(member);
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
        try {
            var loaded = store.load(namespace, NestStateKeys.nameOf(key));
            Object state = loaded.map(this::fromBytes).orElse(null);
            if (costs != null) {
                costs.completed(namespace, StateStoreCostProbe.Operation.LOAD,
                        System.nanoTime() - began, loaded.map(bytes -> (long) bytes.length).orElse(0L));
            }
            if (stats != null) {
                stats.backfill(namespace, System.nanoTime() - began);
            }
            return state;
        } catch (RuntimeException cause) {
            if (costs != null) {
                costs.failed(namespace, StateStoreCostProbe.Operation.LOAD, System.nanoTime() - began);
            }
            throw cause;
        }
    }

    /**
     * The keys of {@code keys} that the store has, asked for in one reach. It is only ever called with keys
     * the map already named, so it stays work over named keys and never becomes a scan.
     *
     * <p><b>One trip, and counted as one.</b> This used to ask the store per key, which made a batch read
     * cost as many trips as it had keys while every count above here still said one reach - the shape a
     * batch that has quietly become a loop always has. Measured on the two paths that use it, a batch of
     * forty identities cost forty trips and a wake-up reading sixty-four buckets cost twenty-nine.
     *
     * <p>The keys are carried both ways: the store is asked with the names the keys render to, and what
     * comes back is put under the key objects the caller passed, because that is what the map above files
     * entries by.
     */
    @Override
    public Map<Object, Object> loadAll(Collection<Object> keys) {
        if (keys.isEmpty()) {
            return Map.of();
        }
        Map<String, Object> byName = new LinkedHashMap<>();
        for (Object key : keys) {
            byName.put(NestStateKeys.nameOf(key), key);
        }
        long began = System.nanoTime();
        try {
            Map<String, byte[]> found = store.loadAll(namespace, byName.keySet());
            Map<Object, Object> loaded = new LinkedHashMap<>();
            long payloadBytes = 0;
            for (Map.Entry<String, byte[]> state : found.entrySet()) {
                byte[] bytes = state.getValue();
                loaded.put(byName.get(state.getKey()), fromBytes(bytes));
                if (costs != null) {
                    payloadBytes += bytes.length;
                }
            }
            if (costs != null) {
                costs.completed(namespace, StateStoreCostProbe.Operation.LOAD_ALL,
                        System.nanoTime() - began, payloadBytes);
            }
            if (stats != null) {
                stats.backfill(namespace, System.nanoTime() - began);
            }
            return loaded;
        } catch (RuntimeException cause) {
            if (costs != null) {
                costs.failed(namespace, StateStoreCostProbe.Operation.LOAD_ALL, System.nanoTime() - began);
            }
            throw cause;
        }
    }

    /** Nothing, always - and an empty answer rather than a null one. See the class note. */
    @Override
    public Iterable<Object> loadAllKeys() {
        return List.of();
    }

    @Override
    public void store(Object key, Object value) {
        if (costs == null) {
            store.save(namespace, NestStateKeys.nameOf(key), toBytes(value));
            return;
        }
        long began = System.nanoTime();
        try {
            byte[] bytes = toBytes(value);
            store.save(namespace, NestStateKeys.nameOf(key), bytes);
            costs.completed(namespace, StateStoreCostProbe.Operation.SAVE,
                    System.nanoTime() - began, bytes.length);
        } catch (RuntimeException cause) {
            costs.failed(namespace, StateStoreCostProbe.Operation.SAVE, System.nanoTime() - began);
            throw cause;
        }
    }

    @Override
    public void storeAll(Map<Object, Object> map) {
        map.forEach(this::store);
    }

    @Override
    public void delete(Object key) {
        if (costs == null) {
            store.delete(namespace, NestStateKeys.nameOf(key));
            return;
        }
        long began = System.nanoTime();
        try {
            store.delete(namespace, NestStateKeys.nameOf(key));
            costs.completed(namespace, StateStoreCostProbe.Operation.DELETE,
                    System.nanoTime() - began, 0);
        } catch (RuntimeException cause) {
            costs.failed(namespace, StateStoreCostProbe.Operation.DELETE, System.nanoTime() - began);
            throw cause;
        }
    }

    @Override
    public void deleteAll(Collection<Object> keys) {
        keys.forEach(this::delete);
    }

    /**
     * The state as bytes. What is inside belongs to the operator that built it and is written the same
     * way it already travels between members, so the cold layer introduces no second shape that has to
     * be kept in step with the first. A state that cannot be written is a defect in the state class,
     * not something a user did, so it crashes bare.
     */
    private byte[] toBytes(Object value) {
        if (!(value instanceof Serializable)) {
            throw new IllegalArgumentException("nest state of type " + value.getClass().getName()
                    + " cannot be written to the state layer");
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        } catch (IOException cause) {
            throw new IllegalStateException("could not write nest state", cause);
        }
        byte[] encoded = bytes.toByteArray();
        if (costs != null) {
            costs.serialized(namespace, StateStoreCostProbe.Codec.ENCODE, encoded.length);
        }
        return encoded;
    }

    private Object fromBytes(byte[] bytes) {
        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            Object state = in.readObject();
            if (costs != null) {
                costs.serialized(namespace, StateStoreCostProbe.Codec.DECODE, bytes.length);
            }
            return state;
        } catch (IOException | ClassNotFoundException cause) {
            throw new IllegalStateException("could not read back nest state", cause);
        }
    }

}
