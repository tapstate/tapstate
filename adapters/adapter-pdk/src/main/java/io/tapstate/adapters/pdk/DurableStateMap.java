package io.tapstate.adapters.pdk;

import io.tapdata.entity.utils.cache.KVMap;
import io.tapstate.spi.store.KeyedStateStore;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The scratch map a connector reaches through its driving context, held under a namespace of its own so
 * that what it writes is still there the next time the same pipeline node is opened.
 *
 * <p>A connector keeps notes for itself: the identity it minted on its first run, the name of a resource
 * it created on the source, how far it has checkpointed. It writes them expecting to find them again,
 * and it reads their absence as "this is my first run" -- so a map that does not outlive one open does
 * not merely lose data, it tells the connector something untrue about itself, and the connector acts on
 * it. That is why this is durable rather than a cache: there is no read here that tolerates a miss.
 *
 * <p>One instance belongs to one open and must be handed to the connector as a single reference for its
 * whole life; a connector may compare the map it is given against the one it was bound with, and a fresh
 * wrapper per call fails that comparison.
 *
 * <p>Storing nothing under a key removes it -- the way a connector expires a checkpoint -- so a key that
 * holds nothing and a key that was never written are the same state, as they are in the map this
 * replaces.
 *
 * <p>Values are stored by value, not by reference. A read (including the incumbent returned by
 * {@code putIfAbsent}) returns a detached snapshot with mutable lists, maps and byte arrays. Editing
 * that snapshot changes no stored state until {@code put} is called with it. Reads always consult the
 * store so another connector's writes are visible; no per-open object cache hides them.
 *
 * <p>A map can carry notes over from namespaces that kept them before, key by key: a key its own namespace
 * does not hold is looked for in each of those in turn, and the first value found is written into its own
 * namespace and read from there from then on. Nothing is copied wholesale -- the store cannot list a
 * namespace, and a note nobody asks for is one nobody needs. A key removed here is removed from those too,
 * so that it cannot come back from where it was carried from.
 */
final class DurableStateMap implements KVMap<Object> {

    private final KeyedStateStore store;
    private final String namespace;
    private final List<String> carriedFrom;

    DurableStateMap(KeyedStateStore store, String namespace) {
        this(store, namespace, List.of());
    }

    DurableStateMap(KeyedStateStore store, String namespace, List<String> carriedFrom) {
        this.store = Objects.requireNonNull(store, "store");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.carriedFrom = List.copyOf(Objects.requireNonNull(carriedFrom, "carriedFrom"));
    }

    @Override
    public void init(String mapKey, Class<Object> valueClass) {
        // The namespace this map writes under is settled when the connector is opened, by whoever knows
        // which node it was opened for. A name offered here would be a second answer to that.
    }

    @Override
    public void put(String key, Object value) {
        if (value == null) {
            forget(key);
            return;
        }
        store.save(namespace, key, ConnectorStateCodec.encode(value));
    }

    @Override
    public Object putIfAbsent(String key, Object value) {
        if (value == null) {
            // Nothing to claim the key with; report what is there without touching it.
            return get(key);
        }
        Object carried = get(key);
        if (carried != null) {
            return carried;
        }
        return store.saveIfAbsent(namespace, key, ConnectorStateCodec.encode(value))
                .map(ConnectorStateCodec::decode)
                .orElse(null);
    }

    @Override
    public Object get(String key) {
        Optional<byte[]> own = store.load(namespace, key);
        if (own.isPresent()) {
            return ConnectorStateCodec.decode(own.get());
        }
        for (String earlier : carriedFrom) {
            Optional<byte[]> kept = store.load(earlier, key);
            if (kept.isPresent()) {
                // Written here before it is read from here, so whoever opens these notes next finds it
                // without looking back; a concurrent carrier that got there first wins, and so does its value.
                return store.saveIfAbsent(namespace, key, kept.get())
                        .map(ConnectorStateCodec::decode)
                        .orElseGet(() -> ConnectorStateCodec.decode(kept.get()));
            }
        }
        return null;
    }

    @Override
    public Object remove(String key) {
        Object previous = get(key);
        forget(key);
        return previous;
    }

    /** Deletes {@code key} here and wherever it could be carried from, so no later read brings it back. */
    private void forget(String key) {
        store.delete(namespace, key);
        carriedFrom.forEach(earlier -> store.delete(earlier, key));
    }

    @Override
    public void clear() {
        // Naming the namespace is the only bulk operation the store has, and it is the one that fits:
        // there is no way to list the keys, and nothing here needs one. What was carried from goes with
        // it, or a key cleared here would come back from there on the next read.
        store.dropNamespace(namespace);
        carriedFrom.forEach(store::dropNamespace);
    }

    @Override
    public void reset() {
        clear();
    }
}
