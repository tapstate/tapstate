package io.tapstate.adapters.pdk;

import io.tapdata.entity.utils.cache.KVMap;
import io.tapstate.spi.store.KeyedStateStore;
import java.util.Objects;

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
 */
final class DurableStateMap implements KVMap<Object> {

    private final KeyedStateStore store;
    private final String namespace;

    DurableStateMap(KeyedStateStore store, String namespace) {
        this.store = Objects.requireNonNull(store, "store");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
    }

    @Override
    public void init(String mapKey, Class<Object> valueClass) {
        // The namespace this map writes under is settled when the connector is opened, by whoever knows
        // which node it was opened for. A name offered here would be a second answer to that.
    }

    @Override
    public void put(String key, Object value) {
        if (value == null) {
            store.delete(namespace, key);
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
        return store.saveIfAbsent(namespace, key, ConnectorStateCodec.encode(value))
                .map(ConnectorStateCodec::decode)
                .orElse(null);
    }

    @Override
    public Object get(String key) {
        return store.load(namespace, key).map(ConnectorStateCodec::decode).orElse(null);
    }

    @Override
    public Object remove(String key) {
        Object previous = get(key);
        store.delete(namespace, key);
        return previous;
    }

    @Override
    public void clear() {
        // Naming the namespace is the only bulk operation the store has, and it is the one that fits:
        // there is no way to list the keys, and nothing here needs one.
        store.dropNamespace(namespace);
    }

    @Override
    public void reset() {
        store.dropNamespace(namespace);
    }
}
