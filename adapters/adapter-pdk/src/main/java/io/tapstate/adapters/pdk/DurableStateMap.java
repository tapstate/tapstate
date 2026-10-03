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
 * namespace, and a note nobody asks for is one nobody needs. Those namespaces are other nodes' own notes, and
 * are never written to: a key removed here is removed here and marked removed under a key of its own, and a
 * cleared map marks itself cleared, so that nothing comes back from where it was carried from while every other
 * node keeps what it kept. The key itself is left absent rather than written over, so that claiming it again
 * stays one act of the store's.
 */
final class DurableStateMap implements KVMap<Object> {

    /** What a mark holds: only whether one is there is ever read. */
    private static final byte[] MARK = new byte[0];

    /** The key a cleared map that carries notes over keeps, saying nothing is to be carried over any more. */
    private static final String CARRYING_ENDED = "__tapstate.carried-over.ended__";

    /** The prefix of the key a map that carries notes over keeps for each key forgotten here. */
    private static final String FORGOTTEN = "__tapstate.carried-over.forgotten__.";

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
        Object present = get(key);
        if (present != null) {
            return present;
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
        if (carriedFrom.isEmpty() || store.load(namespace, CARRYING_ENDED).isPresent() || forgotten(key)) {
            return null;
        }
        for (String earlier : carriedFrom) {
            Optional<byte[]> kept = store.load(earlier, key);
            if (kept.isPresent()) {
                // Written here before it is read from here, so whoever opens these notes next finds it
                // without looking back; a concurrent carrier or claimer that got there first wins.
                Optional<byte[]> first = store.saveIfAbsent(namespace, key, kept.get());
                if (first.isPresent()) {
                    return ConnectorStateCodec.decode(first.get());
                }
                if (forgotten(key)) {
                    // Forgotten here while it was being carried over: the forgetting stands.
                    store.delete(namespace, key);
                    return null;
                }
                return ConnectorStateCodec.decode(kept.get());
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

    /**
     * Removes {@code key} here. Where notes are carried over it is marked removed as well, so no later read
     * brings it back from where it was carried from, and that namespace -- another node's own -- is left alone.
     * Marked before it is removed, so no read in between finds it gone with nothing to keep it from coming back.
     */
    private void forget(String key) {
        if (!carriedFrom.isEmpty()) {
            store.save(namespace, FORGOTTEN + key, MARK);
        }
        store.delete(namespace, key);
    }

    private boolean forgotten(String key) {
        return store.load(namespace, FORGOTTEN + key).isPresent();
    }

    @Override
    public void clear() {
        // Naming the namespace is the only bulk operation the store has, and it is the one that fits:
        // there is no way to list the keys, and nothing here needs one. What notes were carried from is
        // another node's and stays; the mark left here is what keeps it from being carried over again.
        store.dropNamespace(namespace);
        if (!carriedFrom.isEmpty()) {
            store.save(namespace, CARRYING_ENDED, MARK);
        }
    }

    @Override
    public void reset() {
        clear();
    }
}
