package io.tapstate.adapters.pdk;

import io.tapdata.entity.utils.cache.KVMap;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.KeyedStateStore;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
 * <p>A physical capture can carry existing notes from its known earlier nodes, one requested key at a
 * time. Conflicting or unreadable values are refused before any carry. Current shared values remain
 * authoritative. Removal and clearing publish durable migration markers before deleting shared values,
 * so earlier private state remains intact and cannot resurrect notes the connector explicitly expired.
 */
final class DurableStateMap implements KVMap<Object> {

    private final KeyedStateStore store;
    private final String namespace;
    private final List<String> carriedFrom;
    private final String migrationNamespace;
    private static final byte[] BLOCKED = {1};
    private static final String CLEARED = "cleared";

    DurableStateMap(KeyedStateStore store, String namespace) {
        this(store, namespace, List.of(), false);
    }

    DurableStateMap(KeyedStateStore store, String namespace, List<String> carriedFrom, boolean shared) {
        this.store = Objects.requireNonNull(store, "store");
        this.namespace = Objects.requireNonNull(namespace, "namespace");
        this.carriedFrom = List.copyOf(Objects.requireNonNull(carriedFrom, "carriedFrom"));
        this.migrationNamespace = shared ? "pdk.notes-migration." + namespace : null;
        if (!shared && !carriedFrom.isEmpty()) {
            throw new IllegalArgumentException("only a shared capture can carry earlier connector notes");
        }
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
        if (!carriedFrom.isEmpty()) {
            Object carried = get(key);
            if (carried != null) {
                return carried;
            }
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
        if (carriedFrom.isEmpty() || blocked(CLEARED) || blocked(removedKey(key))) {
            return null;
        }
        byte[] candidate = null;
        for (String earlier : carriedFrom) {
            Optional<byte[]> kept = store.load(earlier, key);
            if (kept.isEmpty()) {
                continue;
            }
            if (candidate != null && !Arrays.equals(candidate, kept.get())) {
                throw new TapstateException(ConnectorError.STATE_UNREADABLE, Map.of("detail",
                        "earlier connector notes disagree for key '" + key
                                + "'; restore verified notes for this physical capture before resuming"), null);
            }
            candidate = kept.get();
        }
        if (candidate == null) {
            return null;
        }
        Object decoded = ConnectorStateCodec.decode(candidate);
        return store.saveIfAbsent(namespace, key, candidate)
                .map(ConnectorStateCodec::decode).orElse(decoded);
    }

    @Override
    public Object remove(String key) {
        Object previous = get(key);
        forget(key);
        return previous;
    }

    private static String removedKey(String key) {
        return "removed:" + key;
    }

    private boolean blocked(String key) {
        Optional<byte[]> marker = store.load(migrationNamespace, key);
        if (marker.isEmpty()) {
            return false;
        }
        if (!Arrays.equals(BLOCKED, marker.get())) {
            throw new TapstateException(ConnectorError.STATE_UNREADABLE,
                    Map.of("detail", "the shared connector-note migration marker is unreadable"), null);
        }
        return true;
    }

    /** Keep earlier nodes intact while preventing an explicitly removed shared note from returning. */
    private void forget(String key) {
        if (migrationNamespace != null) {
            store.save(migrationNamespace, removedKey(key), BLOCKED);
        }
        store.delete(namespace, key);
    }

    @Override
    public void clear() {
        // Naming the namespace is the only bulk operation the store has, and it is the one that fits:
        // there is no way to list the keys, and nothing here needs one.
        if (migrationNamespace != null) {
            store.save(migrationNamespace, CLEARED, BLOCKED);
        }
        store.dropNamespace(namespace);
    }

    @Override
    public void reset() {
        clear();
    }
}
