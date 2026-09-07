package io.tapstate.runtime.engine.join;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapStoreFactory;
import io.tapstate.spi.store.KeyedStateStore;

import java.util.Objects;
import java.util.Properties;

/**
 * Builds the cold layer under one join state map. One store serves one map and files under that map's
 * name, which is what stops two maps answering each other's keys in the store as surely as they cannot
 * in memory.
 *
 * <p><b>It is named in the configuration rather than placed in it.</b> A map configuration added while
 * the member is running is written down and broadcast, and a live store does not survive being written
 * down. A name survives it, and the member that receives the name builds its own - which is also what
 * lets a second member have a cold layer at all, since an instance placed in the configuration was only
 * ever the instance one member held.
 *
 * <p>The substrate builds the store by name and hands it nothing, so what it needs comes from the member
 * - reached at the store's {@code init}, the one point the substrate offers a store built this way. A
 * member that runs join maps therefore has to be told its store through {@link #bindTo} before a map is
 * used; a member that was not is a wiring mistake and says so rather than quietly keeping nothing.
 */
public final class JoinStateMapStoreFactory implements MapStoreFactory<Object, Object> {

    /**
     * Where the member holds the layer behind its join state maps. Its own key rather than the one the
     * nest maps use: the two are wired separately and may one day be told about different layers, and a
     * key shared between them would make that impossible to express without either noticing.
     */
    public static final String USER_CONTEXT_KEY = "tapstate.join.state-store";

    /** Required: the substrate builds this by name, so it has to be buildable with nothing. */
    public JoinStateMapStoreFactory() {
    }

    /**
     * Tells {@code member} what is behind its join state maps. Called before any join map is used - the
     * store is resolved as a map starts, not as the configuration is written, so binding afterwards is
     * too late for a map that has already begun.
     */
    public static void bindTo(HazelcastInstance member, KeyedStateStore store) {
        Objects.requireNonNull(member, "member").getUserContext()
                .put(USER_CONTEXT_KEY, Objects.requireNonNull(store, "store"));
    }

    /**
     * The store {@code member} was told to use, never null. A map configured to read through to a layer
     * that is not there cannot degrade to keeping nothing: what it holds would be gone at the first
     * eviction, and the configuration read back would still say a store was behind it.
     */
    static KeyedStateStore boundTo(HazelcastInstance member) {
        Object store = member.getUserContext().get(USER_CONTEXT_KEY);
        if (store == null) {
            throw new IllegalStateException("no join state store is bound to this member, and a join "
                    + "state map on it is configured to read through to one");
        }
        return (KeyedStateStore) store;
    }

    @Override
    public MapLoader<Object, Object> newMapStore(String mapName, Properties properties) {
        return new JoinStateMapStore(mapName);
    }
}
