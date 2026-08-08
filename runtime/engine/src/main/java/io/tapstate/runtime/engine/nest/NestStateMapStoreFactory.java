package io.tapstate.runtime.engine.nest;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.MapStoreFactory;
import io.tapstate.spi.store.KeyedStateStore;

import java.util.Objects;
import java.util.Properties;

/**
 * Builds the cold layer under one nest state map. One store serves one map and files under that map's
 * name, which is what stops two vertices answering each other's keys in the store as surely as they
 * cannot in memory.
 *
 * <p><b>It is named in the configuration rather than placed in it, and that is the point of this class
 * existing.</b> A map configuration added while the member is running is written down and broadcast, and
 * a live store put into one does not survive being written down - measured as a serialization failure at
 * the moment the configuration was added, with nothing wrong with the configuration itself. A name
 * survives it, and the member that receives the name builds its own.
 *
 * <p>The same indirection is what lets a second member have a cold layer at all: an instance placed in
 * the configuration was only ever the instance <em>this</em> member held, so any other member would have
 * received a passenger that was not there. Single-member today, so nothing had noticed.
 *
 * <p>The substrate builds this by name and hands it nothing, so what it needs comes from the member -
 * reached at {@link NestStateMapStore#init}, the one point the substrate offers a store built this way.
 * A member that runs nest maps therefore has to be told its store through {@link #bindTo} before a map
 * is used; a member that was not is a wiring mistake and says so rather than quietly keeping nothing.
 */
public final class NestStateMapStoreFactory implements MapStoreFactory<Object, Object> {

    /**
     * Where the member holds the layer behind its nest state maps. Shared between the assembly root that
     * puts it there and the store that picks it up, because a name agreed in two places would eventually
     * be spelled two ways - and the store would then find nothing, on a member configured correctly.
     */
    public static final String USER_CONTEXT_KEY = "tapstate.nest.state-store";

    /** Required: the substrate builds this by name, so it has to be buildable with nothing. */
    public NestStateMapStoreFactory() {
    }

    /**
     * Tells {@code member} what is behind its nest state maps. Called before any nest map is used - the
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
            throw new IllegalStateException("no nest state store is bound to this member, and a nest "
                    + "state map on it is configured to read through to one");
        }
        return (KeyedStateStore) store;
    }

    @Override
    public MapLoader<Object, Object> newMapStore(String mapName, Properties properties) {
        return new NestStateMapStore(mapName);
    }
}
