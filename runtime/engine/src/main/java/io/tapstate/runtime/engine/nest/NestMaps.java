package io.tapstate.runtime.engine.nest;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizePolicy;

/**
 * What every map holding nest state is configured to be. The maps themselves are created on demand, by
 * name, as vertices start asking for them; this is the one place that decides what they are when they
 * appear, and it does so by a pattern over the name every nest namespace shares.
 *
 * <p>The alternative is not "no configuration" but the substrate's own defaults, and each of those is
 * wrong here in a way that does not announce itself:
 *
 * <ul>
 *   <li><b>A backup replica per entry.</b> Paid on every write, and redundant twice over: this state is
 *       rebuildable from the source, and the member that would hold the replica is the same one that
 *       holds the entry.</li>
 *   <li><b>Expiry.</b> A resolver mapping that expires stops answering for the children that point at
 *       it, and they wait forever for a parent that is still there. An assembly that expires is rebuilt
 *       from whatever arrives next and emitted as though it were whole.</li>
 *   <li><b>Eviction with nothing behind the map.</b> The same silent loss, reached by size rather than by
 *       time. Eviction is what turns a bounded heap into a bounded heap plus a disk - but only once
 *       something stands behind the map to load an evicted entry back from. Until then, evicting is
 *       losing, which is why it is configured on the shape that has a store and on no other.</li>
 * </ul>
 *
 * <p>Values are kept as the objects they are, because that is what the way they are written needs. A
 * write carries the state to the key it belongs to instead of putting it across the map, and what a
 * carried write is handed on an object-format map is the state rather than a copy of it - so the write
 * serializes none of the document. The format follows the access rather than the intuition: read out
 * with a plain get this format would be the more expensive of the two, and the reason it is not is that
 * the expensive half of the pair has been removed rather than moved.
 *
 * <p><b>No index may be defined on these maps.</b> An index turns what a carried write is handed back
 * into a clone of the state, which puts the copy back with nothing to show for it and nothing reporting
 * it. Nothing defines one today; the ban is what keeps that true.
 *
 * <p>Reached only through the settings, never directly. What a map is configured to hold and what a level
 * is allowed to hold are two halves of one capacity decision, and they only mean anything against each
 * other; a second way in here is how they come to be set from two places that cannot see each other's
 * number. Anything a deployment configures about a nest is added to the settings, not to this.
 */
final class NestMaps {

    /**
     * The prefix every nest state map name begins with. It is shared with the naming so that the pattern
     * and the names cannot drift apart: a rename on one side alone would leave every map on the
     * substrate defaults, which costs a replica per write and evicts nothing yet reports nothing either.
     */
    static final String NAMESPACE_PREFIX = "nest.";

    /**
     * The smallest memory budget that still means what it says, which is the substrate's partition count.
     * The budget is spent per partition rather than per map: below this it has been rounded up to one entry
     * per partition before it is enforced, so what is held is the partition count regardless of what was
     * asked for - a configured 10 measured 82 resident.
     */
    static final int SMALLEST_MEANINGFUL_MEMORY_BUDGET = 271;

    private NestMaps() {
    }

    /**
     * The configuration every nest state map takes with a store behind it: read through that store when a
     * key that is not in memory is asked for, and written through it as a key is handled. The store is
     * named rather than placed here, and is resolved on the member the map runs on.
     *
     * <p>The write is through rather than behind, and that is the decision here rather than the default
     * that happens to agree with it. A queued write would be held in memory, and the only copy of that
     * queue would be a backup replica - which these maps deliberately do not keep. So a crash with a
     * queue outstanding loses the tail of it, and loses it silently: nothing failed, the entries simply
     * were never there. Delay buys throughput only where a replica makes the queue survivable.
     *
     * <p>Loading stays lazy, which here means nothing is loaded up front at all: the store answers the
     * "which keys do you have" question with none, so there is no keyspace to preload and a restart pays
     * only for the keys it is actually asked about.
     */
    static MapConfig backedStateMaps(long entriesHeldInMemory) {
        return backedStateMaps(NAMESPACE_PREFIX + "*", entriesHeldInMemory);
    }

    /**
     * As above, for the single namespace {@code name} rather than for all of them. An exact name wins over
     * the pattern for that namespace alone, which is how one pipeline's budget applies to its own levels
     * and to nobody else's.
     *
     * <p>This is the configuration that can only be written once the member is already running: it names a
     * namespace, and namespaces come from pipelines, which do not exist when the member starts. That is
     * why the store behind it is named rather than placed - a configuration added late is broadcast, and a
     * live store does not survive being written down.
     */
    static MapConfig backedStateMaps(String name, long entriesHeldInMemory) {
        MapConfig config = stateMaps().setName(name).setMapStoreConfig(new MapStoreConfig()
                .setEnabled(true)
                .setWriteDelaySeconds(0)
                .setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY)
                .setFactoryClassName(NestStateMapStoreFactory.class.getName()));
        // Only ever here, where the store above is what an evicted entry comes back from. On the
        // configuration without one, evicting is losing: the entry is in no other place, and what the map
        // answers afterwards is the absence rather than the state - a resolver that stops answering for
        // children still pointing at it, an assembly rebuilt from whatever arrives next and emitted as
        // though it were whole. Neither says anything while it happens.
        config.getEvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                .setSize(Math.toIntExact(entriesHeldInMemory));
        return config;
    }

    /**
     * The configuration every nest state map takes, named by the pattern that matches all of them, with
     * nothing behind it: what it holds lives only as long as the member does. A namespace that must
     * diverge is configured under its own exact name, which wins over this pattern for that namespace
     * alone.
     */
    static MapConfig stateMaps() {
        return new MapConfig(NAMESPACE_PREFIX + "*")
                .setBackupCount(0)
                .setAsyncBackupCount(0)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setMaxIdleSeconds(0)
                .setStatisticsEnabled(true)
                .setEvictionConfig(new EvictionConfig()
                        .setEvictionPolicy(EvictionPolicy.NONE)
                        .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                        .setSize(Integer.MAX_VALUE));
    }
}
