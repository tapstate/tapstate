package io.tapstate.runtime.engine.join;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MaxSizePolicy;

/**
 * What every map holding join state is configured to be, and what those maps are called. The maps
 * themselves are created on demand, by name, as a join starts asking for them; this is the one place
 * that decides what they are when they appear, and it does so by a pattern over the name every join
 * namespace shares.
 *
 * <p>Three maps per join, and each is load-bearing rather than a cache:
 *
 * <ul>
 *   <li><b>the fact mirror</b> - the current image of each fact row, because a recompute has to re-emit
 *       that row and must be able to read what is now in it;
 *   <li><b>the dimension mirror</b> - the current image of each dimension row, because a fact row
 *       arriving has to be able to look its dimension up;
 *   <li><b>the reverse index</b> - which fact rows reference which dimension key, because a change to a
 *       dimension row has to find the rows it affects.
 * </ul>
 *
 * <p><b>A replica per entry, which the nest maps deliberately do not keep.</b> The difference is what
 * the state is for: nest state can be rebuilt by reading the source again, so a lost replica costs time;
 * these mirrors are what lets a broken target table be rebuilt <em>without</em> reading the source, and
 * a member lost with no replica turns that back into a full re-read. The cold layer holds everything
 * either way - the replica is what makes a member's death a pause rather than a re-warm.
 *
 * <p>Values are kept as the objects they are, because that is what the way they are written needs. A
 * write carries the change to the key it belongs to instead of putting a whole value across the map,
 * and what a carried write is handed on an object-format map is the state rather than a copy of it.
 *
 * <p><b>No index may be defined on these maps.</b> An index turns what a carried write is handed back
 * into a clone of the state, which puts the copy back with nothing to show for it and nothing reporting
 * it. Nothing defines one today; the ban is what keeps that true.
 */
public final class JoinMaps {

    /**
     * The prefix every join state map name begins with. Shared with the naming below so that the
     * pattern and the names cannot drift apart: a rename on one side alone would leave every map on the
     * substrate defaults, which evicts nothing and reports nothing either.
     */
    public static final String NAMESPACE_PREFIX = "join.";

    /**
     * How many entries one member holds of one join map before it starts evicting to the layer behind
     * it. Provisional, and deliberately the same order as the nest default: the budget counts entries
     * rather than bytes, and a reverse-index page and a mirrored row are not the same size - which is
     * exactly why the index is paged, so that an entry stays within an order of magnitude of any other.
     */
    public static final long DEFAULT_ENTRIES_HELD_IN_MEMORY = 4_000;

    private JoinMaps() {
    }

    /** Where one join step keeps the current image of each fact row. */
    public static String factMirror(String pipelineId, String stepId) {
        return NAMESPACE_PREFIX + pipelineId + "." + stepId + ".fact";
    }

    /** Where one join step keeps the current image of each row of one dimension source. */
    public static String dimensionMirror(String pipelineId, String stepId, String source) {
        return NAMESPACE_PREFIX + pipelineId + "." + stepId + ".dim." + source;
    }

    /** Where one join step keeps which fact rows reference which key of one dimension source. */
    public static String reverseIndex(String pipelineId, String stepId, String source) {
        return NAMESPACE_PREFIX + pipelineId + "." + stepId + ".index." + source;
    }

    /**
     * The configuration every join state map takes: read through the cold layer when a key that is not
     * in memory is asked for, and written through it as a key is handled. The store is named rather than
     * placed here, and is resolved on the member the map runs on.
     *
     * <p>The write is through rather than behind. A queued write would be held in memory and would
     * survive a crash only as a backup replica of the queue, which is not what a replica of the map is;
     * a crash with a queue outstanding loses its tail silently - nothing failed, the entries simply were
     * never there.
     *
     * <p>Loading stays lazy, which here means nothing is loaded up front at all: the store answers the
     * "which keys do you have" question with none, so there is no keyspace to preload and a restart pays
     * only for the keys it is actually asked about.
     */
    public static MapConfig backedStateMaps(long entriesHeldInMemory) {
        return backedStateMaps(NAMESPACE_PREFIX + "*", entriesHeldInMemory);
    }

    /**
     * As above, for the single namespace {@code name} rather than for all of them. An exact name wins
     * over the pattern for that namespace alone, which is how one pipeline's budget applies to its own
     * maps and to nobody else's.
     */
    public static MapConfig backedStateMaps(String name, long entriesHeldInMemory) {
        MapConfig config = new MapConfig(name)
                .setBackupCount(1)
                .setAsyncBackupCount(0)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setTimeToLiveSeconds(0)
                .setMaxIdleSeconds(0)
                .setStatisticsEnabled(true)
                .setMapStoreConfig(new MapStoreConfig()
                        .setEnabled(true)
                        .setWriteDelaySeconds(0)
                        .setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY)
                        .setFactoryClassName(JoinStateMapStoreFactory.class.getName()));
        // Only ever with the store above behind it, where an evicted entry comes back from. With
        // nothing behind the map, evicting is losing: the entry is in no other place, and what the map
        // answers afterwards is the absence rather than the state - a fact row that stops being
        // rebuildable, a bucket that forgets the rows it named. Neither says anything while it happens.
        config.getEvictionConfig()
                .setEvictionPolicy(EvictionPolicy.LRU)
                .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
                .setSize(Math.toIntExact(entriesHeldInMemory));
        return config;
    }

    /**
     * The configuration a join state map takes with nothing behind it: what it holds lives only as long
     * as the member does. Never evicts, because evicting with no layer behind it is losing.
     */
    public static MapConfig stateMaps() {
        return new MapConfig(NAMESPACE_PREFIX + "*")
                .setBackupCount(1)
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
