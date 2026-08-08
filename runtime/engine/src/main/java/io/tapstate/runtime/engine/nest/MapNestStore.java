package io.tapstate.runtime.engine.nest;

import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * A nest store over one distributed map - the map the vertex's own name resolves to.
 *
 * <p>What this buys over keeping the entries in the store object is that the entries stop belonging to the
 * object. A processor is built again on every restart and once per parallel instance, and each of those
 * builds its own store; when the name is what identifies the state, all of them address the same entries and
 * a restart resumes instead of beginning empty.
 *
 * <p>Two of the three calls deliberately use the form that does not hand back what was there before.
 * A write returns the previous value if asked, and the previous value here is a whole document that nobody
 * reads - asking for it would pay a full serialization of it on every single write. The same holds for
 * removal. Reading the map is the only place a value is meant to travel.
 *
 * <p><b>A write is carried to its key rather than put across the map.</b> Putting a state serializes the
 * whole assembled document every time; handing it to the key it belongs to serializes none of it, because
 * the key is on the member the write is made from and what travels is a reference. Measured on both:
 * carried, a write costs nothing that grows with the document, where a put costs one full copy of it and
 * so grows with every row the document has ever absorbed.
 *
 * <p>It is a whole state that is carried, not a description of what changed in it, so a write still
 * replaces what was there in one step: nothing is left half-applied by a write that fails partway. Getting
 * to no copy at all would mean the state never leaving its partition, which is a different shape of
 * operator than this one.
 *
 * <p>This holds a live map and is therefore built on the member that will use it, never carried to one. It
 * is left plainly non-serializable rather than hiding the field, so that carrying one fails loudly at the
 * moment it is carried instead of arriving with its state silently gone.
 */
final class MapNestStore<S> implements NestStore<S> {

    private static final long serialVersionUID = 1L;

    private final IMap<Object, S> map;
    private final NestStateStats stats;
    private final NestStateGauge gauge;

    MapNestStore(IMap<Object, S> map) {
        this(map, null, NestStateGauge.NONE);
    }

    MapNestStore(IMap<Object, S> map, NestStateStats stats, NestStateGauge gauge) {
        this.map = Objects.requireNonNull(map, "map");
        this.stats = stats;
        this.gauge = Objects.requireNonNull(gauge, "gauge");
    }

    @Override
    public S load(Object key) {
        countAccess();
        S state = map.get(key);
        publishReading();
        return state;
    }

    /**
     * Leaves the namespace's current readings, after whatever changed them. Every state operation does it,
     * rather than a sample taken on a clock: how full the state is only ever changes when one of these
     * runs, so a reading left by the last of them is current for as long as no further one happens - where
     * a clock would leave a burst that filled the state and then went quiet reported at whatever it held
     * before the burst, which is the one moment an alarm on it needed to fire.
     *
     * <p>Affordable per operation because none of the four costs a round trip. The count of keys is the
     * map's own local reckoning rather than its cluster-wide size, which is a field read instead of an
     * operation - and on a member holding all of a pipeline's partitions the two are the same number.
     */
    private void publishReading() {
        if (stats == null) {
            return;
        }
        String namespace = map.getName();
        NestStateStats.Counted counted = stats.counted(namespace);
        gauge.reading(namespace, count(),
                counted.accesses(), counted.backfills(), counted.backfillNanos() / 1_000_000L);
    }

    @Override
    public void save(Object key, S state) {
        countAccess();
        map.executeOnKey(key, new Carried<>(state));
        publishReading();
    }

    @Override
    public void remove(Object key) {
        countAccess();
        map.delete(key);
        publishReading();
    }

    /**
     * Asked of the map rather than remembered, and asked of its own local reckoning rather than of the
     * cluster: the local number is a field read where the cluster-wide one is an operation, and on a member
     * holding all of a pipeline's partitions the two are the same number anyway.
     *
     * <p><b>It counts what is resident, which is every key there is only while nothing is evicted.</b> That
     * is how these maps are configured and so it is exact as they stand. A namespace switched over to
     * evicting - trading speed for capacity - stops being counted completely by this: entries that have
     * gone to the layer behind the map are not resident and are not in the number, so what comes back is
     * how much is being held rather than how much exists. Anything reading this as a total has to know that
     * about the namespace it is reading it for.
     */
    @Override
    public long count() {
        return map.getLocalMapStats().getOwnedEntryCount();
    }

    /**
     * Counts one reach for the state, whichever kind it is. A write counts as much as a read because a
     * write can miss as thoroughly: a state carried to a key that is not in memory sends the substrate to
     * the layer behind first, exactly as a read does. Measured, a key touched for the first time costs two
     * trips in the one event - one for the read and one for the write that follows it - so a ratio taken
     * over reads alone would report more of the reading served from memory than ever was.
     */
    private void countAccess() {
        if (stats != null) {
            stats.access(map.getName());
        }
    }

    /**
     * Puts a state where its key already is. It answers nothing on purpose: whatever it answered would be
     * serialized on the way back, which is the cost being avoided in the first place.
     *
     * <p>It has no backup form. These maps keep no replica - the state is rebuildable from the source and
     * from what stands behind the map - so there is no second copy for one to be applied to.
     */
    private static final class Carried<S> implements EntryProcessor<Object, S, Void>, Serializable {

        private static final long serialVersionUID = 1L;

        private final S state;

        Carried(S state) {
            this.state = state;
        }

        @Override
        public Void process(Map.Entry<Object, S> entry) {
            entry.setValue(state);
            return null;
        }

        @Override
        public EntryProcessor<Object, S, Void> getBackupProcessor() {
            return null;
        }
    }
}
