package io.tapstate.runtime.engine.nest;

import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Where one nest vertex keeps its state between events, one entry per key the vertex is partitioned by.
 *
 * <p>A vertex reads an entry when an event for that key arrives and writes it back once the batch it was
 * drained in is done, never per event: a hot key touched many times in one drain is stored once. Nothing
 * here enumerates keys, and nothing may - a vertex that could list its own entries would load them all on
 * the way back from a restart, which is the one thing the state layer exists to avoid.
 */
public interface NestStore<S> extends Serializable {

    /** The state held under {@code key}, or null when this vertex has never seen that key. */
    S load(Object key);

    /**
     * The states held under {@code keys}, with keys this vertex has never seen simply absent from the
     * result. Asking for many at once rather than one at a time is the whole point: a document resolving
     * two hundred references pays one round trip, not two hundred.
     *
     * <p><b>An implementation with anything remote behind it must override this.</b> The default loops,
     * which is free where the state is on the same heap and is exactly the degeneration to avoid where it
     * is not - and a batch that has quietly become N round trips reads identically to one that has not,
     * every document still correct, only slower. Nothing but a count of the trips can tell them apart,
     * which is why one is kept.
     */
    default Map<Object, S> loadAll(Collection<Object> keys) {
        Map<Object, S> loaded = new LinkedHashMap<>();
        for (Object key : keys) {
            S state = load(key);
            if (state != null) {
                loaded.put(key, state);
            }
        }
        return loaded;
    }

    /** Stores {@code state} under {@code key}, replacing whatever was there. */
    void save(Object key, S state);

    /** Removes the entry under {@code key} entirely, if there is one. */
    void remove(Object key);

    /**
     * Reports that one key of this vertex is holding {@code pending} changes for something that has not
     * arrived, so that how deep a wait has ever got can be read from outside the run.
     *
     * <p>Told rather than worked out here, and told by whoever already has the number: what waits lives
     * inside one entry, so arriving at it independently would mean reading the entry and counting its queue
     * again. The one place that already has it is the limit on how much a key may hold, which is why the
     * report is made there and cannot disagree with what the limit sees.
     *
     * <p>Nothing follows from a deep queue on its own - it is neither an error nor an approaching one, since
     * whether a parent is coming is not knowable from how much arrived before it. So this is published and
     * not judged. Doing nothing with it is the ordinary case, which is why it does nothing by default.
     */
    default void holding(long pending) {
    }

    /**
     * How many keys this vertex holds. It is asked of the state itself every time rather than counted up as
     * entries are written, because state outlives the run that wrote it: a tally started with the process
     * would report an almost empty vertex on the way back from a restart, and report it least accurately in
     * exactly the case a count is wanted for, which is a vertex that has grown too wide to carry.
     *
     * <p>Not a listing, and must not become one. A number is a number wherever it is kept; enumerating the
     * keys to arrive at it would read the whole keyspace, which is the one thing this layer exists to
     * avoid. An implementation that cannot answer without listing must not implement this by listing.
     */
    long count();
}
