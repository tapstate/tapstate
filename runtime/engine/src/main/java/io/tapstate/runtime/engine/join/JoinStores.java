package io.tapstate.runtime.engine.join;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The three things a join keeps, reached through named operations rather than through the maps
 * themselves, so the same driver runs against plain maps in a case and against the distributed ones in
 * a job without knowing the difference.
 *
 * <p>Each is load-bearing rather than a cache. The fact mirror is what a recompute re-reads the row
 * from; the dimension mirror is what an arriving fact row looks its dimension up in; the reverse index
 * is what a changed dimension row finds its fact rows through.
 *
 * <p><b>The writes are operations here, not {@code put} on an exposed map, and that is the point of
 * this shape.</b> Handing out the map hands out one write protocol - read it, change it, put it back -
 * and over a distributed map that protocol is the one measured to lose updates whenever two writers
 * touch a key at once: 19 090 keys collided in a run, 14 715 of them lost, with the row count right,
 * the job running and nothing reported. Naming the write instead ("add this fact key to this bucket")
 * lets an implementation make it a single atomic change to one entry, and leaves the caller unable to
 * express the losing version at all.
 *
 * <p>Reads are not restricted the same way. A distributed map answers a key from wherever it is, so
 * nothing has to be co-located with what it reads - only with what it writes.
 */
public interface JoinStores {

    /** The current image of one fact row, or null where there is none. */
    Map<String, Object> fact(String factKey);

    /**
     * The fact rows under {@code factKeys}, absent ones simply missing.
     *
     * <p><b>This exists because a recompute reads one fact row per row it re-emits, and the two ways
     * of doing that differ by three orders of magnitude on a large fan-out.</b> A million keys asked
     * one at a time against a store on the other side of a network is a million round trips; asked in
     * pages it is a few thousand. Neither answers differently, so nothing but the clock reports which
     * one is happening - and the natural diagnosis of the slow one points at the store.
     */
    Map<String, Map<String, Object>> factsUnder(Collection<String> factKeys);

    /** Records the current image of one fact row. */
    void putFact(String factKey, Map<String, Object> row);

    /** Forgets one fact row. Forgetting what is not there is not an error. */
    void removeFact(String factKey);

    /** The current image of one row of one dimension source, or null where there is none. */
    Map<String, Object> dimensionRow(String source, String dimensionKey);

    /**
     * Records the current image of one dimension row, under the key it is matched by, and answers with
     * whatever row that key already held - null where it held none.
     *
     * <p><b>The answer is what makes a lost row sayable at all.</b> One key holds one row, so a write
     * under an occupied key replaces what was there; where the arrival is a different row rather than a
     * newer image of the same one, the replaced row becomes unreachable and every fact row under that
     * key joins to the arrival instead. Nothing downstream can tell - the target is merely short a row,
     * and each row it does hold looks entirely ordinary - so the write is the only place it can be
     * observed. Discarding what came back, which is what both implementations used to do, is what left
     * it silent.
     *
     * <p>The one write here that carries a value back rather than sending one out. Over a distributed
     * map that costs the replaced row a trip home, and where the key is not in memory it is a read of
     * the layer behind the map as well - both inherent, because a key's occupant cannot be reported
     * without being asked for.
     */
    Map<String, Object> putDimensionRow(String source, String dimensionKey, Map<String, Object> row);

    /** Forgets one dimension row. */
    void removeDimensionRow(String source, String dimensionKey);

    /** How many pages of fact keys reference {@code dimensionKey}; zero where none do. */
    int indexPageCount(String source, String dimensionKey);

    /**
     * One page of the fact keys referencing {@code dimensionKey}, empty where that page holds none.
     * Handed out a page at a time rather than whole: a bucket is as large as the fan-out under one
     * dimension key, and not materialising all of it is what paging was for.
     */
    List<String> indexPage(String source, String dimensionKey, int page);

    /** Records that {@code factKey} references {@code dimensionKey}. */
    void indexAdd(String source, String dimensionKey, String factKey);

    /** Removes one record of {@code factKey} referencing {@code dimensionKey}. */
    void indexRemove(String source, String dimensionKey, String factKey);
}
