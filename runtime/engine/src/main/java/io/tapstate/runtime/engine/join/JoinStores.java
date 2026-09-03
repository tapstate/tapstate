package io.tapstate.runtime.engine.join;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The three things a join keeps, handed in rather than built, so the same driver runs against plain
 * maps in a case and against the distributed ones in a job without knowing the difference.
 *
 * <p>Each is load-bearing rather than a cache. The fact mirror is what a recompute re-reads the row
 * from; the dimension mirror is what an arriving fact row looks its dimension up in; the reverse index
 * is what a changed dimension row finds its fact rows through.
 */
public interface JoinStores {

    /** The current image of each fact row, by the fact row's own key. */
    Map<String, Map<String, Object>> facts();

    /** The current image of each row of one dimension source, by that source's join key. */
    Map<String, Map<String, Object>> dimension(String source);

    /** Which fact rows reference which key of one dimension source, a page per entry. */
    Map<ReverseBucket.At, ReverseBucket> index(String source);

    /**
     * The fact rows under {@code keys}, absent ones simply missing.
     *
     * <p><b>This exists because a recompute reads one fact row per row it re-emits, and the two ways
     * of doing that differ by three orders of magnitude on a large fan-out.</b> A million keys asked
     * one at a time against a store on the other side of a network is a million round trips; asked in
     * pages it is a few thousand. Neither answers differently, so nothing but the clock reports which
     * one is happening - and the natural diagnosis of the slow one points at the store.
     *
     * <p>The default loops openly, which is the honest thing for a map that cannot do better. An
     * implementation over a map that can fetch a batch overrides it, and is saying so by overriding.
     */
    default Map<String, Map<String, Object>> factsUnder(Collection<String> keys) {
        Map<String, Map<String, Object>> rows = new LinkedHashMap<>();
        Map<String, Map<String, Object>> facts = facts();
        for (String key : keys) {
            Map<String, Object> row = facts.get(key);
            if (row != null) {
                rows.put(key, row);
            }
        }
        return rows;
    }
}
