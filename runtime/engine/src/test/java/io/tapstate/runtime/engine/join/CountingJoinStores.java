package io.tapstate.runtime.engine.join;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * The plain-map state, plus a count of how the fact rows were asked for.
 *
 * <p><b>Both counters exist because asking for one key and asking for a page of them answer
 * identically.</b> Nothing above this can tell a run that made one trip for five hundred keys from a
 * run that made five hundred trips - the rows come back the same and the only difference is the
 * clock, on a store far enough away to have one. So the two are counted apart, and a case that means
 * to hold a batch read to being a batch says so by naming both.
 *
 * <p>It sits in its own file rather than inside one case class because the vertex and the driver
 * under it both have to be held to this, and they are two case classes.
 */
final class CountingJoinStores implements JoinStores {

    private final MapJoinStores held;

    /** How many times a page of fact keys was asked for, and how many keys those asks carried. */
    int batchReads;
    int keysRead;

    /** How many times a single fact key was asked for on its own. */
    int singleReads;

    CountingJoinStores(int pageSize) {
        this.held = new MapJoinStores(pageSize);
    }

    /** Puts every counter back to zero, so a case counts what it caused rather than all of it. */
    void forgetCounts() {
        batchReads = 0;
        keysRead = 0;
        singleReads = 0;
    }

    @Override
    public Map<String, Object> fact(String factKey) {
        singleReads++;
        return held.fact(factKey);
    }

    @Override
    public Map<String, Map<String, Object>> factsUnder(Collection<String> factKeys) {
        batchReads++;
        keysRead += factKeys.size();
        return held.factsUnder(factKeys);
    }

    @Override
    public void putFact(String factKey, Map<String, Object> row) {
        held.putFact(factKey, row);
    }

    @Override
    public void removeFact(String factKey) {
        held.removeFact(factKey);
    }

    @Override
    public Map<String, Object> dimensionRow(String source, String dimensionKey) {
        return held.dimensionRow(source, dimensionKey);
    }

    @Override
    public void putDimensionRow(String source, String dimensionKey, Map<String, Object> row) {
        held.putDimensionRow(source, dimensionKey, row);
    }

    @Override
    public void removeDimensionRow(String source, String dimensionKey) {
        held.removeDimensionRow(source, dimensionKey);
    }

    @Override
    public int indexPageCount(String source, String dimensionKey) {
        return held.indexPageCount(source, dimensionKey);
    }

    @Override
    public List<String> indexPage(String source, String dimensionKey, int page) {
        return held.indexPage(source, dimensionKey, page);
    }

    @Override
    public void indexAdd(String source, String dimensionKey, String factKey) {
        held.indexAdd(source, dimensionKey, factKey);
    }

    @Override
    public void indexRemove(String source, String dimensionKey, String factKey) {
        held.indexRemove(source, dimensionKey, factKey);
    }
}
