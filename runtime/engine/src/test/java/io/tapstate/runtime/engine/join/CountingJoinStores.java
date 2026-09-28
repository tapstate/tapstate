package io.tapstate.runtime.engine.join;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
 *
 * <p><b>It spells out every call rather than extending {@link ForwardingJoinStores}, deliberately.</b>
 * Each call here is a decision about whether it is counted, and {@link #writes} is exact only while no
 * write reaches the store uncounted - so a call added to {@link JoinStores} has to fail to compile here,
 * where that decision is made, rather than arrive forwarded and uncounted.
 */
final class CountingJoinStores implements JoinStores {

    private final JoinStores held;

    /** How many times a page of fact keys was asked for, and how many keys those asks carried. */
    int batchReads;
    int keysRead;

    /** How many times a single fact key was asked for on its own. */
    int singleReads;

    /** How many times an index bucket's page count was asked for. */
    int pageCountReads;

    /**
     * How many times one page of an index bucket was read. A fact row arriving again confirms its
     * index entry page by page, so this is what shows a confirmation that stopped starting where the
     * previous row of its bucket was found.
     */
    int pageReads;

    /**
     * How many times several pages were asked at once which fact keys they name, and how many pages
     * those asks carried. A batch of rows arriving again is confirmed through these, so they are what
     * shows it still being asked about a batch at a time rather than a row at a time.
     */
    int nameReads;
    int pagesAsked;

    /**
     * How many calls changed something - either mirror, either direction, the index with them, and the
     * record of the batches a run took in whole.
     *
     * <p><b>It is here for the same reason the read counts are, one step further along.</b> The reads
     * catch an operator that went back to asking one key at a time; nothing caught an operator that
     * simply started doing more of everything. Measured: writing the fact mirror twice per row instead
     * of once made a full phase 59 percent slower and was let through, because a ratio wide enough to
     * survive an ordinary runner's noise is wider than that. As a count it is exact and the same
     * mutation is caught the moment it lands.
     *
     * <p>One counter over all seven rather than one each: what it is asked to notice is the operator
     * doing more work than it is recorded to, and which call carries it is a thing the failure message
     * can be read for.
     */
    int writes;

    CountingJoinStores(int pageSize) {
        this(new MapJoinStores(pageSize));
    }

    /**
     * Over whatever store was handed in, rather than over plain maps.
     *
     * <p>The counts are of the operator's own asking, so they are the same over any store - which is
     * what lets one number be recorded once and held against every arm and every tier. The cold layer's
     * own trip count cannot do that job: it is a count of what got past the map, so it moves with
     * eviction, and eviction here samples. Measured over three identical runs on the mixed tier, one
     * rebuild reached the layer 344, 272 and 348 times.
     */
    CountingJoinStores(JoinStores held) {
        this.held = held;
    }

    /** Puts every counter back to zero, so a case counts what it caused rather than all of it. */
    void forgetCounts() {
        batchReads = 0;
        keysRead = 0;
        singleReads = 0;
        pageCountReads = 0;
        pageReads = 0;
        nameReads = 0;
        pagesAsked = 0;
        writes = 0;
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
        writes++;
        held.putFact(factKey, row);
    }

    @Override
    public void removeFact(String factKey) {
        writes++;
        held.removeFact(factKey);
    }

    @Override
    public Map<String, Object> dimensionRow(String source, String dimensionKey) {
        return held.dimensionRow(source, dimensionKey);
    }

    @Override
    public Map<String, Object> putDimensionRow(String source, String dimensionKey,
            Map<String, Object> row) {
        writes++;
        return held.putDimensionRow(source, dimensionKey, row);
    }

    @Override
    public void removeDimensionRow(String source, String dimensionKey) {
        writes++;
        held.removeDimensionRow(source, dimensionKey);
    }

    @Override
    public int indexPageCount(String source, String dimensionKey) {
        pageCountReads++;
        return held.indexPageCount(source, dimensionKey);
    }

    @Override
    public List<String> indexPage(String source, String dimensionKey, int page) {
        pageReads++;
        return held.indexPage(source, dimensionKey, page);
    }

    @Override
    public Map<ReverseBucket.At, Set<String>> indexNames(String source,
            Map<ReverseBucket.At, Set<String>> asked) {
        nameReads++;
        pagesAsked += asked.size();
        return held.indexNames(source, asked);
    }

    @Override
    public void indexAdd(String source, String dimensionKey, String factKey) {
        writes++;
        held.indexAdd(source, dimensionKey, factKey);
    }

    @Override
    public void indexRemove(String source, String dimensionKey, String factKey) {
        writes++;
        held.indexRemove(source, dimensionKey, factKey);
    }

    @Override
    public long batchesTakenIn(String writer) {
        return held.batchesTakenIn(writer);
    }

    @Override
    public void putBatchesTakenIn(String writer, long batch) {
        writes++;
        held.putBatchesTakenIn(writer, batch);
    }
}
