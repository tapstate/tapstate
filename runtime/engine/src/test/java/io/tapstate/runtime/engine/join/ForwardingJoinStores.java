package io.tapstate.runtime.engine.join;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The state handed in, with every call passed straight through: a case that needs one call to behave
 * differently - to fail it, or to run something else at that exact point - extends this and overrides
 * that call alone.
 *
 * <p><b>Not abstract, so a call added to {@link JoinStores} fails to compile here, once,</b> rather than
 * in every wrapper a case keeps, each of which would otherwise spell out every call to change one.
 *
 * <p>Public, and published in this module's test-jar, because the end-to-end join cases wrap the state
 * the same way the cases here do.
 */
public class ForwardingJoinStores implements JoinStores {

    private final JoinStores held;

    protected ForwardingJoinStores(JoinStores held) {
        this.held = held;
    }

    @Override
    public Map<String, Object> fact(String factKey) {
        return held.fact(factKey);
    }

    @Override
    public Map<String, Map<String, Object>> factsUnder(Collection<String> factKeys) {
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
    public Map<String, Object> putDimensionRow(String source, String dimensionKey,
            Map<String, Object> row) {
        return held.putDimensionRow(source, dimensionKey, row);
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
    public Map<ReverseBucket.At, Set<String>> indexNames(String source,
            Map<ReverseBucket.At, Set<String>> asked) {
        return held.indexNames(source, asked);
    }

    @Override
    public void indexAdd(String source, String dimensionKey, String factKey) {
        held.indexAdd(source, dimensionKey, factKey);
    }

    @Override
    public void indexRemove(String source, String dimensionKey, String factKey) {
        held.indexRemove(source, dimensionKey, factKey);
    }

    @Override
    public long batchesTakenIn(String writer) {
        return held.batchesTakenIn(writer);
    }

    @Override
    public void putBatchesTakenIn(String writer, long batch) {
        held.putBatchesTakenIn(writer, batch);
    }
}
