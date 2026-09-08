package io.tapstate.runtime.engine.join;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A join's state over ordinary maps: what a case runs against, and the reference the distributed
 * implementation has to agree with.
 *
 * <p>Every write here is a plain change to one map, which is correct because there is one thread. It
 * is deliberately <em>not</em> what an implementation over a distributed map may do - see the note on
 * the interface about the protocol that loses updates - so the two are separate classes rather than
 * one parameterised by which map it was handed.
 *
 * <p>The batch read loops openly. A map in memory has no round trip to save, and pretending to batch
 * would make the honest implementation and the one that matters look alike in a case.
 */
public final class MapJoinStores implements JoinStores {

    private final Map<String, Map<String, Object>> facts;
    private final Map<String, Map<String, Map<String, Object>>> dimensions = new HashMap<>();
    private final Map<String, Map<ReverseBucket.At, ReverseBucket>> indexes = new HashMap<>();
    private final int pageSize;

    public MapJoinStores() {
        this(ReverseIndex.DEFAULT_PAGE_SIZE);
    }

    public MapJoinStores(int pageSize) {
        this.facts = new LinkedHashMap<>();
        this.pageSize = pageSize;
    }

    @Override
    public Map<String, Object> fact(String factKey) {
        return facts.get(factKey);
    }

    @Override
    public Map<String, Map<String, Object>> factsUnder(Collection<String> factKeys) {
        Map<String, Map<String, Object>> rows = new LinkedHashMap<>();
        for (String key : factKeys) {
            Map<String, Object> row = facts.get(key);
            if (row != null) {
                rows.put(key, row);
            }
        }
        return rows;
    }

    @Override
    public void putFact(String factKey, Map<String, Object> row) {
        facts.put(Objects.requireNonNull(factKey, "factKey"), Objects.requireNonNull(row, "row"));
    }

    @Override
    public void removeFact(String factKey) {
        facts.remove(factKey);
    }

    @Override
    public Map<String, Object> dimensionRow(String source, String dimensionKey) {
        return dimension(source).get(dimensionKey);
    }

    @Override
    public void putDimensionRow(String source, String dimensionKey, Map<String, Object> row) {
        dimension(source).put(dimensionKey, row);
    }

    @Override
    public void removeDimensionRow(String source, String dimensionKey) {
        dimension(source).remove(dimensionKey);
    }

    @Override
    public int indexPageCount(String source, String dimensionKey) {
        return index(source).pageCount(dimensionKey);
    }

    @Override
    public List<String> indexPage(String source, String dimensionKey, int page) {
        return index(source).page(dimensionKey, page);
    }

    @Override
    public void indexAdd(String source, String dimensionKey, String factKey) {
        index(source).add(dimensionKey, factKey);
    }

    @Override
    public void indexRemove(String source, String dimensionKey, String factKey) {
        index(source).remove(dimensionKey, factKey);
    }

    /** How many entries this is holding, over all three kinds - what a case looks at to see it settle. */
    public int entries() {
        int held = facts.size();
        for (Map<String, Map<String, Object>> rows : dimensions.values()) {
            held += rows.size();
        }
        for (Map<ReverseBucket.At, ReverseBucket> pages : indexes.values()) {
            held += pages.size();
        }
        return held;
    }

    private Map<String, Map<String, Object>> dimension(String source) {
        return dimensions.computeIfAbsent(source, ignored -> new LinkedHashMap<>());
    }

    private ReverseIndex index(String source) {
        return new ReverseIndex(indexes.computeIfAbsent(source, ignored -> new LinkedHashMap<>()),
                pageSize);
    }
}
