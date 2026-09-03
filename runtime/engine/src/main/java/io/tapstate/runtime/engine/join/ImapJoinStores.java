package io.tapstate.runtime.engine.join;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A join's state on the cluster: three distributed maps, each reading through to the cold layer behind
 * it, reached the way the driver reaches any store.
 *
 * <p><b>The batch read is the reason the fact mirror is asked through {@link #factsUnder} at all.</b>
 * {@code getAll} is what carries a page of keys to the members holding them in one exchange and, where
 * they are not in memory, to the layer beneath in one query. Asking key by key answers identically and
 * takes three orders of magnitude longer on a large recompute - the run is merely slow, and the slowness
 * points at the store rather than at the caller.
 *
 * <p><b>Every write is one change to one entry, applied where that entry lives.</b> Reading a value,
 * changing it and putting it back is the protocol measured to lose updates whenever two writers touch a
 * key at once, and both writers exist here: fact rows are routed by their own key while dimension rows
 * are routed by theirs, so the two reach one bucket from two members. An entry processor runs on the
 * member that owns the entry, serialised against every other one on that key, which is what makes an
 * append safe without having to route anything a particular way.
 *
 * <p><b>How many pages a bucket has is worked out rather than trusted.</b> The head page carries a
 * count, and it is a <em>hint</em>: it is bumped after the append it describes, so a lost bump leaves it
 * low. Reading it as the truth would then hide whole pages of fact rows, which is a fan-out that
 * silently stops part way. So the count is probed upwards from the hint until a page is absent, and the
 * hint only saves the probing from starting at nothing.
 */
public final class ImapJoinStores implements JoinStores {

    private final HazelcastInstance member;
    private final String pipelineId;
    private final String stepId;
    private final int pageSize;

    public ImapJoinStores(HazelcastInstance member, String pipelineId, String stepId) {
        this(member, pipelineId, stepId, ReverseIndex.DEFAULT_PAGE_SIZE);
    }

    public ImapJoinStores(HazelcastInstance member, String pipelineId, String stepId, int pageSize) {
        this.member = Objects.requireNonNull(member, "member");
        this.pipelineId = Objects.requireNonNull(pipelineId, "pipelineId");
        this.stepId = Objects.requireNonNull(stepId, "stepId");
        if (pageSize < 1) {
            throw new IllegalArgumentException("a page holds at least one fact key");
        }
        this.pageSize = pageSize;
    }

    @Override
    public Map<String, Object> fact(String factKey) {
        return facts().get(factKey);
    }

    @Override
    public Map<String, Map<String, Object>> factsUnder(Collection<String> factKeys) {
        if (factKeys.isEmpty()) {
            // A caller on the event path arrives with one routinely, and an exchange asking for no keys
            // is then paid in the common case rather than the odd one.
            return Map.of();
        }
        Set<String> asked = new LinkedHashSet<>(factKeys);
        return new LinkedHashMap<>(facts().getAll(asked));
    }

    @Override
    public void putFact(String factKey, Map<String, Object> row) {
        // set rather than put: put carries the previous value back across the network to be discarded.
        facts().set(factKey, row);
    }

    @Override
    public void removeFact(String factKey) {
        facts().delete(factKey);
    }

    @Override
    public Map<String, Object> dimensionRow(String source, String dimensionKey) {
        return dimension(source).get(dimensionKey);
    }

    @Override
    public void putDimensionRow(String source, String dimensionKey, Map<String, Object> row) {
        dimension(source).set(dimensionKey, row);
    }

    @Override
    public void removeDimensionRow(String source, String dimensionKey) {
        dimension(source).delete(dimensionKey);
    }

    @Override
    public int indexPageCount(String source, String dimensionKey) {
        IMap<ReverseBucket.At, ReverseBucket> pages = index(source);
        ReverseBucket head = pages.get(new ReverseBucket.At(dimensionKey, 0));
        return head == null ? 0 : lastPage(pages, dimensionKey, head) + 1;
    }

    @Override
    public List<String> indexPage(String source, String dimensionKey, int page) {
        ReverseBucket bucket = index(source).get(new ReverseBucket.At(dimensionKey, page));
        return bucket == null ? List.of() : bucket.factKeys();
    }

    /**
     * Appends to the last page, moving on a page at a time while the one tried is full. Two writers
     * deciding at once that a page is full both move on and both append to the next one, because the
     * append is what decides rather than the reading that preceded it.
     */
    @Override
    public void indexAdd(String source, String dimensionKey, String factKey) {
        IMap<ReverseBucket.At, ReverseBucket> pages = index(source);
        ReverseBucket head = pages.get(new ReverseBucket.At(dimensionKey, 0));
        int page = head == null ? 0 : lastPage(pages, dimensionKey, head);
        while (!Boolean.TRUE.equals(
                pages.executeOnKey(new ReverseBucket.At(dimensionKey, page), new Append(factKey, pageSize)))) {
            page++;
        }
        if (page > 0) {
            // The hint, after the append it describes rather than before it: a bump that landed for an
            // append that did not would name a page holding nothing.
            pages.executeOnKey(new ReverseBucket.At(dimensionKey, 0), new Hint(page));
        }
    }

    @Override
    public void indexRemove(String source, String dimensionKey, String factKey) {
        IMap<ReverseBucket.At, ReverseBucket> pages = index(source);
        ReverseBucket head = pages.get(new ReverseBucket.At(dimensionKey, 0));
        if (head == null) {
            return;
        }
        int last = lastPage(pages, dimensionKey, head);
        for (int page = last; page >= 0; page--) {
            if (Boolean.TRUE.equals(pages.executeOnKey(new ReverseBucket.At(dimensionKey, page),
                    new Drop(factKey)))) {
                break;
            }
        }
        trim(pages, dimensionKey);
    }

    /**
     * Drops pages off the end that have emptied, so a bucket churned through does not leave a trail of
     * entries holding nothing - the memory budget over these maps counts entries and is blind to how
     * large one is. Only off the end: an empty page in the middle stays, because removing it would put
     * a hole in the run of pages the count above probes across.
     */
    private void trim(IMap<ReverseBucket.At, ReverseBucket> pages, String dimensionKey) {
        ReverseBucket head = pages.get(new ReverseBucket.At(dimensionKey, 0));
        if (head == null) {
            return;
        }
        int page = lastPage(pages, dimensionKey, head);
        // Read-then-delete would be a page emptied by this thread and refilled by another between the
        // two, so the emptiness is decided where the entry lives and the delete happens there or not
        // at all.
        while (page > 0 && Boolean.TRUE.equals(
                pages.executeOnKey(new ReverseBucket.At(dimensionKey, page), new DropIfEmpty()))) {
            page--;
        }
        pages.executeOnKey(new ReverseBucket.At(dimensionKey, 0), new Hint(page, true));
        if (page == 0) {
            // The head goes too once nothing is under this dimension key at all - a bucket that
            // outlived its rows is an entry spent on nothing, and the budget over these maps counts
            // entries. It refuses while it still says pages follow it.
            pages.executeOnKey(new ReverseBucket.At(dimensionKey, 0), new DropIfEmpty());
        }
    }

    /** The last page of this bucket: the hint, then upwards while a further page is there. */
    private static int lastPage(IMap<ReverseBucket.At, ReverseBucket> pages, String dimensionKey,
            ReverseBucket head) {
        int page = head.furtherPages();
        while (pages.containsKey(new ReverseBucket.At(dimensionKey, page + 1))) {
            page++;
        }
        return page;
    }

    private IMap<String, Map<String, Object>> facts() {
        return member.getMap(JoinMaps.factMirror(pipelineId, stepId));
    }

    private IMap<String, Map<String, Object>> dimension(String source) {
        return member.getMap(JoinMaps.dimensionMirror(pipelineId, stepId, source));
    }

    private IMap<ReverseBucket.At, ReverseBucket> index(String source) {
        return member.getMap(JoinMaps.reverseIndex(pipelineId, stepId, source));
    }

    /** Appends one fact key to a page, or says the page is full. Runs where the entry lives. */
    static final class Append
            implements EntryProcessor<ReverseBucket.At, ReverseBucket, Boolean>, Serializable {

        private static final long serialVersionUID = 1L;

        private final String factKey;
        private final int pageSize;

        Append(String factKey, int pageSize) {
            this.factKey = factKey;
            this.pageSize = pageSize;
        }

        @Override
        public Boolean process(Map.Entry<ReverseBucket.At, ReverseBucket> entry) {
            ReverseBucket bucket = entry.getValue();
            if (bucket == null) {
                entry.setValue(new ReverseBucket(List.of(factKey)));
                return true;
            }
            if (bucket.factKeys().size() >= pageSize) {
                return false;
            }
            List<String> grown = new ArrayList<>(bucket.factKeys());
            grown.add(factKey);
            entry.setValue(new ReverseBucket(grown, bucket.furtherPages()));
            return true;
        }
    }

    /** Removes the first record of one fact key from a page, and says whether there was one. */
    static final class Drop
            implements EntryProcessor<ReverseBucket.At, ReverseBucket, Boolean>, Serializable {

        private static final long serialVersionUID = 1L;

        private final String factKey;

        Drop(String factKey) {
            this.factKey = factKey;
        }

        @Override
        public Boolean process(Map.Entry<ReverseBucket.At, ReverseBucket> entry) {
            ReverseBucket bucket = entry.getValue();
            if (bucket == null) {
                return false;
            }
            int at = bucket.factKeys().indexOf(factKey);
            if (at < 0) {
                return false;
            }
            List<String> left = new ArrayList<>(bucket.factKeys());
            left.remove(at);
            entry.setValue(new ReverseBucket(left, bucket.furtherPages()));
            return true;
        }
    }

    /**
     * Deletes a page that holds nothing, and says whether it did. The head refuses while it still says
     * pages follow it: deleting it would leave those pages with nothing naming them, and the count
     * above would answer that this dimension key has no fact rows at all.
     */
    static final class DropIfEmpty
            implements EntryProcessor<ReverseBucket.At, ReverseBucket, Boolean>, Serializable {

        private static final long serialVersionUID = 1L;

        @Override
        public Boolean process(Map.Entry<ReverseBucket.At, ReverseBucket> entry) {
            ReverseBucket bucket = entry.getValue();
            if (bucket == null) {
                return true;
            }
            if (!bucket.isEmpty()) {
                return false;
            }
            if (entry.getKey().page() == 0 && bucket.furtherPages() > 0) {
                return false;
            }
            entry.setValue(null);
            return true;
        }
    }

    /**
     * Moves the head's page-count hint. Growing it never loses anything - a hint that is too high is
     * probed past - and it <b>creates the head where there is none</b>, which is the one thing that
     * keeps a page from being stranded: an append opening page 1 while the head is being dropped as
     * empty would otherwise bump nothing, and the count would then answer that the bucket is gone
     * while page 1 still holds fact rows.
     *
     * <p>Lowering it happens only after the pages it counted are actually gone, and never invents a
     * head: there is nothing to record.
     */
    static final class Hint
            implements EntryProcessor<ReverseBucket.At, ReverseBucket, Void>, Serializable {

        private static final long serialVersionUID = 1L;

        private final int page;
        private final boolean exact;

        Hint(int page) {
            this(page, false);
        }

        Hint(int page, boolean exact) {
            this.page = page;
            this.exact = exact;
        }

        @Override
        public Void process(Map.Entry<ReverseBucket.At, ReverseBucket> entry) {
            ReverseBucket head = entry.getValue();
            if (head == null) {
                if (!exact) {
                    entry.setValue(new ReverseBucket(List.of(), page));
                }
                return null;
            }
            if (!exact && head.furtherPages() >= page) {
                return null;
            }
            if (head.furtherPages() != page) {
                entry.setValue(new ReverseBucket(head.factKeys(), page));
            }
            return null;
        }
    }
}
