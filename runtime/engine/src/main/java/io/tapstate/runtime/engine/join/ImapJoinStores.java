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
 *
 * <p><b>That walk is paid only where an answer that is too low would be wrong.</b> Its last step is the
 * question that ends it - is the page after the last one there - and the answer is always no; asking a
 * read-through map about a key that is nowhere in memory is a trip to the layer under it, so each walk
 * costs one trip that is certain to find nothing. Counting a bucket's pages and removing a fact key from
 * one both need the true end and pay it. An append does not: it is told by the page it tries that the
 * page is full, and moves on, so starting from the hint costs an extra attempt in the window where the
 * hint lags and nothing at all the rest of the time. Neither does the trailing-page trim behind a
 * removal, which is handed the end the removal already walked to - only the read that decides whether
 * the head itself may go is paid again, and that is asked once a bucket has emptied rather than on
 * every removal.
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
    public Map<String, Object> putDimensionRow(String source, String dimensionKey,
            Map<String, Object> row) {
        // put rather than set, which is the opposite of the fact mirror next door and is bought
        // deliberately. set neither carries the previous value back nor reads it from the layer behind
        // the map, and both are exactly what has to happen here: the row a key already held is the row
        // this write is about to make unreachable, and it is unsayable unless it is asked for. Kept to
        // this one map - the fact mirror has no such question to ask and still uses set.
        return dimension(source).put(dimensionKey, row);
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
     *
     * <p>It starts at the hint rather than walking to the true end, because the page it tries is what
     * tells it whether it started low - and unlike a count or a removal, being told costs an attempt
     * rather than an answer that is wrong.
     */
    @Override
    public void indexAdd(String source, String dimensionKey, String factKey) {
        IMap<ReverseBucket.At, ReverseBucket> pages = index(source);
        ReverseBucket head = pages.get(new ReverseBucket.At(dimensionKey, 0));
        int page = head == null ? 0 : head.furtherPages();
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
        trim(pages, dimensionKey, last);
    }

    /**
     * Drops pages off the end that have emptied, so a bucket churned through does not leave a trail of
     * entries holding nothing - the memory budget over these maps counts entries and is blind to how
     * large one is. Only off the end: an empty page in the middle stays, because removing it would put
     * a hole in the run of pages the count above probes across.
     *
     * <p>The end is the one the removal in front of it already walked to, rather than a second walk to
     * the same place. Nothing between the two opens a page, and were something to, this would start
     * below the end and trim less - which the next removal's trim does instead.
     */
    private void trim(IMap<ReverseBucket.At, ReverseBucket> pages, String dimensionKey, int end) {
        int page = end;
        // Read-then-delete would be a page emptied by this thread and refilled by another between the
        // two, so the emptiness is decided where the entry lives and the delete happens there or not
        // at all.
        while (page > 0 && Boolean.TRUE.equals(
                pages.executeOnKey(new ReverseBucket.At(dimensionKey, page), new DropIfEmpty()))) {
            page--;
        }
        ReverseBucket.At head = new ReverseBucket.At(dimensionKey, 0);
        boolean headHoldsNothing = Boolean.TRUE.equals(pages.executeOnKey(head, new Hint(page, true)));
        // The head goes too once nothing is under this dimension key at all - a bucket that outlived
        // its rows is an entry spent on nothing, and the budget over these maps counts entries. What
        // says nothing is under it is the page after it, read here and now: the head's own count of
        // the pages following it has just been lowered onto an end walked to before any of this, so
        // it does not know about a page an append opened in between, and a head deleted on the
        // strength of it leaves that page named by nothing. Read only once the head holds nothing,
        // which is once in a bucket's life rather than once per removal - the page it asks about is
        // the one that is not there, so the question is a trip to the layer beneath.
        if (page == 0 && headHoldsNothing
                && !pages.containsKey(new ReverseBucket.At(dimensionKey, 1))) {
            pages.executeOnKey(head, new DropIfEmpty(true));
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
     * Deletes a page that holds nothing, and says whether it did.
     *
     * <p><b>The head is refused unless the caller has read the page after it and found nothing
     * there.</b> The head's own count of the pages that follow it is not evidence on its own: a
     * removal lowers it onto the end it walked to, which overwrites the bump of an append that opened
     * a page in between, and a head deleted on the strength of that leaves the page named by nothing
     * and the count answering that this dimension key has no fact rows at all. Nothing running here
     * can look at another entry, so the reading is the caller's and its answer is carried in - and
     * both are required, because each covers the window the other leaves: the reading misses an
     * append that lands after it, and that append's bump is in the count by the time this runs.
     */
    static final class DropIfEmpty
            implements EntryProcessor<ReverseBucket.At, ReverseBucket, Boolean>, Serializable {

        private static final long serialVersionUID = 1L;

        private final boolean nothingFollows;

        /** For any page but the head, which nothing follows by construction. */
        DropIfEmpty() {
            this(false);
        }

        DropIfEmpty(boolean nothingFollows) {
            this.nothingFollows = nothingFollows;
        }

        @Override
        public Boolean process(Map.Entry<ReverseBucket.At, ReverseBucket> entry) {
            ReverseBucket bucket = entry.getValue();
            if (bucket == null) {
                return true;
            }
            if (!bucket.isEmpty()) {
                return false;
            }
            // The head goes on the caller's word that the page after it is not there, and only while
            // its own count agrees that none does. Neither is evidence alone: the reading is stale by
            // the time this runs, and the count was lowered onto an end walked to before that.
            if (entry.getKey().page() == 0 && !(nothingFollows && bucket.furtherPages() == 0)) {
                return false;
            }
            entry.setValue(null);
            return true;
        }
    }

    /**
     * Moves the head's page-count hint, and says whether the head is left holding nothing. The saying
     * is free where the moving already runs, and it is what tells a trim whether dropping the head is
     * even worth a read.
     *
     * <p>Growing the hint never loses anything - a hint that is too high is probed past - and it
     * <b>creates the head where there is none</b>, which is the one thing that keeps a page from being
     * stranded: an append opening page 1 while the head is being dropped as empty would otherwise bump
     * nothing, and the count would then answer that the bucket is gone while page 1 still holds fact
     * rows.
     *
     * <p>Lowering it happens only after the pages it counted are actually gone, and never invents a
     * head: there is nothing to record. It is not a statement that nothing follows the head either -
     * the end it lowers onto was walked to before it ran, and an append may have opened a page since.
     */
    static final class Hint
            implements EntryProcessor<ReverseBucket.At, ReverseBucket, Boolean>, Serializable {

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
        public Boolean process(Map.Entry<ReverseBucket.At, ReverseBucket> entry) {
            ReverseBucket head = entry.getValue();
            if (head == null) {
                if (exact) {
                    return false;
                }
                entry.setValue(new ReverseBucket(List.of(), page));
                return true;
            }
            if (exact ? head.furtherPages() != page : head.furtherPages() < page) {
                entry.setValue(new ReverseBucket(head.factKeys(), page));
            }
            return head.isEmpty();
        }
    }
}
