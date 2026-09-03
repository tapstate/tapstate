package io.tapstate.runtime.engine.join;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Which fact rows reference which dimension key, kept in fixed-size pages so that no entry grows
 * without bound and an append rewrites one page rather than a whole bucket.
 *
 * <p><b>It is derived, never the truth.</b> The truth about which dimension row a fact row points at is
 * the foreign key on the fact row itself, which is kept in the fact mirror. This index only says where
 * to look, and it may say too much: a fact row that was deleted, or that re-pointed elsewhere, can be
 * left named in a bucket it no longer belongs in. Whoever walks a bucket therefore reads each fact row
 * and checks what it now points at before emitting anything - an index that could send a row nowhere
 * near its dimension row would be a silent wrong answer, and no amount of care in maintaining it makes
 * checking unnecessary, because it has to survive being rebuilt as well as being maintained.
 *
 * <p><b>A fact key is added when the fact row arrives, matched or not.</b> A left outer join emits a
 * row with nulls where the dimension row is missing, and when that dimension row is later inserted
 * those rows are supposed to become matches. They can only be found again if they were written down
 * when they missed - an index of matches alone has no record that they exist, and they stay null for
 * ever with nothing reporting it. The cost is stated rather than avoided: the index holds an entry per
 * fact row rather than per matched fact row, and a dimension key that never arrives still occupies a
 * bucket.
 *
 * <p>The backing map is a plain {@link Map} so that this can be driven against an ordinary one in a
 * test and against the distributed one in a run, with nothing here knowing the difference.
 */
public final class ReverseIndex {

    /**
     * How many fact keys one page holds. Aligned with the batch the cold layer reads in, so that
     * walking a page is one round trip for its keys rather than one and a remainder.
     */
    public static final int DEFAULT_PAGE_SIZE = 1_000;

    private final Map<ReverseBucket.At, ReverseBucket> pages;
    private final int pageSize;

    public ReverseIndex(Map<ReverseBucket.At, ReverseBucket> pages) {
        this(pages, DEFAULT_PAGE_SIZE);
    }

    public ReverseIndex(Map<ReverseBucket.At, ReverseBucket> pages, int pageSize) {
        this.pages = Objects.requireNonNull(pages, "pages");
        if (pageSize < 1) {
            throw new IllegalArgumentException("a page holds at least one fact key");
        }
        this.pageSize = pageSize;
    }

    /**
     * Records that {@code factKey} references {@code dimensionKey}. Appends to the last page and opens
     * a new one when that page is full, so what is written is one page and - when a page is opened -
     * the head that counts them. Never the whole bucket: rewriting a million keys to add one is how an
     * initial load turns into something that decays smoothly and never finishes.
     *
     * <p>A fact key already present is appended again rather than ignored. Recognising it would mean
     * reading every page of the bucket on every insert, which is the cost this method exists to avoid,
     * and the duplicate is harmless where it lands: the row is rebuilt twice from the same mirror and
     * the second write carries the same value as the first.
     */
    public void add(String dimensionKey, String factKey) {
        Objects.requireNonNull(dimensionKey, "dimensionKey");
        Objects.requireNonNull(factKey, "factKey");
        ReverseBucket head = pages.get(at(dimensionKey, 0));
        if (head == null) {
            pages.put(at(dimensionKey, 0), new ReverseBucket(List.of(factKey)));
            return;
        }
        int last = head.furtherPages();
        ReverseBucket tail = last == 0 ? head : pages.get(at(dimensionKey, last));
        if (tail != null && tail.factKeys().size() < pageSize) {
            pages.put(at(dimensionKey, last), tail.with(factKey));
            return;
        }
        pages.put(at(dimensionKey, last + 1), new ReverseBucket(List.of(factKey)));
        pages.put(at(dimensionKey, 0), head.withFurtherPages(last + 1));
    }

    /**
     * Removes one record of {@code factKey} referencing {@code dimensionKey}, and answers whether there
     * was one. Pages are searched from the last backwards, because a key being removed soon after it
     * was added is on the page it was added to, and that is the last one.
     *
     * <p>Removing is what keeps a bucket from growing for ever under a dimension key nobody ever
     * changes. It is not what keeps the answers right - the check against the fact mirror does that -
     * so a caller that cannot say which dimension key a row used to point at loses storage, not
     * correctness.
     */
    public boolean remove(String dimensionKey, String factKey) {
        Objects.requireNonNull(dimensionKey, "dimensionKey");
        Objects.requireNonNull(factKey, "factKey");
        ReverseBucket head = pages.get(at(dimensionKey, 0));
        if (head == null) {
            return false;
        }
        int last = head.furtherPages();
        for (int page = last; page >= 0; page--) {
            ReverseBucket bucket = page == 0 ? head : pages.get(at(dimensionKey, page));
            if (bucket == null) {
                continue;
            }
            ReverseBucket without = bucket.without(factKey);
            if (without == bucket) {
                continue;
            }
            write(dimensionKey, page, without, head);
            return true;
        }
        return false;
    }

    /** How many pages {@code dimensionKey}'s bucket has; zero where it has no bucket at all. */
    public int pageCount(String dimensionKey) {
        ReverseBucket head = pages.get(at(dimensionKey, 0));
        return head == null ? 0 : head.furtherPages() + 1;
    }

    /**
     * The fact keys on one page of {@code dimensionKey}'s bucket, empty where that page holds none.
     * Handed out a page at a time rather than whole: a bucket is as large as the fan-out under one
     * dimension key, and materialising all of it is the thing paging was for.
     */
    public List<String> page(String dimensionKey, int page) {
        ReverseBucket bucket = pages.get(at(dimensionKey, page));
        return bucket == null ? List.of() : bucket.factKeys();
    }

    /**
     * Writes {@code without} back as page {@code page}, dropping pages off the end that have emptied so
     * that a bucket churned through does not leave a trail of empty entries behind it. The head is kept
     * while any page follows it, because it is the only place the count of them is written down.
     */
    private void write(String dimensionKey, int page, ReverseBucket without, ReverseBucket head) {
        int last = head.furtherPages();
        if (!without.isEmpty() || page != last) {
            pages.put(at(dimensionKey, page), page == 0 ? without.withFurtherPages(last) : without);
            return;
        }
        while (last > 0) {
            pages.remove(at(dimensionKey, last));
            last--;
            ReverseBucket previous = last == 0 ? head : pages.get(at(dimensionKey, last));
            if (previous != null && !previous.isEmpty()) {
                break;
            }
        }
        if (last == 0) {
            ReverseBucket first = page == 0 ? without : head;
            if (first.isEmpty()) {
                pages.remove(at(dimensionKey, 0));
            } else {
                pages.put(at(dimensionKey, 0), first.withFurtherPages(0));
            }
            return;
        }
        pages.put(at(dimensionKey, 0), head.withFurtherPages(last));
    }

    private static ReverseBucket.At at(String dimensionKey, int page) {
        return new ReverseBucket.At(dimensionKey, page);
    }
}
