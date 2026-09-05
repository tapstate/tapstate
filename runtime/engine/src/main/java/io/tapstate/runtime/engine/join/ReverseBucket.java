package io.tapstate.runtime.engine.join;

import com.hazelcast.partition.PartitionAware;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * One page of the fact keys that reference a dimension key. The reverse index is what turns a change
 * to a dimension row into the set of joined rows that have to be built again, and it is the one
 * structure here whose size under a single key has no upper bound of its own: a dimension row with a
 * million facts pointing at it is a million fact keys.
 *
 * <p><b>It is paged rather than kept whole, and the ceiling that forces this is external.</b> The cold
 * layer writes an entry's whole value as one field of one document, and the store behind it refuses a
 * document past a fixed size. An unpaged bucket is therefore not slow at a million keys - it cannot be
 * written at all, and it fails on the day one dimension key crosses the line, as a coded io error from
 * somewhere that reads like the store is broken.
 *
 * <p><b>Paged rather than sharded.</b> A fixed number of shards bounds the entry too, but the number
 * can never be changed afterwards: re-sharding means visiting every dimension key, and the layer under
 * this deliberately offers no way to list them. Pages are opened as they are needed and closed as they
 * empty, so nothing has to be chosen up front and nothing has to be migrated later.
 *
 * <p>Three things follow from paging that a single entry does not give: an append rewrites one page
 * instead of the whole bucket, no page approaches the document ceiling, and the number of entries is
 * related to the number of bytes again - which matters because the memory budget over these maps counts
 * entries and is blind to how large one is.
 *
 * @param factKeys      the fact keys on this page, in the order they were added
 * @param furtherPages  how many pages follow this one. <b>Meaningful on page 0 only</b>: it is read to
 *                      find the end of the bucket, and a later page carries zero because nothing asks
 *                      it. Keeping the count in one place is what makes an append a bounded read
 *                      rather than a walk
 */
public record ReverseBucket(List<String> factKeys, int furtherPages) implements Serializable {

    private static final long serialVersionUID = 1L;

    public ReverseBucket {
        Objects.requireNonNull(factKeys, "factKeys");
        factKeys = Collections.unmodifiableList(new ArrayList<>(factKeys));
        if (furtherPages < 0) {
            throw new IllegalArgumentException("a bucket cannot be in fewer than no further pages");
        }
    }

    /** A page with nothing after it, which is what every page but the head of a long bucket is. */
    public ReverseBucket(List<String> factKeys) {
        this(factKeys, 0);
    }

    /**
     * Where one page of one bucket lives.
     *
     * <p><b>Every page of a bucket is placed by the dimension key, not by the page number.</b> The
     * processor that reads a bucket is the one the dimension key was routed to, so pages scattered by
     * their own hash would be read across members by the one member that already holds the key they
     * belong to - the whole bucket fetched over the network to answer a change that arrived locally.
     */
    public record At(String dimensionKey, int page) implements Serializable, PartitionAware<String> {

        private static final long serialVersionUID = 1L;

        public At {
            Objects.requireNonNull(dimensionKey, "dimensionKey");
            if (page < 0) {
                throw new IllegalArgumentException("a bucket has no page before its first");
            }
        }

        @Override
        public String getPartitionKey() {
            return dimensionKey;
        }
    }

    /** Whether this page holds nothing, which is not a thing worth keeping an entry for. */
    public boolean isEmpty() {
        return factKeys.isEmpty();
    }

    /** This page with {@code factKey} appended. */
    ReverseBucket with(String factKey) {
        List<String> grown = new ArrayList<>(factKeys);
        grown.add(factKey);
        return new ReverseBucket(grown, furtherPages);
    }

    /**
     * This page with the first occurrence of {@code factKey} gone, or this page unchanged when it does
     * not hold one. The first occurrence rather than all of them: a repeat is a redelivered insert, and
     * the redelivery it came from will be matched by a redelivery of whatever removes it.
     */
    ReverseBucket without(String factKey) {
        int at = factKeys.indexOf(factKey);
        if (at < 0) {
            return this;
        }
        List<String> left = new ArrayList<>(factKeys);
        left.remove(at);
        return new ReverseBucket(left, furtherPages);
    }

    /** This page, saying that {@code furtherPages} follow it. */
    ReverseBucket withFurtherPages(int furtherPages) {
        return new ReverseBucket(factKeys, furtherPages);
    }
}
