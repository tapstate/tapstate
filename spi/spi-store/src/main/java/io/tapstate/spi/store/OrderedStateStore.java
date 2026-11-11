package io.tapstate.spi.store;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * The cold layer under an operator that needs two things {@link KeyedStateStore} leaves out on purpose:
 * several entries changed together or not at all, and the least or greatest key of a range.
 *
 * <p>Keys are bytes and sort as bytes: unsigned, left to right, a shorter key before any key it begins.
 * The caller chooses bytes whose order is the order it needs; the store never looks inside a key or a
 * state. One store holds the entries of one operator placement and nothing else.
 *
 * <p><b>A commit is whole.</b> Every save and delete of one {@link #commit} is applied, or none is, and a
 * writer that no longer holds the pipeline's claim has its commit refused. Entries the caller needs to
 * agree with each other go in one commit; nothing else keeps them in step.
 *
 * <p><b>Reads are not fenced.</b> They see only committed entries, and a member that lost the claim may
 * still read until it next has to prove it. What a caller keeps from a read is a copy that the next
 * commit by anyone else can make stale; a single writer per key, which partitioning gives, is what makes
 * that safe.
 *
 * <p>{@link #first} is the one ordered read the data path gets, and it reads one entry. {@link #scan}
 * walks a range a page at a time and is for maintenance that an operator asks for, never for handling
 * an event: a path that scanned to serve an event would read a whole group to change one row of it.
 */
public interface OrderedStateStore {

    /** One stored entry. The arrays are the caller's to keep: the store hands out and takes copies. */
    record Entry(byte[] key, byte[] state) {
        public Entry {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(state, "state");
        }
    }

    /** Which end of a range {@link #first} returns. */
    enum End { LOWEST, HIGHEST }

    /** The states of {@code keys}, in the order asked, with a null where a key holds nothing. */
    List<byte[]> loadAll(List<byte[]> keys);

    /**
     * Applies every save and every delete, or none. A key may appear once across both lists. Deleting a
     * key that holds nothing is not an error. A commit larger than {@link #commitLimit} is refused before
     * anything is written.
     */
    void commit(List<Entry> saves, List<byte[]> deletes);

    /** How much one commit may carry. A caller with more splits it where its own entries allow. */
    Limit commitLimit();

    /** The bound of one commit: how many saves and deletes together, and how many bytes of keys and states. */
    record Limit(int operations, long bytes) {
        public Limit {
            if (operations < 1 || bytes < 1) {
                throw new IllegalArgumentException("a commit limit admits at least one operation and one byte");
            }
        }
    }

    /**
     * The entry with the lowest or highest key in {@code [from, to)}, or empty when the range holds none.
     * Reads one entry however many the range holds.
     */
    Optional<Entry> first(byte[] from, byte[] to, End end);

    /**
     * Up to {@code limit} entries of {@code [from, to)} in ascending key order, starting after
     * {@code after} when it is not null. A page shorter than {@code limit} is the last one.
     */
    List<Entry> scan(byte[] from, byte[] to, byte[] after, int limit);
}
