package io.tapstate.control.core;

/**
 * Looks at what a target holds, without changing it and without reading it through: whether it holds
 * anything at all, and roughly how much.
 *
 * <p>A probe that cannot tell throws the coded error that says why; a table that does not exist yet
 * holds nothing, and says so rather than failing.
 */
public interface TargetProbe {

    /** What the table {@code table} on the source {@code connection} holds. */
    TargetRows rows(String connection, String table);

    /**
     * What a target holds.
     *
     * @param empty      whether it holds nothing, which is always known when this is returned
     * @param count      how many rows it holds, when it said; null when it did not
     * @param countExact whether {@code count} is exact rather than an estimate from the store's metadata
     */
    record TargetRows(boolean empty, Long count, boolean countExact) {

        /** A target that holds nothing, or does not exist yet. */
        public static final TargetRows EMPTY = new TargetRows(true, 0L, true);
    }
}
