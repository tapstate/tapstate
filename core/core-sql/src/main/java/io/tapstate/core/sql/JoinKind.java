package io.tapstate.core.sql;

/**
 * The join kinds a plan may state.
 *
 * <p>There is no right-outer member, on purpose. A right outer join is rewritten into a left outer
 * join over swapped sides before it ever reaches a plan, so no execution carrier is handed one and
 * none can implement it slightly differently from the others. Leaving the member out is what makes
 * that structural instead of a convention every carrier author has to remember.
 */
public enum JoinKind {

    /** Only the rows that matched on both sides. */
    INNER,

    /** Every row of the left side, padded with nulls wherever the right side had no match. */
    LEFT,

    /**
     * Every row of both sides. Derivable from the SQL but not executable here -- it is reported so
     * that the validation layer can refuse it by name rather than crash on it at run time.
     */
    FULL
}
