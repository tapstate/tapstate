package io.tapstate.cli;

/**
 * One table's initial-snapshot progress as read back from the server: how far the full load of the table
 * has advanced. {@code rowsTotal} and {@code donePct} are nullable — a null total means the total is not
 * yet wired (unavailable) and is rendered as unavailable, never faked as 0 or 100%. This mirrors the
 * server's per-table snapshot shape independently (rule R6: the CLI carries no shared control type).
 *
 * @param rowsDone  rows loaded so far
 * @param rowsTotal the table's total row estimate, or null when unavailable
 * @param donePct   the completion percentage, or null when the total is unavailable
 * @param landed    whether the target has durably confirmed the table's whole load, or null from a server that
 *                  does not say
 */
record RemoteTableSnapshot(long rowsDone, Long rowsTotal, Integer donePct, Boolean landed) {

    /** A table's progress from a server that does not say whether its load landed. */
    RemoteTableSnapshot(long rowsDone, Long rowsTotal, Integer donePct) {
        this(rowsDone, rowsTotal, donePct, null);
    }
}
