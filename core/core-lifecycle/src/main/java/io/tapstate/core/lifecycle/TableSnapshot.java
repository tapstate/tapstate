package io.tapstate.core.lifecycle;

/**
 * A table's initial-snapshot progress: how far the full load of one table has advanced. This is the
 * per-table shape inside an observation's snapshot dataset. Before delivery is confirmed,
 * {@code rowsTotal} is the last discovery's estimate. After confirmation it is the recorded number
 * of rows loaded, or that estimate for older loads without a recorded count. Both {@code rowsTotal}
 * and {@code donePct} are nullable: a null total means
 * no count is available, and progress with no total is not dressed up as a completed load.
 *
 * <p>A table is reported once its load has been read through, and it is then either landing or landed. Landing,
 * its rows are still being written to the target; landed, the target has confirmed every one of them durably. That
 * is the table's turn from its load to its changes: a sink whose writers share one target table's rows writes that
 * table's changes only once its load has landed, so a table read through and not landed may have changes held for
 * it.
 *
 * @param rowsDone  rows loaded so far
 * @param rowsTotal the table's total row estimate, or null when unavailable
 * @param donePct   the completion percentage, or null when the total is unavailable
 * @param landed    whether the target has durably confirmed every row of the table's load
 */
public record TableSnapshot(long rowsDone, Long rowsTotal, Integer donePct, boolean landed) {

    /** A table read through whose load the target has not confirmed yet. */
    public TableSnapshot(long rowsDone, Long rowsTotal, Integer donePct) {
        this(rowsDone, rowsTotal, donePct, false);
    }
}
