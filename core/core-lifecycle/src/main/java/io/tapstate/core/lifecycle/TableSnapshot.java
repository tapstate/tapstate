package io.tapstate.core.lifecycle;

/**
 * A table's initial-snapshot progress: how far the full load of one table has advanced. This is the
 * per-table shape inside an observation's snapshot dataset. Before delivery is confirmed,
 * {@code rowsTotal} is the last discovery's estimate. After confirmation it is the recorded number
 * of rows loaded, or that estimate for older loads without a recorded count. Both {@code rowsTotal}
 * and {@code donePct} are nullable: a null total means
 * no count is available, and progress with no total is not dressed up as a completed load.
 *
 * @param rowsDone  rows loaded so far
 * @param rowsTotal the table's total row estimate, or null when unavailable
 * @param donePct   the completion percentage, or null when the total is unavailable
 */
public record TableSnapshot(long rowsDone, Long rowsTotal, Integer donePct) {
}
