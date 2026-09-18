package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * What a pipeline's run reports about its bounded initial load: how far each table's load got, and the
 * moment the load these totals accumulate from began.
 *
 * <p>The start travels with the rows for the same reason it does on the readings either side of this one.
 * A total means nothing without what it accumulates from: a pipeline restarted onto a fresh load and one
 * whose count went backwards are the same observation otherwise, and the second of those is a fault.
 *
 * <p>The start belongs to the load and not to any one table in it. Every table here was loaded by one run
 * of one pipeline, and a table has no account of its own to open — what a second run produces is a whole
 * new reading, not a later point on this one.
 *
 * <p>An empty reading is a pipeline with no load to report — one reading a change stream alone, or one
 * that is not running — and it is not the same as a table present at zero rows. A table this pipeline
 * loaded nothing for is absent, so "no load ran" and "the load found nothing" stay apart.
 */
public record SnapshotReading(Map<String, TableSnapshot> byTable, Instant countingSince) {

    /** A reading from a pipeline with no bounded load: change-stream only, or not running. */
    public static final SnapshotReading NONE = new SnapshotReading(Map.of(), null);

    public SnapshotReading {
        byTable = byTable == null ? Map.of() : Map.copyOf(byTable);
        if (!byTable.isEmpty() && countingSince == null) {
            throw new IllegalArgumentException(
                    "a reading that counted rows says what it counted them from: totals without a start"
                            + " hand every consumer a stream in which a restart and a decrease are the"
                            + " same observation");
        }
    }

    /** When the load behind these totals began, absent for a reading that reports no load. */
    public Optional<Instant> start() {
        return Optional.ofNullable(countingSince);
    }
}
