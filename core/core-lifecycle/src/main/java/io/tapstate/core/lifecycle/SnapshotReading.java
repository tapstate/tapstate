package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * What a pipeline reports about its bounded initial load: how far each table's load got, and the
 * moment the current reading began. Confirmed rows may be carried from an earlier run that did the load.
 *
 * <p>The start travels with the rows for the same reason it does on the readings either side of this one.
 * A total means nothing without what it accumulates from: a pipeline restarted onto a fresh load and one
 * whose count went backwards are the same observation otherwise, and the second of those is a fault.
 *
 * <p>The start belongs to the reading and not to any one table in it. A replacement opens a new
 * observation series even when a durable completion mark lets it retain the prior load's progress.
 *
 * <p>An empty reading is a pipeline with no load to report — one reading a change stream alone, or one
 * that is not running — and it is not the same as a table present at zero rows. A table this pipeline
 * selected for an empty load stays present at zero rows, so "no load ran" and "the load found
 * nothing" stay apart.
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
