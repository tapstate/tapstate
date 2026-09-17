package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * What a pipeline's run reports about rows that reached their target durably: how many of each table and
 * source operation, how recent the newest of them is per table, and the moment the counting began.
 *
 * <p>The bytes are broken out by table and not also by operation, unlike the rows: what a reader does with
 * them is compare ends or divide by rows, and the operation a row came from says nothing about how much of
 * it there was.
 *
 * <p>The four travel together because none of them is readable without the others. A running total means
 * nothing without what it accumulates from — a restart and a decrease are the same observation otherwise —
 * and how many rows arrived says nothing about whether they are current, which is the question a pipeline
 * that is quietly falling behind answers with a healthy-looking count.
 *
 * <p>What is carried for recency is an event time and not a distance. How far behind a table is keeps
 * growing while nothing arrives, so it is worked out against the clock at the moment somebody asks; a
 * distance carried here would be frozen where the run last settled something.
 *
 * <p>An empty reading is a run with nothing to report, and is not the same as a table present at zero:
 * a table this pipeline has delivered nothing for is absent, so "not measured" and "measured empty" stay
 * apart in the one place a reader could still act on the difference.
 *
 * <p>{@code deliveryDurationByTable} is the distribution of how long each settled row took from the moment
 * the source stamped it to the moment its write was confirmed, per table, over the registered bucket
 * bounds. A distribution and not an average, because the rows that took longest are the ones anybody
 * looking came for, and an average is where they disappear. It accumulates from the same start as the
 * counts: one histogram per table since the run began counting.
 */
public record DeliveryReading(Map<String, Map<String, Long>> rowsByTableAndOp,
        Map<String, Long> bytesByTable, Map<String, Long> newestEventTimeByTable,
        Instant countingSince, Map<String, HistogramValue> deliveryDurationByTable) {

    /** A reading from a run reporting nothing — no live job, or one that has settled nothing yet. */
    public static final DeliveryReading NONE =
            new DeliveryReading(Map.of(), Map.of(), Map.of(), null, Map.of());

    public DeliveryReading {
        Map<String, Map<String, Long>> rows = new LinkedHashMap<>();
        if (rowsByTableAndOp != null) {
            rowsByTableAndOp.forEach((table, byOp) -> rows.put(table, Map.copyOf(byOp)));
        }
        rowsByTableAndOp = Map.copyOf(rows);
        bytesByTable = bytesByTable == null ? Map.of() : Map.copyOf(bytesByTable);
        newestEventTimeByTable =
                newestEventTimeByTable == null ? Map.of() : Map.copyOf(newestEventTimeByTable);
        deliveryDurationByTable =
                deliveryDurationByTable == null ? Map.of() : Map.copyOf(deliveryDurationByTable);
        if ((!rowsByTableAndOp.isEmpty() || !bytesByTable.isEmpty() || !deliveryDurationByTable.isEmpty())
                && countingSince == null) {
            throw new IllegalArgumentException(
                    "a reading with totals in it says what it counted them from: totals without a start"
                            + " hand every consumer a stream in which a restart and a decrease are the"
                            + " same observation");
        }
    }

    /** A reading carrying no distribution of delivery durations — the shape callers used before it was measured. */
    public DeliveryReading(Map<String, Map<String, Long>> rowsByTableAndOp,
            Map<String, Long> bytesByTable, Map<String, Long> newestEventTimeByTable,
            Instant countingSince) {
        this(rowsByTableAndOp, bytesByTable, newestEventTimeByTable, countingSince, Map.of());
    }

    /** When the counting behind these totals began, absent for a reading that counted nothing. */
    public Optional<Instant> start() {
        return Optional.ofNullable(countingSince);
    }

    /** Whether this run reported nothing at all, as opposed to reporting nothing delivered. */
    public boolean isEmpty() {
        return rowsByTableAndOp.isEmpty() && bytesByTable.isEmpty()
                && newestEventTimeByTable.isEmpty() && deliveryDurationByTable.isEmpty();
    }
}
