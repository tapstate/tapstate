package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * What a pipeline's capture reports about rows it took from its sources: how many of each table and source
 * operation, and the moment the counting began.
 *
 * <p>The counterpart of {@link DeliveryReading}, and deliberately not the same record. Capture carries no
 * recency reading: how current a pipeline is, is a question about the far end - the newest row that reached
 * a target - and answering it from what has been read instead would say a pipeline is caught up while
 * everything it read sits in a queue. One record for both sides would have to carry that reading as
 * permanently empty on this one, which is an invitation to fill it with the nearest number to hand.
 *
 * <p>The start travels with the totals for the reason it does there: a running total without what it counts
 * from hands every consumer a stream in which a restart and a decrease are the same observation.
 *
 * <p>An empty reading is a capture with nothing to report, and is not the same as a table present at zero:
 * a table this pipeline has read nothing from is absent, so "not measured" and "measured empty" stay apart.
 */
public record CaptureReading(Map<String, Map<String, Long>> rowsByTableAndOp, Instant countingSince) {

    /** A reading from a pipeline capturing nothing — no live capture, or one that has taken nothing yet. */
    public static final CaptureReading NONE = new CaptureReading(Map.of(), null);

    public CaptureReading {
        Map<String, Map<String, Long>> rows = new LinkedHashMap<>();
        if (rowsByTableAndOp != null) {
            rowsByTableAndOp.forEach((table, byOp) -> rows.put(table, Map.copyOf(byOp)));
        }
        rowsByTableAndOp = Map.copyOf(rows);
        if (!rowsByTableAndOp.isEmpty() && countingSince == null) {
            throw new IllegalArgumentException(
                    "a reading that counted rows says what it counted them from: totals without a start"
                            + " hand every consumer a stream in which a restart and a decrease are the"
                            + " same observation");
        }
    }

    /** When the counting behind these totals began, absent for a reading that counted nothing. */
    public Optional<Instant> start() {
        return Optional.ofNullable(countingSince);
    }
}
