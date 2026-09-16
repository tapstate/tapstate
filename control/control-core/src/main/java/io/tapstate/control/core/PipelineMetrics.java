package io.tapstate.control.core;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The metrics read face: a pipeline's open map of numeric run statistics ({@code name -> value}) plus the
 * one source position this product records, per table ({@code table -> opaque srcpos}). The field set is
 * deliberately not fixed — adding a metric is a map entry, not a contract-shape change. The position rides
 * in its own map rather than the numeric one because a srcpos (binlog/GTID/LSN) is an opaque String, not a
 * count; a read face presents the two together. Either map is empty when its source is not wired yet
 * (unavailable), never faked.
 *
 * <p><strong>The position recorded here is the target-acked one, and its name says so.</strong> It is how
 * far the target has confirmed writes — not how far the source could be read to, and not how far this
 * pipeline has processed. Those are three different facts that move at three different times: a run
 * reading and processing normally while its target refuses writes moves two of them and not this one. A
 * position named only "offset" is read as whichever of the three the reader came looking for, and that is
 * what makes a stalled target and an idle source the same reading.
 *
 * <p><strong>The two positions this product does not record are named, not omitted</strong>
 * ({@link #POSITIONS_NOT_COLLECTED}). An absent field reads exactly like a concept the product does not
 * have, and only one of those is an answer — a reader who sees nothing cannot tell "not measured" from
 * "measured, and there is nothing yet".
 *
 * <p><strong>Keyed per table, but one position per chain.</strong> A chain is one read of one source's
 * change log and confirms one position for every table on it, so each table the chain carries shows the
 * same value. Two tables never disagree here, and a reader watching one table move is watching the chain.
 * Saying so is the same rule as the paragraph above applied to this map's shape: a per-table key that
 * quietly held a per-chain fact would be the name claiming a measurement nobody takes.
 */
public record PipelineMetrics(
        String pipelineId, Map<String, Long> metrics, Map<String, String> targetAckedPosition) {

    /**
     * The positions this face names but does not record, in the order a reader meets them along a pipeline:
     * how far the source can currently be read to, and how far this pipeline has processed. Neither is
     * collected anywhere in the product today — obtaining either is that capability's own work, and wiring
     * one removes its name from here on the way through.
     *
     * <p>These are the names a reader sees, not codes to be translated on the way out: the field that does
     * carry a position is called {@code targetAckedPosition} on every face, and a name that changed shape
     * between the wire and the screen would be a fourth thing to keep in step for no reader's benefit.
     */
    public static final List<String> POSITIONS_NOT_COLLECTED =
            List.of("sourceHeadPosition", "processedPosition");

    public PipelineMetrics {
        Objects.requireNonNull(pipelineId, "pipelineId");
        metrics = metrics == null ? Map.of() : Map.copyOf(metrics);
        targetAckedPosition = targetAckedPosition == null ? Map.of() : Map.copyOf(targetAckedPosition);
    }
}
