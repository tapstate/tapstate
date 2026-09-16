package io.tapstate.control.restapi;

import com.fasterxml.jackson.annotation.JsonInclude;
import io.tapstate.control.core.PipelineMetrics;

import java.util.List;
import java.util.Map;

/**
 * The metrics read face on the wire: the pipeline id, the open map of numeric run statistics, the per-table
 * target-acked source position ({@code table -> opaque srcpos}), and the names of the positions this
 * product does not record.
 *
 * <p>The position rides beside the metrics map rather than inside it. A position is a string and every
 * metrics cell is a number, so nesting them put one string-valued cell in an otherwise numeric map and made
 * every reader type-test a cell before using it — the control read model already keeps the two apart, and
 * this is the wire saying the same thing. The position is absent until one is acked, mirroring the
 * never-faked, empty-is-unavailable rule the numeric metrics follow.
 *
 * <p><strong>The field is named for the position it carries.</strong> {@code targetAckedPosition} is how
 * far the target has confirmed writes; it is not how far the source could be read to and not how far the
 * pipeline has processed, and it must not be read as either. Those two are the ones a reader assumes when
 * a field is called just an offset, and on a run whose target has stopped accepting writes they are the
 * two that keep moving while this one does not.
 *
 * <p><strong>{@code positionsNotCollected} names what is missing instead of leaving it out.</strong> A
 * position that simply does not appear reads the same as a position the product has no concept of, and a
 * caller cannot act on the difference it cannot see. Listing the names makes "we do not measure this" a
 * reading in its own right, and keeps a future wiring of one of them an addition a caller can notice
 * rather than a field that quietly starts appearing.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
record PipelineMetricsResponse(String pipelineId, Map<String, Long> metrics,
        Map<String, String> targetAckedPosition, List<String> positionsNotCollected) {

    static PipelineMetricsResponse of(PipelineMetrics metrics) {
        return new PipelineMetricsResponse(metrics.pipelineId(), metrics.metrics(),
                metrics.targetAckedPosition().isEmpty() ? null : metrics.targetAckedPosition(),
                PipelineMetrics.POSITIONS_NOT_COLLECTED);
    }
}
