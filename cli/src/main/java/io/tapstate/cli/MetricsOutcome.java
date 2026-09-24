package io.tapstate.cli;

import java.time.Instant;
import java.util.List;
import java.util.Map;

/**
 * The outcome of a remote pipeline metrics read ({@code GET /api/pipelines/{id}/metrics}). Either the read
 * found the pipeline's open map of numeric run statistics ({@code name -> value}) plus the per-table
 * target-acked source position ({@code table -> opaque srcpos}), or it was refused with a coded reason (a
 * pipeline that has published no observation is {@code monitor.no-observation}), or the server could not be
 * reached. Either map is empty when its source is not wired yet (unavailable), never faked. Sealed so the
 * caller renders each branch without try/catch, mirroring the never-throw transport seam.
 */
sealed interface MetricsOutcome {

    /**
     * The read found the pipeline's open map of numeric run statistics, the per-table target-acked source
     * position, and the names of the positions the server does not record.
     *
     * @param targetAckedPosition   how far the target has confirmed writes, per table. Empty before the
     *                              first ack. Not how far the source could be read to and not how far the
     *                              pipeline has processed — naming it is the whole point of carrying it
     *                              under this name rather than as a bare offset
     * @param positionsNotCollected the positions the server named as not recorded, in its order. Carried
     *                              rather than assumed, so a server that starts collecting one stops
     *                              saying it does not and this CLI needs no release of its own to agree
     * @param facts                 the single-valued points of the measured facts beside the map, each with
     *                              the time it was observed -- what a rate is read from. Empty from a
     *                              server that sends no facts
     */
    record Found(String pipelineId, Map<String, Long> metrics, Map<String, String> targetAckedPosition,
            List<String> positionsNotCollected, List<FactPoint> facts) implements MetricsOutcome {

        public Found {
            metrics = metrics == null ? Map.of() : Map.copyOf(metrics);
            targetAckedPosition = targetAckedPosition == null ? Map.of() : Map.copyOf(targetAckedPosition);
            positionsNotCollected = positionsNotCollected == null ? List.of() : List.copyOf(positionsNotCollected);
            facts = facts == null ? List.of() : List.copyOf(facts);
        }

        /** A reading from a server that sends no measured facts — the shape before they were carried. */
        Found(String pipelineId, Map<String, Long> metrics, Map<String, String> targetAckedPosition,
                List<String> positionsNotCollected) {
            this(pipelineId, metrics, targetAckedPosition, positionsNotCollected, List.of());
        }

        /** A reading from a server that named no uncollected positions — the shape before they were named. */
        Found(String pipelineId, Map<String, Long> metrics, Map<String, String> targetAckedPosition) {
            this(pipelineId, metrics, targetAckedPosition, List.of());
        }

        /** Numeric stats only, no positions at all — the shape callers used before positions were surfaced. */
        Found(String pipelineId, Map<String, Long> metrics) {
            this(pipelineId, metrics, Map.of(), List.of());
        }
    }

    /**
     * One point of one measured metric, as the face's {@code facts} carry it: the metric's name, the
     * attributes that make this point one series rather than another, when it was observed, and its
     * value. Only points that carry a single value ride here -- a distribution has none, and nothing on
     * this side reads one. An older server sends no facts and reads back as none.
     */
    record FactPoint(String name, Map<String, String> attributes, Instant observedAt, Long value) {

        public FactPoint {
            attributes = attributes == null ? Map.of() : Map.copyOf(attributes);
        }
    }

    /** The server refused the read with a coded reason already rendered to a message. */
    record Rejected(String code, String message) implements MetricsOutcome {
    }

    /** The server could not be reached (connection refused, timeout, or a malformed target). */
    record Unreachable() implements MetricsOutcome {
    }
}
