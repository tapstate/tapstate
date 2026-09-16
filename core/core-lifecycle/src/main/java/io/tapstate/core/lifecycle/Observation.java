package io.tapstate.core.lifecycle;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * The per-pipeline observation: the latest read-only projection of a running pipeline's state,
 * metrics and snapshot progress, keyed by pipeline id. One doc per pipeline, overwritten in place
 * (latest wins, not a time series). The runtime publishes it; the control read faces read it. The
 * shape is an external contract — adding a metric is a map entry, not a shape change; adding a field
 * is backward compatible, changing or removing one is breaking. The real Mongo serialization lives in
 * an adapter; this record is the shape.
 *
 * <ul>
 *   <li>{@code pipelineId} — the primary key, one observation per pipeline.</li>
 *   <li>{@code state} — the lifecycle state, the small stable status dataset.</li>
 *   <li>{@code metrics} — an open map of numeric run statistics ({@code name -> count}); empty when none
 *       are wired yet (unavailable), never faked.</li>
 *   <li>{@code snapshot} — per-table initial-load progress; empty outside a snapshot phase or when
 *       unavailable.</li>
 *   <li>{@code positions} — per-table source positions ({@code table -> opaque srcpos}), and exactly one
 *       kind of position: how far the target has confirmed writes (binlog/GTID/LSN). It is not how far the
 *       source could be read to and not how far the pipeline has processed — neither of those is recorded
 *       anywhere, and a reader who takes this for one of them reads a stalled target as an idle source. A
 *       String, not a count, so it rides here rather than the numeric metrics map; a read face presents it
 *       alongside metrics, under a name that says which position it is. The plain name here is the stored
 *       field's own: renaming it would rename a key in every document already written, for a reader who is
 *       a read face rather than a person. Empty when unwired.</li>
 *   <li>{@code failure} — why the run died, coded, or {@code null} while the pipeline is healthy. The
 *       state says a job died and the error count says it was counted; this says what killed it, so the
 *       reason is readable as data rather than only as a log line.</li>
 *   <li>{@code observedAt} — when this projection was taken, or {@code null} when it is not known: a
 *       document written before the field existed, or a publisher that did not record one. Without it a
 *       healthy run whose state has not changed and a run whose publisher stopped four minutes ago are
 *       the same bytes. Null means exactly "not known" and must never be filled in from another clock —
 *       a read face substituting its own time would report every stalled pipeline as fresh.</li>
 * </ul>
 */
public record Observation(
        String pipelineId,
        PipelineState state,
        Map<String, Long> metrics,
        Map<String, TableSnapshot> snapshot,
        Map<String, String> positions,
        ObservationFailure failure,
        Instant observedAt) {

    public Observation {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(state, "state");
        // A state-only observation is normal before metric / snapshot / position sources are wired: null
        // reads as an empty (unavailable) map, and the copy makes the stored projection immutable. A null
        // failure is the healthy case and stays null — absence, not an empty-coded failure.
        metrics = metrics == null ? Map.of() : Map.copyOf(metrics);
        snapshot = snapshot == null ? Map.of() : Map.copyOf(snapshot);
        positions = positions == null ? Map.of() : Map.copyOf(positions);
    }

    /**
     * An observation carrying no observation time — the shape callers used before the projection recorded
     * when it was taken. Backward compatible: observedAt reads null, which reads as "not known" rather
     * than as any particular time.
     */
    public Observation(
            String pipelineId,
            PipelineState state,
            Map<String, Long> metrics,
            Map<String, TableSnapshot> snapshot,
            Map<String, String> positions,
            ObservationFailure failure) {
        this(pipelineId, state, metrics, snapshot, positions, failure, null);
    }

    /**
     * An observation of a pipeline with no failure to report — the shape callers used before the coded
     * failure was added, and the one every healthy publish takes. Backward compatible: failure reads null.
     */
    public Observation(
            String pipelineId,
            PipelineState state,
            Map<String, Long> metrics,
            Map<String, TableSnapshot> snapshot,
            Map<String, String> positions) {
        this(pipelineId, state, metrics, snapshot, positions, null);
    }

    /**
     * A state/metrics/snapshot observation with no source positions — the shape callers used before the
     * per-table offset projection was added. Backward compatible: positions read as empty (unavailable).
     */
    public Observation(
            String pipelineId, PipelineState state, Map<String, Long> metrics, Map<String, TableSnapshot> snapshot) {
        this(pipelineId, state, metrics, snapshot, Map.of(), null);
    }
}
