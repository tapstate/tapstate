package io.tapstate.control.core;

import io.tapstate.core.lifecycle.TableSnapshot;
import java.util.Map;
import java.util.Objects;

/**
 * The snapshot read face: a pipeline's per-table initial-load progress. Empty outside a snapshot phase
 * or when unavailable; a table with no total reports its total as unavailable rather than faking 0 or
 * 100%.
 */
public record PipelineSnapshot(String pipelineId, Map<String, TableSnapshot> snapshot,
        io.tapstate.core.lifecycle.InitialLoad initialLoad,
        io.tapstate.core.lifecycle.InitialLoad.Phase phase) {

    public PipelineSnapshot(String pipelineId, Map<String, TableSnapshot> snapshot) {
        this(pipelineId, snapshot, null, null);
    }

    public PipelineSnapshot {
        Objects.requireNonNull(pipelineId, "pipelineId");
        snapshot = snapshot == null ? Map.of() : Map.copyOf(snapshot);
    }
}
