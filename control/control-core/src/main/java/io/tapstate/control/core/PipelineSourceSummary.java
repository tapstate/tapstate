package io.tapstate.control.core;

import io.tapstate.core.model.Metadata;

import java.util.Objects;

/** Stable Source identity and display metadata embedded in a Pipeline projection. */
public record PipelineSourceSummary(String id, Metadata metadata, String connector, boolean resolved) {

    public PipelineSourceSummary {
        Objects.requireNonNull(id, "id");
        if (resolved) {
            Objects.requireNonNull(connector, "connector");
        }
    }

    public PipelineSourceSummary(String id, Metadata metadata, String connector) {
        this(id, metadata, connector, true);
    }

    public static PipelineSourceSummary unresolved(String id) {
        return new PipelineSourceSummary(id, null, null, false);
    }
}
