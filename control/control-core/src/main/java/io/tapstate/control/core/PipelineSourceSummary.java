package io.tapstate.control.core;

import io.tapstate.core.model.Metadata;

import java.util.Objects;

/** Stable Source identity and display metadata embedded in a Pipeline projection. */
public record PipelineSourceSummary(String id, Metadata metadata, String connector) {

    public PipelineSourceSummary {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(connector, "connector");
    }
}
