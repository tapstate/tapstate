package io.tapstate.control.core;

import io.tapstate.core.model.Metadata;

import java.util.Map;
import java.util.Objects;

/** Stable Source identity and display metadata embedded in a Pipeline projection. */
public record PipelineSourceSummary(String id, Metadata metadata, String connector, boolean resolved,
        Map<String, Object> nodeConfig, String alias) {

    public PipelineSourceSummary {
        Objects.requireNonNull(id, "id");
        nodeConfig = SourceDraft.copyJsonMap(nodeConfig, false);
        if (resolved) {
            Objects.requireNonNull(connector, "connector");
        }
    }

    public PipelineSourceSummary(String id, Metadata metadata, String connector, boolean resolved,
            Map<String, Object> nodeConfig) {
        this(id, metadata, connector, resolved, nodeConfig, null);
    }

    String binding() {
        return alias == null ? id : alias;
    }

    public PipelineSourceSummary(String id, Metadata metadata, String connector, boolean resolved) {
        this(id, metadata, connector, resolved, Map.of());
    }

    public PipelineSourceSummary(String id, Metadata metadata, String connector) {
        this(id, metadata, connector, true);
    }

    public static PipelineSourceSummary unresolved(String id) {
        return new PipelineSourceSummary(id, null, null, false);
    }
}
