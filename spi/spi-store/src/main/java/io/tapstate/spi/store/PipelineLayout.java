package io.tapstate.spi.store;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Persisted editor-only layout for one Pipeline; it is not part of the canonical artifact. */
public record PipelineLayout(String pipelineId, Map<String, NodePosition> nodes, Viewport viewport) {

    public PipelineLayout {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (pipelineId.isBlank()) {
            throw new IllegalArgumentException("pipelineId must not be blank");
        }
        Objects.requireNonNull(nodes, "nodes");
        nodes.forEach((id, position) -> {
            if (id == null || id.isBlank()) {
                throw new IllegalArgumentException("node id must not be blank");
            }
            Objects.requireNonNull(position, "node position");
        });
        nodes = Map.copyOf(new LinkedHashMap<>(nodes));
    }

    /** React Flow-compatible canvas coordinates for one stable graph node id. */
    public record NodePosition(double x, double y) {
        public NodePosition {
            if (!Double.isFinite(x) || !Double.isFinite(y)) {
                throw new IllegalArgumentException("node position must be finite");
            }
        }
    }

    /** React Flow-compatible pan and zoom state for the full canvas. */
    public record Viewport(double x, double y, double zoom) {
        public Viewport {
            if (!Double.isFinite(x) || !Double.isFinite(y) || !Double.isFinite(zoom) || zoom <= 0) {
                throw new IllegalArgumentException("viewport must be finite with a positive zoom");
            }
        }
    }
}
