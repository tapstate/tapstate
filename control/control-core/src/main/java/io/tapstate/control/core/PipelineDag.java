package io.tapstate.control.core;

import java.util.List;
import java.util.Objects;

/** Immutable semantic graph projected from normalized Pipeline DSL wiring. */
public record PipelineDag(List<PipelineDagNode> nodes, List<PipelineDagEdge> edges) {

    public PipelineDag {
        Objects.requireNonNull(nodes, "nodes");
        Objects.requireNonNull(edges, "edges");
        nodes = List.copyOf(nodes);
        edges = List.copyOf(edges);
    }
}
