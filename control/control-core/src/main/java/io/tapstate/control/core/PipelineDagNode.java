package io.tapstate.control.core;

import java.util.List;
import java.util.Objects;

/** One semantic Pipeline graph node; coordinates deliberately live outside the artifact view. */
public record PipelineDagNode(String id, String type, String label, String detail, List<String> tableRefs) {

    public PipelineDagNode {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(label, "label");
        tableRefs = tableRefs == null ? List.of() : List.copyOf(tableRefs);
    }

    /** Keeps the graph response compatible for nodes that do not represent several table names. */
    public PipelineDagNode(String id, String type, String label, String detail) {
        this(id, type, label, detail, List.of());
    }
}
