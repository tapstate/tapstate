package io.tapstate.control.core;

import java.util.Objects;

/** One semantic Pipeline graph node; coordinates deliberately live outside the artifact view. */
public record PipelineDagNode(String id, String type, String label, String detail) {

    public PipelineDagNode {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(type, "type");
        Objects.requireNonNull(label, "label");
    }
}
