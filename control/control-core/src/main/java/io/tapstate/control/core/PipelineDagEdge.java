package io.tapstate.control.core;

import java.util.Objects;

/** One directed semantic dependency between Pipeline graph nodes. */
public record PipelineDagEdge(String id, String source, String target, String label) {

    public PipelineDagEdge {
        Objects.requireNonNull(id, "id");
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(target, "target");
    }
}
