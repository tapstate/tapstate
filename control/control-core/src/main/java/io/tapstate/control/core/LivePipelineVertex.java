package io.tapstate.control.core;

import java.util.List;
import java.util.Objects;

/** One vertex of a running pipeline, with the processor instances the engine reports for it. */
public record LivePipelineVertex(String name, List<LivePipelineProcessor> processors) {

    public LivePipelineVertex {
        name = Objects.requireNonNull(name, "name");
        processors = List.copyOf(Objects.requireNonNull(processors, "processors"));
    }
}
