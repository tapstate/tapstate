package io.tapstate.control.core;

import io.tapstate.core.model.PipelineResource;

import java.util.List;
import java.util.Map;

/** Public, secret-free description of the exact candidate and terminal selected for execution. */
public record PipelinePreviewExecutionSpec(
        String pipelineId,
        String candidateHash,
        PipelineResource pipeline,
        String outputId,
        String outputKind,
        int rootLimit,
        String consistency,
        List<Map<String, Object>> contexts) {

    public PipelinePreviewExecutionSpec {
        contexts = contexts.stream().map(Map::copyOf).toList();
    }
}
