package io.tapstate.core.validation;

/** Exact validation tenure. A node name alone never authorizes a callback. */
public record ValidationOwner(String clusterId, String pipelineId, String nodeId,
                              String bootId, long generation, long fence) {
    public ValidationOwner {
        clusterId = ValidationError.identifier(clusterId, "clusterId");
        pipelineId = ValidationError.identifier(pipelineId, "pipelineId");
        nodeId = ValidationError.identifier(nodeId, "nodeId");
        bootId = ValidationError.identifier(bootId, "bootId");
        ValidationError.positive(generation, "generation");
        ValidationError.positive(fence, "fence");
    }
}
