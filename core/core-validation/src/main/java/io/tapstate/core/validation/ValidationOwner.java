package io.tapstate.core.validation;

/** Exact validation tenure. A node name alone never authorizes a callback. */
public record ValidationOwner(String clusterId, String pipelineId, String nodeId,
                              String bootId, long claimGeneration, long executionGeneration,
                              long topologyRevision) {
    public ValidationOwner {
        clusterId = ValidationError.identifier(clusterId, "clusterId");
        pipelineId = ValidationError.identifier(pipelineId, "pipelineId");
        nodeId = ValidationError.identifier(nodeId, "nodeId");
        bootId = ValidationError.identifier(bootId, "bootId");
        ValidationError.positive(claimGeneration, "claimGeneration");
        ValidationError.positive(executionGeneration, "executionGeneration");
        if (topologyRevision < 0) throw ValidationError.invalid("topologyRevision");
    }
}
