package io.tapstate.spi.store;

import java.util.Objects;

/** A fixed submitting Pipeline and Source snapshot, with the existing Pipeline and node authority. */
public record CapturePreparationRequest(PipelineIntentScope pipeline,WorkloadClaimFence originalPipeline,
        WorkloadClaimFence originalNode,String sourceId,PipelinePreparationInputs inputs) {
    public CapturePreparationRequest {
        Objects.requireNonNull(pipeline,"pipeline");Objects.requireNonNull(originalPipeline,"originalPipeline");Objects.requireNonNull(originalNode,"originalNode");
        Objects.requireNonNull(inputs,"inputs");
        if(sourceId==null || sourceId.isBlank() || !inputs.sourceHashes().containsKey(sourceId) || inputs.selectedTables().get(sourceId).isEmpty()
                || originalPipeline.key().type()!=WorkloadClaimType.PIPELINE_ACTUATION || originalPipeline.executionGeneration()<1
                || !pipeline.clusterId().equals(originalPipeline.key().clusterId()) || !pipeline.pipelineId().equals(originalPipeline.key().resourceId())
                || originalNode.key().type()!=WorkloadClaimType.NODE_SESSION || !pipeline.clusterId().equals(originalNode.key().clusterId())
                || !originalNode.key().resourceId().equals(originalNode.owner().nodeId()) || !originalPipeline.owner().equals(originalNode.owner()))
            throw new IllegalArgumentException("capture preparation requires one original Pipeline, node and source");
    }
}
