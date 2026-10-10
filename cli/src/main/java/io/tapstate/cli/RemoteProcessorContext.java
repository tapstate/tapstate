package io.tapstate.cli;

/** Native initialization fields as supplied by the server, with no inferred local placement. */
record RemoteProcessorContext(String pipelineId, String vertex, String jobId, String runtimeExecutionId,
        Long claimGeneration, Long executionGeneration, Long profileGeneration,
        String nodeId, String bootId, String memberUuid, String memberAddress,
        Integer memberIndex, Integer localProcessorIndex, Integer globalProcessorIndex,
        Integer localParallelism, Integer totalParallelism, Integer memberCount, String initializedAt) {}
