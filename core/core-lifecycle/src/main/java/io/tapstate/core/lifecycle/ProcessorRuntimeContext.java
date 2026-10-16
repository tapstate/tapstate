package io.tapstate.core.lifecycle;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/** Native processor initialization facts, read from the processor's actual execution context. */
public record ProcessorRuntimeContext(
        String pipelineId, String vertex, String jobId, String runtimeExecutionId,
        long claimGeneration, long executionGeneration, long profileGeneration,
        String nodeId, String bootId, String memberUuid, String memberAddress,
        int memberIndex, int localProcessorIndex, int globalProcessorIndex,
        int localParallelism, int totalParallelism, int memberCount, Instant initializedAt)
        implements Serializable {
    private static final long serialVersionUID = 1L;

    public ProcessorRuntimeContext {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(vertex, "vertex");
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(runtimeExecutionId, "runtimeExecutionId");
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(bootId, "bootId");
        Objects.requireNonNull(memberUuid, "memberUuid");
        Objects.requireNonNull(memberAddress, "memberAddress");
        Objects.requireNonNull(initializedAt, "initializedAt");
        if (claimGeneration < 1 || executionGeneration < 1 || profileGeneration < 1
                || memberIndex < 0 || localProcessorIndex < 0 || globalProcessorIndex < 0
                || localParallelism < 1 || totalParallelism < 1 || memberCount < 1
                || memberIndex >= memberCount || localProcessorIndex >= localParallelism
                || globalProcessorIndex >= totalParallelism) {
            throw new IllegalArgumentException("native processor context is out of range");
        }
    }
}
