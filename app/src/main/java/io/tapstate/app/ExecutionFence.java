package io.tapstate.app;

import java.io.Serializable;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;
import java.util.Objects;

/**
 * The two generations one submitted run is fenced by, fixed into the job as it is submitted and carried
 * to every member that runs a piece of it.
 *
 * <p>They answer different questions and neither stands in for the other. The claim generation says who
 * is entitled to drive the pipeline, and it moves only when ownership changes hands. The execution
 * generation says which run this is, and it moves on every submission — including a resubmission by the
 * same owner, which is not a change of ownership but is certainly a different run. A member holding a
 * job from the previous run therefore reads the same claim generation and a stale execution generation,
 * which is exactly the case that a claim generation alone could not tell apart from being current.
 *
 * <p>A scoped run also carries the fixed physical state identity and submitting owner. Numeric
 * generations alone cannot distinguish two cache incarnations or two physical state generations.
 *
 * <p>{@link Serializable} because it travels on the DAG to whichever members run the sink vertices, the
 * same way the coordinates behind a sink writer and a durable ack do.
 */
record ExecutionFence(String pipelineId, long claimGeneration, long executionGeneration,
        PipelineIntentScope scope, String ownerNodeId, String ownerBootId)
        implements Serializable {

    ExecutionFence(String pipelineId, long claimGeneration, long executionGeneration) {
        this(pipelineId, claimGeneration, executionGeneration, null, null, null);
    }

    ExecutionFence(PipelineIntentScope scope, WorkloadClaimFence claim) {
        this(Objects.requireNonNull(scope, "scope").pipelineId(), claim.claimGeneration(),
                claim.executionGeneration(), scope, claim.owner().nodeId(), claim.owner().bootId());
        if (claim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || !claim.key().clusterId().equals(scope.clusterId())
                || !claim.key().resourceId().equals(scope.pipelineId())) {
            throw new IllegalArgumentException("the run scope and claim must name the same Pipeline");
        }
    }

    ExecutionFence {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (pipelineId.isBlank()) {
            throw new IllegalArgumentException("pipelineId must not be blank");
        }
        if (scope == null ? ownerNodeId != null || ownerBootId != null
                : !scope.pipelineId().equals(pipelineId) || ownerNodeId == null || ownerNodeId.isBlank()
                        || ownerBootId == null || ownerBootId.isBlank()) {
            throw new IllegalArgumentException("a scoped run carries its original owner and Pipeline identity");
        }
        if (claimGeneration < 1 || executionGeneration < 1) {
            throw new IllegalArgumentException("a fenced run carries both of its generations");
        }
    }
}
