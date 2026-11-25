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
 * generations alone cannot distinguish two cache incarnations or two physical state generations. Its
 * original topology revision also stays fixed; reacquiring ownership cannot authorize an older callback.
 *
 * <p>{@link Serializable} because it travels on the DAG to whichever members run the sink vertices, the
 * same way the coordinates behind a sink writer and a durable ack do.
 */
record ExecutionFence(String pipelineId, long claimGeneration, long executionGeneration,
        PipelineIntentScope scope, String ownerNodeId, String ownerBootId, Long topologyRevision)
        implements Serializable {

    ExecutionFence(String pipelineId, long claimGeneration, long executionGeneration) {
        this(pipelineId, claimGeneration, executionGeneration, null, null, null, null);
    }

    ExecutionFence(PipelineIntentScope scope, WorkloadClaimFence claim) {
        this(Objects.requireNonNull(scope, "scope").pipelineId(), claim.claimGeneration(),
                claim.executionGeneration(), scope, claim.owner().nodeId(), claim.owner().bootId(), claim.topologyRevision());
        if (claim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || !claim.key().clusterId().equals(scope.clusterId())
                || !claim.key().resourceId().equals(scope.pipelineId())) {
            throw new IllegalArgumentException("the run scope and claim must name the same Pipeline");
        }
    }

    /** A deterministic hot-map attempt key; the physical cold address remains independent of it. */
    String operatorExecutionKey() {
        if (scope == null) throw new IllegalStateException("an operator binding requires a scoped execution");
        try {
            var bytes = new java.io.ByteArrayOutputStream();
            try (var out = new java.io.DataOutputStream(bytes)) {
                text(out, scope.clusterId()); out.writeLong(scope.bindingVersion()); text(out, scope.cacheIncarnation());
                text(out, scope.pipelineId()); out.writeLong(scope.stateGeneration());
                text(out, ownerNodeId); text(out, ownerBootId); out.writeLong(claimGeneration); out.writeLong(executionGeneration); out.writeLong(topologyRevision);
            }
            return java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(bytes.toByteArray()));
        } catch (java.io.IOException | java.security.NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
    private static void text(java.io.DataOutputStream out, String value) throws java.io.IOException { out.writeInt(value.length()); out.writeChars(value); }

    ExecutionFence {
        Objects.requireNonNull(pipelineId, "pipelineId");
        if (pipelineId.isBlank()) {
            throw new IllegalArgumentException("pipelineId must not be blank");
        }
        if (scope == null ? ownerNodeId != null || ownerBootId != null || topologyRevision != null
                : !scope.pipelineId().equals(pipelineId) || ownerNodeId == null || ownerNodeId.isBlank()
                        || ownerBootId == null || ownerBootId.isBlank() || topologyRevision == null || topologyRevision < 0) {
            throw new IllegalArgumentException("a scoped run carries its original owner and Pipeline identity");
        }
        if (claimGeneration < 1 || executionGeneration < 1) {
            throw new IllegalArgumentException("a fenced run carries both of its generations");
        }
    }
}
