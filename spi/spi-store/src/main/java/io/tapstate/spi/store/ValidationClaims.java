package io.tapstate.spi.store;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationOwner;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** Binds one Pipeline's validation owner to the existing durable claim protocol. */
public final class ValidationClaims {
    private ValidationClaims() {}

    public static WorkloadClaimKey key(String clusterId, String pipelineId) {
        if (pipelineId == null || pipelineId.isBlank() || pipelineId.length() > 512
                || !StandardCharsets.UTF_8.newEncoder().canEncode(pipelineId)) {
            throw invalid("pipelineId");
        }
        String resource = "validation:v1:" + Base64.getUrlEncoder().withoutPadding()
                .encodeToString(pipelineId.getBytes(StandardCharsets.UTF_8));
        return new WorkloadClaimKey(clusterId, WorkloadClaimType.VALIDATION, resource);
    }

    /** A new owner tenure must be advanced once; joining another run keeps that same tenure. */
    public static ValidationOwner owner(String pipelineId, WorkloadClaim claim) {
        Objects.requireNonNull(claim, "claim");
        if (!claim.key().equals(key(claim.key().clusterId(), pipelineId))
                || claim.executionGeneration() < 1
                || claim.contextExecutionGeneration() != claim.executionGeneration()
                || claim.executionClaimGeneration() != claim.claimGeneration()
                || !claim.executionNodeIds().equals(Set.of(claim.owner().nodeId()))) {
            throw invalid("validation.claim");
        }
        return new ValidationOwner(claim.key().clusterId(), pipelineId, claim.owner().nodeId(),
                claim.owner().bootId(), claim.claimGeneration(), claim.executionGeneration(),
                claim.topologyRevision());
    }

    /** All fence components are required on each protected write; this conversion grants no lease. */
    public static WorkloadClaimFence fence(ValidationOwner owner) {
        Objects.requireNonNull(owner, "owner");
        return new WorkloadClaimFence(key(owner.clusterId(), owner.pipelineId()),
                new WorkloadOwner(owner.nodeId(), owner.bootId()), owner.claimGeneration(),
                owner.executionGeneration(), owner.topologyRevision());
    }

    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
}
