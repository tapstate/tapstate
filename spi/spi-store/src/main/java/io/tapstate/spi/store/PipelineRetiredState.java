package io.tapstate.spi.store;

import java.util.Objects;
import java.util.UUID;

/** A retained terminal CACHE identity; it permits no business write or counter reset. */
public record PipelineRetiredState(PipelineIntentScope scope,String operationId,WorkloadClaimFence claim,long epoch,long writeFence,
        String preparationId,String inventoryHash) {
    public PipelineRetiredState {
        Objects.requireNonNull(scope);Objects.requireNonNull(claim);
        if(operationId==null || operationId.isBlank() || claim.key().type()!=WorkloadClaimType.PIPELINE_ACTUATION
                || !scope.clusterId().equals(claim.key().clusterId()) || !scope.pipelineId().equals(claim.key().resourceId())
                || epoch<1 || epoch>=Long.MAX_VALUE-1 || writeFence<1 || writeFence>=Long.MAX_VALUE-1
                || preparationId==null || !UUID.fromString(preparationId).toString().equals(preparationId)
                || inventoryHash==null || !inventoryHash.matches("[a-f0-9]{64}"))throw new IllegalArgumentException("invalid retired CACHE identity");
    }
}
