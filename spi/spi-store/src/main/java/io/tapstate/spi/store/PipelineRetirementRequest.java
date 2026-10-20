package io.tapstate.spi.store;

import java.util.Objects;
import java.util.Optional;

/** One explicit deletion of a fixed authoring identity and its already-known execution history. */
public record PipelineRetirementRequest(String operationId,PipelineIntentScope scope,String definitionHash,Optional<CacheOriginal> cache) {
    public record CacheOriginal(WorkloadClaimFence claim,long epoch) {
        public CacheOriginal {
            Objects.requireNonNull(claim);
            if(claim.key().type()!=WorkloadClaimType.PIPELINE_ACTUATION || epoch<0 || epoch>=Long.MAX_VALUE-1)throw new IllegalArgumentException("invalid original execution");
        }
    }
    public PipelineRetirementRequest {
        Objects.requireNonNull(scope);cache=Objects.requireNonNull(cache);
        if(operationId==null || operationId.isBlank() || definitionHash==null || !definitionHash.matches("[a-f0-9]{64}"))throw new IllegalArgumentException("invalid retirement request");
        if(cache.isPresent() && (!scope.clusterId().equals(cache.orElseThrow().claim().key().clusterId())
                || !scope.pipelineId().equals(cache.orElseThrow().claim().key().resourceId())))throw new IllegalArgumentException("execution belongs to another Pipeline");
    }
}
