package io.tapstate.spi.store;

import java.util.Objects;
import java.util.Optional;

/** The one completed deletion retained for safe reuse and lost-response queries. */
public record PipelineRetirementResult(String operationId,PipelineIntentScope scope,String definitionHash,String privateScopesHash,
        int privateScopesCount,Optional<PipelineRetiredState> cache) {
    public PipelineRetirementResult {
        Objects.requireNonNull(scope);cache=Objects.requireNonNull(cache);
        if(operationId==null || operationId.isBlank() || definitionHash==null || !definitionHash.matches("[a-f0-9]{64}")
                || privateScopesHash==null || !privateScopesHash.matches("[a-f0-9]{64}") || privateScopesCount<0)throw new IllegalArgumentException("invalid retirement result");
        if(cache.isPresent()) {
            var prior=cache.orElseThrow().scope();
            if(!prior.clusterId().equals(scope.clusterId()) || prior.bindingVersion()!=scope.bindingVersion()
                    || !prior.cacheIncarnation().equals(scope.cacheIncarnation()) || !prior.pipelineId().equals(scope.pipelineId())
                    || prior.stateGeneration()>scope.stateGeneration())throw new IllegalArgumentException("retired CACHE belongs to another lifecycle");
        }
    }
}
