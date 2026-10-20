package io.tapstate.spi.store;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Exact old payloads remain separately retryable after safe retirement and later executions. */
public interface RetiredPipelinePayloads {
    record Original(PipelineIntentScope scope,String operationId,String preparationId,String inventoryHash) {
        public Original {
            Objects.requireNonNull(scope);
            if(operationId==null || operationId.isBlank() || preparationId==null || !java.util.UUID.fromString(preparationId).toString().equals(preparationId)
                    || inventoryHash==null || !inventoryHash.matches("[a-f0-9]{64}"))throw new IllegalArgumentException("invalid retired inventory identity");
        }
    }
    Optional<Original> find(PipelineIntentScope scope);
    /** Page only frozen inventories for one logical Pipeline, in increasing original generation. */
    List<Original> list(String pipelineId,long afterGeneration,int limit);
    /** At most 256 original payload rows; small history and shared database identities remain. */
    long reclaim(Original original,WorkloadClaimFence currentNode);
}
