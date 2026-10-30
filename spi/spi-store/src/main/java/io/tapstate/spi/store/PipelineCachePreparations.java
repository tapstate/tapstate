package io.tapstate.spi.store;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;

/** Controlled initial-checkpoint preparation, separate from ordinary actuation and StateStore.create. */
public interface PipelineCachePreparations {
    /** Consumes the original in-memory issuance once, before any callback or cache IO. */
    Reservation reserveFirst(FirstPipelineCreation invocation, Duration ttl);

    /** Continues only retained original facts; missing claims or checkpoints are never reconstructed. */
    Reservation resume(PipelinePreparation original, WorkloadClaimFence currentNode, Duration ttl);

    /** Loads the retained original source manifest and necessary progress; it grants no creation authority. */
    void verifySources(PipelineIntentScope scope, PipelinePreparationInputs expected);

    /** Rechecks CONFIG outside the CACHE transaction, then verifies or publishes the original initial receipt. */
    PipelinePreparationReceipt seed(Reservation original, PipelineStatePreparations configuration, Instant touchTime);

    /** The scope, nonce and exact original claim remain fixed throughout a seed attempt. */
    record Reservation(PipelineIntentScope scope, String preparationId, Phase phase, WorkloadClaimFence claim) {
        public Reservation {
            Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(preparationId, "preparationId");
            Objects.requireNonNull(phase, "phase"); Objects.requireNonNull(claim, "claim");
            if (!java.util.UUID.fromString(preparationId).toString().equals(preparationId)
                    || claim.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                    || !scope.clusterId().equals(claim.key().clusterId()) || !scope.pipelineId().equals(claim.key().resourceId())) {
                throw new IllegalArgumentException("invalid original Pipeline reservation");
            }
        }
    }
    enum Phase { RESERVED, PREPARED, RETIRED }
}
