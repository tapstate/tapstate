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

    /** Reads the existing immutable preparation manifest without granting creation authority. */
    record SourceInputs(PipelinePreparationInputs inputs, boolean expandedByDiscovery) {
        public SourceInputs { Objects.requireNonNull(inputs, "inputs"); }
    }
    default SourceInputs sourceInputs(PipelineIntentScope scope) {
        throw new UnsupportedOperationException("retained source inputs are not supported");
    }

    /** Extends only an existing manifest; positions, snapshots and physical scope remain intact. */
    default boolean expandSources(PipelineTableExpansion permission, WorkloadClaimFence node) {
        throw new UnsupportedOperationException("dynamic table expansion is not supported");
    }

    /** Rechecks CONFIG outside the CACHE transaction, then verifies or publishes the original initial receipt. */
    PipelinePreparationReceipt seed(Reservation original, PipelineStatePreparations configuration, Instant touchTime);

    default PipelinePreparationReceipt seedDeferred(Reservation original, PipelineStatePreparations configuration,
            PipelineIntentStore.PendingStart pending, Instant touchTime) {
        throw new UnsupportedOperationException("deferred first start is not supported");
    }

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
