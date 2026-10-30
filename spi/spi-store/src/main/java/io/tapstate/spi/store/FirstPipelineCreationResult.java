package io.tapstate.spi.store;

import java.util.Objects;

/** Repeated issue requests return facts, never another first-creation invocation. */
public sealed interface FirstPipelineCreationResult {
    record Issued(FirstPipelineCreation creation) implements FirstPipelineCreationResult {
        public Issued { Objects.requireNonNull(creation, "creation"); }
    }
    record AlreadyIssued(PipelinePreparation preparation) implements FirstPipelineCreationResult {
        public AlreadyIssued {
            Objects.requireNonNull(preparation, "preparation");
            if (preparation.phase() == PipelinePreparation.Phase.UNASSIGNED) {
                throw new IllegalArgumentException("an existing issue result must retain issued facts");
            }
        }
    }
}
