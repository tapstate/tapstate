package io.tapstate.spi.store;

import java.util.Optional;

/** CACHE evidence bound to the original CONFIG retirement, not an authority to recreate missing history. */
public interface PipelineRetirementReceipt {
    PipelineRetirementPreparation preparation();
    Optional<PipelineRetiredState> retiredState();
}
