package io.tapstate.spi.store;

import java.util.Optional;

/** Adapter-issued original CONFIG retirement, reconstructed only from the same persisted operation. */
public interface PipelineRetirementPreparation {
    PipelineRetirementRequest request();
    String privateScopesHash();
    int privateScopesCount();
    PipelinePreparation previousPreparation();
    Optional<PipelineRetiredState> predecessor();
}
