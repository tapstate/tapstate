package io.tapstate.spi.store;

/** Confirmed original CONFIG operation for a controlled existing-row transition, never first-creation authority. */
public interface PipelineRebuildPreparation {
    PipelineRebuildRequest request();
    String preparationId();
    String previousPreparationId();
    String confirmationHash();
}
