package io.tapstate.spi.store;

/**
 * Immutable identity of an initial NEW checkpoint and its matching PREPARED claim, verified together
 * by the original CACHE domain. A configuration status load cannot manufacture this receipt.
 */
public interface PipelinePreparationReceipt {
    PipelineIntentScope scope();
    String preparationId();
}
