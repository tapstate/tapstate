package io.tapstate.spi.store;

/**
 * An implementation-private, non-serializable authority for one CACHE reservation attempt.
 * Ordinary issuance returns it only to the CONFIG CAS winner. Deferred starts may issue another
 * attempt only after checking the current durable intent and its original preparation inputs.
 */
public interface FirstPipelineCreation {
    PipelinePreparation preparation();

    /** Immutable inputs captured for this reservation attempt. */
    PipelinePreparationInputs inputs();

    /** Consumes the sole CACHE logical invocation before callbacks or IO; a second call refuses. */
    void consume();

    /** Rechecks the same original lifecycle guard on each CACHE transaction attempt. */
    void requireActive();
}
