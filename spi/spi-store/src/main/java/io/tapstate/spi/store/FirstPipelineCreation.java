package io.tapstate.spi.store;

/**
 * An implementation-private, non-serializable authority returned only to the first CONFIG CAS winner.
 * Loading persistent preparation facts must never reconstruct this object.
 */
public interface FirstPipelineCreation {
    PipelinePreparation preparation();

    /** The original CONFIG winner carries these immutable inputs only in memory. */
    PipelinePreparationInputs inputs();

    /** Consumes the sole CACHE logical invocation before callbacks or IO; a second call refuses. */
    void consume();

    /** Rechecks the same original lifecycle guard on each CACHE transaction attempt. */
    void requireActive();
}
