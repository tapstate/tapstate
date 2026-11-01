package io.tapstate.spi.store;

/** A coherent CACHE observation, including detached relationships; never deletion authority. */
public interface CaptureClearObservation {
    SharedSrsRecoverySnapshot snapshot();
}
