package io.tapstate.spi.store;

/** Store-issued proof of one fixed CONFIG reservation; callers cannot reconstruct CACHE authority from its values. */
public interface SharedSrsRecoveryPreparation {
    StorageRecoveryCommands.Plan plan();
    CapturePreparation replacement();
    void requireCurrent();
}
