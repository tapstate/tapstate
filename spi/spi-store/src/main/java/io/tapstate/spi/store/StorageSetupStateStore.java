package io.tapstate.spi.store;

import java.util.Objects;

/** The original installation phase and capability generation, kept in the configuration authority. */
public interface StorageSetupStateStore {
    enum Phase { LOCAL_SETUP, CONFIG_PREPARED, STORAGE_BOUND, COMPLETE }
    enum Purpose { SETUP, FIRST_ADMIN }
    record Status(StorageSetupScope scope, Phase phase, long bindingVersion) {
        public Status {
            Objects.requireNonNull(scope, "scope"); Objects.requireNonNull(phase, "phase");
            if (bindingVersion != (phase == Phase.LOCAL_SETUP || phase == Phase.CONFIG_PREPARED ? 0 : 1))
                throw new IllegalArgumentException("setup phase and binding version disagree");
        }
    }
    StorageSetupStateStore guardedBy(Runnable requireActive);
    /** Load only; missing or inconsistent facts never authorize a fresh installation. */
    Status load();
    /** Trusted deployment-owner issuance only. Rotates the original generation, never the operation or binding. */
    Status rotate(StorageSetupScope expected, Purpose purpose);
    /** Recheck the current purpose and generation; final writes additionally compete in their CONFIG transaction. */
    void requireCurrent(StorageSetupScope expected, Purpose purpose);
}
