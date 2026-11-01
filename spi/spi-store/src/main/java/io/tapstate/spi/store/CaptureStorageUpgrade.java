package io.tapstate.spi.store;

import java.util.List;

/** Explicit offline maintenance only; neither handle proves that a physical owner has exited. */
public interface CaptureStorageUpgrade {
    interface CacheObservation {
        CacheBusinessScope scope();
        List<CapturePreparation> captures();
        void requireCurrent();
        void complete(ConfigurationReceipt original);
    }
    interface ConfigurationReceipt {
        CacheObservation original();
        void requireCurrent();
    }
}
