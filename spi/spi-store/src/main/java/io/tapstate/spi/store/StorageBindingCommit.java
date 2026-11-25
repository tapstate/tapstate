package io.tapstate.spi.store;

import java.util.Objects;
import java.util.Optional;

/** Confirmed configuration facts, plus an initial invocation only when this call inserted the binding. */
public record StorageBindingCommit(StorageBinding binding, CachePreparation preparation, Optional<FirstCacheCreation> firstCreation) {
    public StorageBindingCommit {
        Objects.requireNonNull(binding, "binding"); Objects.requireNonNull(preparation, "preparation"); Objects.requireNonNull(firstCreation, "firstCreation");
    }
    @Override public String toString() { return "StorageBindingCommit[redacted]"; }
}
