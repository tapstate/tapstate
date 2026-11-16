package io.tapstate.app;

import io.tapstate.spi.store.StorageBinding;

import java.util.Objects;

/** Selects a specific isolated CACHE in a programmatically assembled compatibility exercise. */
public record IsolatedConnectionDiagnostics(String clusterId, String cacheDatabase) {
    public IsolatedConnectionDiagnostics {
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(cacheDatabase, "cacheDatabase");
    }

    boolean matches(StorageBinding binding) {
        return clusterId.equals(binding.setup().clusterId()) && cacheDatabase.equals(binding.cache().database());
    }

    record Active() {}
}
