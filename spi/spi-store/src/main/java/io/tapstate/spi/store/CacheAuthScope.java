package io.tapstate.spi.store;

import java.util.Objects;

/** The fixed cache authentication identity allocated by the original trusted configuration operation. */
public record CacheAuthScope(String clusterId, long bindingVersion, StorageBinding.Mode mode,
        String cacheIncarnation, long authGeneration) {
    public CacheAuthScope {
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(cacheIncarnation, "cacheIncarnation");
        if (clusterId.isBlank() || cacheIncarnation.isBlank() || bindingVersion < 1 || authGeneration < 1
                || authGeneration == Long.MAX_VALUE) {
            throw new IllegalArgumentException("cache authentication scope must have fixed identities and positive versions");
        }
    }
}
