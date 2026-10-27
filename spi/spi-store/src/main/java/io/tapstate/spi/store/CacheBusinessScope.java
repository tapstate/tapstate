package io.tapstate.spi.store;

/** One fixed business incarnation, independent from authentication-only resets. */
public record CacheBusinessScope(String clusterId, long bindingVersion, String cacheIncarnation) {
    public CacheBusinessScope {
        if (clusterId == null || clusterId.isBlank() || cacheIncarnation == null || cacheIncarnation.isBlank()
                || bindingVersion < 1 || bindingVersion == Long.MAX_VALUE) {
            throw new IllegalArgumentException("invalid cache business scope");
        }
    }
}
