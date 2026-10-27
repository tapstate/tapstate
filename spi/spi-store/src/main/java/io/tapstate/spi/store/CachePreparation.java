package io.tapstate.spi.store;

import java.util.Objects;

/** Fixed cache identities loaded from the original durable configuration operation, never from cache contents. */
public record CachePreparation(String operationId, CacheAuthScope authScope) {
    public CachePreparation {
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(authScope, "authScope");
        if (operationId.isBlank()) throw new IllegalArgumentException("cache preparation operation must be fixed");
    }

    /** This identity alone grants no business readiness or authority to prepare a cache. */
    public CacheBusinessScope businessScope() {
        return new CacheBusinessScope(authScope.clusterId(), authScope.bindingVersion(), authScope.cacheIncarnation());
    }
}
