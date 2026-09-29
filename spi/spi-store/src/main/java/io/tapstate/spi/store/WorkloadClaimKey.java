package io.tapstate.spi.store;

import java.util.Objects;

/** Cluster-scoped identity of one resource whose owner is fenced by a workload claim. */
public record WorkloadClaimKey(String clusterId, WorkloadClaimType type, String resourceId) {

    public WorkloadClaimKey {
        clusterId = required(clusterId, "clusterId");
        type = Objects.requireNonNull(type, "type");
        resourceId = required(resourceId, "resourceId");
    }

    private static String required(String value, String name) {
        Objects.requireNonNull(value, name);
        if (value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return value;
    }
}
