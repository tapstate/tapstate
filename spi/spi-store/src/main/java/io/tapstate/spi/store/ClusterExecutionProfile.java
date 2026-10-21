package io.tapstate.spi.store;

import java.util.Objects;

/** One immutable profile generation, independent of claim and execution generations. */
public record ClusterExecutionProfile(String clusterId, long generation, ExecutionProfile profile) {
    public ClusterExecutionProfile {
        Objects.requireNonNull(clusterId, "clusterId");
        Objects.requireNonNull(profile, "profile");
        if (clusterId.isBlank() || generation < 1) {
            throw new IllegalArgumentException("cluster profile identity and generation are invalid");
        }
    }
}
