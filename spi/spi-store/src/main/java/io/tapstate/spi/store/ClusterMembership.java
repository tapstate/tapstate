package io.tapstate.spi.store;

import java.util.Objects;
import java.util.Set;

/** The last majority-committed ACTIVE node set and its monotonic topology revision. */
public record ClusterMembership(String clusterId, long revision, Set<String> activeNodeIds, long profileGeneration) {

    public ClusterMembership {
        Objects.requireNonNull(clusterId, "clusterId");
        activeNodeIds = Set.copyOf(activeNodeIds);
        if (clusterId.isBlank() || revision < 1 || profileGeneration < 0 || activeNodeIds.isEmpty()
                || activeNodeIds.stream().anyMatch(String::isBlank)) {
            throw new IllegalArgumentException("cluster membership fields are out of range");
        }
    }

    public ClusterMembership(String clusterId, long revision, Set<String> activeNodeIds) {
        this(clusterId, revision, activeNodeIds, 0);
    }

    public int majoritySize() {
        return activeNodeIds.size() / 2 + 1;
    }
}
