package io.tapstate.spi.store;

import java.util.Objects;

/** Exact member incarnation used by an execution, independent from a later registry entry. */
public record ClusterExecutionMember(String nodeId, String bootId, String memberUuid) {
    public ClusterExecutionMember {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(bootId, "bootId");
        Objects.requireNonNull(memberUuid, "memberUuid");
        if (nodeId.isBlank() || bootId.isBlank() || memberUuid.isBlank()) {
            throw new IllegalArgumentException("execution member identity must contain a real node, boot and member UUID");
        }
    }
}
