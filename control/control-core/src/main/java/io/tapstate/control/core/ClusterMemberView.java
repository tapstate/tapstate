package io.tapstate.control.core;

import java.util.Objects;

/** One member of the cluster, as a control-plane reader sees it. */
public record ClusterMemberView(
        String nodeId,
        String memberUuid,
        String bootId,
        String hzAddress,
        String controlUrl,
        ClusterMemberState state) {

    public ClusterMemberView {
        Objects.requireNonNull(state, "state");
    }
}
