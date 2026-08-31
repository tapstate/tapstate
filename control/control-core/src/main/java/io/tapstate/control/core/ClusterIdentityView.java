package io.tapstate.control.core;

import java.util.Objects;

/** Stable cluster identity projected for control-layer consumers. */
public record ClusterIdentityView(String issuer, String clusterId) {

    public ClusterIdentityView {
        Objects.requireNonNull(issuer, "issuer");
        Objects.requireNonNull(clusterId, "clusterId");
    }
}
