package io.tapstate.spi.store;

import java.net.URI;
import java.time.Instant;
import java.util.Objects;

/** Stable node registry with join evidence scoped to its exact boot and profile. */
public record ClusterNodeRegistration(WorkloadClaim nodeSession, ClusterExecutionProfile profile,
        URI controlUrl, boolean joined, String memberUuid, String memberAddress, Instant joinedAt) {
    public ClusterNodeRegistration {
        Objects.requireNonNull(nodeSession, "nodeSession");
        Objects.requireNonNull(profile, "profile");
        Objects.requireNonNull(controlUrl, "controlUrl");
        if (nodeSession.key().type() != WorkloadClaimType.NODE_SESSION
                || !nodeSession.key().clusterId().equals(profile.clusterId())
                || nodeSession.profileGeneration() != profile.generation()
                || !controlUrl.isAbsolute()
                || (joined && (memberUuid == null || memberUuid.isBlank()
                        || memberAddress == null || memberAddress.isBlank() || joinedAt == null))
                || (!joined && (memberUuid != null || memberAddress != null || joinedAt != null))) {
            throw new IllegalArgumentException("node registration does not match its session or join evidence");
        }
    }
}
