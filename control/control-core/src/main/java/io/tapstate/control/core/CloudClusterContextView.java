package io.tapstate.control.core;

/** Credential-free display context exposed by the authenticated Cloud read service. */
public record CloudClusterContextView(
        String organizationId, String clusterId, String organizationName, String clusterName, String region) {
}
