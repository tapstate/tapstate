package io.tapstate.spi.store;

import java.util.Objects;

/** Display context returned by a verified Cloud code exchange and bound to one local session. */
public record CloudSessionContext(
        String organizationId, String clusterId, String organizationName, String clusterName, String region) {

    public CloudSessionContext {
        requireText(organizationId, "organizationId");
        requireText(clusterId, "clusterId");
        requireText(organizationName, "organizationName");
        requireText(clusterName, "clusterName");
        requireText(region, "region");
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Cloud session context " + name + " must be non-blank");
        }
    }
}
