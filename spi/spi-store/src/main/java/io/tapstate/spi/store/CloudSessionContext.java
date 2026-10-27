package io.tapstate.spi.store;

import java.util.Objects;

/** Display context bound to one local session; email is supplied by the verified login identity. */
public record CloudSessionContext(
        String organizationId, String clusterId, String organizationName, String clusterName, String region,
        String ec2PublicIp, String egressAddressStatus, String userEmail) {

    public CloudSessionContext(String organizationId, String clusterId, String organizationName,
            String clusterName, String region) {
        this(organizationId, clusterId, organizationName, clusterName, region, null, null, null);
    }

    public CloudSessionContext(String organizationId, String clusterId, String organizationName,
            String clusterName, String region, String ec2PublicIp, String egressAddressStatus) {
        this(organizationId, clusterId, organizationName, clusterName, region, ec2PublicIp, egressAddressStatus, null);
    }

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
