package io.tapstate.control.core;

/** Current, credential-free address metadata for one managed Cluster. */
public record CloudEgressAddress(String organizationId, String clusterId,
        String ec2PublicIp, String egressAddressStatus) {
}
