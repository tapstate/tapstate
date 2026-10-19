package io.tapstate.control.core;

/** Current mutable egress address metadata reported for one Cloud Cluster. */
public record CloudClusterEgressAddress(
        String organizationId, String clusterId, String ec2PublicIp, String egressAddressStatus) {
}
