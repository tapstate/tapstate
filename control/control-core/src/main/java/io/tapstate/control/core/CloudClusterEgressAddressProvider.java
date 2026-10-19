package io.tapstate.control.core;

import java.util.Optional;

/** Reads the latest egress address for the authenticated Cluster, if Cloud is currently reachable. */
@FunctionalInterface
public interface CloudClusterEgressAddressProvider {

    Optional<CloudClusterEgressAddress> currentAddress(String organizationId, String clusterId);
}
