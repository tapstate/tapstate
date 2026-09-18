package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;

import java.net.URI;
import java.util.Map;

/** Validates the stable identity a network-discoverable member must prove before it is created. */
final class ClusterMemberPreflight {

    private ClusterMemberPreflight() {
    }

    static Identity validate(
            HazelcastProperties hazelcast, ClusterProperties cluster, ControlEndpointProperties control) {
        validateHeartbeat(hazelcast);
        String bindAddress = hazelcast.getBindAddress();
        if (hazelcast.getDiscovery().getMode() == HazelcastProperties.DiscoveryMode.NONE) {
            if (!loopback(bindAddress)) {
                throw new TapstateException(BootError.MEMBER_BIND_ADDRESS_INVALID, Map.of(), null);
            }
            if (cluster.getProfile() != ClusterProperties.Profile.SINGLE) {
                throw new TapstateException(BootError.CLUSTER_PROFILE_INVALID, Map.of(), null);
            }
            return null;
        }
        if (loopback(bindAddress) || unspecified(bindAddress)) {
            throw new TapstateException(BootError.MEMBER_BIND_ADDRESS_INVALID, Map.of(), null);
        }
        if (cluster.getProfile() == null
                || cluster.getProfile() == ClusterProperties.Profile.SINGLE
                || cluster.getProfile() == ClusterProperties.Profile.PRODUCTION_HA
                        && cluster.getBootstrapMinMembers() < 3
                || cluster.getProfile() == ClusterProperties.Profile.PROCESS_FAILURE_ONLY
                        && cluster.getBootstrapMinMembers() != 2) {
            throw new TapstateException(BootError.CLUSTER_PROFILE_INVALID, Map.of(), null);
        }
        String clusterId = required(cluster.getId(), BootError.CLUSTER_ID_REQUIRED);
        try {
            new ClusterIdentity(clusterId);
        } catch (IllegalArgumentException invalid) {
            throw new TapstateException(BootError.CLUSTER_ID_INVALID, Map.of(), invalid);
        }
        String nodeId = required(cluster.getNodeId(), BootError.NODE_ID_REQUIRED);
        String advertised = required(control.getAdvertiseUrl(), BootError.CONTROL_ADVERTISE_URL_INVALID);
        URI controlUrl;
        try {
            controlUrl = URI.create(advertised);
        } catch (IllegalArgumentException invalid) {
            throw new TapstateException(BootError.CONTROL_ADVERTISE_URL_INVALID, Map.of(), invalid);
        }
        if (!controlUrl.isAbsolute()
                || controlUrl.getHost() == null
                || loopback(controlUrl.getHost())
                || !("http".equalsIgnoreCase(controlUrl.getScheme())
                        || "https".equalsIgnoreCase(controlUrl.getScheme()))) {
            throw new TapstateException(BootError.CONTROL_ADVERTISE_URL_INVALID, Map.of(), null);
        }
        if (cluster.getNodeSessionTtl() == null || cluster.getNodeSessionTtl().isNegative()
                || cluster.getNodeSessionTtl().isZero()) {
            throw new TapstateException(BootError.NODE_SESSION_TTL_INVALID, Map.of(), null);
        }
        if (cluster.getNodeSessionRenewInterval() == null
                || cluster.getNodeSessionRenewInterval().isNegative()
                || cluster.getNodeSessionRenewInterval().isZero()
                || cluster.getNodeSessionRenewInterval().compareTo(cluster.getNodeSessionTtl()) >= 0) {
            throw new TapstateException(BootError.NODE_SESSION_RENEW_INTERVAL_INVALID, Map.of(), null);
        }
        if (cluster.getMembershipReconcileInterval() == null
                || cluster.getMembershipReconcileInterval().isNegative()
                || cluster.getMembershipReconcileInterval().isZero()) {
            throw new TapstateException(BootError.CLUSTER_PROFILE_INVALID, Map.of(), null);
        }
        return new Identity(clusterId, nodeId, controlUrl);
    }

    private static void validateHeartbeat(HazelcastProperties hazelcast) {
        if (hazelcast.getHeartbeatInterval() == null
                || hazelcast.getHeartbeatInterval().toSeconds() < 1
                || hazelcast.getMaximumNoHeartbeat() == null
                || hazelcast.getMaximumNoHeartbeat().toSeconds() < 1
                || hazelcast.getMaximumNoHeartbeat().compareTo(hazelcast.getHeartbeatInterval()) <= 0) {
            throw new TapstateException(BootError.HEARTBEAT_CONFIG_INVALID, Map.of(), null);
        }
    }

    static Identity reserve(
            Identity identity,
            ClusterProperties cluster,
            ClusterIdentityStore identities,
            WorkloadClaimStore claims,
            String bootId) {
        if (identities == null || claims == null) {
            throw new TapstateException(BootError.COORDINATION_STORE_REQUIRED, Map.of(), null);
        }
        ClusterIdentity stored = identities.createIfAbsent(new ClusterIdentity(identity.clusterId()));
        if (!stored.clusterId().equals(identity.clusterId())) {
            throw new TapstateException(BootError.CLUSTER_ID_MISMATCH,
                    Map.of("configured", identity.clusterId(), "stored", stored.clusterId()), null);
        }
        WorkloadOwner owner = new WorkloadOwner(identity.nodeId(), bootId);
        WorkloadClaimAttempt attempt = claims.acquire(
                new WorkloadClaimKey(identity.clusterId(), WorkloadClaimType.NODE_SESSION, identity.nodeId()),
                owner, 0, cluster.getNodeSessionTtl());
        if (!attempt.acquired()) {
            throw new TapstateException(BootError.NODE_ID_IN_USE, Map.of("nodeId", identity.nodeId()), null);
        }
        return new Identity(identity.clusterId(), identity.nodeId(), identity.controlUrl(), attempt.claim());
    }

    private static String required(String value, BootError error) {
        if (value == null || value.isBlank()) {
            throw new TapstateException(error, Map.of(), null);
        }
        return value.trim();
    }

    private static boolean loopback(String host) {
        return host == null
                || "localhost".equalsIgnoreCase(host)
                || "::1".equals(host)
                || host.startsWith("127.");
    }

    private static boolean unspecified(String host) {
        return "0.0.0.0".equals(host) || "::".equals(host);
    }

    record Identity(String clusterId, String nodeId, URI controlUrl, WorkloadClaim nodeSession) {

        Identity(String clusterId, String nodeId, URI controlUrl) {
            this(clusterId, nodeId, controlUrl, null);
        }
    }
}
