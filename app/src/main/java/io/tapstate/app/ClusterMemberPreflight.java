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

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/** Validates the stable identity a network-discoverable member must prove before it is created. */
final class ClusterMemberPreflight {

    /** The address a single node's member binds: the one loopback address a member can start on. */
    private static final String SINGLE_NODE_BIND_ADDRESS = "127.0.0.1";

    private static final Pattern IPV4_LITERAL = Pattern.compile(
            "((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)\\.){3}(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)");

    private ClusterMemberPreflight() {
    }

    static Identity validate(
            HazelcastProperties hazelcast, ClusterProperties cluster, ControlEndpointProperties control) {
        validateHeartbeat(hazelcast);
        String bindAddress = hazelcast.getBindAddress();
        // The member matches its bind address against the addresses this host's interfaces carry, so it has
        // to be one of them as written. A host name, or a loopback address the loopback interface does not
        // carry, passed a looser check here and then failed the member's start with an error that does not
        // name this setting.
        if (hazelcast.getDiscovery().getMode() == HazelcastProperties.DiscoveryMode.NONE) {
            if (!SINGLE_NODE_BIND_ADDRESS.equals(bindAddress)) {
                throw new TapstateException(BootError.MEMBER_BIND_ADDRESS_INVALID, Map.of(), null);
            }
            if (cluster.getProfile() != ClusterProperties.Profile.SINGLE) {
                throw new TapstateException(BootError.CLUSTER_PROFILE_INVALID, Map.of(), null);
            }
            return null;
        }
        Optional<InetAddress> bound = literal(bindAddress);
        if (bound.isEmpty() || bound.get().isLoopbackAddress() || bound.get().isAnyLocalAddress()) {
            throw new TapstateException(BootError.MEMBER_BIND_ADDRESS_INVALID, Map.of(), null);
        }
        if (cluster.getProfile() == null
                || cluster.getProfile() == ClusterProperties.Profile.SINGLE
                || (cluster.getProfile() == ClusterProperties.Profile.PRODUCTION_HA
                        && cluster.getBootstrapMinMembers() < 3)
                || (cluster.getProfile() == ClusterProperties.Profile.PROCESS_FAILURE_ONLY
                        && cluster.getBootstrapMinMembers() != 2)) {
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
                || !reachableFromElsewhere(controlUrl.getHost())
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
        if (cluster.getWorkloadClaimTtl() == null
                || cluster.getWorkloadClaimTtl().isNegative()
                || cluster.getWorkloadClaimTtl().isZero()) {
            throw new TapstateException(BootError.WORKLOAD_CLAIM_TTL_INVALID, Map.of(), null);
        }
        if (cluster.getWorkloadClaimRenewInterval() == null
                || cluster.getWorkloadClaimRenewInterval().isNegative()
                || cluster.getWorkloadClaimRenewInterval().isZero()
                || cluster.getWorkloadClaimRenewInterval().compareTo(cluster.getWorkloadClaimTtl()) >= 0) {
            throw new TapstateException(BootError.WORKLOAD_CLAIM_RENEW_INTERVAL_INVALID, Map.of(), null);
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

    /**
     * Whether a URL's {@code host} names a machine another one can reach: neither loopback, by name or by
     * address, nor an unspecified address, which names no machine at all. A URL keeps an IPv6 literal in
     * its brackets, so those come off before the address is read. Any other host name is taken at its
     * word: finding out what it resolves to would ask DNS, from a check that has to answer the same
     * wherever it runs.
     */
    private static boolean reachableFromElsewhere(String host) {
        if ("localhost".equalsIgnoreCase(host)) {
            return false;
        }
        return literal(host)
                .map(address -> !address.isLoopbackAddress() && !address.isAnyLocalAddress())
                .orElse(true);
    }

    /**
     * {@code host} read as the IP address it is written as, or empty when it is not written as one --
     * without ever looking a name up, which {@link InetAddress#getByName} does for anything it cannot read
     * as a literal. So only what is plainly a literal reaches it: four decimal octets, or anything with a
     * colon, which is handed over in brackets so that a malformed one is refused rather than resolved.
     */
    private static Optional<InetAddress> literal(String host) {
        if (host == null) {
            return Optional.empty();
        }
        String bare = host.length() > 2 && host.startsWith("[") && host.endsWith("]")
                ? host.substring(1, host.length() - 1)
                : host;
        String written;
        if (IPV4_LITERAL.matcher(bare).matches()) {
            written = bare;
        } else if (bare.indexOf(':') >= 0) {
            written = "[" + bare + "]";
        } else {
            return Optional.empty();
        }
        try {
            return Optional.of(InetAddress.getByName(written));
        } catch (UnknownHostException notAnAddress) {
            return Optional.empty();
        }
    }

    record Identity(String clusterId, String nodeId, URI controlUrl, WorkloadClaim nodeSession) {

        Identity(String clusterId, String nodeId, URI controlUrl) {
            this(clusterId, nodeId, controlUrl, null);
        }
    }
}
