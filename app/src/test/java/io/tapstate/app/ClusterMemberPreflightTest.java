package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/** Stable identity is reserved before a member exists; a second live boot cannot cross that gate. */
class ClusterMemberPreflightTest {

    @Test
    void twoBootsWithTheSameStableNodeIdCannotBothPassPreflight() {
        ClusterProperties cluster = cluster("cluster-a", "node-a");
        ClusterMemberPreflight.Identity identity = ClusterMemberPreflight.validate(
                clusteredHazelcast(), cluster, control("https://node-a.internal:8080"));
        MemoryIdentityStore identities = new MemoryIdentityStore();
        MemoryClaims claims = new MemoryClaims();

        ClusterMemberPreflight.Identity first =
                ClusterMemberPreflight.reserve(identity, cluster, identities, claims, "boot-1");
        Throwable second = catchThrowable(() ->
                ClusterMemberPreflight.reserve(identity, cluster, identities, claims, "boot-2"));

        assertThat(first.nodeSession().owner()).isEqualTo(new WorkloadOwner("node-a", "boot-1"));
        assertThat(first.nodeSession().claimGeneration()).isEqualTo(1);
        assertThat(second).isInstanceOfSatisfying(TapstateException.class,
                coded -> assertThat(coded.code()).isEqualTo(BootError.NODE_ID_IN_USE));
        assertThat(claims.read(first.nodeSession().key()).map(WorkloadClaimReading::claim))
                .contains(first.nodeSession());
    }

    @Test
    void aConfiguredClusterIdCannotJoinAStoreBelongingToAnotherCluster() {
        ClusterProperties cluster = cluster("cluster-b", "node-a");
        ClusterMemberPreflight.Identity identity = ClusterMemberPreflight.validate(
                clusteredHazelcast(), cluster, control("https://node-a.internal:8080"));
        MemoryIdentityStore identities = new MemoryIdentityStore();
        identities.createIfAbsent(new ClusterIdentity("cluster-a"));

        Throwable mismatch = catchThrowable(() -> ClusterMemberPreflight.reserve(
                identity, cluster, identities, new MemoryClaims(), "boot-1"));

        assertThat(mismatch).isInstanceOfSatisfying(TapstateException.class,
                coded -> assertThat(coded.code()).isEqualTo(BootError.CLUSTER_ID_MISMATCH));
    }

    @Test
    void loopbackIsNotAnAdvertisableClusterControlEndpoint() {
        Throwable invalid = catchThrowable(() -> ClusterMemberPreflight.validate(
                clusteredHazelcast(), cluster("cluster-a", "node-a"), control("http://127.0.0.1:8080")));

        assertThat(invalid).isInstanceOfSatisfying(TapstateException.class,
                coded -> assertThat(coded.code()).isEqualTo(BootError.CONTROL_ADVERTISE_URL_INVALID));
    }

    /**
     * The advertised control URL is what every other member and every client is handed to reach this one,
     * so it has to name an address another machine can reach. A URL hands an IPv6 literal over in brackets,
     * which a loopback check on the bare form never matched, and the unspecified addresses name no machine
     * at all: all of them passed and were published to every peer.
     */
    @Test
    void anAdvertisedControlUrlMustNameAnAddressAnotherMachineCanReach() {
        assertThat(List.of(
                "http://[::1]:8080",
                "http://[0:0:0:0:0:0:0:1]:8080",
                "http://0.0.0.0:8080",
                "http://[::]:8080",
                "http://localhost:8080"))
                .allSatisfy(url -> assertThat(catchThrowable(() -> ClusterMemberPreflight.validate(
                        clusteredHazelcast(), cluster("cluster-a", "node-a"), control(url))))
                        .as(url)
                        .isInstanceOfSatisfying(TapstateException.class, coded ->
                                assertThat(coded.code()).isEqualTo(BootError.CONTROL_ADVERTISE_URL_INVALID)));
        assertThat(ClusterMemberPreflight.validate(
                clusteredHazelcast(), cluster("cluster-a", "node-a"), control("http://[2001:db8::11]:8080")))
                .as("a routable IPv6 literal is advertisable")
                .isNotNull();
    }

    /**
     * A single node binds its member to loopback, and the one loopback address a member can start on is
     * {@code 127.0.0.1}: the member matches its bind address against the addresses this host's interfaces
     * carry, which a host name or another loopback address does not match. Every other spelling passed this
     * check and then failed at member start, with an error that does not name the setting.
     */
    @Test
    void aSingleNodeBindsTheOneLoopbackAddressItsMemberCanStartOn() {
        assertThat(List.of("localhost", "::1", "127.0.0.2"))
                .allSatisfy(bind -> assertThat(catchThrowable(() -> ClusterMemberPreflight.validate(
                        singleNodeHazelcast(bind), new ClusterProperties(), control(null))))
                        .as(bind)
                        .isInstanceOfSatisfying(TapstateException.class, coded ->
                                assertThat(coded.code()).isEqualTo(BootError.MEMBER_BIND_ADDRESS_INVALID)));
        assertThat(ClusterMemberPreflight.validate(
                singleNodeHazelcast("127.0.0.1"), new ClusterProperties(), control(null)))
                .as("a single node proves no identity")
                .isNull();
    }

    /** A clustered member binds an address of this host rather than a name for one, for the same reason. */
    @Test
    void aClusteredMemberBindsAnAddressRatherThanAName() {
        HazelcastProperties hazelcast = clusteredHazelcast();
        hazelcast.setBindAddress("node-a.internal");

        Throwable invalid = catchThrowable(() -> ClusterMemberPreflight.validate(
                hazelcast, cluster("cluster-a", "node-a"), control("https://node-a.internal:8080")));

        assertThat(invalid).isInstanceOfSatisfying(TapstateException.class,
                coded -> assertThat(coded.code()).isEqualTo(BootError.MEMBER_BIND_ADDRESS_INVALID));
    }

    @Test
    void productionHaRequiresAtLeastThreeBootstrapMembers() {
        ClusterProperties cluster = cluster("cluster-a", "node-a");
        cluster.setProfile(ClusterProperties.Profile.PRODUCTION_HA);

        Throwable invalid = catchThrowable(() -> ClusterMemberPreflight.validate(
                clusteredHazelcast(), cluster, control("https://node-a.internal:8080")));

        assertThat(invalid).isInstanceOfSatisfying(TapstateException.class,
                coded -> assertThat(coded.code()).isEqualTo(BootError.CLUSTER_PROFILE_INVALID));

        cluster.setBootstrapMinMembers(3);
        assertThat(ClusterMemberPreflight.validate(
                clusteredHazelcast(), cluster, control("https://node-a.internal:8080")))
                .isNotNull();
    }

    @Test
    void processFailureOnlyNamesAnExactTwoMemberProfile() {
        ClusterProperties cluster = cluster("cluster-a", "node-a");
        cluster.setBootstrapMinMembers(3);

        Throwable invalid = catchThrowable(() -> ClusterMemberPreflight.validate(
                clusteredHazelcast(), cluster, control("https://node-a.internal:8080")));

        assertThat(invalid).isInstanceOfSatisfying(TapstateException.class,
                coded -> assertThat(coded.code()).isEqualTo(BootError.CLUSTER_PROFILE_INVALID));
    }

    @Test
    void heartbeatDetectionWindowMustBeLongerThanItsPositiveInterval() {
        HazelcastProperties hazelcast = clusteredHazelcast();
        hazelcast.setHeartbeatInterval(Duration.ofSeconds(5));
        hazelcast.setMaximumNoHeartbeat(Duration.ofSeconds(5));

        Throwable invalid = catchThrowable(() -> ClusterMemberPreflight.validate(
                hazelcast, cluster("cluster-a", "node-a"), control("https://node-a.internal:8080")));

        assertThat(invalid).isInstanceOfSatisfying(TapstateException.class,
                coded -> assertThat(coded.code()).isEqualTo(BootError.HEARTBEAT_CONFIG_INVALID));
    }

    @Test
    void workloadClaimRenewalMustPrecedeItsPositiveLease() {
        ClusterProperties cluster = cluster("cluster-a", "node-a");
        cluster.setWorkloadClaimTtl(Duration.ofSeconds(10));
        cluster.setWorkloadClaimRenewInterval(Duration.ofSeconds(10));

        Throwable invalid = catchThrowable(() -> ClusterMemberPreflight.validate(
                clusteredHazelcast(), cluster, control("https://node-a.internal:8080")));

        assertThat(invalid).isInstanceOfSatisfying(TapstateException.class,
                coded -> assertThat(coded.code()).isEqualTo(BootError.WORKLOAD_CLAIM_RENEW_INTERVAL_INVALID));
    }

    private static HazelcastProperties clusteredHazelcast() {
        HazelcastProperties properties = new HazelcastProperties();
        properties.getDiscovery().setMode(HazelcastProperties.DiscoveryMode.TCP_IP);
        properties.getDiscovery().getTcpIp().setSeeds(java.util.List.of("10.20.0.11:5701"));
        properties.setBindAddress("10.20.0.11");
        return properties;
    }

    private static HazelcastProperties singleNodeHazelcast(String bindAddress) {
        HazelcastProperties properties = new HazelcastProperties();
        properties.setBindAddress(bindAddress);
        return properties;
    }

    private static ClusterProperties cluster(String clusterId, String nodeId) {
        ClusterProperties properties = new ClusterProperties();
        properties.setId(clusterId);
        properties.setNodeId(nodeId);
        properties.setProfile(ClusterProperties.Profile.PROCESS_FAILURE_ONLY);
        properties.setBootstrapMinMembers(2);
        return properties;
    }

    private static ControlEndpointProperties control(String url) {
        ControlEndpointProperties properties = new ControlEndpointProperties();
        properties.setAdvertiseUrl(url);
        return properties;
    }

    private static final class MemoryIdentityStore implements ClusterIdentityStore {
        private final AtomicReference<ClusterIdentity> stored = new AtomicReference<>();

        @Override
        public Optional<ClusterIdentity> find() {
            return Optional.ofNullable(stored.get());
        }

        @Override
        public ClusterIdentity createIfAbsent(ClusterIdentity proposed) {
            stored.compareAndSet(null, proposed);
            return stored.get();
        }
    }

    private static final class MemoryClaims implements WorkloadClaimStore {

        /** Nothing here reads the lease; it answers with a live one so a read is not a lapsed claim. */
        private static final Duration LIVE_LEASE = Duration.ofSeconds(30);
        private WorkloadClaim current;

        @Override
        public synchronized WorkloadClaimAttempt acquire(
                WorkloadClaimKey key, WorkloadOwner owner, long topologyRevision, Duration ttl) {
            if (current == null || current.owner().equals(owner)) {
                long generation = current == null ? 1 : current.claimGeneration();
                current = new WorkloadClaim(
                        key, owner, generation, 0, topologyRevision, Instant.now().plus(ttl));
                return WorkloadClaimAttempt.acquired(current);
            }
            return WorkloadClaimAttempt.refused(current);
        }

        @Override
        public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            return Optional.empty();
        }

        @Override
        public boolean release(WorkloadClaim expected) {
            return false;
        }

        @Override
        public Optional<WorkloadClaim> advanceExecution(
                WorkloadClaim expected, long topologyRevision, java.util.Set<String> executionNodeIds) {
            return Optional.empty();
        }

        @Override
        public Optional<WorkloadClaim> recordExecutionFailure(
                WorkloadClaim expected, boolean afterMemberLoss) {
            return Optional.empty();
        }

        @Override
        public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
            return Optional.ofNullable(current).map(claim -> new WorkloadClaimReading(claim, LIVE_LEASE));
        }
    }
}
