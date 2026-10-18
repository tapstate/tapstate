package io.tapstate.app;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.mongostore.SourceConfigKeyringSession;
import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.ClusterMembershipStore;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Configuration key sessions cannot gate admission; startup failure releases only its original node claim. */
class SourceConfigNodeAdmissionTest {

    @Test
    void aConfigurationKeySessionCannotStopAdmissionAndStartupFailureOnlyReleasesTheOriginalNodeClaim() {
        InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
        List<String> order = new ArrayList<>();
        ClusterProperties cluster = new ClusterProperties();
        cluster.setId("keyring-admission-cluster");
        cluster.setNodeId("candidate-node");
        cluster.setProfile(ClusterProperties.Profile.PROCESS_FAILURE_ONLY);
        cluster.setBootstrapMinMembers(2);
        ControlEndpointProperties control = new ControlEndpointProperties();
        control.setAdvertiseUrl("http://10.20.0.11:8080");
        BeforeMemberProperties properties = new BeforeMemberProperties(order);
        properties.setBindAddress("10.20.0.11");
        properties.getDiscovery().setMode(HazelcastProperties.DiscoveryMode.TCP_IP);
        properties.getDiscovery().getTcpIp().setSeeds(List.of("10.20.0.12:5701"));

        WorkloadClaim other = claims.acquire(new WorkloadClaimKey(cluster.getId(),
                        WorkloadClaimType.NODE_SESSION, "other-node"), new WorkloadOwner("other-node", "other-boot"),
                0, cluster.getNodeSessionTtl()).claim();
        RejectingKeyring keyring = new RejectingKeyring(claims, order, other);
        var membersBefore = new HashSet<>(Hazelcast.getAllHazelcastInstances());

        assertThatThrownBy(() -> attemptMember(properties, cluster, control, claims, keyring))
                .isSameAs(properties.beforeConstruction);

        assertThat(order).containsExactly("reserve-identity", "member-configuration");
        assertThat(keyring.candidate).isNull(); assertThat(keyring.ackTtl).isNull();
        var released = claims.read(new WorkloadClaimKey(cluster.getId(), WorkloadClaimType.NODE_SESSION, cluster.getNodeId())).orElseThrow();
        assertThat(released.leased()).isFalse();
        assertThat(released.claim().owner().nodeId()).isEqualTo(cluster.getNodeId());
        assertThat(released.claim().owner().bootId()).isNotBlank();
        assertThat(released.claim().claimGeneration()).isOne();
        assertThat(claims.read(other.key()).orElseThrow().leased()).isTrue();
        assertThat(keyring.pendingAcks).containsExactly(other);
        assertThat(Hazelcast.getAllHazelcastInstances()).containsExactlyInAnyOrderElementsOf(membersBefore);
    }

    @Test
    void aSingleProfileReservesItsOriginalNodeAndNeverAcknowledgesTheConfigurationKey() {
        InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
        List<String> order = new ArrayList<>();
        BeforeMemberProperties properties = new BeforeMemberProperties(order);
        ClusterProperties cluster = new ClusterProperties();
        RejectingKeyring keyring = new RejectingKeyring(claims, order);
        var membersBefore = new HashSet<>(Hazelcast.getAllHazelcastInstances());

        assertThatThrownBy(() -> attemptMember(properties, cluster, new ControlEndpointProperties(), claims, keyring))
                .isSameAs(properties.beforeConstruction);

        assertThat(order).containsExactly("read-existing-identity", "read-existing-identity", "member-configuration");
        var released = claims.read(new WorkloadClaimKey("single-existing-cluster", WorkloadClaimType.NODE_SESSION, "single-existing-cluster")).orElseThrow();
        assertThat(released.leased()).isFalse(); assertThat(released.claim().owner().bootId()).isNotBlank();
        assertThat(keyring.candidate).isNull();
        assertThat(keyring.pendingAcks).isEmpty();
        assertThat(Hazelcast.getAllHazelcastInstances()).containsExactlyInAnyOrderElementsOf(membersBefore);
    }

    private static void attemptMember(HazelcastProperties properties, ClusterProperties cluster,
            ControlEndpointProperties control, WorkloadClaimStore claims, SourceConfigKeyringSession keyring) {
        DefaultListableBeanFactory beans = new DefaultListableBeanFactory();
        ClusterIdentityStore identities = new ClusterIdentityStore() {
            private ClusterIdentity identity = cluster.getProfile() == ClusterProperties.Profile.SINGLE ? new ClusterIdentity("single-existing-cluster") : null;

            @Override public Optional<ClusterIdentity> find() {
                if (cluster.getProfile() == ClusterProperties.Profile.SINGLE) ((BeforeMemberProperties) properties).order.add("read-existing-identity");
                return Optional.ofNullable(identity);
            }

            @Override public ClusterIdentity createIfAbsent(ClusterIdentity proposed) {
                ((BeforeMemberProperties) properties).order.add("reserve-identity");
                if (identity == null) identity = proposed;
                return identity;
            }
        };
        beans.registerSingleton("clusterIdentity", identities);
        beans.registerSingleton("workloadClaims", claims);
        beans.registerSingleton("sourceConfigKeyring", keyring);
        ClusterMembershipStore memberships = org.mockito.Mockito.mock(ClusterMembershipStore.class);
        org.mockito.Mockito.when(memberships.read("single-existing-cluster")).thenReturn(Optional.empty());
        beans.registerSingleton("memberships", memberships);
        HazelcastInstance member = null;
        try {
            member = new HazelcastConfiguration().hazelcastMember(properties, cluster, control,
                    null, null, null, null, NestSettings.defaults(), null, null, null,
                    beans.getBeanProvider(ClusterIdentityStore.class), beans.getBeanProvider(WorkloadClaimStore.class),
                    new ClusterMembershipGate(cluster), BootId.fresh(), beans.getBeanProvider(StorePort.class), beans.getBeanProvider(ClusterMembershipStore.class));
            throw new AssertionError("The admission test must stop before constructing a member");
        } finally {
            if (member != null) member.shutdown();
        }
    }

    private static final class BeforeMemberProperties extends HazelcastProperties {
        private final List<String> order;
        private final AssertionError beforeConstruction = new AssertionError("member construction boundary reached");

        BeforeMemberProperties(List<String> order) {
            this.order = order;
        }

        @Override String getClusterName() {
            // Preflight never reads this property; memberConfig does so before invoking the real factory.
            // A failure here must clean the reserved node claim without binding any sockets.
            order.add("member-configuration");
            throw beforeConstruction;
        }
    }

    private static final class RejectingKeyring implements SourceConfigKeyringSession {
        private final InMemoryWorkloadClaimStore claims;
        private final List<String> order;
        private final Set<WorkloadClaim> pendingAcks = new HashSet<>();
        private final TapstateException refusal = new TapstateException(
                StoreError.SOURCE_CONFIG_KEYRING_NOT_READY, Map.of(), null);
        private WorkloadClaim candidate;
        private Duration ackTtl;

        RejectingKeyring(InMemoryWorkloadClaimStore claims, List<String> order, WorkloadClaim... existingAcks) {
            this.claims = claims;
            this.order = order;
            pendingAcks.addAll(List.of(existingAcks));
        }

        @Override public void acknowledge(WorkloadClaim session, Duration ttl) {
            order.add("acknowledge-current-epoch");
            assertThat(claims.read(session.key()).orElseThrow().leased()).isTrue();
            assertThat(claims.read(session.key()).orElseThrow().claim()).isEqualTo(session);
            candidate = session;
            ackTtl = ttl;
            // A failed acknowledgement may leave partial per-boot state; cleanup must target this exact boot.
            pendingAcks.add(session);
            throw refusal;
        }

        @Override public void release(WorkloadClaim session) {
            order.add("release-exact-ack");
            assertThat(session).isSameAs(candidate);
            assertThat(claims.read(session.key()).orElseThrow().leased()).isFalse();
            assertThat(pendingAcks.remove(session)).isTrue();
        }
    }
}
