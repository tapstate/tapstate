package io.tapstate.app;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.mongostore.SourceConfigKeyringSession;
import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
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

/** Proves keyring admission and exact lease cleanup before member construction, without creating a member. */
class SourceConfigNodeAdmissionTest {

    @Test
    void aRejectedCurrentEpochReleasesOnlyItsExactNodeSessionBeforeMemberConstruction() {
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
                .isSameAs(keyring.refusal)
                .isInstanceOfSatisfying(TapstateException.class,
                        failure -> assertThat(failure.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_NOT_READY));

        assertThat(order).containsExactly("reserve-identity", "acknowledge-current-epoch", "release-exact-ack");
        assertThat(keyring.candidate).isNotNull();
        assertThat(keyring.candidate.key()).isEqualTo(new WorkloadClaimKey(cluster.getId(),
                WorkloadClaimType.NODE_SESSION, cluster.getNodeId()));
        assertThat(keyring.candidate.owner().nodeId()).isEqualTo(cluster.getNodeId());
        assertThat(keyring.candidate.owner().bootId()).isNotBlank();
        assertThat(keyring.ackTtl).isEqualTo(cluster.getNodeSessionTtl());
        var released = claims.read(keyring.candidate.key()).orElseThrow();
        assertThat(released.leased()).isFalse();
        assertThat(released.claim().owner()).isEqualTo(keyring.candidate.owner());
        assertThat(released.claim().claimGeneration()).isEqualTo(keyring.candidate.claimGeneration());
        assertThat(released.claim().executionGeneration()).isEqualTo(keyring.candidate.executionGeneration());
        assertThat(claims.read(other.key()).orElseThrow().leased()).isTrue();
        assertThat(keyring.pendingAcks).containsExactly(other);
        assertThat(Hazelcast.getAllHazelcastInstances()).containsExactlyInAnyOrderElementsOf(membersBefore);
    }

    @Test
    void theExistingSingleProfileReachesConfigurationWithoutAcquiringOrAcknowledgingANodeSession() {
        InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
        List<String> order = new ArrayList<>();
        BeforeMemberProperties properties = new BeforeMemberProperties(order);
        ClusterProperties cluster = new ClusterProperties();
        RejectingKeyring keyring = new RejectingKeyring(claims, order);
        var membersBefore = new HashSet<>(Hazelcast.getAllHazelcastInstances());

        assertThatThrownBy(() -> attemptMember(properties, cluster, new ControlEndpointProperties(), claims, keyring))
                .isSameAs(properties.beforeConstruction);

        assertThat(order).containsExactly("member-configuration");
        assertThat(keyring.candidate).isNull();
        assertThat(keyring.pendingAcks).isEmpty();
        assertThat(Hazelcast.getAllHazelcastInstances()).containsExactlyInAnyOrderElementsOf(membersBefore);
    }

    private static void attemptMember(HazelcastProperties properties, ClusterProperties cluster,
            ControlEndpointProperties control, WorkloadClaimStore claims, SourceConfigKeyringSession keyring) {
        DefaultListableBeanFactory beans = new DefaultListableBeanFactory();
        ClusterIdentityStore identities = new ClusterIdentityStore() {
            private ClusterIdentity identity;

            @Override public Optional<ClusterIdentity> find() { return Optional.ofNullable(identity); }

            @Override public ClusterIdentity createIfAbsent(ClusterIdentity proposed) {
                ((BeforeMemberProperties) properties).order.add("reserve-identity");
                if (identity == null) identity = proposed;
                return identity;
            }
        };
        beans.registerSingleton("clusterIdentity", identities);
        beans.registerSingleton("workloadClaims", claims);
        beans.registerSingleton("sourceConfigKeyring", keyring);
        HazelcastInstance member = null;
        try {
            member = new HazelcastConfiguration().hazelcastMember(properties, cluster, control,
                    null, null, null, null, NestSettings.defaults(), null, null, null,
                    beans.getBeanProvider(ClusterIdentityStore.class), beans.getBeanProvider(WorkloadClaimStore.class),
                    beans.getBeanProvider(SourceConfigKeyringSession.class), new ClusterMembershipGate(cluster));
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
            // Removing or moving the acknowledgement crosses this boundary and fails without binding sockets.
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
