package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import io.tapstate.adapters.mongostore.SourceConfigKeyringSession;
import io.tapstate.adapters.mongostore.StoreError;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.core.lifecycle.ParallelismBudget;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.ClusterExecutionProfile;
import io.tapstate.spi.store.ClusterNodeReading;
import io.tapstate.spi.store.ClusterNodeRegistration;
import io.tapstate.spi.store.ClusterNodeReservation;
import io.tapstate.spi.store.ClusterProfileStore;
import io.tapstate.spi.store.ExecutionProfile;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.any;

/** Proves keyring admission and exact lease cleanup before member construction, without creating a member. */
class SourceConfigNodeAdmissionTest {

    @Test
    void aRejectedCurrentEpochReleasesOnlyItsExactNodeSessionBeforeMemberConstruction() {
        ProfileAdmissionStore claims = new ProfileAdmissionStore();
        List<String> order = new ArrayList<>();
        ClusterProperties cluster = new ClusterProperties();
        cluster.setId("keyring-admission-cluster");
        cluster.setNodeId("candidate-node");
        cluster.setProfile(ClusterProperties.Profile.PROCESS_FAILURE_ONLY);
        cluster.setBootstrapMinMembers(2);
        ControlEndpointProperties control = new ControlEndpointProperties();
        control.setAdvertiseUrl("http://10.20.0.11:8080");
        BeforeMemberProperties properties = new BeforeMemberProperties(order, false);
        properties.setBindAddress("10.20.0.11");
        properties.getDiscovery().setMode(HazelcastProperties.DiscoveryMode.TCP_IP);
        properties.getDiscovery().getTcpIp().setSeeds(List.of("10.20.0.12:5701"));

        ExecutionProfileFactory factory = profileFactory(properties, cluster);
        ExecutionProfile actual = factory.create(HazelcastConfiguration.memberConfig(properties, null,
                NestSettings.defaults(), null), null);
        WorkloadClaim other = claims.reserve(cluster.getId(), new WorkloadOwner("other-node", "other-boot"),
                URI.create("http://10.20.0.12:8080"), actual, cluster.getNodeSessionTtl()).node().registration().nodeSession();
        order.clear();
        RecordingKeyring keyring = new RecordingKeyring(claims, order, other);
        var membersBefore = new HashSet<>(Hazelcast.getAllHazelcastInstances());

        try (var nativeFactory = mockStatic(Hazelcast.class)) {
            nativeFactory.when(() -> Hazelcast.newHazelcastInstance(any(Config.class)))
                    .thenThrow(new AssertionError("native member construction boundary reached"));
            assertThatThrownBy(() -> attemptMember(properties, cluster, control, claims, keyring))
                    .isSameAs(keyring.refusal)
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(StoreError.SOURCE_CONFIG_KEYRING_NOT_READY));
            nativeFactory.verify(() -> Hazelcast.newHazelcastInstance(any(Config.class)), never());
        }

        assertThat(order).containsExactly("member-configuration", "reserve-identity", "acknowledge-current-epoch", "release-exact-ack");
        assertThat(claims.profile(cluster.getId()).orElseThrow().profile()).isEqualTo(actual);
        assertThat(keyring.candidate).isNotNull();
        assertThat(keyring.candidate.key()).isEqualTo(new WorkloadClaimKey(cluster.getId(),
                WorkloadClaimType.NODE_SESSION, cluster.getNodeId()));
        assertThat(keyring.candidate.owner().nodeId()).isEqualTo(cluster.getNodeId());
        assertThat(keyring.candidate.owner().bootId()).isNotBlank();
        assertThat(keyring.candidate.profileGeneration()).isEqualTo(claims.profile(cluster.getId()).orElseThrow().generation());
        assertThat(claims.unprofiledAcquisitions).isZero();
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
    void anInactiveMemberDuringRequiredContextSetupReleasesBothExactAdmissionReservations() {
        ProfileAdmissionStore claims = new ProfileAdmissionStore();
        List<String> order = new ArrayList<>();
        ClusterProperties cluster = new ClusterProperties();
        cluster.setId("keyring-context-cluster");
        cluster.setNodeId("candidate-node");
        cluster.setProfile(ClusterProperties.Profile.PROCESS_FAILURE_ONLY);
        cluster.setBootstrapMinMembers(2);
        ControlEndpointProperties control = new ControlEndpointProperties();
        control.setAdvertiseUrl("http://10.20.0.11:8080");
        BeforeMemberProperties properties = new BeforeMemberProperties(order, false);
        properties.setBindAddress("10.20.0.11");
        properties.getDiscovery().setMode(HazelcastProperties.DiscoveryMode.TCP_IP);
        properties.getDiscovery().getTcpIp().setSeeds(List.of("10.20.0.12:5701"));
        ExecutionProfile actual = profileFactory(properties, cluster).create(
                HazelcastConfiguration.memberConfig(properties, null, NestSettings.defaults(), null), null);
        WorkloadClaim other = claims.reserve(cluster.getId(), new WorkloadOwner("other-node", "other-boot"),
                URI.create("http://10.20.0.12:8080"), actual, cluster.getNodeSessionTtl()).node().registration().nodeSession();
        claims.allowJoin = true;
        RecordingKeyring keyring = new RecordingKeyring(claims, order, false, other);
        HazelcastInstance member = mock(HazelcastInstance.class);
        AtomicBoolean active = new AtomicBoolean(true);
        var context = spy(new ConcurrentHashMap<String, Object>());
        HazelcastInstanceNotActiveException cause = new HazelcastInstanceNotActiveException();
        doAnswer(call -> { active.set(false); return null; }).when(member).shutdown();
        when(member.getUserContext()).thenAnswer(call -> {
            if (!active.get()) throw cause;
            return context;
        });
        doAnswer(call -> {
            Object previous = call.callRealMethod();
            member.shutdown();
            return previous;
        }).when(context).put(eq(HazelcastConfiguration.NODE_JOINED_IDENTITY_CONTEXT_KEY), any());
        Throwable failed;
        try (var nativeFactory = mockStatic(Hazelcast.class)) {
            nativeFactory.when(() -> Hazelcast.newHazelcastInstance(any(Config.class))).thenAnswer(call -> {
                Config config = call.getArgument(0);
                Cluster nativeCluster = mock(Cluster.class);
                Member local = mock(Member.class);
                when(member.getCluster()).thenReturn(nativeCluster);
                when(nativeCluster.getLocalMember()).thenReturn(local);
                when(local.getUuid()).thenReturn(UUID.fromString("b6f7ac41-03d8-45d2-96fd-44c68ef013ac"));
                when(local.getAddress()).thenReturn(new Address(properties.getBindAddress(), config.getNetworkConfig().getPort()));
                when(local.getAttribute(anyString())).thenAnswer(attribute ->
                        config.getMemberAttributeConfig().getAttributes().get(attribute.<String>getArgument(0)));
                return member;
            });
            failed = catchThrowable(() -> attemptMember(properties, cluster, control, claims, keyring));
            nativeFactory.verify(() -> Hazelcast.newHazelcastInstance(any(Config.class)), times(1));
        }

        assertThat(claims.read(keyring.candidate.key()).orElseThrow().leased())
                .as("the failed factory created no renewal owner, so its exact NODE reservation is released")
                .isFalse();
        assertThat(keyring.pendingAcks).containsExactly(other);
        assertThat(claims.read(other.key()).orElseThrow().leased()).isTrue();
        assertThat(context).doesNotContainKey(HazelcastConfiguration.NODE_SESSION_OWNER_CONTEXT_KEY);
        assertThat(context).containsKey(HazelcastConfiguration.NODE_JOINED_IDENTITY_CONTEXT_KEY);
        assertThat(failed).isInstanceOfSatisfying(TapstateException.class, failure -> {
            assertThat(failure.code()).isEqualTo(BootError.HAZELCAST_UNAVAILABLE);
            assertThat(failure.getCause()).isSameAs(cause);
        });
    }

    @Test
    void theExistingSingleProfileReachesConfigurationWithoutAcquiringOrAcknowledgingANodeSession() {
        ProfileAdmissionStore claims = new ProfileAdmissionStore();
        List<String> order = new ArrayList<>();
        BeforeMemberProperties properties = new BeforeMemberProperties(order);
        ClusterProperties cluster = new ClusterProperties();
        RecordingKeyring keyring = new RecordingKeyring(claims, order);
        var membersBefore = new HashSet<>(Hazelcast.getAllHazelcastInstances());

        assertThatThrownBy(() -> attemptMember(properties, cluster, new ControlEndpointProperties(), claims, keyring))
                .isSameAs(properties.beforeConstruction);

        assertThat(order).containsExactly("member-configuration");
        assertThat(keyring.candidate).isNull();
        assertThat(keyring.pendingAcks).isEmpty();
        assertThat(Hazelcast.getAllHazelcastInstances()).containsExactlyInAnyOrderElementsOf(membersBefore);
    }

    private static void attemptMember(HazelcastProperties properties, ClusterProperties cluster,
            ControlEndpointProperties control, ProfileAdmissionStore claims, SourceConfigKeyringSession keyring) {
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
                    beans.getBeanProvider(SourceConfigKeyringSession.class), new ClusterMembershipGate(cluster),
                    beans.getBeanProvider(ClusterProfileStore.class), profileFactory(properties, cluster));
            throw new AssertionError("The admission test must stop before constructing a member");
        } finally {
            if (member != null) member.shutdown();
        }
    }

    private static ExecutionProfileFactory profileFactory(HazelcastProperties properties, ClusterProperties cluster) {
        return new ExecutionProfileFactory(properties, cluster, new ExecutionProfileProperties(),
                ParallelismBudget.DEFAULTS, new ClusterCapacityProperties().limits());
    }

    /** Adds the profile port's metadata to the existing memory lease fixture for assembly-boundary checks. */
    private static final class ProfileAdmissionStore implements WorkloadClaimStore, ClusterProfileStore {
        private final InMemoryWorkloadClaimStore leases = new InMemoryWorkloadClaimStore();
        private final Map<WorkloadClaimKey, ClusterExecutionProfile> bindings = new java.util.LinkedHashMap<>();
        private ClusterExecutionProfile accepted;
        private int unprofiledAcquisitions;
        private boolean allowJoin;

        @Override public synchronized ClusterNodeReservation reserve(String clusterId, WorkloadOwner owner,
                URI controlUrl, ExecutionProfile proposed, Duration ttl) {
            if (accepted == null) accepted = new ClusterExecutionProfile(clusterId, 1, proposed);
            if (!accepted.clusterId().equals(clusterId) || !accepted.profile().equals(proposed)) {
                return new ClusterNodeReservation(ClusterNodeReservation.Outcome.INCOMPATIBLE, null, accepted);
            }
            WorkloadClaimKey key = new WorkloadClaimKey(clusterId, WorkloadClaimType.NODE_SESSION, owner.nodeId());
            WorkloadClaimAttempt attempt = leases.acquire(key, owner, 0, ttl);
            if (!attempt.acquired()) return new ClusterNodeReservation(ClusterNodeReservation.Outcome.NODE_IN_USE, null, accepted);
            bindings.put(key, accepted);
            WorkloadClaimReading held = read(key).orElseThrow();
            return new ClusterNodeReservation(ClusterNodeReservation.Outcome.ACQUIRED,
                    new ClusterNodeReading(new ClusterNodeRegistration(held.claim(), accepted, controlUrl,
                            false, null, null, null), held.leaseRemaining()), accepted);
        }

        @Override public Optional<ClusterExecutionProfile> profile(String clusterId) {
            return Optional.ofNullable(accepted).filter(profile -> profile.clusterId().equals(clusterId));
        }
        @Override public List<ClusterNodeReading> nodes(String clusterId) { return List.of(); }
        @Override public boolean markJoined(WorkloadClaim expected, String uuid, String address) {
            if (!allowJoin) throw new AssertionError("a rejected keyring epoch must not reach native join publication");
            WorkloadClaimReading held = read(expected.key()).orElseThrow();
            assertThat(held.leased()).isTrue();
            assertThat(WorkloadClaimFence.from(held.claim())).isEqualTo(WorkloadClaimFence.from(expected));
            assertThat(UUID.fromString(uuid)).isNotNull();
            assertThat(address).isNotBlank();
            return true;
        }
        @Override public WorkloadClaimAttempt acquire(WorkloadClaimKey key, WorkloadOwner owner, long topology, Duration ttl) {
            unprofiledAcquisitions++;
            throw new AssertionError("profile admission must reserve through the actual profile port");
        }
        @Override public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            throw new AssertionError("a pre-member epoch refusal must not start a renewal owner");
        }
        @Override public synchronized boolean release(WorkloadClaim expected) {
            WorkloadClaimReading actual = read(expected.key()).orElseThrow();
            if (!WorkloadClaimFence.from(expected).equals(WorkloadClaimFence.from(actual.claim()))) return false;
            return leases.release(leases.read(expected.key()).orElseThrow().claim());
        }
        @Override public Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected, long topology, Set<String> nodes) {
            throw new AssertionError("a node admission cannot allocate a pipeline execution");
        }
        @Override public Optional<WorkloadClaim> recordExecutionFailure(WorkloadClaim expected, boolean memberLoss) {
            throw new AssertionError("a node admission cannot classify a pipeline failure");
        }
        @Override public synchronized Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
            return leases.read(key).map(reading -> {
                WorkloadClaim raw = reading.claim();
                ClusterExecutionProfile profile = bindings.get(key);
                if (profile == null) throw new AssertionError("the fixture has no recorded profile binding");
                WorkloadClaim node = new WorkloadClaim(raw.key(), raw.owner(), raw.claimGeneration(), raw.executionGeneration(),
                        raw.topologyRevision(), raw.leaseUntil(), raw.contextExecutionGeneration(), raw.executionClaimGeneration(),
                        raw.executionNodeIds(), raw.failureClaimGeneration(), raw.failureAfterMemberLoss(), profile.generation());
                return new WorkloadClaimReading(node, reading.leaseRemaining());
            });
        }
    }

    private static final class BeforeMemberProperties extends HazelcastProperties {
        private final List<String> order;
        private final boolean rejectConfiguration;
        private final AssertionError beforeConstruction = new AssertionError("member construction boundary reached");

        BeforeMemberProperties(List<String> order) {
            this(order, true);
        }

        BeforeMemberProperties(List<String> order, boolean rejectConfiguration) {
            this.order = order;
            this.rejectConfiguration = rejectConfiguration;
        }

        @Override String getClusterName() {
            order.add("member-configuration");
            if (rejectConfiguration) throw beforeConstruction;
            return super.getClusterName();
        }
    }

    private static final class RecordingKeyring implements SourceConfigKeyringSession {
        private final ProfileAdmissionStore claims;
        private final List<String> order;
        private final boolean refuse;
        private final Set<WorkloadClaim> pendingAcks = new HashSet<>();
        private final TapstateException refusal = new TapstateException(
                StoreError.SOURCE_CONFIG_KEYRING_NOT_READY, Map.of(), null);
        private WorkloadClaim candidate;
        private Duration ackTtl;

        RecordingKeyring(ProfileAdmissionStore claims, List<String> order, WorkloadClaim... existingAcks) {
            this(claims, order, true, existingAcks);
        }

        RecordingKeyring(ProfileAdmissionStore claims, List<String> order, boolean refuse, WorkloadClaim... existingAcks) {
            this.claims = claims;
            this.order = order;
            this.refuse = refuse;
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
            if (refuse) throw refusal;
        }

        @Override public void release(WorkloadClaim session) {
            order.add("release-exact-ack");
            assertThat(session).isSameAs(candidate);
            assertThat(claims.read(session.key()).orElseThrow().leased()).isFalse();
            assertThat(pendingAcks.remove(session)).isTrue();
        }
    }
}
