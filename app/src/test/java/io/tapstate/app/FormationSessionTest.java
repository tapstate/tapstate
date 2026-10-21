package io.tapstate.app;

import com.hazelcast.cluster.Cluster;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.mongostore.SourceConfigKeyringSession;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Native formation must not consume the reserved session before its renewal owner exists. */
class FormationSessionTest {
    @Test
    void aSlowNativeFormationRetainsTheSameLiveNodeReservation() throws Exception {
        Attempt attempt = form(true, false);
        assertThat(attempt.failure()).isSameAs(attempt.afterTransfer());
        verify(attempt.profiles()).markJoined(any(), anyString(), anyString());
        verify(attempt.member()).shutdown();
        verify(attempt.claims()).release(any());
        verify(attempt.keyring()).release(any());
    }

    @Test
    void aSessionLostBeforeMemberPublicationCannotPublishAJoinOrStartARuntimeOwner() throws Exception {
        Attempt attempt = form(false, false);
        assertThat(attempt.failure()).isInstanceOfSatisfying(TapstateException.class,
                failed -> assertThat(failed.code()).isEqualTo(BootError.PROFILE_SESSION_LOST));
        verify(attempt.profiles(), never()).markJoined(any(), anyString(), anyString());
        verify(attempt.member()).shutdown();
        verify(attempt.claims(), never()).release(any());
        verify(attempt.keyring(), never()).release(any());
    }

    @Test
    void aFactoryFailureClosesTheTemporaryOwnerAndItsExactAcknowledgement() throws Exception {
        Attempt attempt = form(true, true);
        assertThat(attempt.failure()).isInstanceOfSatisfying(TapstateException.class, failed -> {
            assertThat(failed.code()).isEqualTo(BootError.HAZELCAST_UNAVAILABLE);
            assertThat(failed.getCause()).isSameAs(attempt.nativeFailure());
        });
        verify(attempt.profiles(), never()).markJoined(any(), anyString(), anyString());
        verify(attempt.member(), never()).shutdown();
        verify(attempt.claims()).release(any());
        verify(attempt.keyring()).release(any());
    }

    private static Attempt form(boolean renews, boolean failsInFactory) throws Exception {
        Duration ttl = Duration.ofMillis(300);
        InMemoryWorkloadClaimStore leases = new InMemoryWorkloadClaimStore();
        WorkloadClaimStore claims = mock(WorkloadClaimStore.class);
        ClusterProfileStore profiles = mock(ClusterProfileStore.class);
        ClusterIdentityStore identities = mock(ClusterIdentityStore.class);
        ExecutionProfile proposed = mock(ExecutionProfile.class);
        when(proposed.hash()).thenReturn("a".repeat(64));
        ClusterExecutionProfile profile = new ClusterExecutionProfile("formation-session", 1, proposed);
        when(identities.createIfAbsent(any())).thenAnswer(call -> call.getArgument(0));
        when(profiles.reserve(anyString(), any(), any(), any(), any())).thenAnswer(call -> {
            leases.clockAt(Instant.now());
            WorkloadOwner owner = call.getArgument(1);
            WorkloadClaimKey key = new WorkloadClaimKey("formation-session", WorkloadClaimType.NODE_SESSION, owner.nodeId());
            WorkloadClaim acquired = leases.acquire(key, owner, 0, ttl).claim();
            WorkloadClaim session = profiled(acquired);
            return new ClusterNodeReservation(ClusterNodeReservation.Outcome.ACQUIRED,
                    new ClusterNodeReading(new ClusterNodeRegistration(session, profile, call.getArgument(2),
                            false, null, null, null), ttl), profile);
        });
        CountDownLatch firstRenewal = new CountDownLatch(1);
        when(claims.renew(any(), any())).thenAnswer(call -> {
            firstRenewal.countDown();
            if (!renews) return Optional.empty();
            WorkloadClaim expected = call.getArgument(0);
            WorkloadClaim current = leases.read(expected.key()).orElseThrow().claim();
            assertThat(WorkloadClaimFence.from(profiled(current))).isEqualTo(WorkloadClaimFence.from(expected));
            return leases.renew(current, call.getArgument(1)).map(FormationSessionTest::profiled);
        });
        when(claims.release(any())).thenAnswer(call -> {
            WorkloadClaim expected = call.getArgument(0);
            return leases.release(leases.read(expected.key()).orElseThrow().claim());
        });
        when(profiles.markJoined(any(), anyString(), anyString())).thenAnswer(call -> {
            WorkloadClaim expected = call.getArgument(0);
            WorkloadClaimReading actual = leases.read(expected.key()).orElseThrow();
            assertThat(actual.leased()).as("the reserved session stays live throughout native formation").isTrue();
            assertThat(WorkloadClaimFence.from(profiled(actual.claim()))).isEqualTo(WorkloadClaimFence.from(expected));
            return true;
        });
        DefaultListableBeanFactory beans = new DefaultListableBeanFactory();
        beans.registerSingleton("identities", identities);
        beans.registerSingleton("claims", claims);
        beans.registerSingleton("profiles", profiles);
        SourceConfigKeyringSession keyring = mock(SourceConfigKeyringSession.class);
        beans.registerSingleton("keyring", keyring);
        ExecutionProfileFactory factory = mock(ExecutionProfileFactory.class);
        when(factory.create(any(), any())).thenReturn(proposed);
        ClusterProperties cluster = new ClusterProperties();
        cluster.setId("formation-session");
        cluster.setNodeId("node-a");
        cluster.setProfile(ClusterProperties.Profile.PRODUCTION_HA);
        cluster.setBootstrapMinMembers(3);
        cluster.setNodeSessionTtl(ttl);
        cluster.setNodeSessionRenewInterval(Duration.ofMillis(30));
        cluster.setWorkloadClaimTtl(ttl);
        cluster.setWorkloadClaimRenewInterval(Duration.ofMillis(30));
        HazelcastProperties properties = new HazelcastProperties();
        properties.setBindAddress("10.20.0.11");
        properties.getDiscovery().setMode(HazelcastProperties.DiscoveryMode.TCP_IP);
        properties.getDiscovery().getTcpIp().setSeeds(List.of("10.20.0.12:5701"));
        ControlEndpointProperties control = new ControlEndpointProperties();
        control.setAdvertiseUrl("http://10.20.0.11:8080");
        HazelcastInstance member = mock(HazelcastInstance.class);
        IllegalStateException afterTransfer = new IllegalStateException("stop after the formation owner transferred");
        ConcurrentHashMap<String, Object> context = new ConcurrentHashMap<>() {
            @Override public Object put(String key, Object value) {
                if (key.equals(HazelcastConfiguration.NODE_SESSION_CONTEXT_KEY) && containsKey(key)) {
                    throw afterTransfer;
                }
                return super.put(key, value);
            }
        };
        when(member.getUserContext()).thenReturn(context);
        Cluster nativeCluster = mock(Cluster.class);
        Member local = mock(Member.class);
        when(member.getCluster()).thenReturn(nativeCluster);
        when(nativeCluster.getLocalMember()).thenReturn(local);
        when(local.getUuid()).thenReturn(UUID.randomUUID());
        when(local.getAddress()).thenReturn(new Address("10.20.0.11", 5701));
        Throwable failure;
        HazelcastException nativeFailure = new HazelcastException("native formation failed");
        try (var nativeFactory = mockStatic(Hazelcast.class)) {
            nativeFactory.when(() -> Hazelcast.newHazelcastInstance(any(Config.class))).thenAnswer(call -> {
                if (failsInFactory) throw nativeFailure;
                if (!renews) assertThat(firstRenewal.await(1, TimeUnit.SECONDS)).isTrue();
                Config config = call.getArgument(0);
                when(local.getAttribute(anyString())).thenAnswer(attribute ->
                        config.getMemberAttributeConfig().getAttributes().get(attribute.<String>getArgument(0)));
                long pastOriginalLease = System.nanoTime() + ttl.plusMillis(100).toNanos();
                while (System.nanoTime() < pastOriginalLease) {
                    leases.clockAt(Instant.now());
                    Thread.sleep(5);
                }
                leases.clockAt(Instant.now());
                return member;
            });
            failure = catchThrowable(() -> new HazelcastConfiguration().hazelcastMember(properties, cluster, control,
                    null, null, null, null, NestSettings.defaults(), null, null, null,
                    beans.getBeanProvider(ClusterIdentityStore.class), beans.getBeanProvider(WorkloadClaimStore.class),
                    beans.getBeanProvider(SourceConfigKeyringSession.class), new ClusterMembershipGate(cluster),
                    beans.getBeanProvider(ClusterProfileStore.class), factory));
        }
        return new Attempt(failure, afterTransfer, nativeFailure, profiles, member, claims, keyring);
    }

    private record Attempt(Throwable failure, IllegalStateException afterTransfer, HazelcastException nativeFailure,
                           ClusterProfileStore profiles, HazelcastInstance member, WorkloadClaimStore claims,
                           SourceConfigKeyringSession keyring) { }

    private static WorkloadClaim profiled(WorkloadClaim raw) {
        return new WorkloadClaim(raw.key(), raw.owner(), raw.claimGeneration(), raw.executionGeneration(),
                raw.topologyRevision(), raw.leaseUntil(), raw.contextExecutionGeneration(), raw.executionClaimGeneration(),
                raw.executionNodeIds(), raw.failureClaimGeneration(), raw.failureAfterMemberLoss(), 1);
    }
}
