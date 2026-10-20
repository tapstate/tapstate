package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleService;
import io.tapstate.core.common.TapstateException;
import io.tapstate.adapters.mongostore.SourceConfigKeyringSession;
import io.tapstate.spi.store.ClusterIdentity;
import io.tapstate.spi.store.ClusterIdentityStore;
import io.tapstate.spi.store.StorePort;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.runtime.engine.nest.NestSettings;
import java.time.Duration;
import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.UUID;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.support.StaticListableBeanFactory;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class NodeSessionStartupTest {

    @Test
    void aLeaseConstructionFailureReleasesTheReservedSessionAndKeyring() {
        assertThat(failLeaseConstruction(Duration.ofSeconds(5), Duration.ofNanos(500_000), null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void anOverflowingLeaseDurationDoesNotStrandItsReservation() {
        assertThat(failLeaseConstruction(Duration.ofDays(106_752), Duration.ofMillis(10), null))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void cleanupStillReleasesTheKeyringAndPreservesTheConstructionFailure() {
        IllegalStateException releaseFailure = new IllegalStateException("owned release failure");
        assertThat(failLeaseConstruction(Duration.ofSeconds(5), Duration.ofNanos(500_000), releaseFailure))
                .isInstanceOf(IllegalArgumentException.class).hasSuppressedException(releaseFailure);
    }

    private static Throwable failLeaseConstruction(Duration ttl, Duration interval, RuntimeException releaseFailure) {
        ClusterProperties cluster = cluster();
        cluster.setNodeSessionTtl(ttl);
        cluster.setNodeSessionRenewInterval(interval);
        ControlEndpointProperties control = new ControlEndpointProperties();
        control.setAdvertiseUrl("https://node-a.internal:8080");
        RecordingClaims claims = new RecordingClaims();
        claims.releaseFailure = releaseFailure;
        RecordingKeyring keyring = new RecordingKeyring(claims);
        Throwable failure = org.assertj.core.api.Assertions.catchThrowable(() ->
                new HazelcastConfiguration().hazelcastMember(clusteredProperties(), cluster, control,
                        null, null, null, null, NestSettings.defaults(), null, null, null,
                        provider(ClusterIdentityStore.class, new MemoryIdentity()),
                        provider(WorkloadClaimStore.class, claims), provider(SourceConfigKeyringSession.class, keyring),
                        new ClusterMembershipGate(cluster), new BootId("boot-startup"), provider(StorePort.class, null),
                        config -> { throw new AssertionError("member factory must not run after lease setup fails"); }));
        assertThat(failure).isNotNull();
        assertThat(claims.acquisitions).hasValue(1);
        assertThat(keyring.acknowledgements).hasValue(1);
        assertThat(claims.releases).hasValue(1);
        assertThat(keyring.releases).hasValue(1);
        return failure;
    }

    @Test
    void theReservedNodeSessionRenewsWhileTheMemberFactoryIsStillRunning() {
        ClusterProperties cluster = new ClusterProperties();
        cluster.setId("startup-cluster");
        cluster.setNodeId("node-a");
        cluster.setProfile(ClusterProperties.Profile.PROCESS_FAILURE_ONLY);
        cluster.setBootstrapMinMembers(2);
        cluster.setNodeSessionTtl(Duration.ofSeconds(5));
        cluster.setNodeSessionRenewInterval(Duration.ofMillis(10));
        HazelcastProperties properties = new HazelcastProperties();
        properties.getDiscovery().setMode(HazelcastProperties.DiscoveryMode.TCP_IP);
        properties.getDiscovery().getTcpIp().setSeeds(java.util.List.of("10.20.0.11:5701"));
        properties.setBindAddress("10.20.0.11");
        ControlEndpointProperties control = new ControlEndpointProperties();
        control.setAdvertiseUrl("https://node-a.internal:8080");
        RecordingClaims claims = new RecordingClaims();
        RecordingKeyring keyring = new RecordingKeyring(claims);
        IllegalStateException factoryFinished = new IllegalStateException("owned factory control finished");

        assertThatThrownBy(() -> new HazelcastConfiguration().hazelcastMember(
                properties, cluster, control, null, null, null, null, NestSettings.defaults(),
                null, null, null, provider(ClusterIdentityStore.class, new MemoryIdentity()),
                provider(WorkloadClaimStore.class, claims), provider(SourceConfigKeyringSession.class, keyring),
                new ClusterMembershipGate(cluster), new BootId("boot-startup"), provider(StorePort.class, null),
                config -> {
                    assertThat(claims.acquisitions).hasValue(1);
                    assertThat(keyring.acknowledgements).hasValueGreaterThanOrEqualTo(1);
                    claims.factoryRunning.set(true);
                    try {
                        assertThat(claims.renewedDuringFactory.await(2, TimeUnit.SECONDS))
                                .as("the exact reserved node session renews before member creation returns")
                                .isTrue();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError("the owned startup control was interrupted", interrupted);
                    }
                    throw factoryFinished;
                }))
                .isSameAs(factoryFinished);
        assertThat(claims.acquisitions).hasValue(1);
        assertThat(claims.releases).hasValue(1);
        assertThat(keyring.acknowledgements).hasValueGreaterThanOrEqualTo(2);
        assertThat(keyring.releases).hasValue(1);
    }

    @Test
    void aLostStartupTerminatesTheMemberCapturedBeforeTheFactoryReturns() throws Exception {
        RecordingClaims claims = new RecordingClaims();
        RecordingKeyring keyring = new RecordingKeyring(claims);
        NodeSessionStartup startup = startup(claims, keyring);
        OwnedMember member = new OwnedMember(startup, true);
        try {
            startup.setHazelcastInstance(member.instance);
            claims.refuseRenewals.set(true);
            assertThat(member.stopRequested.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(startup.isCurrent()).isFalse();
            assertThat(member.running).isTrue();
            assertThat(member.terminations).hasValue(1);
            assertThat(member.shutdowns).hasValue(0);
            member.allowStop.countDown();
            assertThat(member.stopped.await(2, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> startup.handOff(member.instance))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(BootError.NODE_SESSION_LOST));
            assertThat(claims.acquisitions).hasValue(1);
            assertThat(claims.releases).hasValue(0);
            assertThat(keyring.releases).hasValue(0);
        } finally {
            member.allowStop.countDown();
            startup.abort();
            assertThat(member.stopped.await(2, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void anInstanceReturnedAfterItsSessionWasLostIsStoppedAndNeverHandedOff() {
        RecordingClaims claims = new RecordingClaims();
        RecordingKeyring keyring = new RecordingKeyring(claims);
        NodeSessionStartup startup = startup(claims, keyring);
        OwnedMember member = new OwnedMember(startup, false);
        try {
            claims.refuseRenewals.set(true);
            await().atMost(Duration.ofSeconds(2)).until(() -> !startup.isCurrent());
            assertThatThrownBy(() -> startup.handOff(member.instance))
                    .isInstanceOfSatisfying(TapstateException.class,
                            failure -> assertThat(failure.code()).isEqualTo(BootError.NODE_SESSION_LOST));
            assertThat(member.running).isFalse();
            assertThat(member.terminations).hasValue(1);
            assertThat(claims.acquisitions).hasValue(1);
            assertThat(claims.releases).hasValue(0);
        } finally {
            startup.abort();
        }
    }

    @Test
    void lossAfterHandoffKeepsTheExistingGracefulMemberStop() throws Exception {
        RecordingClaims claims = new RecordingClaims();
        RecordingKeyring keyring = new RecordingKeyring(claims);
        NodeSessionStartup startup = startup(claims, keyring);
        OwnedMember member = new OwnedMember(startup, false);
        try {
            startup.setHazelcastInstance(member.instance);
            startup.handOff(member.instance);
            claims.refuseRenewals.set(true);
            assertThat(member.stopped.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(member.shutdowns).hasValue(1);
            assertThat(member.terminations).hasValue(0);
            assertThat(claims.releases).hasValue(0);
        } finally {
            startup.abort();
        }
    }

    @Test
    void theLibraryLifecycleCallbackDoesNotWaitForAStoreRelease() throws Exception {
        RecordingClaims claims = new RecordingClaims();
        claims.holdRelease.set(true);
        RecordingKeyring keyring = new RecordingKeyring(claims);
        NodeSessionStartup startup = startup(claims, keyring);
        CountDownLatch returned = new CountDownLatch(1);
        Thread callback = Thread.ofPlatform().daemon().name("owned-node-session-lifecycle-control").unstarted(() -> {
            try {
                startup.stateChanged(new LifecycleEvent(LifecycleEvent.LifecycleState.SHUTTING_DOWN));
            } finally {
                returned.countDown();
            }
        });
        try {
            callback.start();
            assertThat(claims.releaseStarted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(returned.await(2, TimeUnit.SECONDS))
                    .as("a lifecycle callback returns while the coordination store is still blocked")
                    .isTrue();
            assertThat(startup.isCurrent()).isFalse();
        } finally {
            claims.allowRelease.countDown();
            callback.join(2000);
            startup.abort();
        }
        await().atMost(Duration.ofSeconds(2)).untilAsserted(() -> assertThat(keyring.releases).hasValue(1));
        assertThat(claims.releases).hasValue(1);
    }

    private static NodeSessionStartup startup(RecordingClaims claims, RecordingKeyring keyring) {
        Duration ttl = Duration.ofSeconds(5);
        long askedAt = System.nanoTime();
        WorkloadClaim claim = claims.acquire(
                new WorkloadClaimKey("startup-cluster", WorkloadClaimType.NODE_SESSION, "node-a"),
                new WorkloadOwner("node-a", "boot-startup"), 0, ttl).claim();
        keyring.acknowledge(claim, ttl);
        return new NodeSessionStartup(claims, claim, askedAt, ttl, Duration.ofMillis(10), keyring);
    }

    @Test
    void aFailureAfterTheFactoryReturnsStopsTheMemberAndItsPreStartedLease() {
        ClusterProperties cluster = cluster();
        HazelcastProperties properties = clusteredProperties();
        ControlEndpointProperties control = new ControlEndpointProperties();
        control.setAdvertiseUrl("https://node-a.internal:8080");
        RecordingClaims claims = new RecordingClaims();
        RecordingKeyring keyring = new RecordingKeyring(claims);
        IllegalStateException wiringFailure = new IllegalStateException("owned post-factory wiring failed");
        AtomicReference<NodeSessionStartup> createdStartup = new AtomicReference<>();
        AtomicReference<OwnedMember> createdMember = new AtomicReference<>();
        ObjectProvider<StorePort> failingStore = new ObjectProvider<>() {
            @Override public StorePort getObject() { throw wiringFailure; }
            @Override public StorePort getObject(Object... arguments) { throw wiringFailure; }
            @Override public StorePort getIfAvailable() { throw wiringFailure; }
            @Override public StorePort getIfUnique() { throw wiringFailure; }
        };
        try {
            assertThatThrownBy(() -> new HazelcastConfiguration().hazelcastMember(
                    properties, cluster, control, null, null, null, null, NestSettings.defaults(),
                    null, null, null, provider(ClusterIdentityStore.class, new MemoryIdentity()),
                    provider(WorkloadClaimStore.class, claims), provider(SourceConfigKeyringSession.class, keyring),
                    new ClusterMembershipGate(cluster), new BootId("boot-startup"), failingStore,
                    config -> {
                        NodeSessionStartup startup = config.getListenerConfigs().stream()
                                .map(listener -> listener.getImplementation())
                                .filter(NodeSessionStartup.class::isInstance)
                                .map(NodeSessionStartup.class::cast).findFirst().orElseThrow();
                        OwnedMember member = new OwnedMember(startup, false);
                        createdStartup.set(startup);
                        createdMember.set(member);
                        startup.setHazelcastInstance(member.instance);
                        return member.instance;
                    })).isSameAs(wiringFailure);
            assertThat(createdMember.get().running)
                    .as("a member whose bean wiring failed is stopped before the exception escapes")
                    .isFalse();
            assertThat(createdStartup.get().isCurrent()).isFalse();
            assertThat(claims.releases).hasValue(1);
            assertThat(keyring.releases).hasValue(1);
        } finally {
            if (createdStartup.get() != null) createdStartup.get().abort();
        }
    }

    @Test
    void abortStopsItsMemberBeforeWaitingForCoordinationStoreRelease() throws Exception {
        RecordingClaims claims = new RecordingClaims();
        claims.holdRelease.set(true);
        RecordingKeyring keyring = new RecordingKeyring(claims);
        NodeSessionStartup startup = startup(claims, keyring);
        OwnedMember member = new OwnedMember(startup, false);
        startup.setHazelcastInstance(member.instance);
        Thread abort = Thread.ofPlatform().daemon().name("owned-node-session-abort-control")
                .unstarted(startup::abort);
        try {
            abort.start();
            assertThat(claims.releaseStarted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(member.running)
                    .as("store cleanup cannot keep the owned member alive after startup failure")
                    .isFalse();
            assertThat(startup.isCurrent()).isFalse();
        } finally {
            claims.allowRelease.countDown();
            abort.join(2000);
            startup.abort();
        }
        assertThat(abort.isAlive()).isFalse();
    }

    private static ClusterProperties cluster() {
        ClusterProperties cluster = new ClusterProperties();
        cluster.setId("startup-cluster");
        cluster.setNodeId("node-a");
        cluster.setProfile(ClusterProperties.Profile.PROCESS_FAILURE_ONLY);
        cluster.setBootstrapMinMembers(2);
        cluster.setNodeSessionTtl(Duration.ofSeconds(5));
        cluster.setNodeSessionRenewInterval(Duration.ofMillis(10));
        return cluster;
    }

    private static HazelcastProperties clusteredProperties() {
        HazelcastProperties properties = new HazelcastProperties();
        properties.getDiscovery().setMode(HazelcastProperties.DiscoveryMode.TCP_IP);
        properties.getDiscovery().getTcpIp().setSeeds(java.util.List.of("10.20.0.11:5701"));
        properties.setBindAddress("10.20.0.11");
        return properties;
    }

    private static <T> ObjectProvider<T> provider(Class<T> type, T value) {
        StaticListableBeanFactory factory = new StaticListableBeanFactory();
        if (value != null) factory.addBean("owned-fixture", value);
        return factory.getBeanProvider(type);
    }

    private static final class MemoryIdentity implements ClusterIdentityStore {
        private ClusterIdentity identity;

        @Override
        public Optional<ClusterIdentity> find() {
            return Optional.ofNullable(identity);
        }

        @Override
        public ClusterIdentity createIfAbsent(ClusterIdentity proposed) {
            if (identity == null) identity = proposed;
            return identity;
        }
    }

    private static final class RecordingKeyring implements SourceConfigKeyringSession {
        private final RecordingClaims claims;
        private final AtomicInteger acknowledgements = new AtomicInteger();
        private final AtomicInteger releases = new AtomicInteger();

        private RecordingKeyring(RecordingClaims claims) {
            this.claims = claims;
        }

        @Override
        public void acknowledge(WorkloadClaim claim, Duration ttl) {
            acknowledgements.incrementAndGet();
            if (claims.factoryRunning.get()) claims.renewedDuringFactory.countDown();
        }

        @Override
        public void release(WorkloadClaim claim) {
            releases.incrementAndGet();
        }
    }

    private static final class RecordingClaims implements WorkloadClaimStore {
        private final InMemoryWorkloadClaimStore delegate = new InMemoryWorkloadClaimStore();
        private final AtomicInteger acquisitions = new AtomicInteger();
        private final AtomicInteger releases = new AtomicInteger();
        private final AtomicBoolean factoryRunning = new AtomicBoolean();
        private final AtomicBoolean refuseRenewals = new AtomicBoolean();
        private final AtomicBoolean holdRelease = new AtomicBoolean();
        private RuntimeException releaseFailure;
        private final CountDownLatch renewedDuringFactory = new CountDownLatch(1);
        private final CountDownLatch releaseStarted = new CountDownLatch(1);
        private final CountDownLatch allowRelease = new CountDownLatch(1);

        @Override
        public WorkloadClaimAttempt acquire(WorkloadClaimKey key, WorkloadOwner owner,
                long topologyRevision, Duration ttl) {
            acquisitions.incrementAndGet();
            return delegate.acquire(key, owner, topologyRevision, ttl);
        }

        @Override
        public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            return refuseRenewals.get() ? Optional.empty() : delegate.renew(expected, ttl);
        }

        @Override
        public boolean release(WorkloadClaim expected) {
            releases.incrementAndGet();
            if (releaseFailure != null) throw releaseFailure;
            releaseStarted.countDown();
            if (holdRelease.get()) {
                try {
                    if (!allowRelease.await(5, TimeUnit.SECONDS)) throw new AssertionError("release control not let go");
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("release control interrupted", interrupted);
                }
            }
            return delegate.release(expected);
        }

        @Override
        public Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected, long topologyRevision,
                Set<String> executionNodeIds) {
            return delegate.advanceExecution(expected, topologyRevision, executionNodeIds);
        }

        @Override
        public Optional<WorkloadClaim> recordExecutionFailure(WorkloadClaim expected, boolean afterMemberLoss) {
            return delegate.recordExecutionFailure(expected, afterMemberLoss);
        }

        @Override
        public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) {
            return delegate.read(key);
        }
    }

    private static final class OwnedMember {
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final AtomicInteger terminations = new AtomicInteger();
        private final AtomicInteger shutdowns = new AtomicInteger();
        private final CountDownLatch stopRequested = new CountDownLatch(1);
        private final CountDownLatch allowStop = new CountDownLatch(1);
        private final CountDownLatch stopped = new CountDownLatch(1);
        private final HazelcastInstance instance;

        private OwnedMember(NodeSessionStartup startup, boolean holdStop) {
            String name = "owned-startup-" + UUID.randomUUID();
            Map<String, Object> context = new ConcurrentHashMap<>();
            LifecycleService lifecycle = (LifecycleService) Proxy.newProxyInstance(
                    LifecycleService.class.getClassLoader(), new Class<?>[]{LifecycleService.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("isRunning")) return running.get();
                        if (!method.getName().equals("terminate") && !method.getName().equals("shutdown")) {
                            throw new AssertionError("unexpected lifecycle call: " + method.getName());
                        }
                        if (method.getName().equals("terminate")) terminations.incrementAndGet();
                        else shutdowns.incrementAndGet();
                        startup.stateChanged(new LifecycleEvent(LifecycleEvent.LifecycleState.SHUTTING_DOWN));
                        stopRequested.countDown();
                        if (holdStop && !allowStop.await(5, TimeUnit.SECONDS)) {
                            throw new AssertionError("owned stop control not let go");
                        }
                        running.set(false);
                        startup.stateChanged(new LifecycleEvent(LifecycleEvent.LifecycleState.SHUTDOWN));
                        stopped.countDown();
                        return null;
                    });
            instance = (HazelcastInstance) Proxy.newProxyInstance(
                    HazelcastInstance.class.getClassLoader(), new Class<?>[]{HazelcastInstance.class},
                    (proxy, method, args) -> switch (method.getName()) {
                        case "getName", "toString" -> name;
                        case "getUserContext" -> context;
                        case "getLifecycleService" -> lifecycle;
                        case "shutdown" -> {
                            lifecycle.shutdown();
                            yield null;
                        }
                        default -> throw new AssertionError("unexpected member call: " + method.getName());
                    });
        }
    }
}
