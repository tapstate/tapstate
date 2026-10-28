package io.tapstate.app;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.ManagedContext;
import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimAttempt;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

class NodeSessionStartupMemberTest {

    @Test
    void theRealFactoryRenewsThenRejectsItsLateHandleAfterSessionLoss() throws Exception {
        RefusingClaims claims = new RefusingClaims();
        Duration ttl = Duration.ofSeconds(5);
        long askedAt = System.nanoTime();
        WorkloadClaim claim = claims.acquire(
                new WorkloadClaimKey("owned-startup", WorkloadClaimType.NODE_SESSION, "node-a"),
                new WorkloadOwner("node-a", "owned-boot"), 0, ttl).claim();
        NodeSessionStartup startup = new NodeSessionStartup(
                claims, claim, askedAt, ttl, Duration.ofMillis(10));
        ConstructorBarrier barrier = new ConstructorBarrier();
        String name = "owned-node-session-startup-" + UUID.randomUUID();
        HazelcastProperties properties = new HazelcastProperties();
        properties.setClusterName(name);
        properties.setMemberPort(0);
        Config config = HazelcastConfiguration.memberConfig(properties);
        config.setInstanceName(name);
        config.getNetworkConfig().setPort(0).setPortAutoIncrement(false);
        config.getJetConfig().setEnabled(false);
        config.setProperty("hazelcast.wait.seconds.before.join", "0");
        config.addListenerConfig(new ListenerConfig(startup));
        config.setManagedContext(barrier);
        AtomicReference<HazelcastInstance> returned = new AtomicReference<>();
        var executor = Executors.newSingleThreadExecutor(runnable ->
                Thread.ofPlatform().daemon().name("owned-node-session-factory").unstarted(runnable));
        var factory = executor.submit(() -> {
            HazelcastInstance member = Hazelcast.newHazelcastInstance(config);
            returned.set(member);
            startup.handOff(member);
            return member;
        });
        try {
            assertThat(barrier.entered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(factory.isDone()).isFalse();
            int before = claims.renewals.get();
            await().atMost(Duration.ofSeconds(2)).until(() -> claims.renewals.get() > before);
            assertThat(startup.isCurrent()).isTrue();
            assertThat(barrier.member.get().getLifecycleService().isRunning()).isTrue();

            claims.refuse.set(true);
            await().atMost(Duration.ofSeconds(5)).until(() ->
                    !barrier.member.get().getLifecycleService().isRunning());
            assertThat(startup.isCurrent()).isFalse();
            assertThat(factory.isDone()).as("the owned constructor barrier still holds the factory").isFalse();
            barrier.release.countDown();
            assertThatThrownBy(() -> factory.get(10, TimeUnit.SECONDS))
                    .isInstanceOfSatisfying(ExecutionException.class, failure ->
                            assertThat(failure.getCause()).isInstanceOfSatisfying(TapstateException.class,
                                    coded -> assertThat(coded.code()).isEqualTo(BootError.NODE_SESSION_LOST)));
            assertThat(returned.get()).as("the real factory returned a handle after the first stop").isNotNull();
            assertThat(Hazelcast.getHazelcastInstanceByName(name)).isNull();
            assertThat(claims.acquisitions).hasValue(1);
            assertThat(claims.releases).hasValue(0);
        } finally {
            barrier.release.countDown();
            startup.abort();
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    /** Holds only the test-owned constructor after join; library lifecycle listeners stay nonblocking. */
    private static final class ConstructorBarrier implements ManagedContext, HazelcastInstanceAware {
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final AtomicReference<HazelcastInstance> member = new AtomicReference<>();

        @Override public Object initialize(Object instance) { return instance; }

        @Override public void setHazelcastInstance(HazelcastInstance instance) {
            member.set(instance);
            entered.countDown();
            try {
                if (!release.await(15, TimeUnit.SECONDS)) throw new AssertionError("owned constructor not released");
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new AssertionError("owned constructor interrupted", interrupted);
            }
        }
    }

    private static final class RefusingClaims implements WorkloadClaimStore {
        private final InMemoryWorkloadClaimStore delegate = new InMemoryWorkloadClaimStore();
        private final AtomicBoolean refuse = new AtomicBoolean();
        private final AtomicInteger acquisitions = new AtomicInteger();
        private final AtomicInteger renewals = new AtomicInteger();
        private final AtomicInteger releases = new AtomicInteger();

        @Override public WorkloadClaimAttempt acquire(WorkloadClaimKey key, WorkloadOwner owner,
                long revision, Duration ttl) {
            acquisitions.incrementAndGet();
            return delegate.acquire(key, owner, revision, ttl);
        }
        @Override public Optional<WorkloadClaim> renew(WorkloadClaim expected, Duration ttl) {
            Optional<WorkloadClaim> renewed = refuse.get() ? Optional.empty() : delegate.renew(expected, ttl);
            renewed.ifPresent(current -> {
                assertThat(current.owner()).isEqualTo(expected.owner());
                assertThat(current.claimGeneration()).isEqualTo(expected.claimGeneration());
                renewals.incrementAndGet();
            });
            return renewed;
        }
        @Override public boolean release(WorkloadClaim expected) {
            releases.incrementAndGet();
            return delegate.release(expected);
        }
        @Override public Optional<WorkloadClaimReading> read(WorkloadClaimKey key) { return delegate.read(key); }
        @Override public Optional<WorkloadClaim> advanceExecution(WorkloadClaim expected, long revision,
                Set<String> nodeIds) { return delegate.advanceExecution(expected, revision, nodeIds); }
        @Override public Optional<WorkloadClaim> recordExecutionFailure(WorkloadClaim expected, boolean afterLoss) {
            return delegate.recordExecutionFailure(expected, afterLoss);
        }
    }
}
