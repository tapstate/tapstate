package io.tapstate.app;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.ClusterProfileStore;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicBoolean;
import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

/**
 * Starting the embedded Hazelcast member can fail for operational reasons — most commonly the
 * loopback member port is already held by another server on the same host. That is a user-facing,
 * diagnosable startup failure, so it surfaces as a coded diagnostic (rendered by
 * {@code CodedFailureAnalyzer}) rather than a bare Hazelcast stack trace. A failure that is not
 * Hazelcast's — a programmer error while assembling the member — is not laundered into a code; it
 * crashes bare.
 */
class HazelcastStartupTest {

    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withUserConfiguration(HazelcastConfiguration.class)
            .withPropertyValues(
                    "tapstate.hz.discovery.mode=tcp-ip",
                    "tapstate.hz.discovery.tcp-ip.seeds[0]=127.0.0.1:5781",
                    "tapstate.hz.member-port=5781",
                    "tapstate.hz.bind-address=10.20.0.11",
                    "tapstate.cluster.profile=process-failure-only",
                    "tapstate.cluster.bootstrap-min-members=2");

    @Test
    void clusterDiscoveryCannotStartWithoutStableIdentity() {
        context.run(started -> {
            assertThat(started).hasFailed();
            Throwable root = rootCause(started.getStartupFailure());
            assertThat(root).isInstanceOfSatisfying(TapstateException.class,
                    coded -> assertThat(coded.code()).isEqualTo(BootError.CLUSTER_ID_REQUIRED));
        });
    }

    @Test
    void memberStartupFailureBecomesACodedDiagnostic() {
        HazelcastException cause = new HazelcastException("Ports [5701-5801] are already in use");

        Throwable thrown = catchThrowable(
                () -> HazelcastConfiguration.startMember(() -> {
                    throw cause;
                }));

        assertThat(thrown).isInstanceOf(TapstateException.class);
        TapstateException coded = (TapstateException) thrown;
        assertThat(coded.code()).isEqualTo(BootError.HAZELCAST_UNAVAILABLE);
        assertThat(coded.getCause()).isSameAs(cause);
    }

    @Test
    void aNonHazelcastFailureIsNotLaunderedIntoACode() {
        RuntimeException bug = new IllegalStateException("invariant violated while building the member");

        assertThatThrownBy(() -> HazelcastConfiguration.startMember(() -> {
            throw bug;
        })).isSameAs(bug);
    }

    @Test
    void aSlowPostformationHookCannotConsumeTheSessionBeforeItsOwnerStarts() throws Exception {
        Duration ttl = Duration.ofSeconds(2);
        WorkloadClaimStore store = mock(WorkloadClaimStore.class);
        ClusterProfileStore profiles = mock(ClusterProfileStore.class);
        HazelcastInstance member = mock(HazelcastInstance.class);
        var userContext = new ConcurrentHashMap<String, Object>();
        when(member.getUserContext()).thenReturn(userContext);
        AtomicReference<NodeSessionLease> owned = new AtomicReference<>();
        AtomicInteger owners = new AtomicInteger();
        long originalDeadline = System.nanoTime() + Duration.ofSeconds(1).toNanos();
        long askedAt = originalDeadline - ttl.toNanos();
        WorkloadClaim acquired = bootstrapClaim(Instant.now().plusSeconds(1));
        WorkloadClaim accepted = bootstrapClaim(Instant.now().plus(ttl));
        userContext.put(HazelcastConfiguration.NODE_SESSION_CONTEXT_KEY, acquired);
        CountDownLatch renewed = new CountDownLatch(1);
        when(store.renew(any(), any())).thenAnswer(call -> { renewed.countDown(); return Optional.of(accepted); });
        when(store.release(any())).thenReturn(true);
        try {
            HazelcastInstance completed = HazelcastConfiguration.finishPostFormation(member, () -> {
                owners.incrementAndGet();
                NodeSessionLease lease = new NodeSessionLease(store, acquired, askedAt, ttl, Duration.ofMillis(20),
                        member::shutdown, profiles, HazelcastStartupTest::bootstrapIdentity, bootstrapIdentity());
                owned.set(lease);
                return lease;
            }, () -> {
                try {
                    assertThat(renewed.await(1, TimeUnit.SECONDS))
                            .as("renewal must already run while postformation setup remains blocked").isTrue();
                    while (System.nanoTime() < originalDeadline + Duration.ofMillis(50).toNanos()) Thread.sleep(10);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("interrupted while holding postformation setup", interrupted);
                }
            });
            assertThat(completed).isSameAs(member);
            assertThat(System.nanoTime()).isGreaterThan(originalDeadline);
            assertThat(owned.get().proof().live()).isTrue();
            assertThat(userContext.get(HazelcastConfiguration.NODE_SESSION_CONTEXT_KEY)).isSameAs(accepted);
            NodeSessionLease managed = new HazelcastConfiguration().nodeSessionLease(member, new ClusterProperties(), null, null);
            assertThat(managed).as("the managed bean reuses the factory's one existing renewal owner").isSameAs(owned.get());
            assertThat(owners).hasValue(1);
        } finally {
            if (owned.get() != null) owned.get().close();
        }
    }

    @Test
    void aPostformationFailureClosesItsOwnerAndShutsDownTheMember() {
        WorkloadClaim acquired = bootstrapClaim(Instant.now().plusSeconds(2));
        WorkloadClaimStore store = mock(WorkloadClaimStore.class);
        when(store.renew(any(), any())).thenReturn(Optional.of(acquired));
        when(store.release(any())).thenReturn(true);
        ClusterProfileStore profiles = mock(ClusterProfileStore.class);
        HazelcastInstance member = mock(HazelcastInstance.class);
        when(member.getUserContext()).thenReturn(new ConcurrentHashMap<>());
        AtomicReference<NodeSessionLease> owned = new AtomicReference<>();
        IllegalStateException failure = new IllegalStateException("postformation invariant failed");
        try {
            assertThatThrownBy(() -> HazelcastConfiguration.finishPostFormation(member, () -> {
                NodeSessionLease lease = new NodeSessionLease(store, acquired, System.nanoTime(), Duration.ofSeconds(2),
                        Duration.ofMillis(20), member::shutdown, profiles, HazelcastStartupTest::bootstrapIdentity, bootstrapIdentity());
                owned.set(lease);
                return lease;
            }, () -> { throw failure; })).isSameAs(failure);
            assertThat(owned.get()).isNotNull();
            assertThat(owned.get().proof().live()).isFalse();
            verify(store).release(any());
            verify(member).shutdown();
        } finally {
            if (owned.get() != null) owned.get().close();
        }
    }

    @Test
    void failedOwnerConstructionNeverRunsPostformationHooksAndStopsTheMember() {
        HazelcastInstance member = mock(HazelcastInstance.class);
        AtomicBoolean configured = new AtomicBoolean();
        TapstateException failure = new TapstateException(BootError.PROFILE_SESSION_LOST, Map.of(), null);
        assertThatThrownBy(() -> HazelcastConfiguration.finishPostFormation(member,
                () -> { throw failure; }, () -> configured.set(true))).isSameAs(failure);
        assertThat(configured).isFalse();
        verify(member).shutdown();
    }

    @Test
    void aFailedCompletionDoesNotInterruptOrReleaseAnOwnerAlreadyStoppingGracefully() throws Exception {
        WorkloadClaimStore store = mock(WorkloadClaimStore.class);
        when(store.renew(any(), any())).thenReturn(Optional.empty());
        ClusterProfileStore profiles = mock(ClusterProfileStore.class);
        HazelcastInstance member = mock(HazelcastInstance.class);
        when(member.getUserContext()).thenReturn(new ConcurrentHashMap<>());
        CountDownLatch stopEntered = new CountDownLatch(1);
        CountDownLatch allowStop = new CountDownLatch(1);
        CountDownLatch stopFinished = new CountDownLatch(1);
        AtomicBoolean interruptedStop = new AtomicBoolean();
        NodeSessionLease owner = new NodeSessionLease(store, bootstrapClaim(Instant.now().plusSeconds(2)),
                System.nanoTime(), Duration.ofSeconds(2), Duration.ofMillis(20), () -> {
                    stopEntered.countDown();
                    try { allowStop.await(); }
                    catch (InterruptedException interrupted) {
                        interruptedStop.set(true);
                        Thread.currentThread().interrupt();
                    } finally { stopFinished.countDown(); }
                }, profiles, HazelcastStartupTest::bootstrapIdentity, bootstrapIdentity());
        try {
            assertThat(stopEntered.await(1, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(() -> HazelcastConfiguration.finishPostFormation(member, () -> owner, () -> { }))
                    .isInstanceOfSatisfying(TapstateException.class,
                            coded -> assertThat(coded.code()).isEqualTo(BootError.PROFILE_SESSION_LOST));
            assertThat(stopFinished.await(100, TimeUnit.MILLISECONDS))
                    .as("failed completion cannot cut short the owner's graceful stop").isFalse();
            verify(store, never()).release(any());
        } finally {
            allowStop.countDown();
            assertThat(stopFinished.await(2, TimeUnit.SECONDS)).isTrue();
            owner.close();
        }
        assertThat(interruptedStop).isFalse();
    }

    private static WorkloadClaim bootstrapClaim(Instant until) {
        return new WorkloadClaim(new WorkloadClaimKey("startup-order", WorkloadClaimType.NODE_SESSION, "node-a"),
                new WorkloadOwner("node-a", "15fa0f26-746d-447b-8d72-859c36100a76"), 1, 0, 0,
                until, 0, 0, Set.of(), 0, false, 1);
    }

    private static NodeSessionLease.JoinedIdentity bootstrapIdentity() {
        return new NodeSessionLease.JoinedIdentity("e7c7305b-8c6c-48c7-bc31-73dff33d0afd", "[127.0.0.1]:5701",
                "node-a", "15fa0f26-746d-447b-8d72-859c36100a76", "1",
                "1a66995c4d6e1da8d5458f166420aee0da32906360337991c4af21b2365e500f");
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null) {
            root = root.getCause();
        }
        return root;
    }
}
