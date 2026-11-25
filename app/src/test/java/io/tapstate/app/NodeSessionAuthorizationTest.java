package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class NodeSessionAuthorizationTest {
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final WorkloadOwner OWNER = new WorkloadOwner("cluster-a", "boot-a");

    @Test void aCachedBusinessClaimCannotOutliveItsNodeWhileShutdownAndRenewalAreBlocked() throws Exception {
        var raw = new InMemoryWorkloadClaimStore(); var node = node(raw); var fence = execution(raw);
        var nanos = new AtomicLong(); var renewalEntered = new CountDownLatch(1); var renewalReleased = new CountDownLatch(1);
        var stopping = new CountDownLatch(1); var stopped = new CountDownLatch(1); var shutdownReleased = new CountDownLatch(1);
        var store = mock(WorkloadClaimStore.class);
        when(store.renew(any(), any())).thenAnswer(call -> { renewalEntered.countDown(); renewalReleased.await(5, TimeUnit.SECONDS); return Optional.of(node); });
        try (var lease = new NodeSessionLease(store, node, 0, TTL, Duration.ofMillis(10), () -> {
                    stopping.countDown(); try { shutdownReleased.await(5, TimeUnit.SECONDS); }
                    catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); } finally { stopped.countDown(); }
                }, nanos::get);
             var authorization = new ExecutionAuthorization("cluster-a", raw, TTL, System::nanoTime, lease::active)) {
            assertThat(authorization.require(fence)).isNotNull(); assertThat(renewalEntered.await(2, TimeUnit.SECONDS)).isTrue();
            nanos.set(TTL.toNanos() + 1);
            assertThatThrownBy(() -> authorization.require(fence)).satisfies(NodeSessionAuthorizationTest::notAuthorized);
            assertThat(stopping.await(2, TimeUnit.SECONDS)).isTrue(); assertThat(stopped.getCount()).as("shutdown remains deliberately blocked").isOne();
            assertThat(raw.read(new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders"))).isPresent();
            renewalReleased.countDown(); assertThat(lease.active()).isFalse();
            assertThatThrownBy(() -> authorization.require(fence)).satisfies(NodeSessionAuthorizationTest::notAuthorized);
        } finally { renewalReleased.countDown(); shutdownReleased.countDown(); assertThat(stopped.await(2, TimeUnit.SECONDS)).isTrue(); }
    }

    @Test void startupAlreadyPastTheOriginalProofCannotAuthorizeOrRenewItIntoAValidSession() throws Exception {
        var raw = new InMemoryWorkloadClaimStore(); var node = node(raw); var fence = execution(raw); var nanos = new AtomicLong(TTL.toNanos() + 1);
        var lost = new CountDownLatch(1); var store = mock(WorkloadClaimStore.class);
        try (var lease = new NodeSessionLease(store, node, 0, TTL, Duration.ofMillis(10), lost::countDown, nanos::get);
             var authorization = new ExecutionAuthorization("cluster-a", raw, TTL, System::nanoTime, lease::active)) {
            assertThatThrownBy(() -> authorization.require(fence)).satisfies(NodeSessionAuthorizationTest::notAuthorized);
            assertThat(lost.await(2, TimeUnit.SECONDS)).isTrue(); assertThat(lease.active()).isFalse(); verify(store, never()).renew(any(), any());
        }
    }

    @Test void closingTheGuardRefusesCachedAndFreshProofs() {
        var raw = new InMemoryWorkloadClaimStore(); var fence = execution(raw);
        try (var authorization = new ExecutionAuthorization("cluster-a", raw, TTL)) {
            assertThat(authorization.require(fence)).isNotNull(); authorization.close();
            assertThatThrownBy(() -> authorization.require(fence)).satisfies(NodeSessionAuthorizationTest::notAuthorized);
        }
    }

    @Test void anAdmittedMemberMissingItsBoundGuardCannotFallBackToUnfenced() {
        var member = mock(HazelcastInstance.class); when(member.getUserContext()).thenReturn(new ConcurrentHashMap<>());
        try (var authorization = ExecutionAuthorization.of(member)) {
            assertThatThrownBy(() -> authorization.require(new ExecutionFence("orders", 1, 1))).satisfies(NodeSessionAuthorizationTest::notAuthorized);
        }
    }

    private static WorkloadClaim node(InMemoryWorkloadClaimStore raw) { return raw.acquire(new WorkloadClaimKey("cluster-a", WorkloadClaimType.NODE_SESSION, OWNER.nodeId()), OWNER, 0, TTL).claim(); }
    private static ExecutionFence execution(InMemoryWorkloadClaimStore raw) {
        var claim = raw.acquire(new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders"), OWNER, 1, TTL).claim();
        var run = raw.advanceExecution(claim, 1, Set.of(OWNER.nodeId())).orElseThrow(); return new ExecutionFence("orders", run.claimGeneration(), run.executionGeneration());
    }
    private static void notAuthorized(Throwable error) { assertThat(error).isInstanceOfSatisfying(TapstateException.class, coded -> assertThat(coded.code()).isEqualTo(EngineError.EXECUTION_NOT_AUTHORIZED)); }
}
