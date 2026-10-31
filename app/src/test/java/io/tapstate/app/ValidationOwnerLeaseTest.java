package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.spi.store.ValidationClaims;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationOwnerLeaseTest {
    private static final Duration TTL = Duration.ofSeconds(30);
    private static final WorkloadOwner A = new WorkloadOwner("node-a", "boot-a");
    private static final WorkloadOwner B = new WorkloadOwner("node-b", "boot-b");
    private static final WorkloadClaimKey KEY = ValidationClaims.key("cluster", "orders");

    @Test void expiredAStopsOnceAndCannotRenewOrReleaseBWhenItReturns() {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        var production = store.acquire(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                A, 7, Duration.ofMinutes(5)).claim();
        AtomicLong time = new AtomicLong();
        AtomicInteger stopped = new AtomicInteger();
        AtomicInteger renewCalls = new AtomicInteger();
        AtomicBoolean staleRelease = new AtomicBoolean(true);
        var lease = new ValidationOwnerLease("orders", a, 0, TTL, time::get, () -> true,
                claim -> { renewCalls.incrementAndGet(); return store.renew(claim, TTL); },
                claim -> staleRelease.set(store.release(claim)), reason -> stopped.incrementAndGet());

        assertThat(lease.require()).isEqualTo(WorkloadClaimFence.from(a));
        assertThat(store.acquire(KEY, B, 7, TTL).acquired()).isFalse();
        store.elapse(TTL.plusMillis(1));
        time.set(TTL.toNanos() + 1);
        lease.checkDeadline();
        var b = acquire(store, B);
        lease.renew();
        assertThatThrownBy(lease::require).isInstanceOfSatisfying(TapstateException.class,
                e -> assertThat(e.code()).isEqualTo(ValidationError.OWNER_NOT_AUTHORIZED));
        lease.close();
        lease.close();

        assertThat(stopped).hasValue(1);
        assertThat(renewCalls).hasValue(0);
        assertThat(staleRelease).isFalse();
        assertThat(store.read(KEY).orElseThrow().claim()).isEqualTo(b);
        assertThat(store.read(production.key()).orElseThrow().leased()).isTrue();
        assertThat(store.renew(a, TTL)).isEmpty();
        assertThat(b.claimGeneration()).isGreaterThan(a.claimGeneration());
        assertThat(b.executionGeneration()).isGreaterThan(a.executionGeneration());
    }

    @Test void localDeadlineEndsWorkEvenWhenNobodyHasTakenOver() {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        AtomicLong time = new AtomicLong();
        AtomicInteger stopped = new AtomicInteger();
        try (var lease = lease(store, a, time, stopped)) {
            time.set(TTL.toNanos());
            lease.checkDeadline();
            assertThat(lease.authorized()).isFalse();
            assertThat(lease.lossReason()).isEqualTo(ValidationOwnerLease.LossReason.EXPIRED);
            assertThat(store.read(KEY).orElseThrow().claim().owner()).isEqualTo(A);
            assertThat(stopped).hasValue(1);
        }
    }

    @Test void successfulRenewalIsTimedFromTheRequestAndDoesNotUseResponseTime() {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        AtomicLong time = new AtomicLong();
        AtomicInteger stopped = new AtomicInteger();
        try (var lease = new ValidationOwnerLease("orders", a, 0, TTL, time::get, () -> true,
                claim -> { time.addAndGet(Duration.ofSeconds(5).toNanos()); return store.renew(claim, TTL); },
                store::release, reason -> stopped.incrementAndGet())) {
            time.set(Duration.ofSeconds(20).toNanos());
            lease.renew();
            time.set(Duration.ofSeconds(49).toNanos());
            assertThat(lease.authorized()).isTrue();
            time.set(Duration.ofSeconds(50).toNanos());
            assertThat(lease.authorized()).isFalse();
            assertThat(stopped).hasValue(1);
        }
    }

    @Test void lateSuccessfulRenewalCannotReviveAnExpiredProof() {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        AtomicLong time = new AtomicLong();
        AtomicInteger stopped = new AtomicInteger();
        try (var lease = new ValidationOwnerLease("orders", a, 0, TTL, time::get, () -> true,
                claim -> { var result = store.renew(claim, TTL); time.set(TTL.toNanos()); return result; },
                store::release, reason -> stopped.incrementAndGet())) {
            lease.renew();
            assertThat(lease.authorized()).isFalse();
            assertThat(lease.lossReason()).isEqualTo(ValidationOwnerLease.LossReason.EXPIRED);
            assertThat(stopped).hasValue(1);
        }
    }

    @Test void blockedRenewalCannotBlockTheIndependentExpiryTask() throws Exception {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        AtomicLong time = new AtomicLong();
        AtomicInteger stopped = new AtomicInteger();
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch unblock = new CountDownLatch(1);
        CountDownLatch lost = new CountDownLatch(1);
        var renewals = Executors.newSingleThreadScheduledExecutor();
        var deadlines = Executors.newSingleThreadScheduledExecutor();
        var lease = new ValidationOwnerLease("orders", a, 0, TTL, time::get, () -> true, claim -> {
            calls.incrementAndGet();
            var accepted = store.renew(claim, TTL);
            entered.countDown();
            boolean done = false;
            while (!done) {
                try { done = unblock.await(3, TimeUnit.SECONDS); }
                catch (InterruptedException ignored) { /* Model a driver that does not cooperate with cancellation. */ }
            }
            return accepted;
        }, store::release, reason -> { stopped.incrementAndGet(); lost.countDown(); });
        try {
            lease.maintain(renewals, deadlines, Duration.ofMillis(1), Duration.ofMillis(1));
            assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
            lease.renew();
            assertThat(calls).hasValue(1);
            time.set(TTL.toNanos());
            assertThat(lost.await(3, TimeUnit.SECONDS)).isTrue();
            assertThat(lease.authorized()).isFalse();
            unblock.countDown();
            renewals.submit(() -> { }).get(3, TimeUnit.SECONDS);
            assertThat(lease.authorized()).isFalse();
            assertThat(stopped).hasValue(1);
        } finally {
            unblock.countDown();
            lease.close();
            renewals.shutdownNow();
            deadlines.shutdownNow();
        }
    }

    @Test void aRenewalForAnotherExecutionIsNeverAdopted() {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        AtomicLong time = new AtomicLong();
        AtomicInteger stopped = new AtomicInteger();
        try (var lease = new ValidationOwnerLease("orders", a, 0, TTL, time::get, () -> true,
                claim -> store.advanceExecution(claim, 7, Set.of(A.nodeId())), store::release,
                reason -> stopped.incrementAndGet())) {
            lease.renew();
            assertThat(lease.authorized()).isFalse();
            assertThat(lease.lossReason()).isEqualTo(ValidationOwnerLease.LossReason.RENEW_REFUSED);
            assertThat(stopped).hasValue(1);
        }
    }

    @Test void lostNodeAuthorityStopsValidationBeforeAnyRenewal() {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        AtomicInteger calls = new AtomicInteger();
        AtomicInteger stopped = new AtomicInteger();
        try (var lease = new ValidationOwnerLease("orders", a, 0, TTL, () -> 0, () -> false,
                claim -> { calls.incrementAndGet(); return Optional.of(claim); }, store::release,
                reason -> stopped.incrementAndGet())) {
            lease.renew();
            assertThat(lease.authorized()).isFalse();
            assertThat(lease.lossReason()).isEqualTo(ValidationOwnerLease.LossReason.NODE_NOT_AUTHORIZED);
            assertThat(calls).hasValue(0);
            assertThat(stopped).hasValue(1);
        }
    }

    @Test void sameFenceWithInvalidRenewalContextImmediatelyEndsAuthorization() {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        AtomicInteger stopped = new AtomicInteger();
        var invalidContext = new WorkloadClaim(a.key(), a.owner(), a.claimGeneration(), a.executionGeneration(),
                a.topologyRevision(), a.leaseUntil(), a.contextExecutionGeneration() + 1,
                a.executionClaimGeneration(), a.executionNodeIds(), 0, false);
        try (var lease = new ValidationOwnerLease("orders", a, 0, TTL, () -> 0, () -> true,
                claim -> Optional.of(invalidContext), store::release, reason -> stopped.incrementAndGet())) {
            assertThatThrownBy(lease::renew).isInstanceOfSatisfying(TapstateException.class,
                    e -> assertThat(e.code()).isEqualTo(ValidationError.INVALID_CONTRACT));
            assertThat(lease.authorized()).isFalse();
            assertThat(stopped).hasValue(1);
            assertThat(lease.lossReason()).isEqualTo(ValidationOwnerLease.LossReason.RENEW_REFUSED);
        }
    }

    @Test void localProofUsesTheSameMillisecondPrecisionAsTheDurableLease() {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        AtomicLong time = new AtomicLong();
        AtomicInteger stopped = new AtomicInteger();
        Duration fractionalTtl = TTL.plusNanos(999999);
        try (var lease = new ValidationOwnerLease("orders", a, 0, fractionalTtl, time::get, () -> true,
                claim -> store.renew(claim, TTL), store::release, reason -> stopped.incrementAndGet())) {
            time.set(TTL.toNanos());
            assertThat(lease.authorized()).isFalse();
            assertThat(stopped).hasValue(1);
        }
    }

    @Test void cleanupFailureCannotReplaceTheInvalidRenewalDiagnosis() {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        var invalidContext = new WorkloadClaim(a.key(), a.owner(), a.claimGeneration(), a.executionGeneration(),
                a.topologyRevision(), a.leaseUntil(), a.contextExecutionGeneration() + 1,
                a.executionClaimGeneration(), a.executionNodeIds(), 0, false);
        var cleanup = new IllegalStateException("cancellation failed");
        try (var lease = new ValidationOwnerLease("orders", a, 0, TTL, () -> 0, () -> true,
                claim -> Optional.of(invalidContext), store::release, reason -> { throw cleanup; })) {
            assertThatThrownBy(lease::renew).isInstanceOfSatisfying(TapstateException.class, failure -> {
                assertThat(failure.code()).isEqualTo(ValidationError.INVALID_CONTRACT);
                assertThat(failure.getSuppressed()).containsExactly(cleanup);
            });
            assertThat(lease.authorized()).isFalse();
        }
    }

    @Test void fatalNodeGuardFailurePermanentlyRevokesTheLease() {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        var first = new AtomicBoolean(true);
        var fatal = new AssertionError("fatal node guard");
        var stopped = new AtomicInteger();
        try (var lease = new ValidationOwnerLease("orders", a, 0, TTL, () -> 0,
                () -> { if (first.getAndSet(false)) throw fatal; return true; },
                claim -> store.renew(claim, TTL), store::release, reason -> stopped.incrementAndGet())) {
            assertThatThrownBy(lease::authorized).isSameAs(fatal);
            assertThat(lease.authorized()).isFalse();
            assertThat(stopped).hasValue(1);
        }
    }

    @Test void fatalRenewalFailurePermanentlyRevokesTheLease() {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        var fatal = new AssertionError("fatal renewal");
        var stopped = new AtomicInteger();
        try (var lease = new ValidationOwnerLease("orders", a, 0, TTL, () -> 0, () -> true,
                claim -> { throw fatal; }, store::release, reason -> stopped.incrementAndGet())) {
            assertThatThrownBy(lease::renew).isSameAs(fatal);
            assertThat(lease.authorized()).isFalse();
            assertThat(stopped).hasValue(1);
        }
    }

    @Test void aNodeGuardDefectKeepsItsCauseWhenCancellationAlsoFails() {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        var defect = new IllegalStateException("node guard failed first");
        var cleanup = new IllegalStateException("later cancellation failure");
        try (var lease = new ValidationOwnerLease("orders", a, 0, TTL, () -> 0,
                () -> { throw defect; }, claim -> store.renew(claim, TTL), store::release, reason -> { throw cleanup; })) {
            assertThatThrownBy(lease::authorized).isSameAs(defect);
            assertThat(defect.getSuppressed()).containsExactly(cleanup);
            assertThat(lease.authorized()).isFalse();
        }
    }

    @Test void fatalCancellationStillAttemptsOneExactClaimRelease() {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        var fatal = new AssertionError("fatal cancellation");
        var releases = new AtomicInteger();
        var lease = new ValidationOwnerLease("orders", a, 0, TTL, () -> 0, () -> true,
                claim -> store.renew(claim, TTL), claim -> { releases.incrementAndGet(); store.release(claim); },
                reason -> { throw fatal; });
        assertThatThrownBy(lease::close).isSameAs(fatal);
        assertThat(releases).hasValue(1);
        var b = acquire(store, B);
        lease.close();
        assertThat(releases).hasValue(1);
        assertThat(store.read(KEY).orElseThrow().claim()).isEqualTo(b);
    }

    @Test void fatalReleaseKeepsTheEarlierCancellationDiagnosis() {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        var ordinary = new IllegalStateException("ordinary cancellation failure");
        var fatal = new AssertionError("fatal release");
        var attempts = new AtomicInteger();
        var lease = new ValidationOwnerLease("orders", a, 0, TTL, () -> 0, () -> true,
                claim -> store.renew(claim, TTL), claim -> { attempts.incrementAndGet(); throw fatal; },
                reason -> { throw ordinary; });
        assertThatThrownBy(lease::close).isSameAs(fatal);
        assertThat(fatal.getSuppressed()).containsExactly(ordinary);
        assertThat(lease.authorized()).isFalse();
        lease.close();
        assertThat(attempts).hasValue(1);
    }

    @Test void monotonicClockWrapDoesNotExtendAuthorization() {
        var store = new InMemoryWorkloadClaimStore();
        var a = acquire(store, A);
        long start = Long.MAX_VALUE - TTL.toNanos() / 2;
        AtomicLong time = new AtomicLong(start);
        AtomicInteger stopped = new AtomicInteger();
        try (var lease = new ValidationOwnerLease("orders", a, start, TTL, time::get, () -> true,
                claim -> store.renew(claim, TTL), store::release, reason -> stopped.incrementAndGet())) {
            time.addAndGet(TTL.toNanos() - 1);
            assertThat(lease.authorized()).isTrue();
            time.incrementAndGet();
            assertThat(lease.authorized()).isFalse();
        }
    }

    private static ValidationOwnerLease lease(InMemoryWorkloadClaimStore store, WorkloadClaim claim,
            AtomicLong time, AtomicInteger stopped) {
        return new ValidationOwnerLease("orders", claim, time.get(), TTL, time::get, () -> true,
                expected -> store.renew(expected, TTL), store::release, reason -> stopped.incrementAndGet());
    }

    private static WorkloadClaim acquire(InMemoryWorkloadClaimStore store, WorkloadOwner owner) {
        var attempt = store.acquire(KEY, owner, 7, TTL);
        assertThat(attempt.acquired()).isTrue();
        return store.advanceExecution(attempt.claim(), 7, Set.of(owner.nodeId())).orElseThrow();
    }
}
