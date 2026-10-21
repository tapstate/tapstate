package io.tapstate.runtime.engine.validation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static io.tapstate.runtime.engine.validation.ValidationCandidateQueue.*;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationKey;
import io.tapstate.core.validation.ValidationMode;
import io.tapstate.core.validation.ValidationOwner;
import io.tapstate.core.validation.ValidationRoot;
import io.tapstate.core.validation.ValidationRunIdentity;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ValidationCandidateQueueTest {
    private static final Limits LIMITS = new Limits(4, 65536, 4, 2, 10, 100);

    @Test
    void oldAndNewRootsEnterTogetherAndBatchDuplicatesConsumeOneSlot() {
        var clock = new AtomicLong();
        try (var queue = queue(clock, LIMITS)) {
            assertThat(queue.tryOffer(List.of(root(1), root(2), root(1)))).isEqualTo(Offer.ACCEPTED);
            assertThat(queue.snapshot().roots()).isEqualTo(2);
            clock.set(10);
            var first = queue.poll().orElseThrow(); var second = queue.poll().orElseThrow();
            assertThat(first.observation().root()).isEqualTo(root(1));
            assertThat(second.observation().root()).isEqualTo(root(2));
            assertThat(first.observation().revision()).isEqualTo(second.observation().revision());
            assertThat(first.observation().run()).isEqualTo(run());
            assertThat(queue.poll()).isEmpty();
            queue.settle(first, Completion.SETTLED); queue.settle(second, Completion.SETTLED);
            assertThat(queue.snapshot().roots()).isZero();
        }
    }

    @Test
    void fullAtomicBatchDoesNotEvenDirtyAnExistingRoot() {
        var clock = new AtomicLong();
        try (var queue = queue(clock, new Limits(1, 65536, 4, 2, 0, 100))) {
            queue.tryOffer(List.of(root(1)));
            var lease = queue.poll().orElseThrow();
            var before = queue.snapshot();
            assertThat(queue.tryOffer(List.of(root(1), root(2)))).isEqualTo(Offer.FULL);
            assertThat(queue.snapshot()).isEqualTo(before);
            assertThat(queue.settle(lease, Completion.SETTLED)).isEqualTo(Settlement.REMOVED);
        }
    }

    @Test
    void byteCapacityRejectsTheWholeMovedScope() {
        var clock = new AtomicLong();
        try (var queue = queue(clock, new Limits(4, 1000, 4, 2, 0, 100))) {
            assertThat(queue.tryOffer(List.of(root(1), root(2)))).isEqualTo(Offer.FULL);
            assertThat(queue.snapshot().roots()).isZero();
            assertThat(queue.snapshot().bytes()).isZero();
            assertThat(queue.snapshot().revision()).isZero();
            assertThat(queue.tryOffer(List.of(root(1)))).isEqualTo(Offer.ACCEPTED);
        }
    }

    @Test
    void oldCompletionCannotClearANewerDirtyRevision() {
        var clock = new AtomicLong();
        try (var queue = queue(clock, LIMITS)) {
            queue.tryOffer(List.of(root(1))); clock.set(10);
            var old = queue.poll().orElseThrow();
            queue.tryOffer(List.of(root(1)));
            assertThat(queue.settle(old, Completion.SETTLED)).isEqualTo(Settlement.SUPERSEDED);
            var current = queue.poll().orElseThrow();
            assertThat(current.observation().revision()).isGreaterThan(old.observation().revision());
            assertThat(queue.settle(old, Completion.SETTLED)).isEqualTo(Settlement.ALREADY_SETTLED);
            assertThat(queue.snapshot().leased()).isEqualTo(1);
            queue.settle(current, Completion.SETTLED);
        }
    }

    @Test
    void removedAndRecreatedRootsNeverReuseAnObservationRevision() {
        var clock = new AtomicLong();
        try (var queue = queue(clock, LIMITS)) {
            queue.tryOffer(List.of(root(1))); clock.set(10);
            var old = queue.poll().orElseThrow(); queue.settle(old, Completion.SETTLED);
            queue.tryOffer(List.of(root(1))); clock.set(20);
            var current = queue.poll().orElseThrow();
            assertThat(current.observation().revision()).isGreaterThan(old.observation().revision());
            assertThat(queue.settle(old, Completion.SETTLED)).isEqualTo(Settlement.ALREADY_SETTLED);
            assertThat(queue.snapshot().roots()).isEqualTo(1);
            queue.settle(current, Completion.SETTLED);
        }
    }

    @Test
    void retryBudgetProducesAnExplicitExhaustedLeaseAndCannotAuthorizeMoreChecks() {
        var clock = new AtomicLong();
        try (var queue = queue(clock, LIMITS)) {
            queue.tryOffer(List.of(root(1)));
            clock.set(9); assertThat(queue.poll()).isEmpty();
            for (long now : List.of(10L, 20L)) {
                clock.set(now);
                var check = queue.poll().orElseThrow();
                assertThat(check.windowExhausted()).isFalse();
                assertThat(queue.settle(check, Completion.RETRY)).isEqualTo(Settlement.RETRY_SCHEDULED);
            }
            var exhausted = queue.poll().orElseThrow();
            assertThat(exhausted.windowExhausted()).isTrue();
            assertThatThrownBy(() -> queue.settle(exhausted, Completion.RETRY))
                    .isInstanceOf(TapstateException.class).hasMessageContaining("candidate.exhaustedRetry");
            assertThat(queue.snapshot().leased()).isEqualTo(1);
            assertThat(queue.settle(exhausted, Completion.SETTLED)).isEqualTo(Settlement.REMOVED);
        }
    }

    @Test
    void continuousDirtyHintsCannotResetTheTotalWindow() {
        var clock = new AtomicLong();
        try (var queue = queue(clock, LIMITS)) {
            queue.tryOffer(List.of(root(1)));
            clock.set(99); queue.tryOffer(List.of(root(1)));
            clock.set(100);
            var exhausted = queue.poll().orElseThrow();
            assertThat(exhausted.windowExhausted()).isTrue();
            queue.settle(exhausted, Completion.SETTLED);
        }
    }

    @Test
    void retryAndSupersessionMoveHotRootsBehindOtherDueWork() {
        for (boolean dirty : List.of(false, true)) {
            var clock = new AtomicLong();
            try (var queue = queue(clock, new Limits(4, 65536, 4, 2, 0, 100))) {
                queue.tryOffer(List.of(root(1), root(2)));
                var hot = queue.poll().orElseThrow();
                if (dirty) queue.tryOffer(List.of(root(1)));
                queue.settle(hot, Completion.RETRY);
                var next = queue.poll().orElseThrow();
                assertThat(next.observation().root()).isEqualTo(root(2));
                queue.settle(next, Completion.SETTLED);
            }
        }
    }

    @Test
    void closeRetainsLeasedIdentityBytesUntilTheLeaseReturns() {
        var clock = new AtomicLong();
        var queue = queue(clock, LIMITS);
        queue.tryOffer(List.of(root(1), root(2))); clock.set(10);
        var lease = queue.poll().orElseThrow();
        queue.close();
        assertThat(queue.snapshot().roots()).isEqualTo(1);
        assertThat(queue.snapshot().bytes()).isPositive();
        assertThat(queue.drained().toCompletableFuture().isDone()).isFalse();
        assertThat(queue.poll()).isEmpty();
        assertThat(queue.tryOffer(List.of(root(3)))).isEqualTo(Offer.CLOSED);
        assertThat(queue.settle(lease, Completion.RETRY)).isEqualTo(Settlement.CLOSED);
        assertThat(queue.snapshot().bytes()).isZero();
        assertThat(queue.drained().toCompletableFuture().isDone()).isTrue();
    }

    @Test
    void foreignLeasesAndNonRootScopesCannotMutateTheQueue() {
        var clock = new AtomicLong(10);
        try (var first = queue(clock, new Limits(4, 65536, 4, 2, 0, 100)); var second = queue(clock, LIMITS)) {
            first.tryOffer(List.of(root(1)));
            var lease = first.poll().orElseThrow();
            assertThatThrownBy(() -> second.settle(lease, Completion.SETTLED))
                    .isInstanceOf(TapstateException.class).hasMessageContaining("candidate.foreignLease");
            var descendant = new ValidationRoot("mapping", "output", root(1).rootKey(),
                    List.of(new ValidationRoot.Ancestor(List.of("items"), root(2).rootKey())));
            assertThatThrownBy(() -> second.tryOffer(List.of(root(1), descendant)))
                    .isInstanceOf(TapstateException.class).hasMessageContaining("candidate.wholeRoot");
            assertThat(second.snapshot().roots()).isZero();
            first.settle(lease, Completion.SETTLED);
        }
    }

    @Test
    void producerReturnsBusyWithoutWaitingForAConsumerCriticalSection() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        var clockCalls = new AtomicLong();
        try (var worker = Executors.newSingleThreadExecutor();
             var queue = new ValidationCandidateQueue(run(), LIMITS, () -> {
                 if (clockCalls.incrementAndGet() == 2) {
                     entered.countDown();
                     try { if (!release.await(2, TimeUnit.SECONDS)) throw new AssertionError("release timeout"); }
                     catch (InterruptedException interrupted) { throw new AssertionError(interrupted); }
                 }
                 return 10;
             })) {
            queue.tryOffer(List.of(root(1)));
            var pending = worker.submit(queue::poll);
            try {
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                assertThat(queue.tryOffer(List.of(root(2)))).isEqualTo(Offer.BUSY);
            } finally { release.countDown(); }
            assertThat(pending.get(2, TimeUnit.SECONDS)).isEmpty();
            assertThat(queue.snapshot().roots()).isEqualTo(1);
        }
    }

    @Test
    void completedOldRootWorkCannotSettleALaterHint() throws Exception {
        var clock = new AtomicLong();
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        try (var worker = new java.util.concurrent.ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
                    new java.util.concurrent.ArrayBlockingQueue<>(1), new java.util.concurrent.ThreadPoolExecutor.AbortPolicy());
             var queue = queue(clock, LIMITS)) {
            queue.tryOffer(List.of(root(1))); clock.set(10);
            var lease = queue.poll().orElseThrow();
            var work = ValidationRootWork.submit(lease.observation(), worker, (observation, active) -> {
                entered.countDown();
                try { if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("release timeout"); }
                catch (InterruptedException interrupted) { throw new AssertionError(interrupted); }
                active.run();
                return ValidationRootWorkTest.result(observation);
            }, () -> {}, () -> {}, () -> {}, 32768);
            try {
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                queue.tryOffer(List.of(root(1)));
            } finally { release.countDown(); }
            worker.shutdown(); assertThat(worker.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            var result = work.takeResult().orElseThrow();
            assertThat(result.observation()).isEqualTo(lease.observation());
            assertThat(result.observedAgreement()).isTrue();
            assertThat(queue.settle(lease, Completion.SETTLED)).isEqualTo(Settlement.SUPERSEDED);
            var latest = queue.poll().orElseThrow();
            assertThat(latest.observation().revision()).isGreaterThan(result.observation().revision());
            queue.settle(latest, Completion.SETTLED);
        } finally { release.countDown(); }
    }

    @Test
    void drainCallbacksRunAfterBothCloseAndFinalSettlementReleaseTheLock() throws Exception {
        for (boolean leased : List.of(false, true)) {
            var clock = new AtomicLong();
            try (var worker = Executors.newSingleThreadExecutor();
                 var queue = queue(clock, new Limits(4, 65536, 4, 2, 0, 100))) {
                queue.tryOffer(List.of(root(1)));
                var lease = leased ? queue.poll().orElseThrow() : null;
                var callback = queue.drained().thenApply(ignored -> {
                    try {
                        return worker.submit(() -> queue.tryOffer(List.of(root(2)))).get(5, TimeUnit.SECONDS);
                    } catch (Exception failure) { throw new AssertionError(failure); }
                });
                queue.close();
                if (leased) queue.settle(lease, Completion.SETTLED);
                assertThat(callback.toCompletableFuture().get(5, TimeUnit.SECONDS)).isEqualTo(Offer.CLOSED);
            }
        }
    }

    @Test
    void monotonicTimerWrapDoesNotResetOrPrematurelyExhaustTheWindow() {
        var clock = new AtomicLong(Long.MAX_VALUE - 5);
        try (var queue = queue(clock, LIMITS)) {
            queue.tryOffer(List.of(root(1)));
            clock.set(Long.MIN_VALUE + 3); assertThat(queue.poll()).isEmpty();
            clock.set(Long.MIN_VALUE + 4);
            var lease = queue.poll().orElseThrow();
            assertThat(lease.windowExhausted()).isFalse();
            queue.settle(lease, Completion.SETTLED);
        }
    }

    private static ValidationCandidateQueue queue(AtomicLong clock, Limits limits) {
        return new ValidationCandidateQueue(run(), limits, clock::get);
    }
    private static ValidationRoot root(long key) {
        return new ValidationRoot("mapping", "output", new ValidationKey(List.of(ValidationKey.Part.number(key))), List.of());
    }
    private static ValidationRunIdentity run() {
        return new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.INCREMENTAL, "spec", 1, "rules");
    }
}
