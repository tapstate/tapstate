package io.tapstate.runtime.engine.validation;

import static org.assertj.core.api.Assertions.assertThatCode;

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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ValidationCandidateQueueTest {
    private static final Limits LIMITS = new Limits(4, 65536, 4, 2, 10, 100, 10);
    @Test void theDefaultRetryCadenceDoesNotDependOnTheUsersInitialDelay() {
        assertThat(new Limits(4,65536,4,5,0,100).retryDelayNanos()).isEqualTo(100);
        assertThat(new Limits(4,65536,4,5,1000,100).retryDelayNanos()).isEqualTo(100);
    }
    @Test void anInitialWaitMayExceedTheConfirmationWindowWithoutExhaustingIt() {
        var clock=new AtomicLong();
        assertThatCode(()->{
            try(var queue=queue(clock,new Limits(4,65536,4,5,1000,100,7))){
                queue.tryOffer(List.of(root(1)));clock.set(999);assertThat(queue.poll()).isEmpty();
                clock.set(1000);var first=queue.poll().orElseThrow();assertThat(first.windowExhausted()).isFalse();
                queue.settle(first,Completion.RETRY);clock.set(1100);var exhausted=queue.poll().orElseThrow();
                assertThat(exhausted.windowExhausted()).isTrue();queue.settle(exhausted,Completion.SETTLED);
            }
        }).doesNotThrowAnyException();
    }
    @Test void aHotDelayMayExceedTheFixedConfirmationWindow() {
        var clock=new AtomicLong();
        try(var queue=queue(clock,LIMITS)){
            assertThatCode(()->queue.applyDelay(new DelayChange(run(),1,UUID.randomUUID(),1000))).doesNotThrowAnyException();
            queue.tryOffer(List.of(root(1)));clock.set(999);assertThat(queue.poll()).isEmpty();
            clock.set(1000);var first=queue.poll().orElseThrow();assertThat(first.windowExhausted()).isFalse();
            queue.settle(first,Completion.RETRY);clock.set(1100);var exhausted=queue.poll().orElseThrow();
            assertThat(exhausted.windowExhausted()).isTrue();queue.settle(exhausted,Completion.SETTLED);
        }
    }

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
            clock.set(10);var first=queue.poll().orElseThrow();queue.settle(first,Completion.RETRY);
            clock.set(99); queue.tryOffer(List.of(root(1)));
            clock.set(110);
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

    @Test void applyingDelayChangesOnlyWaitsCreatedAfterTheApplication() {
        var clock = new AtomicLong();
        try (var queue = queue(clock, LIMITS)) {
            queue.tryOffer(List.of(root(1))); var before = queue.snapshot();
            clock.set(1);
            var change = new DelayChange(run(), 1, UUID.randomUUID(), 30);
            assertThat(queue.applyDelay(change)).isEqualTo(DelayApplication.APPLIED);
            assertThat(queue.applyDelay(change)).isEqualTo(DelayApplication.ALREADY_APPLIED);
            assertThat(queue.snapshot()).isEqualTo(before);
            queue.tryOffer(List.of(root(1), root(2)));
            clock.set(10); var first = queue.poll().orElseThrow();
            assertThat(first.observation().root()).isEqualTo(root(1));
            assertThat(queue.settle(first, Completion.RETRY)).isEqualTo(Settlement.RETRY_SCHEDULED);
            clock.set(19); assertThat(queue.poll()).isEmpty();
            clock.set(20);
            assertThat(queue.snapshot().leased()).isZero();
            var due = queue.poll(); assertThat(due.map(lease -> lease.observation().root())).contains(root(1));
            var retried = due.orElseThrow();
            assertThat(retried.observation()).isEqualTo(first.observation()); queue.settle(retried, Completion.SETTLED);
            clock.set(30); assertThat(queue.poll()).isEmpty();
            clock.set(31); var second = queue.poll().orElseThrow();
            assertThat(second.observation().root()).isEqualTo(root(2)); queue.settle(second, Completion.SETTLED);
            assertThat(queue.delayState()).isEqualTo(new DelayState(run(), 1, change.commandId(), 30, false));
        }
    }

    @Test void theInitialSavedWaitIsIndependentOfTheFixedConfirmationRetryCadence() {
        var clock = new AtomicLong();
        try (var queue = queue(clock, new Limits(4, 65536, 4, 5, 30, 100, 7))) {
            queue.tryOffer(List.of(root(1))); clock.set(29); assertThat(queue.poll()).isEmpty();
            clock.set(30); var first = queue.poll().orElseThrow();
            queue.applyDelay(new DelayChange(run(), 1, UUID.randomUUID(), 50));
            queue.settle(first, Completion.RETRY); queue.tryOffer(List.of(root(2)));
            clock.set(36); assertThat(queue.poll()).isEmpty(); clock.set(37);
            var retry = queue.poll().orElseThrow(); assertThat(retry.observation().root()).isEqualTo(root(1)); queue.settle(retry, Completion.SETTLED);
            clock.set(79); assertThat(queue.poll()).isEmpty(); clock.set(80);
            var later = queue.poll().orElseThrow(); assertThat(later.observation().root()).isEqualTo(root(2)); queue.settle(later, Completion.SETTLED);
        }
    }

    @Test void delayApplicationsCannotCrossRunIdentityRevisionOrClosure() {
        var clock = new AtomicLong();
        try (var queue = queue(clock, LIMITS)) {
            var current = new DelayChange(run(), 2, UUID.randomUUID(), 30);
            assertThat(queue.applyDelay(current)).isEqualTo(DelayApplication.APPLIED);
            for (var stale : List.of(new DelayChange(run(), 1, UUID.randomUUID(), 10),
                    new DelayChange(run(), 2, current.commandId(), 20), new DelayChange(run(), 2, UUID.randomUUID(), 30)))
                assertThat(queue.applyDelay(stale)).isEqualTo(DelayApplication.STALE);
            var state = queue.delayState();
            var old = new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "old-boot", 1, 1, 0),
                    "task", "run", ValidationMode.INCREMENTAL, "spec", 1, "rules");
            assertThatThrownBy(() -> queue.applyDelay(new DelayChange(old, 3, UUID.randomUUID(), 10))).isInstanceOf(TapstateException.class);
            assertThatThrownBy(() -> new DelayChange(run(), 3, UUID.randomUUID(), 86_400_000_000_001L)).isInstanceOf(TapstateException.class);
            assertThat(queue.delayState()).isEqualTo(state);
            queue.close();
            assertThat(queue.applyDelay(new DelayChange(run(), 3, UUID.randomUUID(), 10))).isEqualTo(DelayApplication.CLOSED);
            assertThat(queue.delayState()).isEqualTo(new DelayState(run(), 2, current.commandId(), 30, true));
        }
    }

    @Test void aNewDelayCannotExtendTheOriginalConfirmationWindow() {
        var clock = new AtomicLong(); var limits = new Limits(4, 65536, 4, 5, 10, 100);
        try (var queue = queue(clock, limits)) {
            queue.tryOffer(List.of(root(1))); clock.set(10); var checking = queue.poll().orElseThrow();
            clock.set(90); queue.applyDelay(new DelayChange(run(), 1, UUID.randomUUID(), 50));
            assertThat(queue.settle(checking, Completion.RETRY)).isEqualTo(Settlement.RETRY_SCHEDULED);
            clock.set(99); assertThat(queue.poll()).isEmpty();
            clock.set(110); var exhausted = queue.poll().orElseThrow();
            assertThat(exhausted.windowExhausted()).isTrue();
            assertThatThrownBy(() -> queue.settle(exhausted, Completion.RETRY)).isInstanceOf(TapstateException.class);
            queue.settle(exhausted, Completion.SETTLED);
        }
    }

    @Test void aNewDelayCannotResetTheExhaustedConfirmationAttemptBudget() {
        var clock = new AtomicLong();
        try (var queue = queue(clock, LIMITS)) {
            queue.tryOffer(List.of(root(1))); clock.set(10); var first = queue.poll().orElseThrow();
            queue.settle(first, Completion.RETRY); clock.set(20); var second = queue.poll().orElseThrow();
            queue.settle(second, Completion.RETRY);
            queue.applyDelay(new DelayChange(run(), 1, UUID.randomUUID(), 50));
            var exhausted = queue.poll().orElseThrow(); assertThat(exhausted.windowExhausted()).isTrue();
            assertThatThrownBy(() -> queue.settle(exhausted, Completion.RETRY)).isInstanceOf(TapstateException.class);
            queue.settle(exhausted, Completion.SETTLED);
        }
    }

    @Test void concurrentDelayApplicationsKeepTheNewestSchedulingRevision() throws Exception {
        var clock = new AtomicLong();
        try (var queue = queue(clock, LIMITS); var workers = Executors.newFixedThreadPool(2)) {
            var old = new DelayChange(run(), 1, UUID.randomUUID(), 20); var latest = new DelayChange(run(), 2, UUID.randomUUID(), 30);
            var gate = new CountDownLatch(1);
            var a = workers.submit(() -> { gate.await(); return queue.applyDelay(old); });
            var b = workers.submit(() -> { gate.await(); return queue.applyDelay(latest); }); gate.countDown();
            assertThat(a.get(5, TimeUnit.SECONDS)).isIn(DelayApplication.APPLIED, DelayApplication.STALE);
            assertThat(b.get(5, TimeUnit.SECONDS)).isEqualTo(DelayApplication.APPLIED);
            assertThat(queue.delayState()).isEqualTo(new DelayState(run(), 2, latest.commandId(), 30, false));
            queue.tryOffer(List.of(root(1))); clock.set(29); assertThat(queue.poll()).isEmpty();
            clock.set(30); var ready = queue.poll().orElseThrow(); queue.settle(ready, Completion.SETTLED);
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
