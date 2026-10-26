package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PendingAuthenticationCohortsTest {
    private static final Runnable ACTIVE = () -> {};

    @Test void differentEpochsCannotJoinEvenBeforeEitherValidationStarts() throws Exception {
        try (var cohorts = new PendingAuthenticationCohorts<String>(Duration.ofSeconds(30), 4, 2)) {
            var old = new Object(); var next = new Object();
            var first = task(() -> cohorts.validate(old, "cookie", deadline(), () -> "old", ACTIVE));
            waiting(first.thread());
            var second = task(() -> cohorts.validate(next, "cookie", deadline(), () -> "new", ACTIVE));
            waiting(second.thread());
            assertThat(first.future().isDone()).isFalse();
            assertThat(cohorts.validate(next, "cookie", deadline(), () -> { throw new AssertionError(); }, ACTIVE)).isEqualTo("new");
            assertThat(second.future().get(2, TimeUnit.SECONDS)).isEqualTo("new");
            assertThat(cohorts.validate(old, "cookie", deadline(), () -> { throw new AssertionError(); }, ACTIVE)).isEqualTo("old");
            assertThat(first.future().get(2, TimeUnit.SECONDS)).isEqualTo("old");
        }
    }

    @Test void invalidationWakesOldCallsAndAllowsFreshAdmission() throws Exception {
        try (var cohorts = new PendingAuthenticationCohorts<String>(Duration.ofSeconds(30), 4, 2)) {
            var epoch = new Object();
            var first = task(() -> cohorts.validate(epoch, "cookie", deadline(), () -> { throw new AssertionError(); }, ACTIVE));
            waiting(first.thread()); cohorts.invalidate(); unavailable(first.future());
            var next = task(() -> cohorts.validate(epoch, "cookie", deadline(), () -> "fresh", ACTIVE));
            waiting(next.thread());
            assertThat(cohorts.validate(epoch, "cookie", deadline(), () -> { throw new AssertionError(); }, ACTIVE)).isEqualTo("fresh");
            assertThat(next.future().get(2, TimeUnit.SECONDS)).isEqualTo("fresh");
        }
    }

    @Test void leaderBudgetIncludesGatheringAndValidationTime() throws Exception {
        try (var cohorts = new PendingAuthenticationCohorts<String>(Duration.ofSeconds(30), 4, 2)) {
            var epoch = new Object();
            assertThatThrownBy(() -> cohorts.validate(epoch, "cookie", System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(20),
                    () -> { throw new AssertionError("expired before validation"); }, ACTIVE)).isInstanceOf(TapstateException.class);
        }
        try (var cohorts = new PendingAuthenticationCohorts<String>(Duration.ofSeconds(30), 4, 2)) {
            var epoch = new Object(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var first = task(() -> cohorts.validate(epoch, "cookie", System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(200), () -> {
                entered.countDown(); await(release); return "too late";
            }, ACTIVE));
            waiting(first.thread());
            var second = task(() -> cohorts.validate(epoch, "cookie", deadline(), () -> { throw new AssertionError(); }, ACTIVE));
            try {
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                Thread.sleep(220); release.countDown(); unavailable(first.future()); unavailable(second.future());
            } finally { release.countDown(); }
        }
    }

    @Test void frozenCallersShareValidationButLateCallersSeeRevocation() throws Exception {
        try (var cohorts = new PendingAuthenticationCohorts<String>(Duration.ofSeconds(2), 4, 2)) {
            var epoch = new Object(); var calls = new AtomicInteger();
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            Supplier<String> validation = () -> { calls.incrementAndGet(); entered.countDown(); await(release); return "original"; };
            var first = task(() -> cohorts.validate(epoch, "cookie", deadline(), validation, ACTIVE));
            waiting(first.thread());
            var second = task(() -> cohorts.validate(epoch, "cookie", deadline(), validation, ACTIVE));
            try {
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                // The durable read has begun. Revocation must be visible to a later invocation.
                var late = task(() -> cohorts.validate(epoch, "cookie", deadline(), () -> "revoked", ACTIVE));
                assertThat(late.future().get(3, TimeUnit.SECONDS)).isEqualTo("revoked");
                release.countDown();
                assertThat(first.future().get(2, TimeUnit.SECONDS)).isEqualTo("original");
                assertThat(second.future().get(2, TimeUnit.SECONDS)).isEqualTo("original");
                assertThat(calls).hasValue(1);
            } finally { release.countDown(); }
        }
    }

    @Test void callerCannotBorrowAnotherCallersDeadline() throws Exception {
        try (var cohorts = new PendingAuthenticationCohorts<String>(Duration.ofSeconds(2), 4, 2)) {
            var epoch = new Object(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var leader = task(() -> cohorts.validate(epoch, "cookie", deadline(), () -> {
                entered.countDown(); await(release); return "allowed";
            }, ACTIVE));
            waiting(leader.thread());
            // Represents a request which already spent most of its allowance before admission.
            var follower = task(() -> cohorts.validate(epoch, "cookie", System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100),
                    () -> { throw new AssertionError("follower must not validate"); }, ACTIVE));
            try {
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                unavailable(follower.future());
                release.countDown();
                assertThat(leader.future().get(2, TimeUnit.SECONDS)).isEqualTo("allowed");
            } finally { release.countDown(); }
        }
    }

    @Test void aChangedEpochRejectsTheOldResultAndNeverJoinsTheNewOwner() throws Exception {
        try (var cohorts = new PendingAuthenticationCohorts<String>(Duration.ZERO, 4, 2)) {
            var old = new Object(); var current = new AtomicReference<>(old);
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            Runnable fence = () -> { if (current.get() != old) throw StorageRuntime.unavailable(); };
            var first = task(() -> cohorts.validate(old, "cookie", deadline(), () -> {
                entered.countDown(); await(release); return "old";
            }, fence));
            try {
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue(); current.set(new Object());
                assertThat(cohorts.validate(current.get(), "cookie", deadline(), () -> "new", ACTIVE)).isEqualTo("new");
                release.countDown(); unavailable(first.future());
            } finally { release.countDown(); }
        }
    }

    @Test void credentialEqualityDoesNotUseOnlyItsHash() throws Exception {
        assertThat("Aa".hashCode()).isEqualTo("BB".hashCode());
        try (var cohorts = new PendingAuthenticationCohorts<String>(Duration.ofMillis(100), 4, 2)) {
            var epoch = new Object();
            var first = task(() -> cohorts.validate(epoch, "Aa", deadline(), () -> "first", ACTIVE));
            waiting(first.thread());
            assertThat(cohorts.validate(epoch, "BB", deadline(), () -> "second", ACTIVE)).isEqualTo("second");
            assertThat(first.future().get(2, TimeUnit.SECONDS)).isEqualTo("first");
        }
    }

    @Test void saturationUsesIndependentValidationAndCompletedGroupsAreRemoved() throws Exception {
        try (var cohorts = new PendingAuthenticationCohorts<String>(Duration.ZERO, 1, 2)) {
            var epoch = new Object(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var first = task(() -> cohorts.validate(epoch, "cookie", deadline(), () -> {
                entered.countDown(); await(release); return "original";
            }, ACTIVE));
            try {
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue();
                assertThat(cohorts.validate(epoch, "cookie", deadline(), () -> "fresh", ACTIVE)).isEqualTo("fresh");
                release.countDown(); assertThat(first.future().get(2, TimeUnit.SECONDS)).isEqualTo("original");
                assertThat(cohorts.validate(epoch, "cookie", deadline(), () -> "again", ACTIVE)).isEqualTo("again");
            } finally { release.countDown(); }
        }
    }

    @Test void leaderFailureReachesFollowersAndDoesNotPoisonLaterCalls() throws Exception {
        try (var cohorts = new PendingAuthenticationCohorts<String>(Duration.ofSeconds(2), 4, 2)) {
            var epoch = new Object(); var expected = StorageRuntime.unavailable();
            var first = task(() -> cohorts.validate(epoch, "cookie", deadline(), () -> { throw expected; }, ACTIVE));
            waiting(first.thread());
            var second = task(() -> cohorts.validate(epoch, "cookie", deadline(), () -> { throw new AssertionError(); }, ACTIVE));
            for (var result : java.util.List.of(first, second))
                assertThatThrownBy(() -> result.future().get(2, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class).hasCause(expected);
            assertThat(cohorts.validate(epoch, "cookie", deadline(), () -> "fresh", ACTIVE)).isEqualTo("fresh");
        }
    }

    @Test void followerInterruptionDoesNotCancelTheLeader() throws Exception {
        try (var cohorts = new PendingAuthenticationCohorts<String>(Duration.ofSeconds(2), 4, 2)) {
            var epoch = new Object(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var first = task(() -> cohorts.validate(epoch, "cookie", deadline(), () -> {
                entered.countDown(); await(release); return "allowed";
            }, ACTIVE));
            waiting(first.thread());
            var second = task(() -> cohorts.validate(epoch, "cookie", deadline(), () -> { throw new AssertionError(); }, ACTIVE));
            try {
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue(); waiting(second.thread()); second.thread().interrupt();
                unavailable(second.future()); release.countDown();
                assertThat(first.future().get(2, TimeUnit.SECONDS)).isEqualTo("allowed");
            } finally { release.countDown(); }
        }
    }

    @Test void closeWakesWaitingFollowersAndRejectsTheInFlightResult() throws Exception {
        try (var cohorts = new PendingAuthenticationCohorts<String>(Duration.ofSeconds(2), 4, 2)) {
            var epoch = new Object(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var first = task(() -> cohorts.validate(epoch, "cookie", deadline(), () -> {
                entered.countDown(); await(release); return "old";
            }, ACTIVE));
            waiting(first.thread());
            var second = task(() -> cohorts.validate(epoch, "cookie", deadline(), () -> { throw new AssertionError(); }, ACTIVE));
            try {
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue(); cohorts.close(); unavailable(second.future());
                release.countDown(); unavailable(first.future());
                assertThatThrownBy(() -> cohorts.validate(epoch, "cookie", deadline(), () -> "bad", ACTIVE))
                        .isInstanceOf(TapstateException.class);
            } finally { release.countDown(); }
        }
    }

    @Test void interruptedLeaderCannotPublishSuccess() throws Exception {
        try (var cohorts = new PendingAuthenticationCohorts<String>(Duration.ofSeconds(2), 4, 2)) {
            var epoch = new Object(); var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var first = task(() -> cohorts.validate(epoch, "cookie", deadline(), () -> {
                entered.countDown(); await(release); return "bad";
            }, ACTIVE));
            waiting(first.thread());
            var second = task(() -> cohorts.validate(epoch, "cookie", deadline(), () -> { throw new AssertionError(); }, ACTIVE));
            try {
                assertThat(entered.await(2, TimeUnit.SECONDS)).isTrue(); first.thread().interrupt();
                unavailable(first.future()); unavailable(second.future());
            } finally { release.countDown(); }
        }
    }

    private record Running<T>(Thread thread, FutureTask<T> future) {}
    private static <T> Running<T> task(Callable<T> work) {
        var result = new FutureTask<>(work);
        return new Running<>(Thread.ofPlatform().daemon().start(result), result);
    }
    private static long deadline() { return System.nanoTime() + TimeUnit.SECONDS.toNanos(5); }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("test latch expired"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw StorageRuntime.unavailable(); }
    }
    private static void waiting(Thread thread) throws InterruptedException {
        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (thread.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < end) Thread.sleep(1);
        assertThat(thread.getState()).isEqualTo(Thread.State.TIMED_WAITING);
    }
    private static void unavailable(FutureTask<?> result) {
        assertThatThrownBy(() -> result.get(2, TimeUnit.SECONDS)).isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(TapstateException.class);
    }
}
