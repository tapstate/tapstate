package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.*;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationRootWorkTest {
    @Test void successfulResultTransfersOnlyAfterActualExitAndReservationRelease() throws Exception {
        var released = new AtomicInteger();
        var releaseEntered = new CountDownLatch(1);
        var allowRelease = new CountDownLatch(1);
        try (var executor = executor()) {
            var work = submit(executor, (observation, guard) -> result(observation), () -> {}, () -> {}, () -> {
                releaseEntered.countDown(); await(allowRelease); released.incrementAndGet();
            });
            await(releaseEntered);
            assertThat(work.hasExited()).isFalse();
            assertThat(work.takeResult()).isEmpty();
            allowRelease.countDown();
            executor.shutdown(); assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(released).hasValue(1);
            assertThat(work.hasExited()).isTrue();
            assertThat(work.takeResult()).hasValueSatisfying(root -> assertThat(root.observation()).isEqualTo(observation(1)));
            assertThat(work.takeResult()).isEmpty();
        } finally { allowRelease.countDown(); }
    }

    @Test void cancelledNonCooperativeWorkKeepsItsReservationUntilTheBodyReallyReturns() throws Exception {
        var entered = new CountDownLatch(1);
        var unblock = new CountDownLatch(1);
        var released = new AtomicInteger();
        var cancelRequests = new AtomicInteger();
        try (var executor = executor()) {
            var work = submit(executor, (observation, guard) -> { entered.countDown(); await(unblock); return result(observation); },
                    () -> {}, cancelRequests::incrementAndGet, released::incrementAndGet);
            await(entered);
            var cancelled = failure(ValidationError.EXECUTION_CANCELLED);
            work.cancel(cancelled); work.cancel(cancelled);
            assertThat(cancelRequests).hasValue(1);
            assertThat(work.hasExited()).isFalse();
            assertThat(released).hasValue(0);
            assertThat(catchThrowable(work::takeResult)).isSameAs(cancelled);
            unblock.countDown();
            executor.shutdown(); assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(work.hasExited()).isTrue();
            assertThat(released).hasValue(1);
            assertThat(catchThrowable(work::takeResult)).isSameAs(cancelled);
        } finally { unblock.countDown(); }
    }

    @Test void queuedCancellationDoesNotPretendTheExecutorHasRemovedItsRetainedWork() throws Exception {
        var occupied = new CountDownLatch(1);
        var unblock = new CountDownLatch(1);
        var released = new AtomicInteger();
        var checks = new AtomicInteger();
        try (var executor = executor()) {
            executor.execute(() -> { occupied.countDown(); await(unblock); });
            await(occupied);
            var work = submit(executor, (observation, guard) -> { checks.incrementAndGet(); return result(observation); },
                    () -> {}, () -> {}, released::incrementAndGet);
            work.cancel(failure(ValidationError.EXECUTION_CANCELLED));
            assertThat(work.hasExited()).isFalse();
            assertThat(released).hasValue(0);
            unblock.countDown();
            executor.shutdown(); assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(checks).hasValue(0);
            assertThat(released).hasValue(1);
        } finally { unblock.countDown(); }
    }

    @Test void rejectedSubmissionReleasesOnceWithoutRunningTheChecker() {
        var released = new AtomicInteger();
        var calls = new AtomicInteger();
        try (var executor = executor()) {
            executor.shutdown();
            assertThatThrownBy(() -> submit(executor, (observation, guard) -> { calls.incrementAndGet(); return result(observation); },
                    () -> {}, () -> {}, released::incrementAndGet)).isInstanceOfSatisfying(TapstateException.class,
                            error -> assertThat(error.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
            assertThat(released).hasValue(1);
            assertThat(calls).hasValue(0);
        }
    }

    @Test void inlineExecutorsCannotStartBlockingChecksOnTheSubmittingThread() {
        var calls = new AtomicInteger();
        var released = new AtomicInteger();
        var work = ValidationRootWork.submit(observation(1), Runnable::run, (observation, guard) -> {
            calls.incrementAndGet(); return result(observation);
        }, () -> {}, () -> {}, released::incrementAndGet, 1024 * 1024);
        assertThatThrownBy(work::takeResult).isInstanceOfSatisfying(TapstateException.class,
                error -> assertThat(error.args()).containsEntry("field", "rootWork.inlineExecutor"));
        assertThat(calls).hasValue(0);
        assertThat(released).hasValue(1);
    }

    @Test void staleResultsAndOversizedResultsCannotBeTransferred() throws Exception {
        for (boolean stale : List.of(false, true)) {
            var released = new AtomicInteger();
            try (var executor = executor()) {
                var work = ValidationRootWork.submit(observation(1), executor,
                        (observation, guard) -> result(stale ? observation(2) : observation), () -> {}, () -> {}, released::incrementAndGet,
                        stale ? 1024 * 1024 : 1);
                executor.shutdown(); assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                assertThatThrownBy(work::takeResult).isInstanceOfSatisfying(TapstateException.class,
                        error -> assertThat(error.code()).isEqualTo(stale ? ValidationError.INVALID_CONTRACT : ValidationError.VALUE_LIMIT_EXCEEDED));
                assertThat(released).hasValue(1);
            }
        }
    }

    @Test void authorityIsCheckedBeforeSubmissionAndAgainAfterTheCheckerReturns() throws Exception {
        var released = new AtomicInteger();
        var lost = failure(ValidationError.OWNER_NOT_AUTHORIZED);
        try (var executor = executor()) {
            assertThat(catchThrowable(() -> submit(executor, (observation, guard) -> result(observation), () -> { throw lost; },
                    () -> {}, released::incrementAndGet))).isSameAs(lost);
            assertThat(released).hasValue(1);
        }
        var authority = new java.util.concurrent.atomic.AtomicBoolean(true);
        try (var executor = executor()) {
            var work = submit(executor, (observation, guard) -> { authority.set(false); return result(observation); },
                    () -> { if (!authority.get()) throw lost; }, () -> {}, released::incrementAndGet);
            executor.shutdown(); assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(catchThrowable(work::takeResult)).isSameAs(lost);
            assertThat(released).hasValue(2);
        }
    }

    @Test void firstOrdinaryFailureSurvivesCleanupWhileFatalCleanupEscapesTheWorker() throws Exception {
        for (boolean fatal : List.of(false, true)) {
            var first = failure(ValidationError.READ_FAILED);
            var later = fatal ? new AssertionError("fatal release") : failure(ValidationError.READ_CANCELLED);
            var uncaught = new AtomicReference<Throwable>();
            var escaped = new CountDownLatch(1);
            try (var executor = executor()) {
                executor.setThreadFactory(runnable -> {
                    var thread = new Thread(runnable, "validation-test-io");
                    thread.setUncaughtExceptionHandler((ignored, error) -> { uncaught.set(error); escaped.countDown(); });
                    return thread;
                });
                var work = submit(executor, (observation, guard) -> { throw first; }, () -> {}, () -> {}, () -> { raise(later); });
                executor.shutdown(); assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                assertThat(catchThrowable(work::takeResult)).isSameAs(fatal ? later : first);
                if (fatal) { await(escaped); assertThat(uncaught).hasValue(later); }
                assertThat(List.of((fatal ? later : first).getSuppressed())).contains(fatal ? first : later);
            }
        }
    }

    @Test void checkedCancellationCausesCannotBeMistakenForPendingOrSuccessfulWork() throws Exception {
        var entered = new CountDownLatch(1);
        var unblock = new CountDownLatch(1);
        try (var executor = executor()) {
            var work = submit(executor, (observation, guard) -> { entered.countDown(); await(unblock); return result(observation); },
                    () -> {}, () -> {}, () -> {});
            await(entered);
            var reason = new java.io.IOException("external cancellation reason");
            work.cancel(reason);
            unblock.countDown();
            executor.shutdown(); assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(work::takeResult).isInstanceOfSatisfying(TapstateException.class, error -> {
                assertThat(error.code()).isEqualTo(ValidationError.EXECUTION_CANCELLED);
                assertThat(error.getCause()).isSameAs(reason);
            });
        } finally { unblock.countDown(); }
    }

    private static ValidationRootWork submit(ThreadPoolExecutor executor, ValidationRootWork.Checker checker,
            Runnable authorized, Runnable cancel, Runnable exited) {
        return ValidationRootWork.submit(observation(1), executor, checker, authorized, cancel, exited, 1024 * 1024);
    }
    private static ThreadPoolExecutor executor() {
        return new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1), new ThreadPoolExecutor.AbortPolicy());
    }
    static ValidationObservation observation(long revision) {
        return new ValidationObservation(new ValidationRunIdentity(new ValidationOwner("cluster", "pipeline", "node", "boot", 1, 1, 0),
                "task", "run", ValidationMode.FULL, "spec", 1, "rules"),
                new ValidationRoot("mapping", "output", new ValidationKey(List.of(ValidationKey.Part.number(7))), List.of()), revision);
    }
    static ValidationRootCheck result(ValidationObservation observation) {
        try (var root = ValidationRootCheck.builder(observation, ValidationMethod.FULL_COLUMN, ValidationCheckEvent.Participation.SHOULD_NOT_EXIST,
                ValidationRootCheck.TargetPresence.ABSENT, new ValidationRootCheck.Limits(32768, 10, 10, 10, 0))) {
            root.read(ValidationRootCheck.Side.SOURCE, new ValidationCheckEvent.ReadBoundary("source", "complete", Instant.EPOCH, Instant.EPOCH));
            root.read(ValidationRootCheck.Side.TARGET, new ValidationCheckEvent.ReadBoundary("target", "complete", Instant.EPOCH, Instant.EPOCH));
            root.checked(ValidationRootCheck.Check.IDENTITY).checked(ValidationRootCheck.Check.PARTICIPATION)
                    .notApplicable(ValidationRootCheck.Check.STRUCTURE).notApplicable(ValidationRootCheck.Check.VALUES);
            return root.finish();
        }
    }
    private static TapstateException failure(ValidationError code) { return new TapstateException(code, Map.of(), null); }
    private static void raise(Throwable error) { if (error instanceof Error fatal) throw fatal; if (error instanceof RuntimeException failure) throw failure; }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(5, TimeUnit.SECONDS)) throw new AssertionError("fixture latch timed out"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }
}
