package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationObservation;
import io.tapstate.core.validation.ValidationValues;
import io.tapstate.spi.store.ValidationReadHandle;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class ValidationScopedReadTest {
    @Test void requestingCloseDoesNotBlockTheCallerOrPretendWorkersAndRowsHaveExited() throws Exception {
        var raw = new Read();
        raw.closeEntered = new CountDownLatch(1); raw.allowClose = new CountDownLatch(1);
        try (var cleanup = executor(); var read = new ValidationScopedRead(observation(1), raw)) {
            var requested = read.requestClose(observation(1), cleanup);
            await(raw.closeEntered);
            assertThat(requested.toCompletableFuture().isDone()).isFalse();
            assertThat(read.workersExited().toCompletableFuture().isDone()).isFalse();
            assertThat(read.rowsReleased().toCompletableFuture().isDone()).isFalse();
            read.requestClose(observation(1), cleanup); read.close();
            assertThat(raw.closes).hasValue(1);
            assertThatThrownBy(read::next).isInstanceOf(TapstateException.class);
            raw.allowClose.countDown(); requested.toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertThat(read.workersExited().toCompletableFuture().isDone()).isFalse();
            assertThat(read.rowsReleased().toCompletableFuture().isDone()).isFalse();
            raw.workers.complete(null); raw.rows.complete(null);
            assertThat(read.workersExited().toCompletableFuture().get(3, TimeUnit.SECONDS)).isNull();
            assertThat(read.rowsReleased().toCompletableFuture().get(3, TimeUnit.SECONDS)).isNull();
        } finally { raw.allowClose.countDown(); }
    }

    @Test void anotherObservationCannotRequestCloseAndDoesNotConsumeTheValidCloseSlot() throws Exception {
        var raw = new Read();
        try (var cleanup = executor(); var read = new ValidationScopedRead(observation(1), raw)) {
            assertThatThrownBy(() -> read.requestClose(observation(2), cleanup)).isInstanceOfSatisfying(TapstateException.class,
                    error -> assertThat(error.code()).isEqualTo(ValidationError.INVALID_CONTRACT));
            assertThat(raw.closes).hasValue(0);
            read.requestClose(observation(1), cleanup).toCompletableFuture().get(3, TimeUnit.SECONDS);
            assertThat(raw.closes).hasValue(1);
        }
    }

    @Test void rejectedOrInlineDispatchNeverClosesOnTheSubmittingThreadAndAllowsAnExplicitRetry() throws Exception {
        for (boolean inline : java.util.List.of(false, true)) {
            var raw = new Read();
            try (var cleanup = executor(); var read = new ValidationScopedRead(observation(1), raw)) {
                java.util.concurrent.Executor invalid = inline ? Runnable::run : task -> { throw new java.util.concurrent.RejectedExecutionException(); };
                var first = read.requestClose(observation(1), invalid);
                assertThatThrownBy(() -> first.toCompletableFuture().get(3, TimeUnit.SECONDS)).isInstanceOf(java.util.concurrent.ExecutionException.class);
                assertThat(raw.closes).hasValue(0);
                read.requestClose(observation(1), cleanup).toCompletableFuture().get(3, TimeUnit.SECONDS);
                assertThat(raw.closes).hasValue(1);
            }
        }
    }

    @Test void aRejectedTaskCannotCloseTheDelegateIfAnExecutorRetainedIt() throws Exception {
        var raw = new Read();
        var retained = new java.util.concurrent.atomic.AtomicReference<Runnable>();
        try (var cleanup = executor(); var read = new ValidationScopedRead(observation(1), raw)) {
            var rejected = read.requestClose(observation(1), task -> {
                retained.set(task);
                throw new java.util.concurrent.RejectedExecutionException();
            });
            assertThat(rejected.toCompletableFuture()).isCompletedExceptionally();
            read.requestClose(observation(1), cleanup).toCompletableFuture().get(3, TimeUnit.SECONDS);
            cleanup.submit(retained.get()).get(3, TimeUnit.SECONDS);
            assertThat(raw.closes).hasValue(1);
        }
    }

    @Test void normalIoCloseIsSynchronousAndAClosedAttemptIsNotRepeated() {
        var raw = new Read();
        var read = new ValidationScopedRead(observation(1), raw);
        read.close(); read.close();
        assertThat(raw.closes).hasValue(1);
        assertThatThrownBy(read::next).isInstanceOf(TapstateException.class);
    }

    @Test void closeErrorsRemainFailedAndNeverBecomeSuccessfulExitSignals() throws Exception {
        var raw = new Read();
        var failed = new TapstateException(ValidationError.READ_FAILED, Map.of("table", "records"), null);
        raw.closeFailure = failed;
        try (var cleanup = executor()) {
            var read = new ValidationScopedRead(observation(1), raw);
            var close = read.requestClose(observation(1), cleanup);
            assertThatThrownBy(() -> close.toCompletableFuture().get(3, TimeUnit.SECONDS))
                    .isInstanceOfSatisfying(java.util.concurrent.ExecutionException.class, error -> assertThat(error.getCause()).isSameAs(failed));
            read.close();
            assertThat(raw.closes).hasValue(1);
            assertThat(read.workersExited().toCompletableFuture().isDone()).isFalse();
        }
        var direct = new ValidationScopedRead(observation(1), new Read() { @Override public void close() { throw failed; } });
        assertThat(catchThrowable(direct::close)).isSameAs(failed);
    }

    @Test void aRowReturnedAfterCloseWasRequestedIsReleasedInsteadOfHandedToTheChecker() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var rowCloses = new AtomicInteger();
        var raw = new Read() {
            @Override public Optional<Row> next() {
                entered.countDown(); await(release);
                return Optional.of(new Row() {
                    @Override public long ordinal() { return 0; }
                    @Override public ValidationValues.Row values() { throw new AssertionError("late row must not be read"); }
                    @Override public void close() { rowCloses.incrementAndGet(); }
                });
            }
        };
        try (var reader = executor(); var cleanup = executor(); var read = new ValidationScopedRead(observation(1), raw)) {
            var next = reader.submit(read::next); await(entered);
            read.requestClose(observation(1), cleanup).toCompletableFuture().get(3, TimeUnit.SECONDS);
            release.countDown();
            assertThatThrownBy(() -> next.get(3, TimeUnit.SECONDS)).isInstanceOfSatisfying(java.util.concurrent.ExecutionException.class,
                    error -> assertThat(error.getCause()).isInstanceOf(TapstateException.class));
            assertThat(rowCloses).hasValue(1);
        } finally { release.countDown(); }
    }

    private static class Read implements ValidationReadHandle {
        final AtomicInteger closes = new AtomicInteger();
        final CompletableFuture<Void> workers = new CompletableFuture<>();
        final CompletableFuture<Void> rows = new CompletableFuture<>();
        CountDownLatch closeEntered = new CountDownLatch(0);
        CountDownLatch allowClose = new CountDownLatch(0);
        RuntimeException closeFailure;
        @Override public Optional<Row> next() { return Optional.empty(); }
        @Override public void close() { closes.incrementAndGet(); closeEntered.countDown(); await(allowClose); if (closeFailure != null) throw closeFailure; }
        @Override public CompletionStage<Void> workersExited() { return workers.minimalCompletionStage(); }
        @Override public CompletionStage<Void> rowsReleased() { return rows.minimalCompletionStage(); }
    }
    private static ValidationObservation observation(long revision) { return ValidationRootWorkTest.observation(revision); }
    private static ThreadPoolExecutor executor() { return new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2), new ThreadPoolExecutor.AbortPolicy()); }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(3, TimeUnit.SECONDS)) throw new AssertionError("fixture timed out"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }
}
