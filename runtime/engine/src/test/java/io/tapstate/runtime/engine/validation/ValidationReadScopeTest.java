package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
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

class ValidationReadScopeTest {
    @Test void closingAnOpeningScopeCannotMissItsLateHandle() throws Exception {
        var opening = new CountDownLatch(1); var opened = new CountDownLatch(1); var raw = new Read();
        try (var io = executor(); var cleanup = executor()) {
            var scope = scope(cleanup, 1);
            var future = io.submit(() -> scope.open("records", guard -> { opening.countDown(); await(opened); return raw; }));
            await(opening);
            var drained = scope.requestClose(ValidationRootWorkTest.observation(1));
            assertThat(drained.toCompletableFuture().isDone()).isFalse();
            opened.countDown();
            assertThatThrownBy(() -> future.get(3, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
            await(raw.closed);
            assertThat(raw.closes).hasValue(1);
            assertThat(drained.toCompletableFuture().isDone()).isFalse();
            raw.workers.complete(null);
            assertThat(drained.toCompletableFuture().isDone()).isFalse();
            raw.rows.complete(null);
            assertThatThrownBy(() -> drained.toCompletableFuture().get(3, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
        } finally { opened.countDown(); }
    }

    @Test void outstandingRowsAndWorkersKeepTheReadSlotAfterClose() throws Exception {
        try (var cleanup = executor()) {
            var scope = scope(cleanup, 1); var raw = new Read();
            var read = scope.open("records", guard -> raw); read.close();
            assertThatThrownBy(() -> scope.open("other", guard -> { throw new AssertionError("must not open"); }))
                    .isInstanceOfSatisfying(TapstateException.class, e -> assertThat(e.code()).isEqualTo(ValidationError.VALUE_LIMIT_EXCEEDED));
            raw.workers.complete(null);
            assertThatThrownBy(() -> scope.open("other", guard -> new Read())).isInstanceOf(TapstateException.class);
            raw.rows.complete(null);
            var second = new Read(); scope.open("other", guard -> second).close();
            second.rows.complete(null); second.workers.complete(null);
            assertThat(scope.drained().toCompletableFuture().isDone()).isFalse();
            scope.close();
            assertThat(scope.drained().toCompletableFuture().get(3, TimeUnit.SECONDS)).isNull();
        }
    }

    @Test void rejectedCleanupRetainsTheHandleUntilExplicitRetryAndActualExit() throws Exception {
        try (var cleanup = executor()) {
            var accepts = new java.util.concurrent.atomic.AtomicBoolean(); var raw = new Read();
            var scope = scope(task -> { if (!accepts.get()) throw new java.util.concurrent.RejectedExecutionException(); cleanup.execute(task); }, 1);
            scope.open("records", guard -> raw);
            scope.close();
            assertThat(raw.closes).hasValue(0);
            assertThat(scope.drained().toCompletableFuture().isDone()).isFalse();
            accepts.set(true); scope.close(); await(raw.closed);
            raw.workers.complete(null); raw.rows.complete(null);
            assertThatThrownBy(() -> scope.drained().toCompletableFuture().get(3, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
            assertThat(raw.closes).hasValue(1);
        }
    }

    @Test void closeDispatchDoesNotBlockOnNativeCloseOrTouchAnotherObservation() throws Exception {
        var raw = new Read(); raw.allowClose = new CountDownLatch(1);
        try (var cleanup = executor()) {
            var scope = scope(cleanup, 1); scope.open("records", guard -> raw);
            assertThatThrownBy(() -> scope.requestClose(ValidationRootWorkTest.observation(2))).isInstanceOf(TapstateException.class);
            scope.requireActive(); assertThat(raw.closes).hasValue(0);
            scope.close(); await(raw.closed);
            assertThat(scope.drained().toCompletableFuture().isDone()).isFalse();
            raw.workers.complete(null); raw.rows.complete(null);
            assertThat(scope.drained().toCompletableFuture().isDone()).isFalse();
            raw.allowClose.countDown();
            assertThat(scope.drained().toCompletableFuture().get(3, TimeUnit.SECONDS)).isNull();
        } finally { raw.allowClose.countDown(); }
    }

    @Test void anOpeningFailureAndLateFatalRemainFailuresWithoutLeakingEntries() throws Exception {
        try (var cleanup = executor()) {
            var scope = scope(cleanup, 1);
            var original = new TapstateException(ValidationError.READ_FAILED, Map.of("table", "records"), null);
            assertThat(catchThrowable(() -> scope.open("records", guard -> { throw original; }))).isSameAs(original);
            scope.close();
            assertThatThrownBy(() -> scope.drained().toCompletableFuture().get(3, TimeUnit.SECONDS))
                    .satisfies(error -> assertThat(error.getCause()).isSameAs(original));
            var late = scope(cleanup, 1); var raw = new Read();
            late.open("records", guard -> raw).close();
            var fatal = new LinkageError("decoder linkage");
            raw.workers.completeExceptionally(fatal); raw.rows.complete(null);
            assertThat(catchThrowable(late::close)).isSameAs(fatal);
            assertThatThrownBy(() -> late.drained().toCompletableFuture().get(3, TimeUnit.SECONDS))
                    .satisfies(error -> assertThat(error.getCause()).isSameAs(fatal));
        }
    }

    @Test void theOpenerReceivesTheSameLiveGuardAndFailedAuthorityClosesItsHandle() throws Exception {
        try (var cleanup = executor()) {
            var authorized = new java.util.concurrent.atomic.AtomicBoolean(true); var raw = new Read();
            var denied = new TapstateException(ValidationError.OWNER_NOT_AUTHORIZED, Map.of("pipeline", "pipeline"), null);
            var scope = new ValidationReadScope(ValidationRootWorkTest.observation(1), cleanup,
                    () -> { if (!authorized.get()) throw denied; }, 1);
            assertThat(catchThrowable(() -> scope.open("records", guard -> {
                guard.run(); authorized.set(false); return raw;
            }))).isSameAs(denied);
            assertThat(raw.closes).hasValue(1);
            raw.workers.complete(null); raw.rows.complete(null); scope.close();
            assertThatThrownBy(() -> scope.drained().toCompletableFuture().get(3, TimeUnit.SECONDS))
                    .satisfies(error -> assertThat(error.getCause()).isSameAs(denied));
            assertThatThrownBy(() -> scope.open("records", guard -> { throw new AssertionError("closed"); }))
                    .isInstanceOf(TapstateException.class);
        }
    }

    @Test void rootCancellationClosesLateReadsWithoutReleasingTheirRowsAtCheckerExit() throws Exception {
        var opening = new CountDownLatch(1); var opened = new CountDownLatch(1); var raw = new Read();
        var checkerExits = new AtomicInteger();
        try (var io = executor(); var cleanup = executor()) {
            var scope = scope(cleanup, 1);
            var work = ValidationRootWork.submit(ValidationRootWorkTest.observation(1), io, (observation, guard) -> {
                try (scope; var read = scope.open("records", active -> { opening.countDown(); await(opened); return raw; })) {
                    guard.run();
                    return ValidationRootWorkTest.result(observation);
                }
            }, () -> {}, scope::close, checkerExits::incrementAndGet, 1024 * 1024);
            await(opening);
            var cancelled = new TapstateException(ValidationError.EXECUTION_CANCELLED, Map.of("node", "node"), null);
            work.cancel(cancelled);
            assertThat(work.hasExited()).isFalse();
            assertThat(scope.drained().toCompletableFuture().isDone()).isFalse();
            opened.countDown(); io.shutdown(); assertThat(io.awaitTermination(3, TimeUnit.SECONDS)).isTrue();
            await(raw.closed);
            assertThat(work.hasExited()).isTrue(); assertThat(checkerExits).hasValue(1);
            assertThat(catchThrowable(work::takeResult)).isSameAs(cancelled);
            assertThat(scope.drained().toCompletableFuture().isDone()).isFalse();
            raw.workers.complete(null);
            assertThat(scope.drained().toCompletableFuture().isDone()).isFalse();
            raw.rows.complete(null);
            assertThatThrownBy(() -> scope.drained().toCompletableFuture().get(3, TimeUnit.SECONDS)).hasCauseInstanceOf(TapstateException.class);
        } finally { opened.countDown(); }
    }

    private static class Read implements ValidationReadHandle {
        final AtomicInteger closes = new AtomicInteger();
        final CountDownLatch closed = new CountDownLatch(1);
        final CompletableFuture<Void> workers = new CompletableFuture<>();
        final CompletableFuture<Void> rows = new CompletableFuture<>();
        CountDownLatch allowClose = new CountDownLatch(0);
        @Override public Optional<Row> next() { return Optional.empty(); }
        @Override public void close() { closes.incrementAndGet(); closed.countDown(); await(allowClose); }
        @Override public CompletionStage<Void> workersExited() { return workers.minimalCompletionStage(); }
        @Override public CompletionStage<Void> rowsReleased() { return rows.minimalCompletionStage(); }
    }
    private static ValidationReadScope scope(java.util.concurrent.Executor cleanup, int capacity) {
        return new ValidationReadScope(ValidationRootWorkTest.observation(1), cleanup, () -> {}, capacity);
    }
    private static ThreadPoolExecutor executor() { return new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS, new ArrayBlockingQueue<>(2), new ThreadPoolExecutor.AbortPolicy()); }
    private static void await(CountDownLatch latch) {
        try { if (!latch.await(3, TimeUnit.SECONDS)) throw new AssertionError("fixture timed out"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
    }
}
