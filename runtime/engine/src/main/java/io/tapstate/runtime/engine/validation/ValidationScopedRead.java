package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import io.tapstate.core.validation.ValidationObservation;
import io.tapstate.spi.store.ValidationReadHandle;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Binds one read to its original root work. Cooperative cancellation submits the possibly blocking
 * close to an independently admitted bounded cleanup executor; it never follows a task's latest run.
 * Worker exit, decoded-row release and the close attempt are separate signals. None proves release
 * of connector-owned asynchronous/native resources or a successful validation observation.
 */
public final class ValidationScopedRead implements ValidationReadHandle {
    private final ValidationObservation observation;
    private final ValidationReadHandle delegate;
    private final AtomicReference<CloseAttempt> closing = new AtomicReference<>();

    public ValidationScopedRead(ValidationObservation observation, ValidationReadHandle delegate) {
        this.observation = Objects.requireNonNull(observation, "observation");
        this.delegate = Objects.requireNonNull(delegate, "delegate");
        if (!observation.root().ancestors().isEmpty()) throw invalid("scopedRead.root");
    }
    public ValidationObservation observation() { return observation; }
    @Override public CompletionStage<Void> workersExited() { return delegate.workersExited(); }
    @Override public CompletionStage<Void> rowsReleased() { return delegate.rowsReleased(); }

    @Override public Optional<Row> next() {
        if (closing.get() != null) throw cancelled();
        var next = delegate.next();
        if (closing.get() == null) return next;
        var failure = new ValidationFailureLatch();
        failure.record(cancelled());
        try { next.ifPresent(Row::close); }
        catch (RuntimeException | Error error) { failure.record(error); ValidationFailureLatch.addContext(failure.get(), error); }
        if (failure.get() instanceof Error error) throw error;
        throw (RuntimeException)failure.get();
    }

    /**
     * Nonblocking when the supplied executor obeys its admitted contract. Rejected/inline dispatch
     * does not call the handle; a later explicit request may retry that known unstarted attempt.
     * Completion means the close body returned or dispatch failed, never that all resources exited.
     */
    public CompletionStage<Void> requestClose(ValidationObservation expected, Executor cleanup) {
        if (!observation.equals(expected)) throw invalid("scopedRead.observation");
        Objects.requireNonNull(cleanup, "cleanup");
        var selected = select(Thread.currentThread());
        if (selected.created()) {
            try { cleanup.execute(selected.attempt()); }
            catch (RuntimeException | Error error) {
                Throwable reported = error instanceof RejectedExecutionException ? new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                        Map.of("limit", "validation.cleanupQueue"), error) : error;
                selected.attempt().rejectDispatch(reported);
                if (reported instanceof Error fatal) throw fatal;
            }
        }
        return selected.attempt().completion.minimalCompletionStage();
    }

    /** May block. Invoke only on admitted I/O; an already accepted asynchronous close is not repeated. */
    @Override public void close() {
        var selected = select(null);
        if (selected.created()) {
            selected.attempt().run();
            selected.attempt().throwFailure();
        }
    }

    private Selection select(Thread submittingThread) {
        while (true) {
            var previous = closing.get();
            if (previous != null && !previous.rejected) return new Selection(previous, false);
            var candidate = new CloseAttempt(submittingThread);
            if (closing.compareAndSet(previous, candidate)) return new Selection(candidate, true);
        }
    }
    private record Selection(CloseAttempt attempt, boolean created) {}

    private final class CloseAttempt implements Runnable {
        private final Thread submittingThread;
        private final AtomicBoolean entered = new AtomicBoolean();
        private final CompletableFuture<Void> completion = new CompletableFuture<>();
        private final ValidationFailureLatch failure = new ValidationFailureLatch();
        private volatile boolean rejected;
        private CloseAttempt(Thread submittingThread) { this.submittingThread = submittingThread; }
        @Override public void run() {
            if (!entered.compareAndSet(false, true)) return;
            if (Thread.currentThread() == submittingThread) {
                reject(invalid("scopedRead.inlineCleanupExecutor"));
                return;
            }
            try { delegate.close(); completion.complete(null); }
            catch (RuntimeException | Error error) {
                failure.record(error); completion.completeExceptionally(error);
                if (error instanceof Error fatal) throw fatal;
            }
        }
        private void rejectDispatch(Throwable error) {
            // Fence a retained task before allowing another attempt. A started close is never retried.
            if (entered.compareAndSet(false, true)) reject(error);
            else {
                failure.record(error);
                completion.completeExceptionally(error);
            }
        }
        private void reject(Throwable error) {
            failure.record(error);
            rejected = true;
            completion.completeExceptionally(error);
        }
        private void throwFailure() {
            if (failure.get() instanceof Error error) throw error;
            if (failure.get() instanceof RuntimeException error) throw error;
        }
    }
    private TapstateException cancelled() {
        return new TapstateException(ValidationError.EXECUTION_CANCELLED, Map.of("node", observation.run().owner().nodeId()), null);
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
}
