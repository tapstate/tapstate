package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import io.tapstate.core.validation.ValidationObservation;
import io.tapstate.core.validation.ValidationRootCheck;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One admitted root operation on blocking I/O. Cancellation requests shutdown but never declares the
 * worker exited. The reservation callback runs only after the checker unwinds, or rejected submission
 * proves it never started. The owner supplies a bounded executor, real driver deadlines, and an
 * invocation-specific cancellation request. Cancellation and exit callbacks must be bounded,
 * nonblocking local operations; this class is not a resource allocator.
 * The exit callback covers this checker worker only; nested connector/query/decode workers retain
 * their own reservations until their actual exits. Result retention and all local edge queues need
 * separate, pre-admitted byte reservations.
 */
public final class ValidationRootWork {
    @FunctionalInterface
    public interface Checker {
        /** Use the supplied guard throughout actual query and rule work, including after cleanup. */
        ValidationRootCheck check(ValidationObservation observation, Runnable requireActive);
    }

    private final ValidationObservation observation;
    private final Checker checker;
    private final Runnable requireAuthorized;
    private final Runnable requestCancel;
    private final Runnable whenExited;
    private final long maxResultBytes;
    private final Thread submittingThread;
    private final ValidationFailureLatch failure = new ValidationFailureLatch();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean cancellationRequested = new AtomicBoolean();
    private final AtomicBoolean released = new AtomicBoolean();
    private volatile boolean exited;
    private volatile ValidationRootCheck result;

    private ValidationRootWork(ValidationObservation observation, Checker checker, Runnable requireAuthorized,
            Runnable requestCancel, Runnable whenExited, long maxResultBytes) {
        this.observation = Objects.requireNonNull(observation, "observation");
        this.checker = Objects.requireNonNull(checker, "checker");
        this.requireAuthorized = Objects.requireNonNull(requireAuthorized, "requireAuthorized");
        this.requestCancel = Objects.requireNonNull(requestCancel, "requestCancel");
        this.whenExited = Objects.requireNonNull(whenExited, "whenExited");
        if (!observation.root().ancestors().isEmpty() || maxResultBytes < 1 || maxResultBytes > 32L * 1024 * 1024)
            throw invalid("rootWork.limits");
        this.maxResultBytes = maxResultBytes;
        this.submittingThread = Thread.currentThread();
    }

    /** After argument validation this owns the reservation, including queued and cancelled work. */
    public static ValidationRootWork submit(ValidationObservation observation, Executor executor, Checker checker,
            Runnable requireAuthorized, Runnable requestCancel, Runnable whenExited, long maxResultBytes) {
        Objects.requireNonNull(executor, "executor");
        var work = new ValidationRootWork(observation, checker, requireAuthorized, requestCancel, whenExited, maxResultBytes);
        try {
            work.requireActive();
            executor.execute(work::run);
            return work;
        } catch (RuntimeException | Error error) {
            work.record(error instanceof RejectedExecutionException ? new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                    Map.of("limit", "rootWork.ioQueue"), error) : error);
            if (!work.started.get()) work.exit();
            work.throwFailure();
            throw new IllegalStateException("failed root submission has no diagnosis");
        }
    }

    public ValidationObservation observation() { return observation; }
    public boolean hasExited() { return exited; }

    /** Thread-confined consumer. Empty is pending/already transferred, never a completed empty root. */
    public Optional<ValidationRootCheck> takeResult() {
        throwFailure();
        if (!exited) return Optional.empty();
        var value = result;
        result = null;
        throwFailure();
        return Optional.ofNullable(value);
    }

    /** Does not interrupt a shared executor, release its slot, or wait for non-cooperative I/O. */
    public void cancel(Throwable cause) {
        Objects.requireNonNull(cause, "cause");
        record(cause instanceof RuntimeException || cause instanceof Error ? cause
                : new TapstateException(ValidationError.EXECUTION_CANCELLED, Map.of("node", observation.run().owner().nodeId()), cause));
        if (exited) result = null;
        if (cancellationRequested.compareAndSet(false, true) && !exited) {
            try { requestCancel.run(); }
            catch (RuntimeException | Error error) { record(error); }
        }
        if (failure.get() instanceof Error error) throw error;
    }

    private void run() {
        if (!started.compareAndSet(false, true)) throw new IllegalStateException("root worker started twice");
        try {
            // A caller-runs rejection policy must not move blocking business reads onto Jet.
            if (Thread.currentThread() == submittingThread) throw invalid("rootWork.inlineExecutor");
            requireActive();
            var checked = Objects.requireNonNull(checker.check(observation, this::requireActive), "root result");
            if (!observation.equals(checked.observation())) throw invalid("rootWork.observation");
            if (checked.accountedBytes() > maxResultBytes) throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                    Map.of("limit", "rootWork.resultBytes"), null);
            requireActive();
            result = checked;
        } catch (RuntimeException | Error error) {
            record(error);
        } finally {
            exit();
        }
        // Future cancellation must not swallow fatal worker or cleanup errors.
        if (failure.get() instanceof Error error) throw error;
    }

    private void requireActive() {
        throwFailure();
        try { requireAuthorized.run(); }
        catch (RuntimeException | Error error) { record(error); throwFailure(); }
        throwFailure();
    }
    private void exit() {
        if (!released.compareAndSet(false, true)) return;
        try { whenExited.run(); }
        catch (RuntimeException | Error error) { record(error); }
        finally {
            if (failure.get() != null) result = null;
            exited = true;
        }
    }
    private void record(Throwable error) {
        failure.record(error);
        ValidationFailureLatch.addContext(failure.get(), error);
    }
    private void throwFailure() {
        if (failure.get() instanceof Error error) throw error;
        if (failure.get() instanceof RuntimeException error) throw error;
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
}
