package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import io.tapstate.core.validation.ValidationObservation;
import io.tapstate.spi.store.ValidationReadHandle;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;

/**
 * Bounded read ownership for one whole-root operation, including handles still being opened. Opening
 * and ordinary cursor use run on admitted I/O; cooperative cancellation only dispatches cleanup.
 * The caller separately admits native allocations, opening work, cleanup queues and working copies.
 * Drain covers delegate close, adapter worker exit and decoded rows, never opaque native activity.
 */
public final class ValidationReadScope implements AutoCloseable {
    @FunctionalInterface
    public interface Opener {
        /**
         * Use this guard throughout opening and reading. A thrown opening failure must retire any
         * resources that cannot be represented by a returned handle under their own reservations.
         */
        ValidationReadHandle open(Runnable requireActive);
    }

    private final ValidationObservation observation;
    private final Executor cleanup;
    private final Runnable requireAuthorized;
    private final int maxReads;
    private final Object lock = new Object();
    private final LinkedHashSet<Entry> reads = new LinkedHashSet<>();
    private final ValidationFailureLatch failure = new ValidationFailureLatch();
    private final CompletableFuture<Void> drained = new CompletableFuture<>();
    private boolean closed;

    public ValidationReadScope(ValidationObservation observation, Executor cleanup, Runnable requireAuthorized, int maxReads) {
        this.observation = Objects.requireNonNull(observation, "observation");
        this.cleanup = Objects.requireNonNull(cleanup, "cleanup");
        this.requireAuthorized = Objects.requireNonNull(requireAuthorized, "requireAuthorized");
        if (!observation.root().ancestors().isEmpty() || maxReads < 1 || maxReads > 256) throw invalid("readScope.limits");
        this.maxReads = maxReads;
    }

    public ValidationObservation observation() { return observation; }
    public CompletionStage<Void> drained() { return drained.minimalCompletionStage(); }

    /** The entry is retained before calling an opener, so cancellation cannot miss a late handle. */
    public ValidationScopedRead open(String table, Opener opener) {
        ValidationError.fieldName(table, "readScope.table");
        Objects.requireNonNull(opener, "opener");
        requireActive();
        var entry = new Entry(table);
        synchronized (lock) {
            if (closed) throw cancelled();
            if (reads.size() == maxReads) throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                    Map.of("limit", "validation.activeReads"), null);
            reads.add(entry);
        }
        ValidationReadHandle raw;
        try { requireActive(); raw = Objects.requireNonNull(opener.open(this::requireActive), "read handle"); }
        catch (RuntimeException | Error error) {
            record(error);
            synchronized (lock) { reads.remove(entry); }
            completeDrain();
            throw error;
        }
        try { entry.attach(raw); requireActive(); return entry.read; }
        catch (RuntimeException | Error error) {
            record(error);
            // This is the original opening I/O thread, never the cooperative cancellation caller.
            try { entry.read.close(); }
            catch (RuntimeException | Error cleanupFailure) { record(cleanupFailure); }
            throwFailure();
            throw new IllegalStateException("failed opening has no diagnosis");
        } finally { entry.settled(3); }
    }

    public void requireActive() {
        synchronized (lock) { if (closed) throw cancelled(); }
        throwFailure();
        requireAuthorized.run();
        synchronized (lock) { if (closed) throw cancelled(); }
    }

    /** Retriable dispatch for known unstarted cleanup; never closes another root or follows latest. */
    public CompletionStage<Void> requestClose(ValidationObservation expected) {
        if (!observation.equals(expected)) throw invalid("readScope.observation");
        Entry[] active;
        synchronized (lock) { closed = true; active = reads.toArray(Entry[]::new); }
        for (var entry : active) entry.requestClose();
        completeDrain();
        if (failure.get() instanceof Error error) throw error;
        return drained();
    }
    @Override public void close() { requestClose(observation); }

    private final class Entry {
        private final String table;
        private volatile ValidationScopedRead read;
        private boolean openingExited;
        private Entry(String table) { this.table = table; }
        private boolean closeExited;
        private boolean workersExited;
        private boolean rowsReleased;

        private void attach(ValidationReadHandle raw) {
            var tracked = new ValidationReadHandle() {
                @Override public Optional<Row> next() { return raw.next(); }
                @Override public CompletionStage<Void> workersExited() { return raw.workersExited(); }
                @Override public CompletionStage<Void> rowsReleased() { return raw.rowsReleased(); }
                @Override public void close() {
                    try { raw.close(); }
                    catch (RuntimeException | Error error) { record(error); throw error; }
                    finally { settled(0); }
                }
            };
            read = new ValidationScopedRead(observation, tracked);
            raw.workersExited().whenComplete((ignored, error) -> { if (error != null) recordRead(error); settled(1); });
            raw.rowsReleased().whenComplete((ignored, error) -> { if (error != null) recordRead(error); settled(2); });
            boolean closing;
            synchronized (lock) { closing = closed; }
            if (closing) requestClose();
        }
        private void requestClose() {
            var current = read;
            if (current == null) return;
            try { current.requestClose(observation, cleanup).whenComplete((ignored, error) -> { if (error != null) record(error); }); }
            catch (RuntimeException | Error error) { record(error); }
        }
        private void recordRead(Throwable error) {
            while (error instanceof java.util.concurrent.CompletionException && error.getCause() != null) error = error.getCause();
            record(error instanceof RuntimeException || error instanceof Error ? error
                    : new TapstateException(ValidationError.READ_FAILED, Map.of("table", table), error));
        }
        private void settled(int signal) {
            synchronized (lock) {
                if (signal == 0) closeExited = true;
                else if (signal == 1) workersExited = true;
                else if (signal == 2) rowsReleased = true;
                else openingExited = true;
                if (openingExited && closeExited && workersExited && rowsReleased) reads.remove(this);
            }
            completeDrain();
        }
    }

    private void completeDrain() {
        boolean done;
        synchronized (lock) { done = closed && reads.isEmpty(); }
        if (done) {
            var error = failure.get();
            if (error == null) drained.complete(null);
            else drained.completeExceptionally(error);
        }
    }
    private void record(Throwable error) {
        while (error instanceof java.util.concurrent.CompletionException && error.getCause() != null) error = error.getCause();
        failure.record(error);
        ValidationFailureLatch.addContext(failure.get(), error);
    }
    private void throwFailure() {
        if (failure.get() instanceof Error error) throw error;
        if (failure.get() instanceof RuntimeException error) throw error;
        if (failure.get() != null) throw new IllegalStateException("uncoded read scope failure", failure.get());
    }
    private TapstateException cancelled() {
        return new TapstateException(ValidationError.EXECUTION_CANCELLED, Map.of("node", observation.run().owner().nodeId()), null);
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
}
