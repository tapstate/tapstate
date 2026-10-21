package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationCheckEvent;
import io.tapstate.core.validation.ValidationError;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Merges two already compatible native orders without sorting. Single-sided groups remain candidate
 * facts: participation, value checks, stable-window confirmation and result commit happen later.
 * The caller owns the two readers' complete reservations, including samples retained by this merge.
 * Use one admitted blocking-I/O consumer and close each pair before advancing.
 */
public final class ValidationOrderedMerge implements AutoCloseable {
    private final ValidationOrderedGroups source;
    private final ValidationOrderedGroups target;
    private final Object stateLock = new Object();
    private boolean closed;
    private ValidationOrderedGroups.Group sourcePending;
    private ValidationOrderedGroups.Group targetPending;
    private Pair outstanding;
    private boolean sourceEof;
    private boolean targetEof;
    private Throwable failure;

    public ValidationOrderedMerge(ValidationOrderedGroups source, ValidationOrderedGroups target) {
        this.source = Objects.requireNonNull(source, "source");
        this.target = Objects.requireNonNull(target, "target");
        if (source == target) throw new IllegalArgumentException("validation merge needs two independent readers");
    }

    public Optional<Pair> next() {
        try {
            synchronized (stateLock) {
                checkActive();
                if (outstanding != null && !outstanding.released.get()) {
                    throw new IllegalStateException("close the current validation pair before advancing");
                }
                outstanding = null;
            }
            fetch(true);
            fetch(false);
            synchronized (stateLock) {
                checkActive();
                if (sourceEof && targetEof) return Optional.empty();
                int order = sourceEof ? 1 : targetEof ? -1
                        : sourcePending.key().comparison().compareTo(targetPending.key().comparison());
                Pair pair = new Pair(order <= 0 ? sourcePending : null, order >= 0 ? targetPending : null);
                if (order <= 0) sourcePending = null;
                if (order >= 0) targetPending = null;
                outstanding = pair;
                return Optional.of(pair);
            }
        } catch (RuntimeException | Error stopped) {
            if (failure == null) failure = stopped;
            try { close(); }
            catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throwFailure();
            throw stopped;
        }
    }

    private void fetch(boolean fromSource) {
        synchronized (stateLock) {
            checkActive();
            if (fromSource ? sourcePending != null || sourceEof : targetPending != null || targetEof) return;
        }
        // Never hold the ownership lock while waiting for I/O; close must be able to cancel it.
        ValidationOrderedGroups.Group fetched = (fromSource ? source : target).next().orElse(null);
        boolean transferred = false;
        try {
            synchronized (stateLock) {
                checkActive();
                if (fromSource) {
                    sourcePending = fetched;
                    sourceEof = fetched == null;
                } else {
                    targetPending = fetched;
                    targetEof = fetched == null;
                }
                transferred = true;
            }
        } finally {
            if (!transferred && fetched != null) fetched.close();
        }
    }

    private void checkActive() {
        throwFailure();
        if (closed) throw new TapstateException(ValidationError.READ_CANCELLED,
                Map.of("table", "validation-merge"), null);
    }

    private void throwFailure() {
        if (failure instanceof RuntimeException runtime) throw runtime;
        if (failure instanceof Error error) throw error;
    }

    @Override public void close() {
        Pair leased;
        synchronized (stateLock) {
            if (closed) return;
            closed = true;
            leased = outstanding;
            outstanding = null;
            sourcePending = null;
            targetPending = null;
        }
        if (leased != null) leased.close();
        try { source.close(); }
        catch (RuntimeException | Error first) {
            try { target.close(); }
            catch (RuntimeException | Error second) { if (first != second) first.addSuppressed(second); }
            throw first;
        }
        target.close();
    }

    public static final class Pair implements AutoCloseable {
        private final AtomicBoolean released = new AtomicBoolean();
        private final ValidationOrderedGroups.Group source;
        private final ValidationOrderedGroups.Group target;

        private Pair(ValidationOrderedGroups.Group source, ValidationOrderedGroups.Group target) {
            this.source = source;
            this.target = target;
        }

        public Optional<ValidationOrderedGroups.Group> source() { checkOpen(); return Optional.ofNullable(source); }
        public Optional<ValidationOrderedGroups.Group> target() { checkOpen(); return Optional.ofNullable(target); }
        public ValidationCheckEvent.PairState state() {
            checkOpen();
            return source == null ? ValidationCheckEvent.PairState.TARGET_ONLY
                    : target == null ? ValidationCheckEvent.PairState.SOURCE_ONLY : ValidationCheckEvent.PairState.MATCHED;
        }
        private void checkOpen() {
            if (released.get()) throw new IllegalStateException("validation pair has been released");
        }
        @Override public void close() {
            if (!released.compareAndSet(false, true)) return;
            if (source != null) source.close();
            if (target != null) target.close();
        }
    }
}
