package io.tapstate.runtime.engine.validation;

import com.hazelcast.jet.core.AbstractProcessor;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.core.validation.ValidationFailureLatch;
import io.tapstate.core.validation.ValidationObservation;
import io.tapstate.core.validation.ValidationRootCheck;
import io.tapstate.core.validation.ValidationRunIdentity;
import java.util.Map;
import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Cooperative validation-only root stage: at most one admitted I/O operation or backpressured result.
 * Construct on the selected member through ValidationJobGraph. Factory and current-authority checks
 * must be bounded nonblocking local operations. The controller admits the executor, working sets,
 * result bytes and all edge queues before starting the graph; graph completion is not run completion.
 * Downstream confirmation/publication must still verify original run, report and observation fences.
 */
public final class ValidationRootProcessor extends AbstractProcessor {
    @FunctionalInterface
    public interface Factory {
        /** Starts exactly this original observation, with no blocking admission or implicit retry. */
        ValidationRootWork start(ValidationObservation observation, Runnable requireCurrent, long maxResultBytes);
    }

    private final ValidationRunIdentity run;
    private final Factory factory;
    private final Consumer<ValidationObservation> requireCurrent;
    private final long maxResultBytes;
    private final ValidationFailureLatch failure = new ValidationFailureLatch();
    private ValidationRootWork work;
    private ValidationRootCheck pending;
    private boolean closed;

    public ValidationRootProcessor(ValidationRunIdentity run, Factory factory, Consumer<ValidationObservation> requireCurrent, long maxResultBytes) {
        this.run = Objects.requireNonNull(run, "run");
        this.factory = Objects.requireNonNull(factory, "factory");
        this.requireCurrent = Objects.requireNonNull(requireCurrent, "requireCurrent");
        if (maxResultBytes < 1 || maxResultBytes > 32L * 1024 * 1024) throw invalid("rootProcessor.resultBytes");
        this.maxResultBytes = maxResultBytes;
    }

    @Override protected boolean tryProcess(int ordinal, Object item) {
        return guarded(() -> {
            if (ordinal != 0 || !(item instanceof ValidationObservation observation)
                    || !run.equals(observation.run()) || !observation.root().ancestors().isEmpty()) {
                throw invalid("rootProcessor.input");
            }
            if (!drain()) return false;
            requireCurrent.accept(observation);
            var started = Objects.requireNonNull(factory.start(observation, () -> requireCurrent.accept(observation), maxResultBytes), "root work");
            // A misrouted handle may belong to another live observation; never adopt or cancel it.
            if (!started.observation().equals(observation)) throw invalid("rootProcessor.workObservation");
            work = started;
            return true;
        });
    }

    @Override public boolean tryProcess() { return guarded(this::drain); }
    @Override public boolean complete() { return guarded(this::drain); }

    private boolean drain() {
        if (work != null) {
            var completed = work.takeResult();
            requireCurrent.accept(work.observation());
            // Completion may race this poll. The next cooperative turn observes the ready result.
            if (completed.isEmpty()) return false;
            pending = completed.orElseThrow();
            work = null;
            if (pending.accountedBytes() > maxResultBytes) throw new TapstateException(ValidationError.VALUE_LIMIT_EXCEEDED,
                    Map.of("limit", "rootProcessor.resultBytes"), null);
        }
        if (pending != null) {
            requireCurrent.accept(pending.observation());
            if (!tryEmit(pending)) return false;
            pending = null;
        }
        return true;
    }

    private boolean guarded(BooleanSupplier action) {
        throwFailure();
        if (closed) throw new IllegalStateException("validation root processor is closed");
        try { return action.getAsBoolean(); }
        catch (RuntimeException | Error error) {
            failure.record(error);
            pending = null;
            if (work != null) {
                try { work.cancel(failure.get()); }
                catch (RuntimeException | Error cleanup) {
                    failure.record(cleanup);
                    ValidationFailureLatch.addContext(failure.get(), cleanup);
                }
            }
            throwFailure();
            throw new IllegalStateException("failed root processor has no diagnosis");
        }
    }

    /** Requests cancellation only; the worker and nested I/O owners release their actual resources. */
    @Override public void close() {
        if (closed) return;
        closed = true;
        pending = null;
        if (work != null) {
            var active = work;
            work = null;
            active.cancel(failure.get() != null ? failure.get() : new TapstateException(ValidationError.EXECUTION_CANCELLED,
                    Map.of("node", run.owner().nodeId()), null));
        }
    }
    private void throwFailure() {
        if (failure.get() instanceof Error error) throw error;
        if (failure.get() instanceof RuntimeException error) throw error;
    }
    private static TapstateException invalid(String field) {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", field), null);
    }
}
