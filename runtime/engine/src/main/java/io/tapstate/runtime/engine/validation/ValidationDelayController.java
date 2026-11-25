package io.tapstate.runtime.engine.validation;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.validation.ValidationError;
import io.tapstate.spi.store.ValidationControlStore;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Applies one saved scheduling tuple to an existing owner-local queue, then confirms that exact
 * application. Calls belong on admitted control I/O. No store call holds the queue lock. A throw or
 * lost response retains the original operation for reconciliation; it never recreates a queue or
 * claims that the latest saved configuration is already running. Persistent applied facts describe
 * the last confirmed tuple and must be interpreted together with the original run's current state.
 */
public final class ValidationDelayController {
    public enum Application { ACKNOWLEDGED, STALE, CLOSED }
    private final ValidationControlStore control;
    private final ValidationControlStore.Run run;
    private final ValidationCandidateQueue queue;
    private final Runnable requireAuthorized;

    public ValidationDelayController(ValidationControlStore control, ValidationControlStore.Run run,
            ValidationCandidateQueue queue, Runnable requireAuthorized) {
        this.control = Objects.requireNonNull(control); this.run = Objects.requireNonNull(run);
        this.queue = Objects.requireNonNull(queue); this.requireAuthorized = Objects.requireNonNull(requireAuthorized);
        if (run.admission() == null || !run.identity().equals(queue.run())) throw invalid();
    }

    public Application apply(ValidationControlStore.Delay change) {
        Objects.requireNonNull(change);
        if (!run.identity().equals(change.run().identity()) || run.countingGeneration() != change.run().countingGeneration()
                || !run.admission().equals(change.run().admission())) throw invalid();
        requireAuthorized.run();
        if (queue.delayState().closed()) return Application.CLOSED;
        if (control.requestDelay(change) == ValidationControlStore.Mutation.STALE) return Application.STALE;
        requireAuthorized.run();
        var local = queue.applyDelay(new ValidationCandidateQueue.DelayChange(run.identity(), change.schedulingRevision(),
                change.commandId(), TimeUnit.MILLISECONDS.toNanos(change.firstCheckDelayMillis())));
        if (local == ValidationCandidateQueue.DelayApplication.CLOSED) return Application.CLOSED;
        if (local == ValidationCandidateQueue.DelayApplication.STALE) return Application.STALE;
        requireAuthorized.run();
        if (queue.delayState().closed()) return Application.CLOSED;
        if (control.acknowledgeDelay(change) == ValidationControlStore.Mutation.STALE) return Application.STALE;
        requireAuthorized.run();
        return queue.delayState().closed() ? Application.CLOSED : Application.ACKNOWLEDGED;
    }

    private static TapstateException invalid() {
        return new TapstateException(ValidationError.INVALID_CONTRACT, Map.of("field", "control.delayQueue"), null);
    }
}
