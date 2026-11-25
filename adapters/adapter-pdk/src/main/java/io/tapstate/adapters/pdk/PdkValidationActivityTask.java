package io.tapstate.adapters.pdk;

import io.tapstate.core.validation.ValidationFailureLatch;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;

/** Future cancellation and actual body exit are separate facts, including interrupt-ignoring codecs. */
final class PdkValidationActivityTask extends FutureTask<Void> {
    private static final class State {
        final AtomicBoolean entered = new AtomicBoolean();
        final ValidationFailureLatch failure = new ValidationFailureLatch();
        final CompletableFuture<Void> exited = new CompletableFuture<>();
    }
    private final State state;

    PdkValidationActivityTask(Callable<Void> body) { this(new State(), body); }
    private PdkValidationActivityTask(State state, Callable<Void> body) {
        super(() -> {
            try { return body.call(); }
            catch (Exception | Error failure) { state.failure.record(failure); throw failure; }
        });
        this.state = state;
    }
    CompletionStage<Void> exited() { return state.exited.minimalCompletionStage(); }

    @Override public void run() {
        if (!state.entered.compareAndSet(false, true)) return;
        try { super.run(); }
        finally { completeExit(); }
    }
    @Override public boolean cancel(boolean mayInterruptIfRunning) {
        boolean cancelled = super.cancel(mayInterruptIfRunning);
        // If run has not entered, successful FutureTask cancellation prevents the body from starting.
        // The executor's bounded queue may still contain the now payload-free task shell.
        if (cancelled && !state.entered.get()) completeExit();
        return cancelled;
    }
    private void completeExit() {
        Throwable failure = state.failure.get();
        if (failure == null) state.exited.complete(null);
        else state.exited.completeExceptionally(failure);
    }
}
