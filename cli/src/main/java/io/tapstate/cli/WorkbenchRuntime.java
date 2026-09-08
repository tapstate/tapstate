package io.tapstate.cli;

import dev.tamboui.tui.event.Event;

import java.util.Objects;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

/** Owns workbench reduction and effect interpretation for one render-thread lifecycle. */
final class WorkbenchRuntime {

    private static final String WRONG_THREAD_MESSAGE =
            "Workbench state reduction requires the render owner thread";

    private final WorkbenchReducer<WorkbenchState> reducer;
    private final LatestOnlyMailbox.Scheduler scheduler;
    private final BooleanSupplier ownerThread;
    private final Consumer<Event> dispatch;
    private final LatestOnlyMailbox<WorkbenchEvent.SnapshotPublished> mailbox;
    private WorkbenchState state;

    WorkbenchRuntime(
            WorkbenchState initialState,
            LatestOnlyMailbox.Scheduler scheduler,
            BooleanSupplier ownerThread,
            Consumer<Event> dispatch) {
        this.state = Objects.requireNonNull(initialState, "initialState");
        this.reducer = WorkbenchReducer.workbench();
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.ownerThread = Objects.requireNonNull(ownerThread, "ownerThread");
        this.dispatch = Objects.requireNonNull(dispatch, "dispatch");
        this.mailbox = new LatestOnlyMailbox<>(this.scheduler, this::reduce);
    }

    WorkbenchState state() {
        return state;
    }

    void expectSnapshot(WorkbenchSnapshot snapshot) {
        reduce(new WorkbenchEvent.SnapshotExpected(snapshot));
    }

    void publishSnapshot(WorkbenchSnapshot snapshot) {
        mailbox.publish(new WorkbenchEvent.SnapshotPublished(snapshot));
    }

    boolean updateState(UnaryOperator<WorkbenchState> transition) {
        requireOwnerThread();
        WorkbenchState next = Objects.requireNonNull(
                Objects.requireNonNull(transition, "transition").apply(state),
                "Workbench state transition returned null");
        if (next == state) {
            return false;
        }
        state = next;
        return true;
    }

    void runLater(Runnable callback) {
        scheduler.runLater(callback);
    }

    private void reduce(WorkbenchEvent event) {
        requireOwnerThread();
        WorkbenchReducer.Reduction<WorkbenchState> reduction = reducer.reduce(state, event);
        state = reduction.state();
        for (WorkbenchEffect effect : reduction.effects()) {
            switch (effect) {
                case WorkbenchEffect.Render ignored -> dispatch.accept(WorkbenchRedrawEvent.INSTANCE);
            }
        }
    }

    private void requireOwnerThread() {
        if (!ownerThread.getAsBoolean()) {
            throw new IllegalStateException(WRONG_THREAD_MESSAGE);
        }
    }
}
