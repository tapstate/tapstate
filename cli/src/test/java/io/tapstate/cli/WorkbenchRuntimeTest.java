package io.tapstate.cli;

import dev.tamboui.tui.event.Event;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalStateException;

class WorkbenchRuntimeTest {

    @Test
    void workerPublicationChangesStateAndDispatchesManagedRedrawOnlyOnTheOwnerThread() throws Exception {
        RecordingScheduler scheduler = new RecordingScheduler();
        AtomicBoolean ownerThread = new AtomicBoolean(true);
        List<Event> dispatched = new ArrayList<>();
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), scheduler, ownerThread::get, dispatched::add);
        WorkbenchSnapshot expected = new WorkbenchSnapshot(2, 7);
        runtime.expectSnapshot(expected);
        ownerThread.set(false);

        Thread worker = new Thread(() -> runtime.publishSnapshot(expected), "test-refresh-worker");
        worker.start();
        worker.join();

        assertThat(runtime.state().snapshot()).isEmpty();
        assertThat(dispatched).isEmpty();
        assertThat(scheduler.pendingCount()).isEqualTo(1);

        ownerThread.set(true);
        scheduler.runNext();

        assertThat(runtime.state().snapshot()).contains(expected);
        assertThat(dispatched).containsExactly(WorkbenchRedrawEvent.INSTANCE);
    }

    @Test
    void scheduledReductionIsRejectedWhenTheCallbackDoesNotOwnTheRenderThread() {
        RecordingScheduler scheduler = new RecordingScheduler();
        AtomicBoolean ownerThread = new AtomicBoolean(true);
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), scheduler, ownerThread::get, event -> {
                });
        WorkbenchSnapshot expected = new WorkbenchSnapshot(3, 11);
        runtime.expectSnapshot(expected);
        ownerThread.set(false);
        runtime.publishSnapshot(expected);

        assertThatIllegalStateException()
                .isThrownBy(scheduler::runNext)
                .withMessage("Workbench state reduction requires the render owner thread");
        assertThat(runtime.state().snapshot()).isEmpty();
    }

    @Test
    void directUiTransitionUsesTheSameOwnerCheckAndPreservesSnapshotIdentity() {
        RecordingScheduler scheduler = new RecordingScheduler();
        AtomicBoolean ownerThread = new AtomicBoolean(false);
        WorkbenchRuntime runtime = new WorkbenchRuntime(
                WorkbenchState.initial(), scheduler, ownerThread::get, event -> {
                });

        assertThatIllegalStateException()
                .isThrownBy(() -> runtime.updateState(state -> state.select(WorkbenchState.WorkbenchTab.PIPELINES)))
                .withMessage("Workbench state reduction requires the render owner thread");

        ownerThread.set(true);

        assertThat(runtime.updateState(state -> state.select(WorkbenchState.WorkbenchTab.PIPELINES))).isTrue();
        assertThat(runtime.state().selectedTab()).isEqualTo(WorkbenchState.WorkbenchTab.PIPELINES);
        assertThat(runtime.state().snapshot()).isEmpty();
    }

    private static final class RecordingScheduler implements LatestOnlyMailbox.Scheduler {
        private final Queue<Runnable> callbacks = new ArrayDeque<>();

        @Override
        public void runLater(Runnable callback) {
            callbacks.add(callback);
        }

        private void runNext() {
            callbacks.remove().run();
        }

        private int pendingCount() {
            return callbacks.size();
        }
    }
}
