package io.tapstate.cli;

import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** Runs one activation action at a time and returns only sanitized typed outcomes to the UI thread. */
final class WorkbenchActionCoordinator implements AutoCloseable {

    private final WorkbenchRuntime runtime;
    private final ThreadPoolExecutor worker;
    private Future<?> active;
    private boolean closed;

    WorkbenchActionCoordinator(WorkbenchRuntime runtime) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        this.worker = new ThreadPoolExecutor(
                1,
                1,
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1),
                task -> {
                    Thread thread = new Thread(task, "tapstate-action");
                    thread.setDaemon(true);
                    return thread;
                },
                new ThreadPoolExecutor.AbortPolicy());
    }

    synchronized <T> void submit(
            Supplier<T> action, Function<RuntimeException, T> failureResult, Consumer<T> completion) {
        Objects.requireNonNull(action, "action");
        Objects.requireNonNull(failureResult, "failureResult");
        Objects.requireNonNull(completion, "completion");
        if (closed) {
            throw new IllegalStateException("Workbench action coordinator is closed");
        }
        if (active != null) {
            active.cancel(true);
        }
        active = worker.submit(() -> {
            T result;
            try {
                result = action.get();
            } catch (RuntimeException failure) {
                result = failureResult.apply(failure);
            }
            T completed = result;
            runtime.runLater(() -> completeIfOpen(completed, completion));
        });
    }

    private synchronized <T> void completeIfOpen(T result, Consumer<T> completion) {
        if (!closed) {
            completion.accept(result);
        }
    }

    @Override
    public synchronized void close() {
        closed = true;
        if (active != null) {
            active.cancel(true);
            active = null;
        }
        worker.shutdownNow();
    }
}
