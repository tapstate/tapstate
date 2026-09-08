package io.tapstate.cli;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Runs refresh operations on one bounded worker and publishes completed immutable results only to an
 * injected sink. The sink is the integration boundary to the UI-thread mailbox; this class has no
 * terminal, frame, or UI-state dependency.
 */
final class RefreshCoordinator implements AutoCloseable {

    private static final String CLOSED_MESSAGE = "Refresh coordinator is closed";

    private final Object lock = new Object();
    private final Consumer<RefreshResult> sink;
    private final Consumer<Throwable> failureObserver;
    private final AtomicReference<Throwable> workerFailure = new AtomicReference<>();
    private final ThreadPoolExecutor worker;

    private long contextGeneration;
    private long requestSequence;
    private RefreshRequest activeRequest;
    private FutureTask<Void> activeTask;
    private int callbacksInProgress;
    private boolean closed;

    RefreshCoordinator(Consumer<RefreshResult> sink) {
        this(sink, failure -> {
        });
    }

    RefreshCoordinator(Consumer<RefreshResult> sink, Consumer<Throwable> failureObserver) {
        this.sink = Objects.requireNonNull(sink, "sink");
        this.failureObserver = Objects.requireNonNull(failureObserver, "failureObserver");
        this.worker = new ThreadPoolExecutor(
                1,
                1,
                0,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(1),
                refreshThreadFactory(),
                new ThreadPoolExecutor.AbortPolicy());
    }

    RefreshRequest refresh(RefreshRequest.Work work) {
        Objects.requireNonNull(work, "work");
        synchronized (lock) {
            ensureOpen();
            cancelActiveLocked();

            long nextSequence = Math.incrementExact(requestSequence);
            RefreshRequest request = new RefreshRequest(
                    contextGeneration,
                    nextSequence,
                    new RefreshRequest.CancellationToken(),
                    work);
            FutureTask<Void> task = new FutureTask<>(() -> {
                run(request);
                return null;
            });
            activeRequest = request;
            activeTask = task;
            requestSequence = nextSequence;
            try {
                worker.execute(task);
            } catch (RejectedExecutionException rejected) {
                request.cancellationToken().cancel();
                activeRequest = null;
                activeTask = null;
                throw new IllegalStateException(CLOSED_MESSAGE, rejected);
            }
            return request;
        }
    }

    long advanceContext() {
        synchronized (lock) {
            ensureOpen();
            cancelActiveLocked();
            contextGeneration = Math.incrementExact(contextGeneration);
            return contextGeneration;
        }
    }

    long contextGeneration() {
        synchronized (lock) {
            return contextGeneration;
        }
    }

    long latestRequestSequence() {
        synchronized (lock) {
            return requestSequence;
        }
    }

    Optional<Throwable> workerFailure() {
        return Optional.ofNullable(workerFailure.get());
    }

    private void run(RefreshRequest request) {
        RefreshResult result;
        try {
            RefreshResult.Outcome outcome = request.work().load(
                    request.contextGeneration(), request.requestSequence(), request.cancellationToken());
            Objects.requireNonNull(outcome, "Refresh work returned no outcome");
            result = new RefreshResult(
                    request.contextGeneration(), request.requestSequence(), outcome);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            if (request.cancellationToken().isCancelled()) {
                clearOwnership(request);
                return;
            }
            result = unavailable(request);
        } catch (Exception ignored) {
            result = unavailable(request);
        } catch (Error failure) {
            clearOwnership(request);
            observeFailure(failure);
            return;
        }

        publishIfCurrent(request, result);
    }

    private RefreshResult unavailable(RefreshRequest request) {
        return new RefreshResult(
                request.contextGeneration(),
                request.requestSequence(),
                RefreshResult.diagnostic(CliError.WORKBENCH_UNAVAILABLE, Map.of()));
    }

    private void publishIfCurrent(RefreshRequest request, RefreshResult result) {
        boolean publish;
        synchronized (lock) {
            publish = activeRequest == request
                    && !closed
                    && !request.cancellationToken().isCancelled()
                    && result.isCurrent(contextGeneration, requestSequence);
            clearIfActiveLocked(request);
            if (publish) {
                callbacksInProgress++;
            }
        }
        if (!publish) {
            return;
        }
        try {
            sink.accept(result);
        } catch (RuntimeException | Error failure) {
            observeFailure(failure);
        } finally {
            completeCallback();
        }
    }

    private void clearOwnership(RefreshRequest request) {
        synchronized (lock) {
            clearIfActiveLocked(request);
        }
    }

    private void observeFailure(Throwable failure) {
        workerFailure.compareAndSet(null, failure);
        synchronized (lock) {
            if (closed) {
                return;
            }
            callbacksInProgress++;
        }
        try {
            failureObserver.accept(failure);
        } finally {
            completeCallback();
        }
    }

    private void completeCallback() {
        synchronized (lock) {
            callbacksInProgress--;
            if (callbacksInProgress == 0) {
                lock.notifyAll();
            }
        }
    }

    private void cancelActiveLocked() {
        if (activeRequest != null) {
            activeRequest.cancellationToken().cancel();
        }
        if (activeTask != null) {
            activeTask.cancel(true);
            worker.remove(activeTask);
        }
        activeRequest = null;
        activeTask = null;
    }

    private void clearIfActiveLocked(RefreshRequest request) {
        if (activeRequest == request) {
            activeRequest = null;
            activeTask = null;
        }
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException(CLOSED_MESSAGE);
        }
    }

    @Override
    public void close() {
        boolean interrupted = false;
        synchronized (lock) {
            if (!closed) {
                closed = true;
                cancelActiveLocked();
                worker.shutdownNow();
            }
            while (callbacksInProgress > 0) {
                try {
                    lock.wait();
                } catch (InterruptedException interruption) {
                    interrupted = true;
                }
            }
        }
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static ThreadFactory refreshThreadFactory() {
        return task -> {
            Thread thread = new Thread(task, "tapstate-refresh");
            thread.setDaemon(true);
            return thread;
        };
    }
}
