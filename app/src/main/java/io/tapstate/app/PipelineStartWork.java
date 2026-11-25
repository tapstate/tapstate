package io.tapstate.app;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.slf4j.MDC;

/** One optional sibling for a start, sharing two helper threads across the member. */
final class PipelineStartWork<T> implements AutoCloseable {
    private static final ThreadPoolExecutor HELPERS = new ThreadPoolExecutor(0, 2, 30, TimeUnit.SECONDS,
            new SynchronousQueue<>(), task -> ShutdownAwait.daemon(task, "pipeline-start-helper"));

    private CompletableFuture<T> pending;

    void tryStart(Supplier<T> action) {
        var context = MDC.getCopyOfContextMap();
        try {
            pending = CompletableFuture.supplyAsync(() -> {
                if (context != null) MDC.setContextMap(context);
                try {
                    return action.get();
                } finally {
                    MDC.clear();
                }
            }, HELPERS);
        } catch (RejectedExecutionException busy) {
            // An occupied member leaves this work in the original sequential order.
        }
    }

    T awaitOrRun(Supplier<T> sequential) {
        if (pending == null) return sequential.get();
        var accepted = pending;
        pending = null;
        try {
            return accepted.join();
        } catch (CompletionException failure) {
            if (failure.getCause() instanceof RuntimeException runtime) throw runtime;
            if (failure.getCause() instanceof Error error) throw error;
            throw new IllegalStateException("startup work produced a checked failure", failure.getCause());
        }
    }

    @Override public void close() {
        if (pending != null) awaitOrRun(() -> null);
    }
}
