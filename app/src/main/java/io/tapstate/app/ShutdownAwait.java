package io.tapstate.app;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

/** A shutdown attempt shares one result and one monotonic allowance across all owned workers. */
final class ShutdownAwait {
    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(ShutdownAwait.class);
    private static final ThreadLocal<Boolean> WORKER = new ThreadLocal<>();
    private static final ThreadLocal<Long> DEADLINE = new ThreadLocal<>();
    private final AtomicReference<CompletableFuture<Void>> attempt = new AtomicReference<>();

    static long deadline() {
        Long inherited = DEADLINE.get();
        return inherited == null ? System.nanoTime() + Duration.ofSeconds(10).toNanos() : inherited;
    }

    static Long currentDeadline() { return DEADLINE.get(); }

    static void within(Duration allowance, Runnable action) {
        long limit = System.nanoTime() + allowance.toNanos();
        Long inherited = DEADLINE.get();
        if (inherited == null || inherited - limit > 0) runWithDeadline(limit, action);
        else action.run();
    }

    private static void runWithDeadline(long deadline, Runnable action) {
        Long previous = DEADLINE.get();
        DEADLINE.set(deadline);
        try { action.run(); }
        finally {
            if (previous == null) DEADLINE.remove();
            else DEADLINE.set(previous);
        }
    }

    static Thread daemon(Runnable task, String name) {
        var thread = new Thread(workerTask(task), name);
        thread.setDaemon(true);
        return thread;
    }

    static Thread virtual(Runnable task, String name) {
        return Thread.ofVirtual().name(name).unstarted(workerTask(task));
    }

    private static Runnable workerTask(Runnable task) {
        return () -> {
            WORKER.set(true);
            try { task.run(); } finally { WORKER.remove(); }
        };
    }

    static long remaining(long deadline) {
        long remaining = deadline - System.nanoTime();
        if (remaining <= 0 || Thread.currentThread().isInterrupted() || Boolean.TRUE.equals(WORKER.get())) throw StorageRuntime.unavailable();
        return remaining;
    }

    static void executor(ExecutorService worker, long deadline) {
        if(worker.isTerminated())return;
        try {
            if (!worker.awaitTermination(remaining(deadline), TimeUnit.NANOSECONDS)) throw StorageRuntime.unavailable();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw StorageRuntime.unavailable();
        }
    }

    static <T> T future(CompletableFuture<T> future, long deadline) {
        try { return future.get(future.isDone()?0:remaining(deadline), TimeUnit.NANOSECONDS); }
        catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();throw StorageRuntime.unavailable();
        } catch (ExecutionException failure) {
            if(failure.getCause() instanceof Error error)throw error;
            if(failure.getCause() instanceof RuntimeException runtime)throw runtime;
            throw new IllegalStateException("shutdown produced a checked failure",failure.getCause());
        } catch (TimeoutException failure) {
            var unavailable=StorageRuntime.unavailable();unavailable.addSuppressed(failure);throw unavailable;
        }
    }

    @SuppressWarnings("java:S1181") // A failed stage is logged and the original throwable is rethrown.
    static void stage(String name, Runnable action) {
        try {
            action.run();
        } catch (RuntimeException | Error failure) {
            LOG.error("Shutdown stage failed [{}]", name, failure);
            throw failure;
        }
    }

    static void all(Iterable<? extends Runnable> actions) {
        Throwable failure=null;
        for(var action:actions)try{action.run();}catch(RuntimeException|Error stopped){
            if(failure==null)failure=stopped;
            else if(stopped instanceof Error && !(failure instanceof Error)){stopped.addSuppressed(failure);failure=stopped;}
            else if(failure!=stopped)failure.addSuppressed(stopped);
        }
        if(failure instanceof Error error)throw error;
        if(failure instanceof RuntimeException runtime)throw runtime;
    }

    void complete(long deadline, Runnable action) {
        var proposed = new CompletableFuture<Void>();
        var current = attempt.compareAndExchange(null, proposed);
        if (current == null) {
            try {
                remaining(deadline);
                runWithDeadline(deadline, action);
                proposed.complete(null);
            } catch (RuntimeException | Error failure) {
                proposed.completeExceptionally(failure);
                // A later explicit cleanup attempt keeps the same owners, with a fresh allowance.
                attempt.compareAndSet(proposed, null);
                throw failure;
            }
        } else {
            try { current.get(current.isDone()?0:remaining(deadline), TimeUnit.NANOSECONDS); }
            catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw StorageRuntime.unavailable();
            } catch (TimeoutException timeout) { throw StorageRuntime.unavailable(); }
            catch (ExecutionException failure) {
                if (failure.getCause() instanceof Error error) throw error;
                if (failure.getCause() instanceof RuntimeException runtime) throw runtime;
                throw new IllegalStateException("shutdown produced a checked failure", failure.getCause());
            }
        }
    }
}
