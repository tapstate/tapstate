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
    private static final ThreadLocal<Boolean> WORKER = new ThreadLocal<>();
    private static final ThreadLocal<Long> DEADLINE = new ThreadLocal<>();
    private final AtomicReference<CompletableFuture<Void>> attempt = new AtomicReference<>();

    static long deadline() {
        Long inherited = DEADLINE.get();
        return inherited == null ? System.nanoTime() + Duration.ofSeconds(10).toNanos() : inherited;
    }

    static Thread daemon(Runnable task, String name) {
        var thread = new Thread(() -> {
            WORKER.set(true);
            try { task.run(); } finally { WORKER.remove(); }
        }, name);
        thread.setDaemon(true);
        return thread;
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

    static void future(CompletableFuture<?> future, long deadline) {
        try { future.get(future.isDone()?0:remaining(deadline), TimeUnit.NANOSECONDS); }
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
            Long previous = DEADLINE.get();
            try {
                remaining(deadline);
                DEADLINE.set(deadline);
                action.run();
                proposed.complete(null);
            } catch (RuntimeException | Error failure) {
                proposed.completeExceptionally(failure);
                // A later explicit cleanup attempt keeps the same owners, with a fresh allowance.
                attempt.compareAndSet(proposed, null);
                throw failure;
            } finally {
                if (previous == null) DEADLINE.remove(); else DEADLINE.set(previous);
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
