package io.tapstate.app;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/** Tracks accepted checkpoint writes for one original sink writer. */
final class CheckpointWriteAcks {
    private static final Duration CLOSE_ALLOWANCE = Duration.ofSeconds(20);

    private int inFlight;
    private Throwable failure;

    synchronized void track(CompletableFuture<Boolean> write, Supplier<? extends RuntimeException> refused) {
        inFlight++;
        write.whenComplete((committed, error) -> {
            if (error == null && !Boolean.TRUE.equals(committed)) finished(refused.get());
            else finished(error);
        });
    }

    private synchronized void finished(Throwable error) {
        if (failure == null) failure = error;
        inFlight--;
        notifyAll();
    }

    synchronized void check() {
        if (failure instanceof RuntimeException refused) throw refused;
        if (failure instanceof Error fatal) throw fatal;
        if (failure != null) throw new IllegalStateException("checkpoint write failed with a checked exception", failure);
    }

    synchronized void await() {
        Long closing = ShutdownAwait.currentDeadline();
        try {
            while (inFlight != 0 && failure == null) {
                if (closing == null) wait();
                else TimeUnit.NANOSECONDS.timedWait(this, ShutdownAwait.remaining(closing));
            }
            check();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw StorageRuntime.unavailable();
        }
    }

    static void close(Runnable flush) { ShutdownAwait.within(CLOSE_ALLOWANCE, flush); }
}
