package io.tapstate.app;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

/** Tracks accepted checkpoint writes for one original sink writer. */
final class CheckpointWriteAcks {
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
        try {
            while (inFlight != 0 && failure == null) wait();
            check();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw StorageRuntime.unavailable();
        }
    }
}
