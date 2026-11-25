package io.tapstate.runtime.engine;

import io.tapstate.spi.sink.SinkWriter;
import java.util.*;
import java.util.concurrent.*;

/** Owns independent opens until all succeeded, including a result returned after cancellation. */
final class ParallelSinkOpenings {
    @FunctionalInterface interface Opener { SinkWriter open(int index) throws Exception; }
    private final SinkWriter[] writers;
    private final ExecutorService workers;
    private Throwable aborted;

    private ParallelSinkOpenings(int count, int parallelism) {
        writers = new SinkWriter[count];
        workers = Executors.newFixedThreadPool(parallelism,
                Thread.ofPlatform().daemon(true).name("tapstate-sink-open-", 1).factory());
    }

    static List<SinkWriter> open(int first, int count, int parallelism, Opener opener) throws Exception {
        var batch = new ParallelSinkOpenings(count, parallelism);
        var results = new ArrayList<CompletableFuture<SinkWriter>>(count);
        try {
            for (int i = 0; i < count; i++) {
                int slot = i;
                var result = new CompletableFuture<SinkWriter>(); results.add(result);
                batch.workers.execute(() -> {
                    try {
                        var writer = Objects.requireNonNull(opener.open(first + slot));
                        Throwable failed;
                        synchronized (batch) {
                            failed = batch.aborted;
                            if (failed == null) batch.writers[slot] = writer;
                        }
                        if (failed != null) close(writer, failed);
                        result.complete(writer);
                    } catch (Throwable failure) { result.completeExceptionally(failure); }
                });
            }
            var ordered = new ArrayList<SinkWriter>(count);
            for (var result : results) ordered.add(result.get());
            batch.workers.shutdown();
            return ordered;
        } catch (Throwable failure) {
            Throwable original = failure instanceof ExecutionException wrapper ? wrapper.getCause() : failure;
            if (original instanceof InterruptedException) Thread.currentThread().interrupt();
            SinkWriter[] opened;
            synchronized (batch) { batch.aborted = original; opened = batch.writers.clone(); Arrays.fill(batch.writers, null); }
            batch.workers.shutdownNow();
            for (var result : results) result.whenComplete((value, other) -> {
                if (other != null && other != original) original.addSuppressed(other);
            });
            for (var writer : opened) if (writer != null) close(writer, original);
            if (original instanceof InterruptedException) Thread.currentThread().interrupt();
            if (original instanceof Exception exception) throw exception;
            if (original instanceof Error error) throw error;
            throw new IllegalStateException("unexpected opening failure", original);
        }
    }

    static void close(SinkWriter writer, Throwable failure) {
        try { writer.close(); } catch (Throwable closing) { if (closing != failure) failure.addSuppressed(closing); }
    }
}
