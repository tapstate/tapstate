package io.tapstate.app;

import io.tapstate.spi.store.SrsConsumerProgress;
import io.tapstate.spi.store.SrsConsumerRun;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/** Readers registered before a batch starts share its transaction; later readers wait for a fresh batch. */
final class SrsConsumerProgressReads {
    static final String USER_CONTEXT_KEY = "tapstate.scoped-srs-progress-reads";
    private final ConcurrentHashMap<SrsConsumerRun, Queue> pending = new ConcurrentHashMap<>();

    SrsConsumerProgress read(SrsConsumerRun run, Supplier<SrsConsumerProgress> lookup) {
        var ticket = new AtomicReference<Ticket>();
        pending.compute(run, (ignored, queue) -> {
            if (queue == null) {
                var first = new Batch(); first.ready.complete(null); ticket.set(new Ticket(first, true));
                return new Queue(first);
            }
            boolean owner = queue.waiting == null;
            if (owner) queue.waiting = new Batch();
            ticket.set(new Ticket(queue.waiting, owner)); return queue;
        });
        var registered = ticket.get(); var batch = registered.batch();
        if (!registered.owner()) return await(batch.result);
        batch.ready.join();
        try {
            var result = lookup.get(); batch.result.complete(result); return result;
        } catch (RuntimeException | Error failure) {
            // Keep the original failure intact even when it is itself a CompletionException.
            batch.result.completeExceptionally(new CompletionException(failure)); throw failure;
        } finally {
            completeBatch(run, batch);
        }
    }

    private void completeBatch(SrsConsumerRun run, Batch batch) {
        pending.compute(run, (ignored, queue) -> {
            if (queue == null || queue.running != batch) throw new IllegalStateException("progress read lost its original queue");
            if (queue.waiting == null) return null;
            queue.running = queue.waiting; queue.waiting = null;
            queue.running.ready.complete(null); return queue;
        });
    }

    private static final class Queue {
        Batch running;
        Batch waiting;
        Queue(Batch first) { running = first; }
    }
    private static final class Batch {
        final CompletableFuture<Void> ready = new CompletableFuture<>();
        final CompletableFuture<SrsConsumerProgress> result = new CompletableFuture<>();
    }
    private record Ticket(Batch batch, boolean owner) {}

    private static SrsConsumerProgress await(CompletableFuture<SrsConsumerProgress> current) {
        try { return current.join(); }
        catch (CompletionException wrapped) {
            if (wrapped.getCause() instanceof RuntimeException failure) throw failure;
            if (wrapped.getCause() instanceof Error failure) throw failure;
            throw new IllegalStateException("a synchronous progress read produced a checked failure", wrapped.getCause());
        }
    }
}
