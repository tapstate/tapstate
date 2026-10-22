package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.RetiredPipelinePayloads;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.PipelineIntentScope;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Retries bounded, already-retired payload cleanup without blocking live Pipeline convergence or leases. */
final class RetiredPipelinePayloadCleanup implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(RetiredPipelinePayloadCleanup.class);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final RetiredPipelinePayloads payloads;
    private final Supplier<Optional<WorkloadClaimFence>> node;
    private final BooleanSupplier eligible;
    private ScheduledExecutorService worker;
    private PipelineIntentScope after;

    RetiredPipelinePayloadCleanup(Function<Runnable, RetiredPipelinePayloads> factory,
            Supplier<Optional<WorkloadClaimFence>> node, BooleanSupplier eligible) {
        this.node = Objects.requireNonNull(node); this.eligible = Objects.requireNonNull(eligible);
        this.payloads = Objects.requireNonNull(factory.apply(this::requireOpen));
    }

    synchronized void start() {
        requireOpen();
        if (worker != null) throw new IllegalStateException("cleanup already started");
        worker = Executors.newSingleThreadScheduledExecutor(task -> {
            var thread = new Thread(task, "tapstate-retired-payload-cleanup"); thread.setDaemon(true); return thread;
        });
        worker.scheduleWithFixedDelay(this::runOnce, 0, 5, TimeUnit.SECONDS);
    }

    void runOnce() {
        if (closed.get()) return;
        PipelineIntentScope selected = null;
        try {
            if (!eligible.getAsBoolean()) return;
            var originalNode = node.get(); if (originalNode.isEmpty()) return;
            requireOpen(); var page = payloads.listAfter(after, 1); requireOpen();
            if (page.isEmpty()) { after = null; return; }
            selected = page.getFirst();
            var original = payloads.find(selected).orElseThrow(() -> new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null));
            if (!selected.equals(original.scope())) throw new IllegalStateException("cleanup inventory belongs to another scope");
            requireOpen(); long removed = payloads.reclaim(original, originalNode.orElseThrow()); requireOpen();
            if (removed < 0 || removed > 256) throw new IllegalStateException("cleanup exceeded its batch bound");
            // A full batch may leave more of this finite retired payload; continue it on the next pass.
            if (removed < 256) after = selected;
        } catch (TapstateException refused) {
            // Move past a failed original for this sweep; the next wrap retries its unchanged inventory.
            if (selected != null) after = selected;
            if (!closed.get()) LOG.warn("Retired payload cleanup remains pending (pipeline={}, generation={}, code={})",
                    selected == null ? "unreadable" : selected.pipelineId(),
                    selected == null ? "unreadable" : selected.stateGeneration(), refused.code().code());
        } catch (RuntimeException | Error failure) {
            LOG.error("Retired payload cleanup stopped after an unexpected failure", failure);
            throw failure;
        }
    }

    private void requireOpen() {
        if (closed.get()) throw new TapstateException(IoError.CACHE_BUSINESS_FENCED, Map.of(), null);
    }

    @Override public void close() {
        ScheduledExecutorService ending;
        synchronized (this) { closed.set(true); ending = worker; if (ending != null) ending.shutdownNow(); }
        if (ending == null) return;
        try {
            if (!ending.awaitTermination(5, TimeUnit.SECONDS))
                throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of("detail", "retired payload worker has not stopped"), null);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt(); throw new TapstateException(IoError.STORE_UNAVAILABLE,
                    Map.of("detail", "retired payload worker shutdown was interrupted"), interrupted);
        }
    }
}
