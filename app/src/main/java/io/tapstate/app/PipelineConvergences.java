package io.tapstate.app;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;

/** Keeps lifecycle work single-flight per Pipeline and off the scheduling thread. */
final class PipelineConvergences implements AutoCloseable {

    enum Offer { ACCEPTED, COALESCED, REJECTED }

    private final PeriodicObservations work;
    private final Set<String> activeStops = new HashSet<>();
    // A connector may hold a JVM monitor while blocked in I/O. Keep it off both the four
    // convergence slots and the virtual-thread carriers used by other runtime work.
    private final java.util.concurrent.ExecutorService stopOperations = Executors.newThreadPerTaskExecutor(
            Thread.ofPlatform().daemon(true).name("tapstate-pipeline-stop-", 1).factory());
    private boolean closing;

    PipelineConvergences() {
        this(4, 64);
    }

    PipelineConvergences(int concurrency, int capacity) {
        work = new PeriodicObservations(concurrency, capacity, PipelineConvergences.class,
                "Lifecycle convergence", "tapstate-pipeline-convergence-");
    }

    PeriodicObservations.Health health() {
        return work.health();
    }

    Offer offer(String pipelineId, Runnable task) {
        return Offer.valueOf(work.offer(pipelineId, pipelineId, ignored -> task.run()).name());
    }

    synchronized boolean stopping(String pipelineId) {
        return activeStops.contains(pipelineId);
    }

    synchronized void beginStop(String pipelineId, Runnable operation) {
        if (closing || !activeStops.add(pipelineId)) return;
        stopOperations.execute(() -> {
            try { operation.run(); }
            finally {
                synchronized (this) { activeStops.remove(pipelineId); }
            }
        });
    }

    synchronized Set<String> pipelineIds() {
        var pipelines = new HashSet<>(work.pipelineIds());
        pipelines.addAll(activeStops);
        return Set.copyOf(pipelines);
    }

    void retain(Collection<String> ids) {
        work.retain(ids);
    }

    @Override
    public synchronized void close() {
        closing = true;
        work.close();
        stopOperations.shutdownNow();
    }

    void awaitStopped(long deadline) {
        work.awaitStopped(deadline);
        ShutdownAwait.executor(stopOperations, deadline);
    }
}
