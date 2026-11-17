package io.tapstate.app;

import java.util.Collection;
import java.util.Set;

/** Keeps lifecycle work single-flight per Pipeline and off the scheduling thread. */
final class PipelineConvergences implements AutoCloseable {

    enum Offer { ACCEPTED, COALESCED, REJECTED }

    private final PeriodicObservations work;

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

    Set<String> pipelineIds() {
        return work.pipelineIds();
    }

    void retain(Collection<String> ids) {
        work.retain(ids);
    }

    @Override
    public void close() {
        work.close();
    }
}
