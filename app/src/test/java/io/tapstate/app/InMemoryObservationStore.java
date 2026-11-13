package io.tapstate.app;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.ObservationStore;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** Concurrent {@link ObservationStore} double for the convergence wiring tests. */
final class InMemoryObservationStore implements ObservationStore {

    private final Map<String, Observation> docs = new java.util.concurrent.ConcurrentHashMap<>();
    private final Set<String> failingSaves = java.util.concurrent.ConcurrentHashMap.newKeySet();

    void failSaveFor(String pipelineId) {
        failingSaves.add(pipelineId);
    }

    @Override
    public void save(Observation observation) {
        if (failingSaves.contains(observation.pipelineId())) {
            throw new IllegalStateException("observation save failed for " + observation.pipelineId());
        }
        docs.put(observation.pipelineId(), observation);
    }

    @Override
    public Optional<Observation> read(String pipelineId) {
        return Optional.ofNullable(docs.get(pipelineId));
    }

    @Override
    public void delete(String pipelineId) {
        docs.remove(pipelineId);
    }
}
