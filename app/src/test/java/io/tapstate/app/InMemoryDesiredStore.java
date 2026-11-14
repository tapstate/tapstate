package io.tapstate.app;

import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.spi.store.DesiredStore;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** In-memory {@link DesiredStore} double for the convergence wiring tests; insertion-ordered. */
final class InMemoryDesiredStore implements DesiredStore {

    private final Map<String, DesiredState> docs = new LinkedHashMap<>();

    @Override
    public synchronized void save(DesiredState desired) {
        docs.put(desired.pipelineId(), desired);
    }

    void remove(String pipelineId) {
        delete(pipelineId);
    }

    @Override
    public synchronized void delete(String pipelineId) {
        docs.remove(pipelineId);
    }

    @Override
    public synchronized Optional<DesiredState> read(String pipelineId) {
        return Optional.ofNullable(docs.get(pipelineId));
    }

    @Override
    public synchronized List<String> pipelineIds() {
        return new ArrayList<>(docs.keySet());
    }
}
