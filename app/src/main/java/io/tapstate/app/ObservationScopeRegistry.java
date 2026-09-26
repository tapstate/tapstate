package io.tapstate.app;

import io.tapstate.spi.store.ObservationStore;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** The execution identity already acquired at start, retained for local observation publishes. */
final class ObservationScopeRegistry {

    private final ConcurrentHashMap<String, ObservationStore.Scope> current = new ConcurrentHashMap<>();

    ObservationStore.Scope begin(String pipelineId, String incarnation, long generation) {
        ObservationStore.Scope scope = new ObservationStore.Scope(incarnation, generation);
        current.put(Objects.requireNonNull(pipelineId, "pipelineId"), scope);
        return scope;
    }

    Optional<ObservationStore.Scope> current(String pipelineId) {
        return Optional.ofNullable(current.get(pipelineId));
    }

    void discard(String pipelineId, ObservationStore.Scope scope) {
        current.remove(pipelineId, scope);
    }

    void retain(Collection<String> pipelineIds) {
        current.keySet().retainAll(pipelineIds);
    }
}
