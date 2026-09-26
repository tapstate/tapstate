package io.tapstate.app;

import io.tapstate.control.core.PipelineExplanation.Pending;
import io.tapstate.control.core.PipelineExplanation.PendingReason;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/** Local read projection of accepted or capacity-waiting lifecycle work. */
final class LifecyclePendingRegistry {

    private final ConcurrentHashMap<String, Pending> byPipeline = new ConcurrentHashMap<>();

    void put(String pipelineId, PendingReason reason) {
        byPipeline.put(pipelineId, new Pending(reason));
    }

    Optional<Pending> pending(String pipelineId) {
        return Optional.ofNullable(byPipeline.get(pipelineId));
    }

    void clear(String pipelineId) {
        byPipeline.remove(pipelineId);
    }

    void clearAll() {
        byPipeline.clear();
    }

    void retain(Collection<String> pipelineIds) {
        byPipeline.keySet().retainAll(pipelineIds);
    }
}
