package io.tapstate.app;

import io.tapstate.control.core.PipelineExplanation.Pending;
import io.tapstate.control.core.PipelineExplanation.PendingReason;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import io.tapstate.core.lifecycle.PipelineState;

/** Local read projection of accepted or capacity-waiting lifecycle work. */
final class LifecyclePendingRegistry {

    private record Entry(Pending pending, PipelineState capacityTarget, long capacitySinceNanos) {
    }

    private final ConcurrentHashMap<String, Entry> byPipeline = new ConcurrentHashMap<>();

    void put(String pipelineId, PendingReason reason) {
        byPipeline.compute(pipelineId, (ignored, previous) -> {
            PipelineState target = capacityTarget(reason);
            long since = target != null && previous != null && previous.capacityTarget() == target
                    ? previous.capacitySinceNanos() : target != null ? System.nanoTime() : 0L;
            return new Entry(new Pending(reason), target, since);
        });
    }

    Optional<Pending> pending(String pipelineId) {
        return Optional.ofNullable(byPipeline.get(pipelineId)).map(Entry::pending);
    }

    /** A refused local pause has no public pending reason but still waits for the same bounded worker. */
    void rememberCapacity(String pipelineId, PipelineState target) {
        byPipeline.compute(pipelineId, (ignored, previous) -> new Entry(null, target,
                previous != null && previous.capacityTarget() == target
                        ? previous.capacitySinceNanos() : System.nanoTime()));
    }

    /** Reuse the first refused tick for the same intent; a new intent starts a new wait. */
    long capacitySince(String pipelineId, PipelineState target) {
        Entry entry = byPipeline.get(pipelineId);
        if (entry != null && entry.capacityTarget() == target) {
            return entry.capacitySinceNanos();
        }
        return System.nanoTime();
    }

    int capacityCount() {
        return (int) byPipeline.values().stream().filter(entry -> entry.capacityTarget() != null).count();
    }

    private static PipelineState capacityTarget(PendingReason reason) {
        return switch (reason) {
            case START_CAPACITY -> PipelineState.RUNNING;
            case STOP_CAPACITY -> PipelineState.STOPPED;
            default -> null;
        };
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
