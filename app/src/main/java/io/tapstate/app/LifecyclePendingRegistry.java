package io.tapstate.app;

import io.tapstate.control.core.PipelineExplanation.Pending;
import io.tapstate.control.core.PipelineExplanation.PendingReason;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.scheduler.PipelineConverger;
import java.util.Objects;

/** Local read projection of accepted or capacity-waiting lifecycle work. */
final class LifecyclePendingRegistry {

    record Context(ObservationScopeRegistry.BindingIdentity binding, ObservationScopeRecovery.Owner owner) { }

    /** Historical decision evidence classifies a queue label; it never skips actual reconciliation. */
    private record TerminalNoop(DesiredState intent, CheckpointDoc checkpoint, Context context) {
        boolean matches(DesiredState requested, Context current) {
            // A remote incarnation may change before this member sees a local invalidation. Leave
            // claimed and unknown-scope paths provisional until their actual worker decides again.
            return current.owner() == null && (current.binding() == null || current.binding().known())
                    && intent.equals(requested) && context.equals(current);
        }
    }

    private record ActiveDecision(DesiredState intent, Context context, Object workIdentity) {
        boolean matches(DesiredState requested, Context current, Object work) {
            return work != null && workIdentity == work && intent.equals(requested) && context.equals(current);
        }
    }

    private record Entry(Pending pending, PipelineState capacityTarget, long capacitySinceNanos,
            TerminalNoop terminalNoop, ActiveDecision activeDecision) { }


    private final ConcurrentHashMap<String, Entry> byPipeline = new ConcurrentHashMap<>();

    void put(String pipelineId, PendingReason reason) {
        byPipeline.compute(pipelineId, (ignored, previous) -> {
            PipelineState target = capacityTarget(reason);
            long since = target != null && previous != null && previous.capacityTarget() == target
                    ? previous.capacitySinceNanos() : target != null ? System.nanoTime() : 0L;
            return new Entry(new Pending(reason), target, since, null, null);
        });
    }

    /** Applies a provisional label atomically with the latest actual decision for this pipeline. */
    void provisional(String pipelineId, DesiredState intent, Context context, Object workIdentity,
            PendingReason reason) {
        byPipeline.compute(pipelineId, (ignored, previous) -> {
            if (previous != null && previous.activeDecision() != null
                    && previous.activeDecision().matches(intent, context, workIdentity)) {
                return previous;
            }
            if (previous != null && previous.terminalNoop() != null
                    && previous.terminalNoop().matches(intent, context)) {
                return new Entry(null, null, 0L, previous.terminalNoop(), null);
            }
            PipelineState target = capacityTarget(reason);
            long since = target != null && previous != null && previous.capacityTarget() == target
                    ? previous.capacitySinceNanos() : target != null ? System.nanoTime() : 0L;
            return new Entry(new Pending(reason), target, since, null, null);
        });
    }

    /** Called by the existing lifecycle worker after the converge loop chose its actual action. */
    void decided(PipelineConverger.PendingDecision decision, Context context, Object workIdentity) {
        String pipelineId = decision.intent().pipelineId();
        byPipeline.compute(pipelineId, (ignored, previous) -> {
            if (decision.action() == PipelineConverger.PendingAction.START) {
                return new Entry(new Pending(PendingReason.START_PENDING), null, 0L, null,
                        new ActiveDecision(decision.intent(), context, workIdentity));
            }
            if (decision.action() == PipelineConverger.PendingAction.STOP) {
                return new Entry(new Pending(PendingReason.STOP_PENDING), null, 0L, null,
                        new ActiveDecision(decision.intent(), context, workIdentity));
            }
            CheckpointDoc checkpoint = decision.checkpoint().orElse(null);
            PipelineState actual = checkpoint == null ? null : StateJson.parse(checkpoint.stateJson());
            return actual == PipelineState.FAILED || actual == PipelineState.COMPLETED
                    ? new Entry(null, null, 0L, new TerminalNoop(decision.intent(), checkpoint, context), null) : null;
        });
    }

    void discardDecision(String pipelineId) {
        byPipeline.computeIfPresent(pipelineId, (ignored, previous) -> previous.terminalNoop() == null
                ? previous : previous.pending() == null && previous.capacityTarget() == null ? null
                        : new Entry(previous.pending(), previous.capacityTarget(),
                                previous.capacitySinceNanos(), null, previous.activeDecision()));
    }

    Optional<Pending> pending(String pipelineId) {
        return Optional.ofNullable(byPipeline.get(pipelineId)).map(Entry::pending);
    }

    /** A refused local pause has no public pending reason but still waits for the same bounded worker. */
    void rememberCapacity(String pipelineId, PipelineState target) {
        byPipeline.compute(pipelineId, (ignored, previous) -> new Entry(null, target,
                previous != null && previous.capacityTarget() == target
                        ? previous.capacitySinceNanos() : System.nanoTime(), null, null));
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
        byPipeline.computeIfPresent(pipelineId, (ignored, previous) -> previous.terminalNoop() == null
                ? null : new Entry(null, null, 0L, previous.terminalNoop(), null));
    }

    void forget(String pipelineId) {
        byPipeline.remove(pipelineId);
    }

    void clearAll() {
        byPipeline.clear();
    }

    void retain(Collection<String> pipelineIds) {
        byPipeline.keySet().retainAll(pipelineIds);
    }
}
