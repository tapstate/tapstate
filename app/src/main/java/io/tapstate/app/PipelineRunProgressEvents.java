package io.tapstate.app;

import io.tapstate.core.lifecycle.PipelineRunStage;
import io.tapstate.core.lifecycle.PipelineRunProgress;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.WorkloadClaimFence;

import java.time.Instant;
import java.util.function.Supplier;

/** Stage boundaries carry their original command. An observer must validate the fence when publishing. */
@FunctionalInterface
interface PipelineRunProgressEvents {
    enum Edge { ENTERED, LEFT, REACHED, FAILED }

    record Event(PipelineIntentSnapshot intent, WorkloadClaimFence claim, PipelineRunStage stage,
            Edge edge, Instant at, Instant activitySince, long startRevision) {
        Event(PipelineIntentSnapshot intent, WorkloadClaimFence claim, PipelineRunStage stage, Edge edge, Instant at,
                long startRevision) {
            this(intent, claim, stage, edge, at, null, startRevision);
        }

        PipelineRunProgress applyTo(PipelineRunProgress progress) {
            return switch (edge) {
                case ENTERED -> progress.enter(stage, at);
                case LEFT -> progress.leave(stage, activitySince, false);
                case REACHED -> {
                    // This explicit landing event is emitted by the successful bounded job's completion check.
                    var confirmed = stage == PipelineRunStage.INITIAL_LOAD_COMPLETED
                            ? progress.reach(PipelineRunStage.ENGINE_RUNNING, null) : progress;
                    yield confirmed.reach(stage, at);
                }
                case FAILED -> progress.leave(stage, activitySince, true);
            };
        }
    }

    PipelineRunProgressEvents NONE = event -> {};

    void publish(Event event);

    default long startRevision(PipelineIntentSnapshot intent) { return 0; }

    default void bind(PipelineIntentSnapshot intent, WorkloadClaimFence claim, long startRevision,
            PipelineRunProgress.Mode mode, Instant planReady, Instant connectorsReady) {}

    default void observation(PipelineIntentSnapshot intent, WorkloadClaimFence claim, Observation observation) {}

    default <T> T during(Supplier<Event> entered, Supplier<T> work) {
        if (this == NONE) return work.get();
        var original = entered.get();
        publish(original);
        try {
            T result = work.get();
            end(original, Edge.LEFT);
            return result;
        } catch (RuntimeException failure) {
            end(original, Edge.FAILED);
            throw failure;
        }
    }

    private void end(Event original, Edge edge) {
        publish(new Event(original.intent(), original.claim(), original.stage(), edge, original.at(), original.at(),
                original.startRevision()));
    }
}
