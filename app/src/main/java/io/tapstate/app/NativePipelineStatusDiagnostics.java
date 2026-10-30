package io.tapstate.app;

import io.tapstate.control.core.PipelineStatusDiagnostics;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.EngineError;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.PipelineStatePreparations;
import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.StateStore;

/** Diagnoses one retained SINGLE engine loss without writing a checkpoint, observation or claim. */
final class NativePipelineStatusDiagnostics implements PipelineStatusDiagnostics {
    private final CacheBusinessScope original;
    private final PipelineIntentStore intents;
    private final PipelineStatePreparations preparations;
    private final Engine engine;
    private final PipelineActuationOwnership ownership;
    private final BooleanSupplier active;
    private final Function<PipelineIntentScope, StateStore> states;

    NativePipelineStatusDiagnostics(CacheBusinessScope original, PipelineIntentStore intents,
            PipelineStatePreparations preparations, Engine engine, PipelineActuationOwnership ownership, BooleanSupplier active,
            Function<PipelineIntentScope, StateStore> states) {
        this.original = original; this.intents = intents; this.preparations = preparations;
        this.engine = engine; this.ownership = ownership; this.active = active; this.states = states;
    }

    @Override public Optional<ObservationFailure> failure(String pipeline, PipelineState observed) {
        if ((observed != PipelineState.RUNNING && observed != PipelineState.PAUSED) || !active.getAsBoolean()
                || !engine.isLost()) return Optional.empty();
        try {
            var before = intents.loadForActuation(pipeline);
            if (before.isEmpty() || before.orElseThrow().deleted()) return Optional.empty();
            var snapshot = before.orElseThrow();
            var scope = snapshot.scope();
            if (!pipeline.equals(scope.pipelineId()) || !original.clusterId().equals(scope.clusterId()) || original.bindingVersion() != scope.bindingVersion()
                    || !original.cacheIncarnation().equals(scope.cacheIncarnation())
                    || snapshot.desired().targetState() == PipelineState.STOPPED) return Optional.empty();
            var preparation = preparations.read(pipeline);
            if (preparation.isEmpty() || preparation.orElseThrow().phase() != PipelinePreparation.Phase.COMPLETE
                    || !scope.equals(preparation.orElseThrow().scope())) return Optional.empty();
            var stateView = states.apply(scope);
            var checkpoint = stateView.read(pipeline);
            if (checkpoint.isEmpty()) return Optional.empty();
            PipelineState current = StateJson.parse(checkpoint.orElseThrow().stateJson());
            if (current != PipelineState.RUNNING && current != PipelineState.PAUSED) return Optional.empty();
            var proof = ownership.reportingExecution(scope).orElse(null);
            if (proof == null) return Optional.empty();
            var cause = engine.failureOfLostRun(pipeline, proof.execution())
                    .map(failure -> PipelineFailures.of(pipeline, failure))
                    .filter(failure -> EngineError.OUT_OF_MEMORY.code().equals(failure.code()));
            if (cause.isEmpty() || !before.equals(intents.loadForActuation(pipeline))
                    || !preparation.equals(preparations.read(pipeline)) || !checkpoint.equals(stateView.read(pipeline))
                    || !active.getAsBoolean() || !proof.current())
                return Optional.empty();
            return cause;
        } catch (TapstateException unavailable) {
            return Optional.empty();
        }
    }
}
