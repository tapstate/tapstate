package io.tapstate.app;

import io.tapstate.control.core.PipelineRunProgressQuery;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.core.common.JsonWriter;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.PipelineRunProgress;
import io.tapstate.core.lifecycle.PipelineRunStage;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.PipelinePreparation;
import io.tapstate.spi.store.PipelineStatePreparations;
import java.util.List;
import java.util.Optional;
import java.util.function.BooleanSupplier;

/** Facts that can be read from existing Start authority before a progress projection exists. */
final class NativePipelineRunProgressQuery implements PipelineRunProgressQuery {
    private final CacheBusinessScope scope;
    private final PipelineIntentStore intents;
    private final PipelineStatePreparations preparations;
    private final BooleanSupplier active;
    private final MongoCacheDomain cache;

    NativePipelineRunProgressQuery(CacheBusinessScope scope, PipelineIntentStore intents,
            PipelineStatePreparations preparations, BooleanSupplier active) {
        this(scope, intents, preparations, active, null);
    }

    NativePipelineRunProgressQuery(CacheBusinessScope scope, PipelineIntentStore intents,
            PipelineStatePreparations preparations, BooleanSupplier active, MongoCacheDomain cache) {
        this.scope = scope;
        this.intents = intents;
        this.preparations = preparations;
        this.active = active;
        this.cache = cache;
    }

    @Override
    public Optional<Read> find(String pipelineId) {
        if (!active.getAsBoolean()) return Optional.empty();
        try {
            // The common branch uses one actuation read and one admitted bindings read. The second
            // rechecks the first read's intent revision before any previously published stage is used.
            var intent = intents.loadForActuation(pipelineId).orElse(null);
            if (intent == null) return firstStart(pipelineId);
            if (intent.deleted() || !inScope(intent.scope())) return Optional.empty();
            var current = intents.executionBindings(intent.scope()).orElse(null);
            if (current == null || current.intentRevision() != intent.intentRevision() || !active.getAsBoolean()) {
                return Optional.empty();
            }
            long start = startRevision(current);
            if (start > current.intentRevision()) return Optional.empty();
            boolean stopping = intent.desired().targetState() == PipelineState.STOPPED;
            if (cache != null) {
                // CONFIG and CACHE are independent. The second CONFIG read must still name the
                // same committed Start (and valid state generation) before exposing the CACHE row.
                var saved = cache.pipelineRunProgress(intent.scope(), null, () -> {}).latest().orElse(null);
                if (saved != null) {
                    var verified = intents.executionBindings(intent.scope()).orElse(null);
                    if (!current.equals(verified) || !active.getAsBoolean()) return Optional.empty();
                    boolean acceptedStart = current.bindings().handoff() != null ? saved.startRevision() == start
                            : saved.startRevision() >= start && saved.startRevision() <= current.intentRevision();
                    if (acceptedStart) {
                        var progress = saved.progress();
                        if (!stopping) return Optional.of(new Read(progress, progress.awaitingMilestones()));
                        return Optional.of(new Read(new PipelineRunProgress(progress.runId(), progress.version(),
                                List.of(), progress.milestones(), progress.initialLoad(), progress.failedStage(),
                                progress.readMode(), progress.runFailed()), false));
                    }
                }
            }
            return start < 1 ? Optional.empty() : Optional.of(new Read(accepted(intent.scope(), start, null, 3), !stopping));
        } catch (TapstateException unavailable) {
            if (unavailable.code() == IoError.PIPELINE_INTENT_FENCED
                    || unavailable.code() == IoError.STORE_UNAVAILABLE) return Optional.empty();
            throw unavailable;
        }
    }

    private Optional<Read> firstStart(String pipelineId) {
        // Fixed point reads: pending CONFIG intent, its preparation, and the pending intent again
        // to exclude a concurrent stop, replacement or failed preparation.
        var pending = intents.pendingStart(pipelineId).orElse(null);
        if (pending == null || pending.intent().deleted()
                || pending.intent().desired().targetState() != PipelineState.RUNNING
                || !inScope(pending.intent().scope())) return Optional.empty();
        var preparation = preparations.read(pipelineId).orElse(null);
        if (!active.getAsBoolean() || intents.pendingStart(pipelineId).filter(pending::equals).isEmpty()
                || preparation == null || preparation.stateGeneration() != pending.intent().scope().stateGeneration()) {
            return Optional.empty();
        }
        var intent = pending.intent();
        var progress = switch (preparation.phase()) {
            case UNASSIGNED -> accepted(intent.scope(), intent.intentRevision(), PipelineRunStage.WAITING_FOR_SCHEDULER, 1);
            case ISSUED -> intent.scope().equals(preparation.scope())
                    ? accepted(intent.scope(), intent.intentRevision(), PipelineRunStage.RUN_RESOURCES_PREPARING, 2)
                    : null;
            case COMPLETE -> null; // A concurrent preparation completed; the next actuation read can prove it.
        };
        if (progress == null) return Optional.empty();
        return Optional.of(new Read(progress, false));
    }

    private boolean inScope(PipelineIntentScope value) {
        return value.clusterId().equals(scope.clusterId()) && value.bindingVersion() == scope.bindingVersion()
                && value.cacheIncarnation().equals(scope.cacheIncarnation());
    }

    static long startRevision(PipelineIntentStore.ExecutionBindings current) {
        var bindings = current.bindings();
        return bindings.handoff() != null ? bindings.handoff().intentRevision() : bindings.effectiveIntentRevision();
    }

    static PipelineRunProgress accepted(PipelineIntentScope scope, long start, PipelineRunStage activity, long version) {
        String scopeAndStart = JsonWriter.write(List.of(scope.clusterId(), scope.pipelineId(),
                scope.bindingVersion(), scope.cacheIncarnation(), scope.stateGeneration(), start));
        return new PipelineRunProgress(CanonicalHash.ofText(scopeAndStart), version,
                activity == null ? List.of() : List.of(new PipelineRunProgress.Activity(activity, null)),
                List.of(new PipelineRunProgress.Milestone(PipelineRunStage.START_ACCEPTED, null)), null, null);
    }
}
