package io.tapstate.runtime.scheduler;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.spi.store.*;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/** The converge loop's fixed physical state view; the original claim is captured before each write's callbacks. */
public final class ScopedPipelineStateStore implements StateStore {
    private final PipelineIntentScope scope;
    private final long intentRevision;
    private final Supplier<WorkloadClaimFence> originalClaim;
    private final PipelineStoreViews views;
    private final Runnable requireActive;
    public ScopedPipelineStateStore(PipelineIntentScope scope,Supplier<WorkloadClaimFence> originalClaim,PipelineStoreViews views,Runnable requireActive) {
        this(scope, 0L, originalClaim, views, requireActive);
    }

    public ScopedPipelineStateStore(PipelineIntentSnapshot intent, Supplier<WorkloadClaimFence> claim,
            PipelineStoreViews views, Runnable requireActive) {
        this(intent.scope(), intent.intentRevision(), claim, views, requireActive);
    }

    private ScopedPipelineStateStore(PipelineIntentScope scope, long revision, Supplier<WorkloadClaimFence> originalClaim,
            PipelineStoreViews views, Runnable requireActive) {
        this.intentRevision = revision;
        this.scope = Objects.requireNonNull(scope,"scope"); this.originalClaim = Objects.requireNonNull(originalClaim,"originalClaim");
        this.views = Objects.requireNonNull(views,"views"); this.requireActive = Objects.requireNonNull(requireActive,"requireActive");
    }
    private volatile ResumeMetrics.Checkpoint measured;

    public boolean unavailable() {
        var current = measured;
        return intentRevision > 0 && current != null && current.unavailableRevision() == intentRevision;
    }

    public ResumeMetrics.Binding latestBinding() {
        var current = measured;
        return current == null ? null : current.applied();
    }

    public ResumeMetrics.Binding applied(CheckpointDoc expected) {
        var current = measured;
        if (current == null || !current.state().equals(expected)) return null;
        var applied = current.applied();
        return applied != null && applied.revision() == intentRevision && applied.fence().equals(originalClaim.get())
                && !applied.fence().equals(applied.baseline().origin()) ? applied : null;
    }

    @Override public CasOutcome transitionMetrics(String id, long epoch, String next, Instant at,
            ResumeMetrics.Transition transition) {
        var original = originalClaim.get();
        requirePipeline(id);
        return views.pipelineStates(scope, original, requireActive).transitionMetrics(id, epoch, next, at, transition);
    }

    @Override public Optional<CheckpointDoc> read(String pipelineId) {
        requirePipeline(pipelineId);
        var store = views.pipelineStates(scope);
        if (!(store instanceof StateStore.MetricReader reader)) return store.read(pipelineId);
        var reading = reader.readForMetrics(pipelineId);
        measured = reading.orElse(null);
        return reading.map(ResumeMetrics.Checkpoint::state);
    }
    @Override public CasOutcome compareAndSwap(String pipelineId,long epoch,String next,Instant at) {
        WorkloadClaimFence original = Objects.requireNonNull(originalClaim.get(),"originalClaim");
        requirePipeline(pipelineId); return views.pipelineStates(scope,original,requireActive).compareAndSwap(pipelineId,epoch,next,at);
    }
    @Override public CasOutcome compareAndSwap(String pipelineId,long epoch,String next,Instant at,WorkloadClaimFence supplied) {
        WorkloadClaimFence original = Objects.requireNonNull(originalClaim.get(),"originalClaim");
        if (!original.equals(supplied)) throw fenced();
        requirePipeline(pipelineId);
        return views.pipelineStates(scope,original,requireActive).compareAndSwap(pipelineId,epoch,next,at);
    }
    @Override public CasOutcome recordLeaseRecovery(String pipelineId, CheckpointDoc expected,
            CheckpointDoc.LeaseRecovery recovery, Instant at) {
        requirePipeline(pipelineId);
        WorkloadClaimFence original = Objects.requireNonNull(originalClaim.get(), "originalClaim");
        return views.pipelineStates(scope, original, requireActive).recordLeaseRecovery(pipelineId, expected, recovery, at);
    }
    @Override public void create(String pipelineId,String next,Instant at) { throw fenced(); }
    @Override public void delete(String pipelineId) { throw fenced(); }
    private void requirePipeline(String pipelineId) { if (!scope.pipelineId().equals(pipelineId)) throw fenced(); requireActive.run(); }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
}
