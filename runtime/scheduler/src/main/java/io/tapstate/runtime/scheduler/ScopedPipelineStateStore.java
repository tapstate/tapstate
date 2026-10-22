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
    private final Supplier<WorkloadClaimFence> originalClaim;
    private final PipelineStoreViews views;
    private final Runnable requireActive;
    public ScopedPipelineStateStore(PipelineIntentScope scope,Supplier<WorkloadClaimFence> originalClaim,PipelineStoreViews views,Runnable requireActive) {
        this.scope = Objects.requireNonNull(scope,"scope"); this.originalClaim = Objects.requireNonNull(originalClaim,"originalClaim");
        this.views = Objects.requireNonNull(views,"views"); this.requireActive = Objects.requireNonNull(requireActive,"requireActive");
    }
    @Override public Optional<CheckpointDoc> read(String pipelineId) {
        requirePipeline(pipelineId); return views.pipelineStates(scope).read(pipelineId);
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
    @Override public void create(String pipelineId,String next,Instant at) { throw fenced(); }
    @Override public void delete(String pipelineId) { throw fenced(); }
    private void requirePipeline(String pipelineId) { if (!scope.pipelineId().equals(pipelineId)) throw fenced(); requireActive.run(); }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED,Map.of(),null); }
}
