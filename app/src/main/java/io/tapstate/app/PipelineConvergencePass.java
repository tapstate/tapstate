package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.PipelineStoreViews;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimType;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * One controller pass's fixed intent and physical scope. Only the callback of its own successful
 * execution advance may move its claim; every individual write captures that claim before callbacks
 * or IO, so a transaction retry cannot silently become a later command.
 */
final class PipelineConvergencePass {
    private final PipelineIntentSnapshot intent;
    private final AtomicReference<WorkloadClaimFence> claim;
    private final PipelineStoreViews views;
    private final Runnable requireActive;
    private final StateStore state = new StateStore() {
        @Override public Optional<CheckpointDoc> read(String pipelineId) {
            requirePipeline(pipelineId);
            return views.pipelineStates(scope()).read(pipelineId);
        }
        @Override public CasOutcome compareAndSwap(String pipelineId, long epoch, String next, Instant at) {
            WorkloadClaimFence original = claim.get();
            requirePipeline(pipelineId);
            return views.pipelineStates(scope(), original, requireActive).compareAndSwap(pipelineId, epoch, next, at);
        }
        @Override public void create(String pipelineId, String next, Instant at) { throw fenced(); }
        @Override public void delete(String pipelineId) { throw fenced(); }
    };
    private final ObservationStore observations = new ObservationStore() {
        @Override public Optional<Observation> read(String pipelineId) {
            requirePipeline(pipelineId);
            return views.pipelineObservations(scope()).read(pipelineId);
        }
        @Override public void save(Observation value) {
            WorkloadClaimFence original = claim.get();
            requirePipeline(value.pipelineId());
            views.pipelineObservations(scope(), original, requireActive).save(value);
        }
        @Override public void delete(String pipelineId) { throw fenced(); }
    };

    PipelineConvergencePass(PipelineIntentSnapshot intent, WorkloadClaimFence original,
            PipelineStoreViews views, Runnable requireActive) {
        this.intent = Objects.requireNonNull(intent, "intent");
        Objects.requireNonNull(original, "original");
        if (intent.deleted() || original.key().type() != WorkloadClaimType.PIPELINE_ACTUATION
                || !original.key().clusterId().equals(intent.scope().clusterId())
                || !original.key().resourceId().equals(intent.scope().pipelineId())) {
            throw new IllegalArgumentException("a convergence pass requires one live Pipeline intent and its own claim");
        }
        this.claim = new AtomicReference<>(original);
        this.views = Objects.requireNonNull(views, "views");
        this.requireActive = Objects.requireNonNull(requireActive, "requireActive");
    }

    PipelineIntentSnapshot intent() { return intent; }
    PipelineIntentScope scope() { return intent.scope(); }
    WorkloadClaimFence claim() { return claim.get(); }
    StateStore state() { return state; }
    ObservationStore observations() { return observations; }

    /** Called before any later start step may throw; no other pass or asynchronous DAG retains this object. */
    void advanced(WorkloadClaimFence actual) {
        Objects.requireNonNull(actual, "actual");
        WorkloadClaimFence before = claim.get();
        if (!before.key().equals(actual.key()) || !before.owner().equals(actual.owner())
                || before.claimGeneration() != actual.claimGeneration()
                || before.topologyRevision() != actual.topologyRevision()
                || actual.executionGeneration() != Math.addExact(before.executionGeneration(), 1)
                || !claim.compareAndSet(before, actual)) {
            throw new IllegalStateException("a convergence pass can advance only its exact original claim");
        }
    }

    private void requirePipeline(String pipelineId) {
        if (!scope().pipelineId().equals(pipelineId)) throw fenced();
        requireActive.run();
    }

    private static TapstateException fenced() {
        return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
    }
}
