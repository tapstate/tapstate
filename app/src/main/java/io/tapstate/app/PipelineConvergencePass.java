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
    private final io.tapstate.spi.store.RateHistoryStore rateHistory = new io.tapstate.spi.store.RateHistoryStore() {
        @Override public void append(io.tapstate.core.lifecycle.RateSample sample) {
            WorkloadClaimFence original = claim.get(); requirePipeline(sample.pipelineId());
            views.pipelineRateHistory(scope(), original, requireActive).append(sample);
        }
        @Override public Page readPage(String id, Instant from, Instant to, Key after, int limit) {
            requirePipeline(id); return views.pipelineRateHistory(scope()).readPage(id, from, to, after, limit);
        }
        @Override public Optional<Entry> read(String id, Key key) { requirePipeline(id); return views.pipelineRateHistory(scope()).read(id, key); }
        @Override public Optional<Entry> predecessor(String id, Instant at) { requirePipeline(id); return views.pipelineRateHistory(scope()).predecessor(id, at); }
        @Override public Optional<Entry> successor(String id, Instant at) { requirePipeline(id); return views.pipelineRateHistory(scope()).successor(id, at); }
        @Override public void deleteAll(String id) { throw fenced(); }
        @Override public java.time.Duration retention() { requirePipeline(scope().pipelineId()); return views.pipelineRateHistory(scope()).retention(); }
    };
    private final io.tapstate.spi.store.DerivedSchemaStore derivedSchemas = new io.tapstate.spi.store.DerivedSchemaStore() {
        @Override public Optional<io.tapstate.spi.store.DerivedSchema> latest(String id, String step) {
            requirePipeline(id); return views.pipelineDerivedSchemas(scope()).latest(id, step);
        }
        @Override public void record(String id, String step, Map<String, String> schema, String statement, String from, String by) {
            WorkloadClaimFence original = claim.get();
            var captured = java.util.Collections.unmodifiableMap(new java.util.LinkedHashMap<>(schema));
            requirePipeline(id);
            views.pipelineDerivedSchemas(scope(), original, requireActive).record(id, step, captured, statement, from, by);
        }
        @Override public void pin(String id, String step, long version) {
            WorkloadClaimFence original = claim.get(); requirePipeline(id);
            views.pipelineDerivedSchemas(scope(), original, requireActive).pin(id, step, version);
        }
        @Override public Optional<io.tapstate.spi.store.DerivedSchema> pinned(String id, String step) {
            requirePipeline(id); return views.pipelineDerivedSchemas(scope()).pinned(id, step);
        }
        @Override public void delete(String id) { throw fenced(); }
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
    io.tapstate.spi.store.DerivedSchemaStore derivedSchemas() { return derivedSchemas; }
    io.tapstate.spi.store.RateHistoryStore rateHistory() { return rateHistory; }

    io.tapstate.spi.store.NestShapeStore nestShapes(String database) {
        Objects.requireNonNull(database, "database");
        return new io.tapstate.spi.store.NestShapeStore() {
            @Override public Optional<java.util.Set<String>> read(String step) {
                requirePipeline(scope().pipelineId()); return views.nestShapes(scope(), database).read(step);
            }
            @Override public Optional<java.util.Set<String>> recordIfAbsent(String step, java.util.Set<String> paths) {
                WorkloadClaimFence original = claim.get();
                var captured = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(paths));
                requirePipeline(scope().pipelineId());
                return views.nestShapes(scope(), database, original, requireActive).recordIfAbsent(step, captured);
            }
        };
    }

    /** Captures the same original claim and candidate for both publication and the race verdict. */
    void recordNestShape(WorkloadClaimFence original,String database,String pipelineId,String step,java.util.Set<String> paths) {
        Objects.requireNonNull(original,"original");
        var captured = java.util.Collections.unmodifiableSet(new java.util.LinkedHashSet<>(paths));
        requirePipeline(pipelineId);
        views.nestShapes(scope(), database, original, requireActive).recordIfAbsent(step, captured)
                .ifPresent(recorded -> io.tapstate.runtime.engine.nest.NestStateLedger.requireSameShape(step, recorded, captured));
    }

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

    void requirePipeline(String pipelineId) {
        if (!scope().pipelineId().equals(pipelineId)) throw fenced();
        requireActive.run();
    }

    private static TapstateException fenced() {
        return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
    }
}
