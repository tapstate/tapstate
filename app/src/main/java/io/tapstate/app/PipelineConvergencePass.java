package io.tapstate.app;

import io.tapstate.core.common.TapstateException;
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
    private final java.util.function.Consumer<PipelineConvergencePass> advancedObserver;
    private final PipelineIntentSnapshot intent;
    private final AtomicReference<WorkloadClaimFence> claim;
    private final PipelineStoreViews views;
    private final Runnable requireActive;
    private final StateStore state;
    private final AtomicReference<PipelineCaptureBindings> captures = new AtomicReference<>();

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
        this(intent,original,views,requireActive,ignored->{});
    }
    PipelineConvergencePass(PipelineIntentSnapshot intent, WorkloadClaimFence original,
            PipelineStoreViews views, Runnable requireActive,java.util.function.Consumer<PipelineConvergencePass> advancedObserver) {
        this.advancedObserver=Objects.requireNonNull(advancedObserver);
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
        this.state = new io.tapstate.runtime.scheduler.ScopedPipelineStateStore(intent.scope(),claim::get,views,requireActive);
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

    /** A successful capture publishes only the exact source identities of this execution. */
    void bindCapture(WorkloadClaimFence original,PipelineCaptureBindings bindings) {
        Objects.requireNonNull(original,"original"); Objects.requireNonNull(bindings,"bindings");
        requirePipeline(scope().pipelineId());
        if (!original.equals(claim.get()) || !bindings.original().equals(new ExecutionFence(scope(),original))) throw fenced();
        requirePipeline(scope().pipelineId());
        if (!original.equals(claim.get())) throw fenced();
        // No callback can observe a candidate before every admission check has succeeded.
        var before = captures.compareAndExchange(null,bindings);
        if (before != null && !before.equals(bindings)) throw fenced();
    }
    PipelineCaptureBindings captureBindings(WorkloadClaimFence original) {
        Objects.requireNonNull(original,"original"); requirePipeline(scope().pipelineId());
        var fixed = captures.get();
        if (!original.equals(claim.get()) || fixed == null || !fixed.original().equals(new ExecutionFence(scope(),original))) throw fenced();
        return fixed;
    }

    /** Prepares the captured Nest databases before any member resource can be opened. */
    void prepareOperatorDatabases(WorkloadClaimFence original, java.util.Collection<String> databases,
            Runnable commandGuard) {
        Objects.requireNonNull(original, "original");
        Objects.requireNonNull(commandGuard, "commandGuard");
        var targets = new java.util.TreeMap<String, String>();
        for (String database : java.util.List.copyOf(databases)) {
            targets.computeIfAbsent(Objects.requireNonNull(database, "database"),
                    ignored -> java.util.UUID.randomUUID().toString());
        }
        Runnable fixed = () -> {
            requireActive.run();
            if (!original.equals(claim.get())) throw fenced();
            commandGuard.run();
            requireActive.run();
            if (!original.equals(claim.get())) throw fenced();
        };
        fixed.run();
        for (var target : targets.entrySet()) {
            fixed.run();
            views.prepareOperatorDatabase(scope(), target.getKey(), original, target.getValue(), fixed);
            fixed.run();
        }
    }

    Runnable guardOriginal(WorkloadClaimFence original) {
        Objects.requireNonNull(original,"original");
        return () -> {requirePipeline(scope().pipelineId());if(!original.equals(claim.get()))throw fenced();};
    }

    /** Freezes the complete compiled roster before the job can open a target on any member. */
    void prepareSinkTargets(WorkloadClaimFence original,java.util.List<io.tapstate.spi.store.SinkPreparationStore.Input> inputs) {
        var captured=java.util.List.copyOf(inputs);Objects.requireNonNull(original,"original");
        Runnable fixed=guardOriginal(original);
        fixed.run();views.sinkPreparations(scope(),original,fixed).prepareTargets(captured);fixed.run();
    }

    /** Called before any later start step may throw; no other pass or asynchronous DAG retains this object. */
    void advanced(WorkloadClaimFence actual) {
        advance(actual, false);
    }

    /** Only a new submission, after the old run retires, may adopt a forward committed topology. */
    void advancedSubmission(PipelineActuationOwnership.Execution execution) {
        Objects.requireNonNull(execution, "execution");
        if (!execution.allowed() || execution.fence() == null || !scope().equals(execution.fence().scope()))
            throw new IllegalStateException("a submission must carry this pass's original physical scope");
        advance(execution.claimFence(), true);
    }

    private void advance(WorkloadClaimFence actual, boolean submission) {
        Objects.requireNonNull(actual, "actual");
        WorkloadClaimFence before = claim.get();
        if (!before.key().equals(actual.key()) || !before.owner().equals(actual.owner())
                || before.claimGeneration() != actual.claimGeneration()
                || (submission ? before.topologyRevision() > actual.topologyRevision()
                        : before.topologyRevision() != actual.topologyRevision())
                || actual.executionGeneration() != Math.addExact(before.executionGeneration(), 1)
                || !claim.compareAndSet(before, actual)) {
            throw new IllegalStateException("a convergence pass can advance only its exact original claim");
        }
        advancedObserver.accept(this);
    }

    void requirePipeline(String pipelineId) {
        if (!scope().pipelineId().equals(pipelineId)) throw fenced();
        requireActive.run();
    }

    private static TapstateException fenced() {
        return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null);
    }
}
