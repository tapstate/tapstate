package io.tapstate.app;

import io.tapstate.control.core.*;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.*;
import io.tapstate.core.model.canonical.CanonicalHash;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.spi.store.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** One bounded background inventory read; convergence never waits for a source connection. */
final class DynamicSourceTableRefresh implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(DynamicSourceTableRefresh.class);
    private final Consumer<String> action;
    private final long intervalNanos;
    private final AtomicBoolean busy = new AtomicBoolean();
    private final ThreadPoolExecutor worker = new ThreadPoolExecutor(1, 1, 0, TimeUnit.SECONDS,
            new ArrayBlockingQueue<>(1), task -> {
                var thread = new Thread(task, "dynamic-source-tables"); thread.setDaemon(true); return thread;
            });
    private String after;
    private long next;
    private volatile boolean closed;

    DynamicSourceTableRefresh(Consumer<String> action, Duration interval) {
        this.action = Objects.requireNonNull(action);
        if (interval.isNegative() || interval.isZero()) throw new IllegalArgumentException("positive refresh interval required");
        intervalNanos = interval.toNanos();
    }

    synchronized void consider(Collection<String> pipelines) {
        long now = System.nanoTime();
        if (closed || pipelines.isEmpty() || now - next < 0 || !busy.compareAndSet(false, true)) return;
        String chosen = nextPipeline(pipelines, after);
        after = chosen;
        next = now + Math.max(TimeUnit.SECONDS.toNanos(1), intervalNanos / pipelines.size());
        try {
            worker.execute(() -> {
                try { if (!closed) action.accept(chosen); }
                catch (TapstateException failure) { LOG.debug("Source table inventory for {} deferred [{}]", chosen, failure.code()); }
                catch (RuntimeException failure) { LOG.warn("Source table inventory for {} failed [{}]", chosen, failure.getClass().getSimpleName()); }
                finally { busy.set(false); }
            });
        } catch (RejectedExecutionException ending) { busy.set(false); }
    }

    static String nextPipeline(Collection<String> pipelines, String after) {
        return pipelines.stream().filter(id -> after == null || id.compareTo(after) > 0).min(String::compareTo)
                .orElseGet(() -> pipelines.stream().min(String::compareTo).orElseThrow());
    }

    @Override public void close() { closed = true; worker.shutdownNow(); }

    static final class Refresh implements Consumer<String> {
        private final ArtifactStore artifacts;
        private final SchemaStore schemas;
        private final PipelineIntentStore intents;
        private final PipelineStatePreparations configuration;
        private final PipelineCachePreparations cache;
        private final PipelineStoreViews views;
        private final SrsConsumerStore consumers;
        private final SchemaDiscoveryService discovery;
        private final AuditGate audit;
        private final Engine engine;
        private final Supplier<WorkloadClaimFence> node;
        private final Runnable active;

        Refresh(ArtifactStore artifacts, SchemaStore schemas, PipelineIntentStore intents,
                PipelineStatePreparations configuration, PipelineCachePreparations cache, PipelineStoreViews views,
                SrsConsumerStore consumers, SchemaDiscoveryService discovery, AuditGate audit, Engine engine,
                Supplier<WorkloadClaimFence> node, Runnable active) {
            this.artifacts = artifacts; this.schemas = schemas; this.intents = intents;
            this.configuration = configuration; this.cache = cache; this.views = views;
            this.consumers = consumers; this.discovery = discovery; this.audit = audit;
            this.engine = engine; this.node = node; this.active = active;
        }

        @Override public void accept(String id) {
            active.run();
            var original = intents.loadForActuation(id).orElse(null);
            if (original == null || original.deleted() || original.desired().purgeState()
                    || original.desired().targetState() != PipelineState.RUNNING) return;
            var scope = original.scope();
            var checkpoint = views.pipelineStates(scope).read(id).orElse(null);
            if (checkpoint == null) return;
            PipelineState state = StateJson.parse(checkpoint.stateJson());
            if (state == PipelineState.FAILED) {
                var failure = views.pipelineObservations(scope).read(id).map(value -> value.failure()).orElse(null);
                if (failure == null || !failure.code().equals(IoError.PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED.code())) return;
            } else if (state != PipelineState.RUNNING || !runningHere(scope)) return;

            var snapshot = ReadOnlyArtifactSnapshot.capture(artifacts);
            var authored = StoredArtifacts.requirePipeline(snapshot, id);
            var pipeline = PipelineInlining.inline(authored, snapshot);
            // Retaining operator-state shape changes need a separate admission contract.
            if (pipeline.view() != null || pipeline.transforms() != null && !pipeline.transforms().isEmpty()
                    || !(pipeline.serve() instanceof ServeBlock.Inline serve) || serve.sync() == null || serve.sync().isEmpty()
                    || serve.sync().stream().anyMatch(sync -> sync.rename() != null)) return;
            if (pipeline.settings() != null && pipeline.settings().readMode() != null
                    && pipeline.settings().readMode() != ReadMode.SNAPSHOT_AND_CDC) return;
            var sources = new LinkedHashMap<String, SourceResource>();
            for (String source : pipeline.sourceIds()) sources.put(source, StoredArtifacts.requireSource(snapshot, source));
            var dynamic = new HashSet<String>();
            for (var ref : pipeline.sources()) {
                if (!(ref instanceof SourceRef.Spec spec) || !spec.srs()) continue;
                var source = sources.get(ref.id());
                if (source.tables() == null || source.tables().stream().anyMatch(value -> value instanceof TableRef.Regex))
                    dynamic.add(source.id());
            }
            if (dynamic.isEmpty()) return;
            var held = cache.sourceInputs(scope);
            var fixed = held.inputs();
            if (!CanonicalHash.of(authored).equals(fixed.pipelineHash())
                    || !fixed.sourceHashes().keySet().equals(sources.keySet())) return;
            for (var source : sources.values()) if (!CanonicalHash.of(source).equals(fixed.sourceHashes().get(source.id()))) return;
            for (String sourceId : dynamic) {
                active.run(); var source = sources.get(sourceId);
                discovery.discover(sourceId, source.connector(), source.config(), "system:dynamic-tables");
            }
            active.run();
            var models = SourceModelSnapshot.capture(id, sources.values(), schemas);
            var next = models.preparationInputs(scope, authored, pipeline, sources);
            boolean added = next.expandsDiscoveredTables(fixed);
            if (!added && !next.equals(fixed)) return;
            if (!added && (!held.expandedByDiscovery() || serving(scope, dynamic, next.selectedTables()))) return;
            var permission = configuration.authorizeTableExpansion(scope, next);
            active.run();
            audit.dispatch(ControlOperations.PIPELINE_EXPAND_SOURCE_TABLES,
                    new AuditContext("system:dynamic-tables", id, next.pipelineHash()), () -> {
                        cache.expandSources(permission, node.get());
                        active.run();
                        var current = views.pipelineStates(scope).read(id).orElseThrow();
                        PipelineState actual = StateJson.parse(current.stateJson());
                        if (actual != PipelineState.RUNNING && actual != PipelineState.FAILED) return false;
                        if (actual == PipelineState.FAILED) {
                            var failure = views.pipelineObservations(scope).read(id).map(value -> value.failure()).orElse(null);
                            if (failure == null || !failure.code().equals(IoError.PIPELINE_TABLE_SELECTION_REBUILD_REQUIRED.code())) return false;
                        }
                        boolean accepted = permission.requestRestart(original, current.epoch());
                        if (accepted) LOG.info("Requested retaining reassembly after source discovery for pipeline {}", id);
                        return accepted;
                    });
        }

        private boolean runningHere(PipelineIntentScope scope) {
            var run = engine.captureRun(scope.pipelineId()).orElse(null);
            return run != null && run.isCarryingAJob() && run.status() == com.hazelcast.jet.core.JobStatus.RUNNING
                    && run.operatorManifest().executionIdentity() instanceof ExecutionFence fence && scope.equals(fence.scope());
        }

        private boolean serving(PipelineIntentScope scope, Set<String> sources, Map<String, Set<String>> expected) {
            var served = new HashMap<String, Set<String>>();
            for (var member : consumers.forPipeline(scope)) if (!member.detached())
                served.computeIfAbsent(member.context().key().sourceId(), ignored -> new HashSet<>()).addAll(member.tables());
            return sources.stream().allMatch(source -> served.getOrDefault(source, Set.of()).containsAll(expected.get(source)));
        }
    }
}
