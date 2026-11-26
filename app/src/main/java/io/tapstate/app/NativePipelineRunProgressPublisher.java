package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoCachePipelineRunProgressStore.Execution;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineRunProgress;
import io.tapstate.core.lifecycle.PipelineRunStage;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.srs.ChangeStreamReadyObserver;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Bounded background writes at lifecycle and sampling boundaries, with the original writer fence. */
final class NativePipelineRunProgressPublisher implements PipelineRunProgressEvents, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(NativePipelineRunProgressPublisher.class);
    private record EventKey(PipelineIntentScope scope, WorkloadClaimFence claim, Object fact) {}
    private record BindingKey(PipelineIntentScope scope, WorkloadClaimFence claim) {}
    private record Binding(long startRevision, PipelineRunProgress.Mode mode, Instant planReady, Instant connectorsReady) {
        PipelineRunProgress applyTo(PipelineRunProgress progress) {
            return progress.inactive().withMode(mode)
                    .reach(PipelineRunStage.EXECUTION_PLAN_PREPARED, planReady)
                    .reach(PipelineRunStage.CONNECTORS_READY, connectorsReady);
        }
    }
    private static final int MAX_RETAINED = 2048;

    private final MongoCacheDomain cache;
    private final CacheBusinessScope scope;
    private final PipelineIntentStore intents;
    private final Runnable active;
    private final HazelcastInstance member;
    private final Engine engine;
    private final Clock clock;
    private final ChangeStreamReadyObserver cdcObserver = this::cdcReady;
    private final ScheduledThreadPoolExecutor writer = new ScheduledThreadPoolExecutor(1,
            Thread.ofPlatform().daemon().name("tapstate-run-progress").factory());
    private final Semaphore slots = new Semaphore(256);
    private final Set<Object> pending = ConcurrentHashMap.newKeySet();
    private final LinkedHashMap<Object, Boolean> recorded = new LinkedHashMap<>(128, 0.75f, true);
    private final LinkedHashMap<BindingKey, Binding> pendingBindings = new LinkedHashMap<>();
    private boolean prepared;

    NativePipelineRunProgressPublisher(MongoCacheDomain cache, CacheBusinessScope scope, PipelineIntentStore intents,
            Runnable active, HazelcastInstance member, Engine engine, Clock clock) {
        this.cache = cache;
        this.scope = scope;
        this.intents = intents;
        this.active = active;
        this.member = member;
        this.engine = engine;
        this.clock = clock;
        member.getUserContext().put(ChangeStreamReadyObserver.USER_CONTEXT_KEY, cdcObserver);
        writer.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    @Override public long startRevision(PipelineIntentSnapshot intent) {
        try {
            return intents.executionBindings(intent.scope()).filter(value -> value.intentRevision() == intent.intentRevision())
                    .map(NativePipelineRunProgressQuery::startRevision).orElse(0L);
        } catch (RuntimeException unavailable) {
            LOG.debug("Could not identify the original Start for progress publication", unavailable);
            return 0;
        }
    }

    @Override public void publish(Event event) {
        boolean reached = event.edge() == Edge.REACHED;
        boolean engineStage = event.stage() == PipelineRunStage.ENGINE_RUNNING
                || event.stage() == PipelineRunStage.INITIAL_LOAD_COMPLETED;
        Object key = reached ? new EventKey(event.intent().scope(), event.claim(),
                List.of(event.intent().intentRevision(), event.stage())) : event;
        offer(key, reached, engineStage ? 4 : 0,
                () -> update(event.intent(), event.claim(), event.startRevision(),
                        engineStage ? Execution.REQUIRE_BOUND : Execution.KEEP, event::applyTo));
    }

    @Override public void bind(PipelineIntentSnapshot intent, WorkloadClaimFence claim, long startRevision,
            PipelineRunProgress.Mode mode, Instant planReady, Instant connectorsReady) {
        var key = new BindingKey(intent.scope(), claim);
        synchronized (pendingBindings) {
            pendingBindings.put(key, new Binding(startRevision, mode, planReady, connectorsReady));
            if (pendingBindings.size() > MAX_RETAINED) pendingBindings.pollFirstEntry();
        }
        offer(key, false, 0, () -> update(intent, claim, startRevision, Execution.BIND, UnaryOperator.identity()));
    }

    @Override public void observation(PipelineIntentSnapshot intent, WorkloadClaimFence claim, Observation observation) {
        var load = observation.initialLoad();
        var at = observation.observedAt();
        if (load != null && !load.tables().isEmpty() && !load.readMode().equals("cdc_only") && at != null
                && (observation.state() == PipelineState.RUNNING || observation.state() == PipelineState.COMPLETED)) {
            var counts = new PipelineRunProgress.LoadProgress(load.landedTables(), load.tables().size());
            offer(new EventKey(intent.scope(), claim, counts), true, 2,
                    () -> update(intent, claim, 0, Execution.REQUIRE_BOUND, progress -> progress.observeInitialLoad(counts, at)));
        }
        switch (observation.state()) {
            case COMPLETED -> offer(new EventKey(intent.scope(), claim, PipelineRunStage.RUN_COMPLETED), true, 2,
                    () -> update(intent, claim, 0, Execution.REQUIRE_BOUND,
                            progress -> progress.reach(PipelineRunStage.RUN_COMPLETED, at)));
            case STOPPED, FAILED -> offer(new EventKey(intent.scope(), claim, List.of(intent.intentRevision(), observation.state())),
                    true, 0, () -> update(intent, claim, 0, Execution.KEEP, PipelineRunProgress::inactive));
            default -> { }
        }
    }

    private void cdcReady(Object identity, long jobId, long executionId) {
        if (!(identity instanceof ExecutionFence original) || original.scope() == null || executionId == 0) return;
        var claim = new WorkloadClaimFence(new WorkloadClaimKey(original.scope().clusterId(),
                WorkloadClaimType.PIPELINE_ACTUATION, original.pipelineId()), new WorkloadOwner(original.ownerNodeId(), original.ownerBootId()),
                original.claimGeneration(), original.executionGeneration(), original.topologyRevision());
        Instant at = clock.instant();
        offer(new EventKey(original.scope(), claim, PipelineRunStage.CDC_READY), true, 6, () -> {
            var live = engine.captureRun(original.pipelineId()).orElse(null);
            if (live == null || live.jobId() != jobId || !original.equals(live.operatorManifest().executionIdentity())) return false;
            var intent = intents.loadForActuation(original.pipelineId()).filter(i -> i.scope().equals(original.scope())).orElse(null);
            if (intent == null || intent.desired().targetState() == PipelineState.STOPPED) return false;
            return update(intent, claim, 0, Execution.REQUIRE_BOUND,
                    progress -> progress.reach(PipelineRunStage.CDC_READY, at));
        });
    }

    private boolean update(PipelineIntentSnapshot intent, WorkloadClaimFence claim, long originalStart,
            Execution execution, UnaryOperator<PipelineRunProgress> change) {
        var key = new BindingKey(intent.scope(), claim);
        Binding binding;
        synchronized (pendingBindings) {
            binding = execution == Execution.KEEP ? null : pendingBindings.get(key);
        }
        if (execution == Execution.BIND && binding == null) return true;
        active.run();
        var current = intents.executionBindings(intent.scope()).filter(value -> value.intentRevision() == intent.intentRevision()).orElse(null);
        if (current == null) return false;
        long anchor = NativePipelineRunProgressQuery.startRevision(current);
        // A startup failure can stop the connector handoff before this queued event reaches CACHE.
        if (!prepared) {
            cache.preparePipelineRunProgress(scope, active);
            prepared = true;
        }
        var store = cache.pipelineRunProgress(intent.scope(), claim, active);
        long capturedStart = originalStart > 0 ? originalStart : binding == null ? 0 : binding.startRevision();
        long start = capturedStart > 0 ? capturedStart : anchor;
        if (capturedStart == 0 && current.bindings().handoff() == null) {
            // An execution can open a real connector without retaining an artifact handoff.
            // In that case the original bound projection already proved the explicit Start.
            var retained = store.latest().orElse(null);
            if (retained != null && retained.startRevision() >= anchor && retained.startRevision() <= current.intentRevision()) {
                start = retained.startRevision();
            }
        }
        if (start <= 0 || start > current.intentRevision()
                || current.bindings().handoff() != null && anchor != start) return false;
        // If the asynchronous startup write was dropped or failed, a subsequent fact can retry it.
        // The binding is only taken from the exact original claim and the transaction proves that claim.
        boolean written = store.update(start, NativePipelineRunProgressQuery.accepted(intent.scope(), start, null, 3),
                binding == null ? execution : Execution.BIND,
                binding == null ? change : previous -> change.apply(binding.applyTo(previous)));
        if (written && binding != null) synchronized (pendingBindings) { pendingBindings.remove(key, binding); }
        return written;
    }

    private void offer(Object key, boolean remember, int retries, BooleanSupplier operation) {
        if (remember) synchronized (recorded) { if (recorded.containsKey(key)) return; }
        if (!pending.add(key)) return;
        if (!slots.tryAcquire()) {
            pending.remove(key);
            return;
        }
        schedule(key, remember, retries, operation, 0);
    }

    private void schedule(Object key, boolean remember, int retries, BooleanSupplier operation, long delay) {
        try {
            writer.schedule(() -> {
                boolean retry = false;
                try {
                    if (operation.getAsBoolean()) {
                        if (remember) synchronized (recorded) {
                            recorded.put(key, true);
                            if (recorded.size() > MAX_RETAINED) recorded.pollFirstEntry();
                        }
                    } else retry = true;
                } catch (RuntimeException unavailable) {
                    retry = unavailable instanceof TapstateException failure && failure.code() == IoError.STORE_UNAVAILABLE;
                    LOG.debug("Run progress publication was refused or temporarily unavailable for {}", key, unavailable);
                } finally {
                    if (retry && retries > 0 && !writer.isShutdown()) {
                        schedule(key, remember, retries - 1, operation, 200);
                    } else release(key);
                }
            }, delay, TimeUnit.MILLISECONDS);
        } catch (RejectedExecutionException closed) {
            release(key);
        }
    }

    private void release(Object key) {
        if (pending.remove(key)) slots.release();
    }

    @Override public void close() {
        member.getUserContext().remove(ChangeStreamReadyObserver.USER_CONTEXT_KEY, cdcObserver);
        writer.shutdownNow();
    }
}
