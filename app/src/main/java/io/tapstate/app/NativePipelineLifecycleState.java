package io.tapstate.app;

import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.ExecutionPlans;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.spi.store.PipelineIntentScope;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/** Reads the current job without waiting for metrics or changing its claim. */
final class NativePipelineLifecycleState implements AutoCloseable {
    private final Engine engine;
    private final ExecutionPlans plans;
    private final Predicate<ExecutionFence> authorized;
    private final BooleanSupplier active;
    private final ExecutorService readers = new ThreadPoolExecutor(0, 2, 30, TimeUnit.SECONDS,
            new SynchronousQueue<>(), task -> {
                var thread = new Thread(task, "tapstate-lifecycle-read"); thread.setDaemon(true); return thread;
            });

    NativePipelineLifecycleState(Engine engine, ExecutionPlans plans,
            Predicate<ExecutionFence> authorized, BooleanSupplier active) {
        this.engine = engine; this.plans = plans; this.authorized = authorized; this.active = active;
    }

    Map<String, PipelineState> currentStates(Map<String, PipelineIntentScope> scopes) {
        if (scopes.isEmpty()) return Map.of();
        var requested = Map.copyOf(scopes);
        var result = new ConcurrentHashMap<String, PipelineState>();
        Future<?> reading;
        try {
            reading = readers.submit(() -> {
                for (var entry : requested.entrySet()) {
                    if (Thread.currentThread().isInterrupted()) return;
                    var state = readCurrent(entry.getKey(), entry.getValue());
                    if (Thread.currentThread().isInterrupted()) return;
                    state.ifPresent(value -> result.put(entry.getKey(), value));
                }
            });
        } catch (RejectedExecutionException saturated) {
            return Map.of();
        }
        try {
            // One budget for the entire catalog page, never one wait per pipeline.
            reading.get(250, TimeUnit.MILLISECONDS);
        } catch (TimeoutException unavailable) {
            reading.cancel(true);
        } catch (InterruptedException interrupted) {
            reading.cancel(true); Thread.currentThread().interrupt(); return Map.of();
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof RuntimeException cause) throw cause;
            if (failed.getCause() instanceof Error cause) throw cause;
            throw new IllegalStateException("engine lifecycle read failed", failed.getCause());
        }
        return Map.copyOf(result);
    }

    @Override public void close() { readers.shutdownNow(); }

    Optional<PipelineState> currentState(String pipeline, PipelineIntentScope scope) {
        if (scope == null) return Optional.empty();
        return Optional.ofNullable(currentStates(Map.of(pipeline, scope)).get(pipeline));
    }

    private Optional<PipelineState> readCurrent(String pipeline, PipelineIntentScope scope) {
        try {
            return current(pipeline, scope);
        } catch (RuntimeException failure) {
            if (!HazelcastLivePipelineRuns.theClusterIsChanging(failure)) throw failure;
            return Optional.empty();
        }
    }

    private Optional<PipelineState> current(String pipeline, PipelineIntentScope scope) {
        if (scope == null || !pipeline.equals(scope.pipelineId()) || !active.getAsBoolean() || engine.isLost())
            return Optional.empty();
        var run = engine.captureRun(pipeline).orElse(null);
        if (run == null || !(run.operatorManifest().executionIdentity() instanceof ExecutionFence fence)
                || !scope.equals(fence.scope())) return Optional.empty();
        var plan = plans.current(List.of(pipeline)).get(pipeline);
        if (!matches(plan, fence) || !authorized.test(fence)) return Optional.empty();
        PipelineState state = switch (run.status()) {
            case RUNNING -> PipelineState.RUNNING;
            case SUSPENDED -> PipelineState.PAUSED;
            default -> null;
        };
        if (state == null || !active.getAsBoolean() || !authorized.test(fence)
                || !Objects.equals(plan, plans.current(List.of(pipeline)).get(pipeline))) return Optional.empty();
        return Optional.of(state);
    }

    private static boolean matches(ExecutionPlan plan, ExecutionFence fence) {
        return plan != null && plan.pipelineId().equals(fence.pipelineId())
                && Objects.equals(plan.claimGeneration(), fence.claimGeneration())
                && Objects.equals(plan.executionGeneration(), fence.executionGeneration())
                && Objects.equals(plan.topologyRevision(), fence.topologyRevision());
    }
}
