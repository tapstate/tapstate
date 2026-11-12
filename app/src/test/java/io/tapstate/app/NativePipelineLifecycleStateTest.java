package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.hazelcast.jet.core.JobStatus;
import io.tapstate.core.lifecycle.*;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.OperatorRunManifest;
import io.tapstate.spi.store.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class NativePipelineLifecycleStateTest {
    private static final PipelineIntentScope SCOPE = new PipelineIntentScope("cluster", 1, "cache", "orders", 2);
    private static final ExecutionFence FENCE = new ExecutionFence(SCOPE, new WorkloadClaimFence(
            new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
            new WorkloadOwner("remote-owner", "boot"), 3, 4, 5));
    private static final ExecutionPlan PLAN = new ExecutionPlan("orders", 3L, 4L, 5L,
            List.of("remote-owner", "reader"), List.of(), Instant.EPOCH);
    private final Engine engine = mock(Engine.class);
    private final Engine.Run run = mock(Engine.Run.class);
    private final ExecutionPlans plans = mock(ExecutionPlans.class);
    private final AtomicBoolean active = new AtomicBoolean(true);
    private final AtomicBoolean authorized = new AtomicBoolean(true);
    private NativePipelineLifecycleState lifecycle;

    @BeforeEach void setup() {
        when(engine.captureRun("orders")).thenReturn(Optional.of(run));
        var manifest = mock(OperatorRunManifest.class);
        when(manifest.executionIdentity()).thenReturn(FENCE);
        when(run.operatorManifest()).thenReturn(manifest);
        when(run.status()).thenReturn(JobStatus.RUNNING);
        when(plans.current(List.of("orders"))).thenReturn(Map.of("orders", PLAN));
        lifecycle = new NativePipelineLifecycleState(engine, plans,
                fence -> FENCE.equals(fence) && authorized.get(), active::get);
    }

    @AfterEach void close() { lifecycle.close(); }

    @Test void stalledEngineCallsHaveOneBatchBudgetAndCannotAccumulateWorkers() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        when(engine.captureRun(anyString())).thenAnswer(call -> {
            calls.incrementAndGet(); entered.countDown();
            while (release.getCount() != 0) {
                try { release.await(100, java.util.concurrent.TimeUnit.MILLISECONDS); }
                catch (InterruptedException ignored) { /* Model an engine invocation that ignores cancellation. */ }
            }
            return Optional.empty();
        });
        try (var caller = java.util.concurrent.Executors.newSingleThreadExecutor()) {
            try {
                var result = caller.submit(() -> lifecycle.currentStates(Map.of("orders", SCOPE,
                        "second", new PipelineIntentScope("cluster", 1, "cache", "second", 2))));
                assertThat(entered.await(1, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
                assertThat(result.get(500, java.util.concurrent.TimeUnit.MILLISECONDS)).isEmpty();
                for (int i = 0; i < 20; i++) assertThat(lifecycle.currentState("orders", SCOPE)).isEmpty();
                assertThat(calls.get()).isLessThanOrEqualTo(2);
            } finally { release.countDown(); }
        }
    }

    @Test void anotherMemberCanReadTheCurrentRunningJobWithoutMonitoring() {
        assertThat(lifecycle.currentState("orders", SCOPE)).contains(PipelineState.RUNNING);
        verify(engine).captureRun("orders");
        verify(engine, never()).hasLiveJob(anyString());
        verify(engine).isLost();
        verifyNoMoreInteractions(engine);
    }

    @Test void suspendedEngineStateDoesNotWaitForAPausedObservation() {
        when(run.status()).thenReturn(JobStatus.SUSPENDED);
        assertThat(lifecycle.currentState("orders", SCOPE)).contains(PipelineState.PAUSED);
    }

    @Test void aCheckpointWithoutAJobIsNotRunning() {
        when(engine.captureRun("orders")).thenReturn(Optional.empty());
        assertThat(lifecycle.currentState("orders", SCOPE)).isEmpty();
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(strings = {"inactive", "retryable", "master"})
    void recognizedClusterTurnoverLeavesTheStoreProjectionAvailable(String kind) {
        RuntimeException unavailable = switch (kind) {
            case "inactive" -> new com.hazelcast.core.HazelcastInstanceNotActiveException();
            case "retryable" -> new com.hazelcast.spi.exception.RetryableHazelcastException("changing membership");
            default -> new IllegalStateException("Master address unknown: instance is not yet initialized or is shut down");
        };
        when(engine.captureRun("orders")).thenThrow(unavailable);
        assertThat(lifecycle.currentState("orders", SCOPE)).isEmpty();
        verify(engine).captureRun("orders");
    }

    @Test void unexpectedEngineDefectsAreNotHiddenByTheOptionalRead() {
        when(run.status()).thenThrow(new IllegalStateException("broken invariant"));
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> lifecycle.currentState("orders", SCOPE))
                .isInstanceOf(IllegalStateException.class).hasMessage("broken invariant");
    }

    @ParameterizedTest @EnumSource(value = JobStatus.class, names = {"STARTING", "COMPLETING", "FAILED", "COMPLETED"})
    void onlyAnActualActiveEngineStateCanSupplyTheRead(JobStatus status) {
        when(run.status()).thenReturn(status);
        assertThat(lifecycle.currentState("orders", SCOPE)).isEmpty();
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(strings = {"cache", "generation", "absent"})
    void aDifferentCacheOrStateGenerationCannotReuseTheNamedJob(String kind) {
        var scope = switch (kind) {
            case "cache" -> new PipelineIntentScope("cluster", 1, "other-cache", "orders", 2);
            case "generation" -> new PipelineIntentScope("cluster", 1, "cache", "orders", 3);
            default -> null;
        };
        assertThat(lifecycle.currentState("orders", scope)).isEmpty();
        if (scope != null) verify(engine).captureRun("orders");
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(strings = {"missing", "claim", "execution", "topology", "expired"})
    void missingOrReplacedPlanAndExpiredClaimCannotProveRunning(String kind) {
        ExecutionPlan replacement = switch (kind) {
            case "missing" -> null;
            case "claim" -> new ExecutionPlan("orders", 8L, 4L, 5L, List.of(), List.of(), Instant.EPOCH);
            case "execution" -> new ExecutionPlan("orders", 3L, 8L, 5L, List.of(), List.of(), Instant.EPOCH);
            case "topology" -> new ExecutionPlan("orders", 3L, 4L, 8L, List.of(), List.of(), Instant.EPOCH);
            default -> PLAN;
        };
        when(plans.current(List.of("orders"))).thenReturn(replacement == null ? Map.of() : Map.of("orders", replacement));
        if (kind.equals("expired")) authorized.set(false);
        assertThat(lifecycle.currentState("orders", SCOPE)).isEmpty();
        verify(engine).captureRun("orders");
    }

    @ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(strings = {"claim", "context", "plan"})
    void replacementOrRevocationDuringTheEngineReadDiscardsItsResult(String kind) {
        if (kind.equals("claim")) doAnswer(call -> { authorized.set(false); return JobStatus.RUNNING; }).when(run).status();
        else if (kind.equals("context")) doAnswer(call -> { active.set(false); return JobStatus.RUNNING; }).when(run).status();
        else when(plans.current(List.of("orders"))).thenReturn(Map.of("orders", PLAN), Map.of());
        assertThat(lifecycle.currentState("orders", SCOPE)).isEmpty();
        verify(run).status();
    }
}
