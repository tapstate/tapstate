package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoCachePipelineRunProgressStore;
import io.tapstate.adapters.mongostore.MongoCachePipelineRunProgressStore.Execution;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.InitialLoad;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineRunProgress;
import io.tapstate.core.lifecycle.PipelineRunStage;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.engine.OperatorRunManifest;
import io.tapstate.runtime.srs.ChangeStreamReadyObserver;
import io.tapstate.spi.store.CacheBusinessScope;
import io.tapstate.spi.store.ConnectorRunBindings;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class NativePipelineRunProgressPublisherTest {
    @ParameterizedTest
    @EnumSource(value = PipelineState.class, names = {"FAILED", "STOPPED"})
    void terminalObservationRetainsPreparationAfterTheMilestoneAndBindingWritesFail(PipelineState terminal) throws Exception {
        var scope = new PipelineIntentScope("cluster", 1, "cache", "orders", 1);
        var key = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
        var owner = new WorkloadOwner("node", "boot");
        var planning = new WorkloadClaimFence(key, owner, 1, 0, 3);
        var execution = new WorkloadClaimFence(key, owner, 1, 2, 3);
        var start = new PipelineIntentSnapshot(scope, 4,
                new DesiredState("orders", PipelineState.RUNNING, "a".repeat(64)));
        var stopped = new PipelineIntentSnapshot(scope, 5,
                new DesiredState("orders", PipelineState.STOPPED, "a".repeat(64)));
        var terminalIntent = terminal == PipelineState.STOPPED ? stopped : start;
        var cache = mock(MongoCacheDomain.class);
        var planningStore = mock(MongoCachePipelineRunProgressStore.class);
        var executionStore = mock(MongoCachePipelineRunProgressStore.class);
        when(cache.pipelineRunProgress(eq(scope), eq(planning), any())).thenReturn(planningStore);
        when(cache.pipelineRunProgress(eq(scope), eq(execution), any())).thenReturn(executionStore);
        var intents = mock(PipelineIntentStore.class);
        var bindings = new ConnectorRunBindings(scope, 4, List.of(), null);
        when(intents.executionBindings(scope)).thenReturn(Optional.of(new PipelineIntentStore.ExecutionBindings(bindings, 4)));
        var member = mock(HazelcastInstance.class);
        when(member.getUserContext()).thenReturn(new ConcurrentHashMap<>());
        var unavailable = new TapstateException(IoError.STORE_UNAVAILABLE, Map.of(), null);
        when(planningStore.update(eq(4L), any(), eq(Execution.KEEP), any())).thenThrow(unavailable);
        var bindingAttempt = new CountDownLatch(1);
        when(executionStore.update(eq(4L), any(), eq(Execution.BIND), any())).thenAnswer(call -> {
            bindingAttempt.countDown();
            throw unavailable;
        });
        var planned = Instant.parse("2026-11-26T10:00:00Z");
        var connectors = planned.plusSeconds(1);
        var captureStage = PipelineRunStage.SOURCE_CAPTURE_PREPARING;
        var started = NativePipelineRunProgressQuery.accepted(scope, 4, null, 3);
        var preparing = started.enter(captureStage, connectors);
        var previous = terminal == PipelineState.FAILED ? preparing.leave(captureStage, connectors, true) : preparing;
        when(executionStore.latest()).thenReturn(Optional.of(new MongoCachePipelineRunProgressStore.Snapshot(4, previous)));
        var terminalWrite = new CompletableFuture<PipelineRunProgress>();
        when(executionStore.update(eq(4L), any(), eq(Execution.KEEP), any()))
                .thenThrow(unavailable)
                .thenAnswer(call -> {
                    UnaryOperator<PipelineRunProgress> change = call.getArgument(3);
                    terminalWrite.complete(change.apply(previous));
                    return true;
                });
        try (var publisher = new NativePipelineRunProgressPublisher(cache, new CacheBusinessScope("cluster", 1, "cache"),
                intents, () -> {}, member, mock(Engine.class))) {
            publisher.publish(new PipelineRunProgressEvents.Event(start, planning, PipelineRunStage.EXECUTION_PLAN_PREPARED,
                    PipelineRunProgressEvents.Edge.REACHED, planned, 4));
            publisher.publish(new PipelineRunProgressEvents.Event(start, planning, PipelineRunStage.CONNECTORS_READY,
                    PipelineRunProgressEvents.Edge.REACHED, connectors, 4));
            publisher.bind(start, execution, 4, PipelineRunProgress.Mode.SNAPSHOT_ONLY, planned, connectors);
            assertThat(bindingAttempt.await(5, TimeUnit.SECONDS)).isTrue();
            verify(planningStore, times(2)).update(eq(4L), any(), eq(Execution.KEEP), any());
            when(intents.executionBindings(scope)).thenReturn(
                    Optional.of(new PipelineIntentStore.ExecutionBindings(bindings, terminalIntent.intentRevision())));
            publisher.observation(terminalIntent, execution, new Observation(
                    "orders", terminal, Map.of(), Map.of(), Map.of(), null, connectors.plusSeconds(1)));

            var progress = terminalWrite.get(5, TimeUnit.SECONDS);
            assertThat(progress.milestones()).containsExactly(started.milestones().getFirst(),
                    new PipelineRunProgress.Milestone(PipelineRunStage.EXECUTION_PLAN_PREPARED, planned),
                    new PipelineRunProgress.Milestone(PipelineRunStage.CONNECTORS_READY, connectors));
            assertThat(progress.active()).isEmpty();
            assertThat(progress.runFailed()).isEqualTo(terminal == PipelineState.FAILED);
            assertThat(progress.failedStage()).isEqualTo(previous.failedStage());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 7})
    void cdcReadinessSurvivesTemporaryStoreFailures(int failures) throws Exception {
        var scope = new PipelineIntentScope("cluster", 1, "cache", "orders", 1);
        var intent = new PipelineIntentSnapshot(scope, 4, new DesiredState("orders", PipelineState.RUNNING, "a".repeat(64)));
        var claim = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                new WorkloadOwner("node", "boot"), 1, 2, 3);
        var original = new ExecutionFence(scope, claim);
        var engine = mock(Engine.class);
        var run = mock(Engine.Run.class);
        when(run.jobId()).thenReturn(23L);
        when(run.operatorManifest()).thenReturn(new OperatorRunManifest("orders", original, List.of()));
        when(engine.captureRun("orders")).thenReturn(Optional.of(run));
        var cache = mock(MongoCacheDomain.class);
        var store = mock(MongoCachePipelineRunProgressStore.class);
        when(cache.pipelineRunProgress(eq(scope), eq(claim), any())).thenReturn(store);
        var intents = mock(PipelineIntentStore.class);
        when(intents.loadForActuation("orders")).thenReturn(Optional.of(intent));
        when(intents.executionBindings(scope)).thenReturn(Optional.of(new PipelineIntentStore.ExecutionBindings(
                new ConnectorRunBindings(scope, 4, List.of(), null), intent.intentRevision())));
        var context = new ConcurrentHashMap<String, Object>();
        var member = mock(HazelcastInstance.class);
        when(member.getUserContext()).thenReturn(context);
        var written = new CompletableFuture<PipelineRunProgress>();
        var remainingFailures = new AtomicInteger(failures);
        when(store.update(eq(4L), any(), eq(Execution.REQUIRE_BOUND), any()))
                .thenAnswer(call -> {
                    if (remainingFailures.getAndDecrement() > 0) {
                        throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of(), null);
                    }
                    UnaryOperator<PipelineRunProgress> change = call.getArgument(3);
                    written.complete(change.apply(call.getArgument(1)));
                    return true;
                });
        Instant ready = Instant.parse("2026-11-26T10:00:00Z");
        var business = new CacheBusinessScope("cluster", 1, "cache");
        try (var publisher = new NativePipelineRunProgressPublisher(cache, business, intents, () -> {},
                member, engine)) {
            var observer = (ChangeStreamReadyObserver) context.get(ChangeStreamReadyObserver.USER_CONTEXT_KEY);
            assertThat(observer.ready(original, 23, 29, ready)).isFalse();
            if (failures > 1) {
                org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(8))
                        .until(() -> observer.ready(original, 23, 29, ready));
            }
            assertThat(written.get(5, TimeUnit.SECONDS).milestones())
                    .contains(new PipelineRunProgress.Milestone(PipelineRunStage.CDC_READY, ready));
            verify(engine, times(failures + 1)).captureRun("orders");
        }
    }

    @Test
    void liveEngineProgressRetriesTheOriginalBindingAfterTheFirstWriteFails() throws Exception {
        var scope = new PipelineIntentScope("cluster", 1, "cache", "orders", 1);
        var business = new CacheBusinessScope("cluster", 1, "cache");
        var intent = new PipelineIntentSnapshot(scope, 4, new DesiredState("orders", PipelineState.RUNNING, "a".repeat(64)));
        var claim = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                new WorkloadOwner("node", "boot"), 1, 2, 3);
        var cache = mock(MongoCacheDomain.class);
        var store = mock(MongoCachePipelineRunProgressStore.class);
        var intents = mock(PipelineIntentStore.class);
        var member = mock(HazelcastInstance.class);
        when(member.getUserContext()).thenReturn(new ConcurrentHashMap<>());
        when(cache.pipelineRunProgress(eq(scope), eq(claim), any())).thenReturn(store);
        when(intents.executionBindings(scope)).thenReturn(Optional.of(new PipelineIntentStore.ExecutionBindings(
                new ConnectorRunBindings(scope, 0, List.of(), null), intent.intentRevision())));
        var firstWrite = new CountDownLatch(1);
        var recovered = new CompletableFuture<PipelineRunProgress>();
        when(store.update(eq(4L), any(), eq(Execution.BIND), any()))
                .thenAnswer(call -> {
                    firstWrite.countDown();
                    throw new TapstateException(IoError.STORE_UNAVAILABLE, Map.of(), null);
                })
                .thenAnswer(call -> {
                    UnaryOperator<PipelineRunProgress> change = call.getArgument(3);
                    recovered.complete(change.apply(call.getArgument(1)));
                    return true;
                });
        Instant prepared = Instant.parse("2026-11-26T10:00:00Z");
        Instant ready = prepared.plusSeconds(1);
        Instant running = ready.plusSeconds(1);
        try (var publisher = new NativePipelineRunProgressPublisher(cache, business, intents, () -> {},
                member, mock(Engine.class))) {
            publisher.bind(intent, claim, 4, PipelineRunProgress.Mode.SNAPSHOT_ONLY, prepared, ready);
            assertThat(firstWrite.await(5, TimeUnit.SECONDS)).isTrue();
            publisher.publish(new PipelineRunProgressEvents.Event(intent, claim, PipelineRunStage.ENGINE_RUNNING,
                    PipelineRunProgressEvents.Edge.REACHED, running, 0));
            var progress = recovered.get(5, TimeUnit.SECONDS);
            assertThat(progress.readMode()).isEqualTo(PipelineRunProgress.Mode.SNAPSHOT_ONLY);
            assertThat(progress.milestones()).containsExactly(
                    new PipelineRunProgress.Milestone(PipelineRunStage.START_ACCEPTED, null),
                    new PipelineRunProgress.Milestone(PipelineRunStage.EXECUTION_PLAN_PREPARED, prepared),
                    new PipelineRunProgress.Milestone(PipelineRunStage.CONNECTORS_READY, ready),
                    new PipelineRunProgress.Milestone(PipelineRunStage.ENGINE_RUNNING, running));
        }
    }

    @ParameterizedTest
    @EnumSource(value = PipelineState.class, names = {"COMPLETED", "FAILED"})
    void observationsKeepCompletionAndFailureDistinct(PipelineState state) throws Exception {
        var scope = new PipelineIntentScope("cluster", 1, "cache", "orders", 1);
        var intent = new PipelineIntentSnapshot(scope, 4, new DesiredState("orders", PipelineState.RUNNING, "a".repeat(64)));
        var claim = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "orders"),
                new WorkloadOwner("node", "boot"), 1, 2, 3);
        var cache = mock(MongoCacheDomain.class);
        var store = mock(MongoCachePipelineRunProgressStore.class);
        when(cache.pipelineRunProgress(eq(scope), eq(claim), any())).thenReturn(store);
        var intents = mock(PipelineIntentStore.class);
        when(intents.executionBindings(scope)).thenReturn(Optional.of(new PipelineIntentStore.ExecutionBindings(
                new ConnectorRunBindings(scope, 4, List.of(), null), intent.intentRevision())));
        var member = mock(HazelcastInstance.class);
        when(member.getUserContext()).thenReturn(new ConcurrentHashMap<>());
        var started = NativePipelineRunProgressQuery.accepted(scope, 4, null, 3)
                .beginExecution(PipelineRunProgress.Mode.SNAPSHOT_ONLY);
        var current = new AtomicReference<>(started);
        var terminal = new CompletableFuture<PipelineRunProgress>();
        var execution = state == PipelineState.FAILED ? Execution.KEEP : Execution.REQUIRE_BOUND;
        when(store.update(eq(4L), any(), eq(execution), any())).thenAnswer(call -> {
            UnaryOperator<PipelineRunProgress> change = call.getArgument(3);
            var updated = current.updateAndGet(change);
            if (updated.runFailed() || updated.milestones().stream().anyMatch(m -> m.stage() == PipelineRunStage.RUN_COMPLETED)) {
                terminal.complete(updated);
            }
            return true;
        });
        Instant at = Instant.parse("2026-11-26T10:00:00Z");
        var load = state == PipelineState.COMPLETED ? new InitialLoad("execution", at, "snapshot_only",
                Map.of("orders", new InitialLoad.Table(InitialLoad.Stage.LANDED, 2L, 2L))) : null;
        var observed = new Observation("orders", state, Map.of(), Map.of(), Map.of(), null, at, List.of(), load);
        try (var publisher = new NativePipelineRunProgressPublisher(cache, new CacheBusinessScope("cluster", 1, "cache"),
                intents, () -> {}, member, mock(Engine.class))) {
            publisher.observation(intent, claim, observed);
            var progress = terminal.get(5, TimeUnit.SECONDS);
            assertThat(progress.awaitingMilestones()).isFalse();
            if (state == PipelineState.COMPLETED) {
                assertThat(progress.runFailed()).isFalse();
                assertThat(progress.milestones()).containsExactly(started.milestones().getFirst(),
                        new PipelineRunProgress.Milestone(PipelineRunStage.INITIAL_LOAD_COMPLETED, at),
                        new PipelineRunProgress.Milestone(PipelineRunStage.RUN_COMPLETED, at));
            } else {
                assertThat(progress.runFailed()).isTrue();
                assertThat(progress.milestones()).isEqualTo(started.milestones());
            }
        }
    }
}
