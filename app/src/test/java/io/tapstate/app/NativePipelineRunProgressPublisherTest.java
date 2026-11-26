package io.tapstate.app;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.hazelcast.core.HazelcastInstance;
import io.tapstate.adapters.mongostore.MongoCacheDomain;
import io.tapstate.adapters.mongostore.MongoCachePipelineRunProgressStore;
import io.tapstate.adapters.mongostore.MongoCachePipelineRunProgressStore.Execution;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineRunProgress;
import io.tapstate.core.lifecycle.PipelineRunStage;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.engine.Engine;
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
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;

class NativePipelineRunProgressPublisherTest {
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
                member, mock(Engine.class), Clock.systemUTC())) {
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
}
