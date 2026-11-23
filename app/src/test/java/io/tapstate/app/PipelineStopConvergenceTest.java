package io.tapstate.app;

import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.runtime.scheduler.RebuildAdmission;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.PipelineStoreViews;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class PipelineStopConvergenceTest {
    enum Hold { TARGET_CLOSE, STATE_WRITE, INTENT_READ, PURGE_CLOSE }

    @ParameterizedTest
    @EnumSource(Hold.class)
    void aHundredStopsRotateWhileFourOriginalOperationsAreStillHeld(Hold blockedAt) throws Exception {
        Clock clock = Clock.systemUTC();
        List<String> ids = IntStream.range(0, 100).mapToObj(i -> "pipeline-" + i).toList();
        List<String> held = ids.subList(0, 4);
        var listed = new AtomicReference<>(held);
        var entered = new CountDownLatch(4);
        var release = new CountDownLatch(1);
        var ioMonitors = new ConcurrentHashMap<String, Object>();
        var actual = new InMemoryStateStore();
        var calls = new ConcurrentHashMap<String, AtomicInteger>();
        Set<String> cleared = ConcurrentHashMap.newKeySet();
        var intents = mock(PipelineIntentStore.class);
        var states = mock(PipelineStoreViews.class);
        var ownership = mock(PipelineActuationOwnership.class);
        var claims = new InMemoryWorkloadClaimStore();
        var owner = new WorkloadOwner("node", "boot");
        for (String id : ids) actual.create(id, StateJson.of(PipelineState.RUNNING), clock.instant());
        when(intents.scanCandidates()).thenAnswer(call -> listed.get().stream()
                .map(id -> new PipelineIntentStore.ScanCandidate(id, 0,
                        new PipelineIntentScope("cluster", 1, "cache", id, 1), 1, PipelineState.STOPPED)).toList());
        when(intents.loadForActuation(anyString())).thenAnswer(call -> {
            String id = call.getArgument(0);
            if (blockedAt == Hold.INTENT_READ && held.contains(id)) hold(ioMonitors, id, entered, release);
            var scope = new PipelineIntentScope("cluster", 1, "cache", id, 1);
            return Optional.of(new PipelineIntentSnapshot(scope, 1,
                    new DesiredState(id, PipelineState.STOPPED, "definition", blockedAt == Hold.PURGE_CLOSE && !cleared.contains(id))));
        });
        when(ownership.permit(any(PipelineIntentScope.class))).thenAnswer(call -> {
            PipelineIntentScope scope = call.getArgument(0);
            var key = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, scope.pipelineId());
            return new PipelineActuationOwnership.Permit(true, claims.acquire(key, owner, 1, Duration.ofMinutes(1)).claim());
        });
        when(ownership.enterPass(any())).thenReturn(mock(PipelineActuationOwnership.Pass.class));
        StateStore scopedState = spy(actual);
        doAnswer(call -> {
            String id = call.getArgument(0);
            if (blockedAt == Hold.STATE_WRITE && held.contains(id)) hold(ioMonitors, id, entered, release);
            return call.callRealMethod();
        }).when(scopedState).compareAndSwap(anyString(), anyLong(), anyString(), any());
        when(states.pipelineStates(any())).thenReturn(scopedState);
        when(states.pipelineStates(any(), any(), any())).thenReturn(scopedState);
        LifecycleActuator actuator = mock(LifecycleActuator.class);
        doAnswer(call -> {
            String id = call.getArgument(0);
            calls.computeIfAbsent(id, ignored -> new AtomicInteger()).incrementAndGet();
            if ((blockedAt == Hold.TARGET_CLOSE || blockedAt == Hold.PURGE_CLOSE) && held.contains(id))
                hold(ioMonitors, id, entered, release);
            return null;
        }).when(actuator).stop(anyString(), eq(false));
        doAnswer(call -> { cleared.add(call.getArgument(0)); return null; }).when(actuator).clearStopped(anyString());
        var scoped = new ConvergenceDriver.ScopedPorts(intents, states, () -> {},
                ignored -> actuator, ignored -> RebuildAdmission.never(), ignored -> {});
        var converger = new PipelineConverger(mock(DesiredStore.class), mock(StateStore.class),
                mock(LifecycleActuator.class), clock, RebuildAdmission.never());
        try (var work = new PipelineConvergences()) {
            var driver = ConvergenceDriver.scoped(converger, mock(ObservationPublisher.class), null,
                    MetricsExport.none(), () -> true, ownership, scoped, null, work);
            try {
                driver.reconcile();
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                listed.set(ids.subList(0, 50));
                await().atMost(Duration.ofSeconds(7)).until(() -> {
                    driver.reconcile();
                    return stopped(scopedState, ids.subList(4, 50));
                });
                listed.set(ids);
                await().atMost(Duration.ofSeconds(7)).until(() -> {
                    driver.reconcile();
                    return stopped(scopedState, ids.subList(50, 100));
                });
                assertThat(held).allSatisfy(id -> assertThat(scopedState.read(id).orElseThrow().stateJson())
                        .isEqualTo(StateJson.of(PipelineState.RUNNING)));
            } finally {
                release.countDown();
            }
            await().atMost(Duration.ofSeconds(7)).until(() -> {
                driver.reconcile();
                return stopped(scopedState, ids) && (blockedAt != Hold.PURGE_CLOSE || cleared.size() == 100);
            });
            assertThat(calls).hasSize(100).allSatisfy((id, count) -> assertThat(count).hasValue(1));
            verify(actuator, times(blockedAt == Hold.PURGE_CLOSE ? 100 : 0)).clearStopped(anyString());
        }
    }

    @org.junit.jupiter.api.Test
    void anOldStopHintCannotCancelANewerRunIntent() {
        var intents = mock(PipelineIntentStore.class);
        var scope = new PipelineIntentScope("cluster", 1, "cache", "pipeline", 1);
        when(intents.scanCandidates()).thenReturn(List.of(
                new PipelineIntentStore.ScanCandidate("pipeline", 0, scope, 1, PipelineState.STOPPED)));
        var reads = new AtomicInteger();
        when(intents.loadForActuation("pipeline")).thenAnswer(call -> {
            reads.incrementAndGet();
            return Optional.of(new PipelineIntentSnapshot(scope, 2,
                    new DesiredState("pipeline", PipelineState.RUNNING, "new-definition")));
        });
        var states = mock(PipelineStoreViews.class);
        var ownership = mock(PipelineActuationOwnership.class);
        var actuator = mock(LifecycleActuator.class);
        var scoped = new ConvergenceDriver.ScopedPorts(intents, states, () -> {},
                ignored -> actuator, ignored -> RebuildAdmission.never(), ignored -> {});
        var converger = new PipelineConverger(mock(DesiredStore.class), mock(StateStore.class),
                actuator, Clock.systemUTC(), RebuildAdmission.never());
        try (var work = new PipelineConvergences()) {
            var driver = ConvergenceDriver.scoped(converger, mock(ObservationPublisher.class), null,
                    MetricsExport.none(), () -> true, ownership, scoped, null, work);
            driver.reconcile();
            await().atMost(Duration.ofSeconds(3)).until(() -> reads.get() == 1 && !work.stopping("pipeline"));
            verify(ownership, never()).permit(any(PipelineIntentScope.class));
            verifyNoInteractions(states, actuator);
        }
    }

    private static boolean stopped(StateStore state, List<String> ids) {
        return ids.stream().allMatch(id -> state.read(id).orElseThrow().stateJson().equals(StateJson.of(PipelineState.STOPPED)));
    }

    private static void hold(ConcurrentHashMap<String, Object> monitors, String id,
            CountDownLatch entered, CountDownLatch release) throws InterruptedException {
        synchronized (monitors.computeIfAbsent(id, ignored -> new Object())) {
            entered.countDown();
            assertThat(release.await(25, TimeUnit.SECONDS)).isTrue();
        }
    }
}
