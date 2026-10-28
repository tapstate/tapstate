package io.tapstate.app;

import io.tapstate.core.alerting.*;
import io.tapstate.core.lifecycle.*;
import io.tapstate.spi.store.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;
import static org.mockito.ArgumentMatchers.*;

class PipelineAlertRuntimeTest {
    static final Instant T = AlertEvaluationHostTest.T;
    final AlertEvaluationHostTest fixture = new AlertEvaluationHostTest();
    final ConcurrentMap<AlarmKey, AlertStore.Stored<AlertStateRecord.Head>> rows = new ConcurrentHashMap<>();
    final ConcurrentMap<String, PipelineIntentSnapshot> intents = new ConcurrentHashMap<>();
    final PipelineIntentStore intentStore = mock(PipelineIntentStore.class);
    PipelineAlertRuntimeTest() {
        when(intentStore.loadForActuation(anyString())).thenAnswer(c -> Optional.ofNullable(intents.get(c.getArgument(0))));
        when(fixture.store.state(any(), any())).thenAnswer(c -> Optional.ofNullable(rows.get(c.getArgument(0))));
        var definition = AlertEvaluationHostTest.definition("pipeline.failed");
        when(fixture.store.evaluationRules(any(), anyInt())).thenReturn(new AlertStore.DefinitionPage(List.of(definition), null));
        when(fixture.store.evaluationStates(any(),anyInt())).thenAnswer(c->new AlertStore.EvaluationPage(List.copyOf(rows.values()),null));
        doAnswer(c -> {
            AlertStore.Commit commit = c.getArgument(0); var change = commit.state();
            rows.compute(change.after().snapshot().rule().key(), (key, before) -> new AlertStore.Stored<>(before == null ? 1 : before.revision() + 1, change.after()));
            return AlertStore.Outcome.STAGED;
        }).when(fixture.store).commit(any());
    }
    PipelineConvergencePass pass(String id) {
        var scope = new PipelineIntentScope("cluster", 1, "cache", id, 1);
        var intent = new PipelineIntentSnapshot(scope, 1, new DesiredState(id, PipelineState.RUNNING, "revision"));
        intents.put(id, intent);
        var claim = new WorkloadClaimFence(new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, id),
                new WorkloadOwner("node", "boot"), 1, 1, 1);
        var pass = mock(PipelineConvergencePass.class);
        when(pass.scope()).thenReturn(scope); when(pass.claim()).thenReturn(claim); when(pass.intent()).thenReturn(intent);
        return pass;
    }
    PipelineAlertRuntime runtime() {
        return new PipelineAlertRuntime(() -> new PipelineAlertRuntime.Services(fixture.store, fixture.host), intentStore,
                (scope, claim) -> Optional.of(()->{}), () -> { }, fixture.clock);
    }
    static Observation observation(String id, Instant at, PipelineState state) {
        return new Observation(id, state, Map.of(), Map.of(), Map.of(), null, at, List.of());
    }
    @Test void oneBlockedObjectDoesNotOccupyBothWorkersAndStarveAnotherObject() throws Exception {
        var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
        doAnswer(c -> {
            AlertStore.Commit commit = c.getArgument(0);
            if (commit.state().after().snapshot().rule().key().resource().id().equals("a")) {
                entered.countDown();
                try { assertThat(release.await(5, TimeUnit.SECONDS)).isTrue(); }
                catch(InterruptedException interrupted){Thread.currentThread().interrupt();return AlertStore.Outcome.CONFLICT;}
            }
            var head = commit.state().after(); rows.put(head.snapshot().rule().key(), new AlertStore.Stored<>(1, head));
            return AlertStore.Outcome.STAGED;
        }).when(fixture.store).commit(any());
        try (var runtime = runtime()) {
            runtime.tick(); await().atMost(Duration.ofSeconds(3)).until(() -> !runtime.health().degraded());
            var a = pass("a"); var b = pass("b");
            runtime.published(a, observation("a", T, PipelineState.FAILED), null);
            assertThat(entered.await(3, TimeUnit.SECONDS)).isTrue();
            fixture.now.set(T.plusSeconds(1));
            runtime.published(a, observation("a", T.plusSeconds(1), PipelineState.FAILED), null);
            runtime.published(b, observation("b", T, PipelineState.FAILED), null);
            try { await().atMost(Duration.ofSeconds(2)).until(() -> rows.keySet().stream().anyMatch(k -> k.resource().id().equals("b"))); }
            finally { release.countDown(); }
            await().atMost(Duration.ofSeconds(3)).until(()->runtime.health().queuedInputs()==0);
        } finally { release.countDown(); }
    }
    @Test void freshnessRunsWithoutPublicationsAndStopDoesNotInventRecovery() {
        var rule=AlertEvaluationHostTest.definition("pipeline.observation-stale");
        when(fixture.store.evaluationRules(any(),anyInt())).thenReturn(new AlertStore.DefinitionPage(List.of(rule),null));
        try(var runtime=runtime()){
            runtime.tick();await().atMost(Duration.ofSeconds(3)).until(()->!runtime.health().degraded());
            var pass=pass("orders");runtime.expecting(pass);
            fixture.now.set(T.plusSeconds(121));runtime.tick();
            await().atMost(Duration.ofSeconds(3)).until(()->rows.values().stream().anyMatch(v->v.value().snapshot().status()==AlertSnapshot.Status.FIRING));
            var original=rows.values().iterator().next().value();
            var stopped=new PipelineIntentSnapshot(pass.scope(),2,new DesiredState("orders",PipelineState.STOPPED,"revision"));
            intents.put("orders",stopped);var next=mock(PipelineConvergencePass.class);var originalScope=pass.scope();var originalClaim=pass.claim();
            when(next.scope()).thenReturn(originalScope);when(next.claim()).thenReturn(originalClaim);when(next.intent()).thenReturn(stopped);
            fixture.now.set(T.plusSeconds(122));runtime.published(next,observation("orders",fixture.now.get(),PipelineState.STOPPED),null);runtime.tick();
            await().atMost(Duration.ofSeconds(3)).until(()->rows.values().iterator().next().value().snapshot().quality()==AlertSnapshot.Quality.UNKNOWN);
            var after=rows.values().iterator().next().value();
            assertThat(after.episode().value()).isEqualTo(original.episode().value());
            assertThat(after.snapshot().rule().generation()).isEqualTo(original.snapshot().rule().generation());
            assertThat(after.snapshot().status()).isEqualTo(AlertSnapshot.Status.FIRING);
        }
    }
    @Test void aQueueRefusalCannotBridgeThePreviousPendingWindow() {
        var original=AlertEvaluationHostTest.definition("pipeline.failed");
        var rule=new AlertConfiguration.Rule(original.header(),original.name(),original.typeId(),1,original.resources(),
                Map.of("trigger-for",java.math.BigDecimal.valueOf(4)),null,true,null).validated(BuiltinAlarmTypes.catalog());
        when(fixture.store.evaluationRules(any(),anyInt())).thenReturn(new AlertStore.DefinitionPage(List.of(rule),null));
        try(var runtime=runtime()){
            runtime.tick();await().atMost(Duration.ofSeconds(3)).until(()->!runtime.health().degraded());
            var pass=pass("orders");runtime.published(pass,observation("orders",T,PipelineState.FAILED),null);
            await().atMost(Duration.ofSeconds(3)).until(()->!rows.isEmpty());
            var tooMany=new ArrayList<MetricFact>();for(int i=0;i<4097;i++)tooMany.add(new MetricFact("test.quantity"+i,MetricType.GAUGE,"1",List.of()));
            fixture.now.set(T.plusSeconds(2));runtime.published(pass,new Observation("orders",PipelineState.FAILED,
                    Map.of(),Map.of(),Map.of(),null,fixture.now.get(),tooMany),null);
            assertThat(runtime.health().rejected()).isPositive();assertThat(runtime.health().degraded()).isTrue();
            fixture.now.set(T.plusSeconds(4));runtime.published(pass,observation("orders",fixture.now.get(),PipelineState.FAILED),null);
            await().atMost(Duration.ofSeconds(3)).until(()->T.plusSeconds(4).equals(rows.values().iterator().next().value().snapshot().lastObservedAt()));
            var state=rows.values().iterator().next().value().snapshot();
            assertThat(state.status()).isEqualTo(AlertSnapshot.Status.PENDING);assertThat(state.pendingSince()).isEqualTo(T.plusSeconds(4));
        }
    }
    @Test void boundedQueueAndCloseReleasePendingWorkWithoutReopeningTheHost() throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
        doAnswer(c->{entered.countDown();try{release.await(5,TimeUnit.SECONDS);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();return AlertStore.Outcome.CONFLICT;}return AlertStore.Outcome.STAGED;}).when(fixture.store).commit(any());
        var runtime=runtime();
        try{
            runtime.tick();await().atMost(Duration.ofSeconds(3)).until(()->!runtime.health().degraded());
            var pass=pass("orders");runtime.published(pass,observation("orders",T,PipelineState.FAILED),null);
            assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue();
            for(int i=0;i<80;i++)runtime.published(pass,observation("orders",T,PipelineState.FAILED),null);
            assertThat(runtime.health().rejected()).isPositive();assertThat(runtime.health().queuedInputs()).isLessThanOrEqualTo(64);
            assertThat(runtime.health().queuedBytes()).isLessThanOrEqualTo(8*1024*1024);
            runtime.close();release.countDown();
            await().atMost(Duration.ofSeconds(3)).until(()->runtime.health().queuedInputs()==0);
            assertThat(runtime.health().queuedBytes()).isZero();assertThat(runtime.health().closed()).isTrue();
            int requests=mockingDetails(fixture.store).getInvocations().size();runtime.tick();runtime.published(pass,observation("orders",T,PipelineState.FAILED),null);
            assertThat(mockingDetails(fixture.store).getInvocations()).hasSize(requests);
        }finally{release.countDown();runtime.close();}
    }
    @Test void exhaustedIndependentTickCommitsAreReportedAsDegradedCollection() {
        try(var runtime=runtime()){
            runtime.tick();await().atMost(Duration.ofSeconds(3)).until(()->!runtime.health().degraded());
            var pass=pass("orders");runtime.published(pass,observation("orders",T,PipelineState.RUNNING),null);
            await().atMost(Duration.ofSeconds(3)).until(()->!rows.isEmpty());
            var conflicts=new AtomicInteger();doAnswer(c->{conflicts.incrementAndGet();return AlertStore.Outcome.CONFLICT;}).when(fixture.store).commit(any());
            fixture.now.set(T.plusSeconds(16));runtime.tick();
            await().atMost(Duration.ofSeconds(3)).until(()->conflicts.get()>=3);
            await().atMost(Duration.ofSeconds(1)).untilAsserted(()->assertThat(runtime.health().degraded()).isTrue());
        }
    }
    @Test void theOwnedFreshnessTimerRunsWithoutAnyConvergenceOrManualTick() {
        var rule=AlertEvaluationHostTest.definition("pipeline.observation-stale");
        when(fixture.store.evaluationRules(any(),anyInt())).thenReturn(new AlertStore.DefinitionPage(List.of(rule),null));
        try(var runtime=runtime()){
            var pass=pass("orders");runtime.expecting(pass);fixture.now.set(T.plusSeconds(121));runtime.start();
            await().atMost(Duration.ofSeconds(4)).until(()->rows.values().stream().anyMatch(r->r.value().snapshot().status()==AlertSnapshot.Status.FIRING));
            assertThat(runtime.health().closed()).isFalse();
        }
    }
    @Test void closeCancelsQueuedReservationsEvenAfterTheirSlotWasRemoved() throws Exception {
        var entered=new CountDownLatch(2);var release=new CountDownLatch(1);
        doAnswer(c->{entered.countDown();try{release.await(5,TimeUnit.SECONDS);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();}return AlertStore.Outcome.CONFLICT;})
                .when(fixture.store).commit(any());
        var runtime=runtime();
        try{
            runtime.tick();await().atMost(Duration.ofSeconds(3)).until(()->!runtime.health().degraded());
            runtime.published(pass("b"),observation("b",T,PipelineState.FAILED),null);
            runtime.published(pass("c"),observation("c",T,PipelineState.FAILED),null);
            assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue();
            runtime.published(pass("a"),observation("a",T,PipelineState.FAILED),null);
            runtime.retain(Set.of("b","c"));runtime.close();release.countDown();
            await().atMost(Duration.ofSeconds(2)).until(()->runtime.health().queuedInputs()==0);
            assertThat(runtime.health().queuedBytes()).isZero();
        }finally{release.countDown();runtime.close();}
    }
    @Test void closingDuringServiceCreationDisposesTheUnpublishedPlanner() throws Exception {
        var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var planning=mock(AlertPlanningRuntime.class);
        when(fixture.store.evaluationRules(any(),anyInt())).thenAnswer(call->{entered.countDown();boolean interrupted=false;for(;;)try{release.await();break;}catch(InterruptedException expected){interrupted=true;}if(interrupted)Thread.currentThread().interrupt();return new AlertStore.DefinitionPage(List.of(),null);});
        var runtime=new PipelineAlertRuntime(()->new PipelineAlertRuntime.Services(fixture.store,fixture.host,BuiltinAlarmTypes.catalog(),planning),intentStore,(scope,claim)->Optional.of(()->{}),()->{},fixture.clock);
        try{runtime.tick();assertThat(entered.await(3,TimeUnit.SECONDS)).isTrue();runtime.close();release.countDown();verify(planning,timeout(3000)).close();verify(planning,never()).start();assertThat(runtime.health().closed()).isTrue();}
        finally{release.countDown();runtime.close();}
    }

}
