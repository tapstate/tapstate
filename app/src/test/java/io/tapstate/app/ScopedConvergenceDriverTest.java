package io.tapstate.app;

import com.sun.net.httpserver.HttpServer;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.runtime.scheduler.RebuildAdmission;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.IoError;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.PipelineIntentScope;
import io.tapstate.spi.store.PipelineIntentSnapshot;
import io.tapstate.spi.store.PipelineIntentStore;
import io.tapstate.spi.store.PipelineStoreViews;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.BooleanSupplier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ScopedConvergenceDriverTest {
    private static final Instant T0 = Instant.parse("2026-10-18T00:00:00Z");
    private static final PipelineIntentScope FIRST = scope(1);
    private static final WorkloadOwner OWNER = new WorkloadOwner("node", "boot");
    private static final WorkloadClaimKey KEY = new WorkloadClaimKey("cluster", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
    private final InMemoryWorkloadClaimStore claims = new InMemoryWorkloadClaimStore();
    private final Intents intents = new Intents();
    private final Views views = new Views();
    private PipelineActuationOwnership ownership = mock(PipelineActuationOwnership.class);
    private boolean realOwnership;
    private final AtomicBoolean eligible = new AtomicBoolean(true);
    private final AtomicBoolean carrying = new AtomicBoolean();
    private final List<PipelineConvergencePass> passes = new ArrayList<>();
    private final List<List<String>> retained = new ArrayList<>();
    private boolean deny;
    private RuntimeException startFailure;
    private io.tapstate.runtime.scheduler.RateSampler sampler;
    private boolean sampleMetrics;
    private Consumer<PipelineConvergencePass> onStart = this::advance;
    private boolean readyToComplete, releasedSnapshot, completionObserved, pendingCleanup, rejectCleanup;
    private final List<String> completionTrace = new ArrayList<>();
    private Runnable onFinalObservation = () -> {};
    private PipelineAlertFeed alertFeed = PipelineAlertFeed.none();
    private MetricsExport metricsExport = MetricsExport.none();
    private final java.util.concurrent.atomic.AtomicLong nanoTime = new java.util.concurrent.atomic.AtomicLong();
    private RebuildAdmission rebuildPolicy = RebuildAdmission.never();
    private Consumer<PipelineIntentStore.PendingStart> deferredStart;
    private PipelineConvergences pipelineConvergences;
    private PeriodicObservations periodicObservations;
    private NativeObservationSampling nativeSampling;
    private ProcessObservationWriter observationWrites;
    private StoppedTerminalRecheck stoppedRecheck;

    private ConvergenceDriver driver() {
        claims.acquire(KEY, OWNER, 7, Duration.ofMinutes(1));
        if (!realOwnership) when(ownership.permit(any(PipelineIntentScope.class))).thenAnswer(call -> deny
                ? PipelineActuationOwnership.Permit.denied()
                : new PipelineActuationOwnership.Permit(true, claims.read(KEY).orElseThrow().claim()));
        if (!realOwnership) when(ownership.permit(any(PipelineIntentScope.class), eq(true)))
                .thenAnswer(call -> ownership.permit(call.<PipelineIntentScope>getArgument(0)));
        if (!realOwnership) when(ownership.enterPass(any())).thenReturn(mock(PipelineActuationOwnership.Pass.class));
        var bindings = new ConvergenceDriver.ScopedPorts(intents, views, () -> {}, pass -> {
            passes.add(pass);
            return new LifecycleActuator() {
                @Override public void start(String id) { onStart.accept(pass); }
                @Override public void pause(String id) { carrying.set(false); }
                @Override public void resume(String id) { carrying.set(true); }
                @Override public void stop(String id, boolean purge) { carrying.set(false); }
                @Override public Optional<Throwable> failure(String id) { return Optional.empty(); }
                @Override public Optional<Throwable> lost(String id) { return Optional.empty(); }
                @Override public boolean isCarryingAJob(String id) { return carrying.get(); }
                @Override public Optional<Completion> completion(String id) {
                    return readyToComplete ? Optional.of(new Completion("original-job",pass.claim())) : Optional.empty();
                }
                @Override public void complete(String id,Completion completion) {
                    assertThat(completion.fence()).isEqualTo(pass.claim()); pendingCleanup=true; finishCleanup();
                }
                @Override public void retryCompletion(String id) { if(pendingCleanup)finishCleanup(); }
                private void finishCleanup() {
                    completionTrace.add("cleanup"); releasedSnapshot=true;
                    if(rejectCleanup)throw fenced();
                    pendingCleanup=false; carrying.set(false);
                }
            };
        }, pass -> rebuildPolicy, ids -> retained.add(List.copyOf(ids)), alertFeed, deferredStart, null, nativeSampling, observationWrites, stoppedRecheck);
        StateStore poisonState = mock(StateStore.class, call -> { throw new AssertionError("unscoped state accessed"); });
        ObservationStore poisonObservations = mock(ObservationStore.class, call -> { throw new AssertionError("unscoped observation accessed"); });
        var poisonDesired = mock(io.tapstate.spi.store.DesiredStore.class,
                call -> { throw new AssertionError("unscoped intent accessed"); });
        LifecycleActuator poisonActuator = mock(LifecycleActuator.class,
                call -> { throw new AssertionError("unscoped actuator accessed"); });
        RebuildAdmission poisonRebuilds = new RebuildAdmission() {
            @Override public boolean admits(String id) { throw new AssertionError("unscoped rebuild admission"); }
            @Override public void recordFailure(String id) { throw new AssertionError("unscoped failure recording"); }
            @Override public void retain(java.util.Collection<String> ids) { throw new AssertionError("unscoped rebuild retention"); }
        };
        var converger = new PipelineConverger(poisonDesired, poisonState, poisonActuator,
                Clock.fixed(T0, ZoneOffset.UTC), poisonRebuilds);
        return ConvergenceDriver.scoped(converger, new ObservationPublisher(poisonState, poisonObservations,
                id -> sampleMetrics ? java.util.OptionalLong.of(5) : java.util.OptionalLong.empty(), id -> Map.of(), id -> {
                    if(releasedSnapshot)throw new AssertionError("completed observations must not sample released capture");
                    return readyToComplete ? new io.tapstate.core.lifecycle.SnapshotReading(
                            Map.of("orders",new io.tapstate.core.lifecycle.TableSnapshot(8,8L,100,true)),T0)
                            : io.tapstate.core.lifecycle.SnapshotReading.NONE;
                }),
                sampler, metricsExport, eligible::get, ownership, bindings, periodicObservations, pipelineConvergences, nanoTime::get);
    }

    @org.junit.jupiter.api.RepeatedTest(2)
    void aNeverAdmittedFinalRetriesWithoutRepeatingAcknowledgedHistory() throws Exception {
        repairRejectedFinal("queue");
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(strings={"rules-changed","producer-changed","new-stop","history-invalid",
            "accepted-timer","source-missing","source-late","source-after-check","unknown-commit","jet-changed",
            "checkpoint-changed","new-run","purge","restart"})
    void unprovenFinalRecoveryKeepsTheCompleteConvergencePath(String mode) throws Exception {
        repairRejectedFinal(mode);
    }

    private void repairRejectedFinal(String mode) throws Exception {
        var f=this;var a=new PipelineAlertRuntimeTest();var catalog=io.tapstate.core.alerting.BuiltinAlarmTypes.catalog();
        var alarmScope=AlertEvaluationHostTest.definition("pipeline.failed").header().identity().scope();
        var rules=new ArrayList<io.tapstate.core.alerting.AlertConfiguration.Definition>();var providers=new java.util.HashSet<String>();
        for(var type:catalog.types()){
            var header=io.tapstate.core.alerting.AlertConfiguration.Header.create(io.tapstate.core.alerting.AlertConfiguration.Identity.create(io.tapstate.core.alerting.AlertConfiguration.Kind.RULE,alarmScope,"builtin-"+type.id()),io.tapstate.core.alerting.AlertChannelPolicy.Deployment.ON_PREM,java.time.Instant.now(),"operator");
            rules.add(new io.tapstate.core.alerting.AlertConfiguration.Rule(header,"Rule",type.id(),type.version(),new io.tapstate.core.alerting.AlertConfiguration.Resources(type.resourceKinds(),true,java.util.Set.of()),type.validatedParameters(Map.of()),null,true,null).validated(catalog).enrolled(null));providers.add(type.providerId());
        }
        when(a.fixture.store.evaluationRules(any(),org.mockito.ArgumentMatchers.anyInt())).thenReturn(new io.tapstate.spi.store.AlertStore.DefinitionPage(rules,null));
        var blocked=new java.util.concurrent.CountDownLatch(2);var release=new java.util.concurrent.CountDownLatch(1);var targetCommits=new java.util.concurrent.atomic.AtomicInteger();var unknown=new java.util.concurrent.atomic.AtomicBoolean(mode.equals("unknown-commit"));var rechecks=new java.util.concurrent.atomic.AtomicInteger();var lateEntered=new java.util.concurrent.CountDownLatch(1);var lateRelease=new java.util.concurrent.CountDownLatch(1);
        org.mockito.Mockito.doAnswer(call->{if(!mode.equals("unknown-commit")){blocked.countDown();assertThat(release.await(5,TimeUnit.SECONDS)).isTrue();}return Optional.ofNullable(a.rows.get(call.getArgument(0)));}).when(a.fixture.store).state(any(),any());
        org.mockito.Mockito.doAnswer(call->{io.tapstate.spi.store.AlertStore.Commit c=call.getArgument(0);var change=c.state();if(change.after().snapshot().rule().key().resource().kind().equals("pipeline"))targetCommits.incrementAndGet();a.rows.put(change.after().snapshot().rule().key(),new io.tapstate.spi.store.AlertStore.Stored<>(change.revision()+1,change.after()));if(change.after().snapshot().rule().key().resource().kind().equals("pipeline")&&unknown.getAndSet(false))throw new TapstateException(IoError.STORE_UNAVAILABLE,Map.of("detail","UnknownTransactionCommitResult"),null);return io.tapstate.spi.store.AlertStore.Outcome.STAGED;}).when(a.fixture.store).commit(any());
        var receipt=new java.util.concurrent.atomic.AtomicReference<TerminalPublication.Receipt>();var publication=new java.util.concurrent.atomic.AtomicReference<TerminalPublication>();
        var evaluator=new AlertEvaluationHost(a.fixture.store,catalog,io.tapstate.core.alerting.AlertChannelPolicy.Deployment.ON_PREM,java.time.Clock.systemUTC(),"queue-witness",()->{});
        try(var runtime=new PipelineAlertRuntime(()->new PipelineAlertRuntime.Services(a.fixture.store,evaluator,catalog,null,null,providers),f.intents,(scope,claim)->Optional.of(()->{}),()->{},java.time.Clock.systemUTC());
            var writer=new ProcessObservationWriter(mock(io.tapstate.adapters.mongostore.MongoCacheDomain.class),false);
            var stopWorkers=new PipelineConvergences(1,2)) {
            f.pipelineConvergences=stopWorkers;
            when(f.ownership.finishStoppedRelease("orders")).thenReturn(true);
            f.observationWrites=writer;f.nativeSampling=mock(NativeObservationSampling.class);f.sampleMetrics=true;
            f.sampler=mock(io.tapstate.runtime.scheduler.RateSampler.class);when(f.sampler.idle(any())).thenReturn(true);
            f.stoppedRecheck=(processed,checkpoint,original,tenure)->{rechecks.incrementAndGet();
                if(mode.equals("source-after-check")&&rechecks.get()==1){
                    var source=runtime.pipelineGuard(new ExecutionFence(FIRST,WorkloadClaimFence.from(f.claims.read(KEY).orElseThrow().claim())),()->{throw ThreeLayerStorePort.refused();});
                    runtime.resourceInput(new io.tapstate.core.alerting.AlarmKey.Resource("source","owned-after-check","one"),"source.connection-failed",1,"producer",source,()->{},new io.tapstate.core.alerting.AlertInput.Operational(java.time.Instant.now(),"a".repeat(64),io.tapstate.core.alerting.OperationalEvidence.Capability.INITIALIZE,false,"b".repeat(64),null));source.close();org.awaitility.Awaitility.await().until(()->runtime.health().queuedInputs()==0);
                }
                if(mode.equals("jet-changed"))return false;assertThat(processed).isEqualTo(f.intents.snapshot);f.views.check(processed.scope(),original,()->{});return checkpoint.equals(f.views.pipelineStates(FIRST,original,()->{}).read("orders").orElseThrow());};
            f.intents.snapshot=new PipelineIntentSnapshot(FIRST,4,new DesiredState("orders",PipelineState.STOPPED,"original"));
            f.alertFeed=new PipelineAlertFeed(){
                public void expecting(PipelineConvergencePass p){runtime.expecting(p);}
                public void published(PipelineConvergencePass p,Observation o,Throwable t){runtime.published(p,o,t);}
                public void unavailable(PipelineConvergencePass p){runtime.unavailable(p);}
                public void retain(java.util.Collection<String> ids){runtime.retain(ids);}
                public boolean needsFrequentTerminalSamples(PipelineIntentScope scope){return runtime.needsFrequentTerminalSamples(scope);}
                public boolean needsFrequentTerminalSamples(PipelineIntentScope scope,PipelineState state){return runtime.needsFrequentTerminalSamples(scope,state);}
                public boolean needsFreshTerminalMetrics(PipelineIntentScope scope){return runtime.needsFreshTerminalMetrics(scope);}
                public TerminalPublication.Receipt terminalPublished(PipelineConvergencePass p,TerminalPublication t,Observation o,Throwable x,boolean measured){publication.set(t);var r=runtime.terminalPublished(p,t,o,x,measured);receipt.set(r);return r;}
            };
            runtime.tick();org.awaitility.Awaitility.await().until(()->runtime.health().queuedInputs()==0);
            try(var driver=f.driver()) {
                var local=mock(PipelineActuationOwnership.Pass.class);when(f.ownership.enterPass(FIRST)).thenReturn(local);
                var handed=mock(PipelineActuationOwnership.StopRelease.class);when(handed.status()).thenReturn(PipelineActuationOwnership.StopReleaseStatus.CONFIRMED);
                when(local.relinquishStopped(eq(FIRST),any())).thenReturn(handed);
                var originalClaim=f.claims.read(KEY).orElseThrow().claim();f.claims.advanceExecution(originalClaim,7,java.util.Set.of("node")).orElseThrow();
                if(!mode.equals("unknown-commit"))for(int i=0;i<64;i++)runtime.resourceInput(new io.tapstate.core.alerting.AlarmKey.Resource("source","independent-"+i,"one"),"source.connection-failed",1,"producer",()->{},new io.tapstate.core.alerting.AlertInput.Operational(java.time.Instant.now(),"a".repeat(64),io.tapstate.core.alerting.OperationalEvidence.Capability.INITIALIZE,false,"b".repeat(64),null));
                if(!mode.equals("unknown-commit")){assertThat(blocked.await(2,TimeUnit.SECONDS)).isTrue();assertThat(runtime.health().queuedInputs()).isEqualTo(64);}
                if(mode.equals("source-missing")){
                    var fence=WorkloadClaimFence.from(f.claims.read(KEY).orElseThrow().claim());
                    var source=runtime.pipelineGuard(new ExecutionFence(FIRST,fence),()->{});
                    runtime.resourceInput(new io.tapstate.core.alerting.AlarmKey.Resource("source","owned-source","one"),"source.connection-failed",1,"producer",source,()->{},new io.tapstate.core.alerting.AlertInput.Operational(java.time.Instant.now(),"a".repeat(64),io.tapstate.core.alerting.OperationalEvidence.Capability.INITIALIZE,false,"b".repeat(64),null));source.close();
                }
                driver.reconcile();org.awaitility.Awaitility.await().until(()->receipt.get()!=null&&!stopWorkers.stopping("orders"));writer.flush();if(mode.equals("unknown-commit"))org.awaitility.Awaitility.await().until(()->runtime.health().queuedInputs()==0);var history=writer.terminalHistory(publication.get(),null);
                int measured=(int)f.views.writes.stream().filter(w->w.kind().equals("observation")).count();int historyCount=(int)f.views.writes.stream().filter(w->w.kind().equals("rate")).count();
                assertThat(history.status()).isEqualTo(TerminalPublication.Status.COMPLETE);assertThat(receipt.get().status()).isEqualTo(TerminalPublication.Status.FAILED);assertThat(targetCommits).hasValue(mode.equals("unknown-commit")?1:0);verify(local,never()).relinquishStopped(any(),any());
                release.countDown();org.awaitility.Awaitility.await().until(()->runtime.health().queuedInputs()==0);
                if(mode.equals("source-late")){
                    var source=runtime.pipelineGuard(new ExecutionFence(FIRST,WorkloadClaimFence.from(f.claims.read(KEY).orElseThrow().claim())),()->{lateEntered.countDown();try{assertThat(lateRelease.await(5,TimeUnit.SECONDS)).isTrue();}catch(InterruptedException interrupted){throw new AssertionError(interrupted);}});
                    runtime.resourceInput(new io.tapstate.core.alerting.AlarmKey.Resource("source","late-owned-source","one"),"source.connection-failed",1,"producer",source,()->{},new io.tapstate.core.alerting.AlertInput.Operational(java.time.Instant.now(),"a".repeat(64),io.tapstate.core.alerting.OperationalEvidence.Capability.INITIALIZE,false,"b".repeat(64),null));source.close();assertThat(lateEntered.await(2,TimeUnit.SECONDS)).isTrue();
                }
                if(mode.equals("rules-changed")){
                    var r=(io.tapstate.core.alerting.AlertConfiguration.Rule)rules.getFirst();var edited=new io.tapstate.core.alerting.AlertConfiguration.Rule(r.header().next(java.time.Instant.now(),"operator"),"Edited",r.typeId(),r.typeVersion(),r.resources(),r.parameters(),r.severityOverride(),r.enabled(),r.metric()).validated(catalog).enrolled(null);var changed=new ArrayList<>(rules);changed.set(0,edited);when(a.fixture.store.evaluationRules(any(),org.mockito.ArgumentMatchers.anyInt())).thenReturn(new io.tapstate.spi.store.AlertStore.DefinitionPage(changed,null));runtime.tick();org.awaitility.Awaitility.await().until(()->runtime.health().queuedInputs()==0);
                }
                if(mode.equals("producer-changed")){var current=f.claims.read(KEY).orElseThrow().claim();f.claims.advanceExecution(current,7,java.util.Set.of("node")).orElseThrow();}
                if(mode.equals("new-run"))f.intents.snapshot=new PipelineIntentSnapshot(FIRST,5,new DesiredState("orders",PipelineState.RUNNING,"original"));
                if(mode.equals("purge"))f.intents.snapshot=new PipelineIntentSnapshot(FIRST,5,new DesiredState("orders",PipelineState.STOPPED,"original",true));
                if(mode.equals("checkpoint-changed")){var cp=f.views.state(FIRST).read("orders").orElseThrow();f.views.state(FIRST).compareAndSwap("orders",cp.epoch(),StateJson.of(PipelineState.STOPPED),T0.plusSeconds(1));}
                if(mode.equals("new-stop"))f.intents.snapshot=new PipelineIntentSnapshot(FIRST,5,new DesiredState("orders",PipelineState.STOPPED,"original"));
                if(mode.equals("history-invalid"))writer.expecting(f.passes.getFirst());
                if(mode.equals("accepted-timer")){for(int turn=0;turn<4&&targetCommits.get()==0;turn++){runtime.tick();org.awaitility.Awaitility.await().until(()->runtime.health().queuedInputs()==0);}assertThat(targetCommits.get()).isPositive();}
                f.nanoTime.set(Duration.ofSeconds(31).toNanos());var rescanning=mode.equals("restart")?f.driver():driver;int beforeLoads=f.intents.loads;rescanning.schedule();org.awaitility.Awaitility.await().until(()->!stopWorkers.stopping("orders")&&(!mode.equals("new-run")||f.carrying.get()));writer.flush();if(mode.equals("new-run"))awaitPasses(rescanning);
                if(mode.equals("source-late")){verify(local,never()).relinquishStopped(any(),any());lateRelease.countDown();}
                org.awaitility.Awaitility.await().until(()->runtime.health().queuedInputs()==0);
                int measuredAfter=(int)f.views.writes.stream().filter(w->w.kind().equals("observation")).count();int historiesAfter=(int)f.views.writes.stream().filter(w->w.kind().equals("rate")).count();
                if(mode.equals("queue")) {
                    assertThat(measuredAfter).as("only the unadmitted final alert needs retry").isEqualTo(measured);
                    assertThat(historiesAfter).isEqualTo(historyCount);assertThat(history.status()).isEqualTo(TerminalPublication.Status.COMPLETE);
                    verify(local,never()).relinquishStopped(any(),any());
                    driver.schedule();org.awaitility.Awaitility.await().untilAsserted(()->verify(local).relinquishStopped(eq(FIRST),any()));
                    assertThat(rechecks.get()).isGreaterThanOrEqualTo(2);
                } else if(mode.equals("new-run")){
                    assertThat(f.carrying).isTrue();assertThat(f.intents.loads).isGreaterThan(beforeLoads);assertThat(history.status()).isEqualTo(TerminalPublication.Status.FAILED);verify(local,never()).relinquishStopped(any(),any());
                } else if(mode.equals("purge")){
                    assertThat(f.intents.loads).isGreaterThan(beforeLoads);verify(local,never()).relinquishStopped(any(),any());
                } else {
                    assertThat(measuredAfter).as(mode+" retains the complete old path").isEqualTo(measured+1);
                    assertThat(historiesAfter).isEqualTo(historyCount+1);verify(local,never()).relinquishStopped(any(),any());
                }
                if(rescanning!=driver)rescanning.close();
            }finally{release.countDown();lateRelease.countDown();}
        }
    }

    @Test void reusedTerminalProjectionFeedsLifecycleAlertsButDoesNotInventMetricHistory() {
        intents.snapshot = new PipelineIntentSnapshot(FIRST, 4,
                new DesiredState("orders", PipelineState.STOPPED, "original"));
        var received = new ArrayList<Observation>();
        var exported = new java.util.concurrent.atomic.AtomicInteger();
        sampler = mock(io.tapstate.runtime.scheduler.RateSampler.class);
        alertFeed = new PipelineAlertFeed() {
            public boolean needsFreshTerminalMetrics(PipelineIntentScope scope) { return false; }
            public void expecting(PipelineConvergencePass pass) { }
            public void published(PipelineConvergencePass pass, Observation value, Throwable failure) { received.add(value); }
            public void unavailable(PipelineConvergencePass pass) { throw new AssertionError("publication failed"); }
            public void retain(java.util.Collection<String> ids) { }
        };
        metricsExport = new MetricsExport() {
            public void offer(String id, PipelineState state, Instant at, List<io.tapstate.core.lifecycle.MetricFact> facts) {
                exported.incrementAndGet();
            }
            public void forgetPipelinesOutside(java.util.Collection<String> ids) { }
        };
        try (var driver = driver()) {
            driver.reconcile();
            driver.reconcile();
            assertThat(received).hasSize(2);
            assertThat(received).allMatch(value -> value.state() == PipelineState.STOPPED);
            assertThat(exported).hasValue(1);
            verify(sampler, times(1)).offer(any(Observation.class), eq(FIRST), any());
        }
    }

    @Test void unchangedStoppedPipelineDoesNotContinuouslyReadAndPublish() throws Exception {
        intents.snapshot = new PipelineIntentSnapshot(FIRST, 4,
                new DesiredState("orders", PipelineState.STOPPED, "original"));
        try (var driver = driver()) {
            driver.reconcile();
            assertThat(views.observations(FIRST).read("orders")).get()
                    .extracting(Observation::state).isEqualTo(PipelineState.STOPPED);
            int completedLoads = intents.loads;
            int completedWrites = views.writes.size();
            for (int i = 0; i < 5; i++) scheduledPass(driver);
            assertThat(intents.loads).as("unchanged terminal intent must not repeat full storage admission")
                    .isEqualTo(completedLoads);
            assertThat(views.writes).hasSize(completedWrites);
            intents.snapshot = new PipelineIntentSnapshot(FIRST, 5,
                    new DesiredState("orders", PipelineState.RUNNING, "original"));
            scheduledPass(driver);
            assertThat(carrying).isTrue();
            assertThat(views.observations(FIRST).read("orders")).get()
                    .extracting(Observation::state).isEqualTo(PipelineState.RUNNING);
        }
    }

    private static TerminalPublication.Receipt receipt(java.util.concurrent.atomic.AtomicReference<TerminalPublication.Status> state) {
        return new TerminalPublication.Receipt() {
            public TerminalPublication.Status status() { return state.get(); }
            public boolean seal() { return state.get() == TerminalPublication.Status.COMPLETE; }
        };
    }

    @Test void stoppedTenureWaitsForHistoryAndAlertsAndDoesNotReacquireForAnUnchangedIntent() throws Exception {
        var history = new java.util.concurrent.atomic.AtomicReference<>(TerminalPublication.Status.PENDING);
        var alerts = new java.util.concurrent.atomic.AtomicReference<>(TerminalPublication.Status.PENDING);
        var rechecks = new java.util.concurrent.CopyOnWriteArrayList<StoppedTerminalRecheck.Tenure>();
        stoppedRecheck = (processed, checkpoint, original, tenure) -> {
            assertThat(processed).isEqualTo(intents.snapshot);
            assertThat(checkpoint.pipelineId()).isEqualTo(FIRST.pipelineId());
            rechecks.add(tenure);
            return true;
        };
        observationWrites = mock(ProcessObservationWriter.class);
        org.mockito.Mockito.doAnswer(call -> { call.<Runnable>getArgument(1).run(); return null; })
                .when(observationWrites).now(any(), any());
        when(observationWrites.terminalHistory(any(), org.mockito.ArgumentMatchers.nullable(ProcessObservationWriter.History.class)))
                .thenReturn(receipt(history));
        alertFeed = mock(PipelineAlertFeed.class);
        when(alertFeed.terminalPublished(any(), any(), any(), org.mockito.ArgumentMatchers.isNull(), org.mockito.ArgumentMatchers.anyBoolean()))
                .thenReturn(receipt(alerts));
        nativeSampling = mock(NativeObservationSampling.class);
        when(ownership.finishStoppedRelease("orders")).thenReturn(true);
        intents.snapshot = new PipelineIntentSnapshot(FIRST, 4, new DesiredState("orders", PipelineState.STOPPED, "original"));
        try (var workers = new PipelineConvergences(1, 2)) {
            pipelineConvergences = workers;
            try (var driver = driver()) {
                var local = mock(PipelineActuationOwnership.Pass.class);
                when(ownership.enterPass(FIRST)).thenReturn(local);
                var release = mock(PipelineActuationOwnership.StopRelease.class);
                when(release.status()).thenReturn(PipelineActuationOwnership.StopReleaseStatus.CONFIRMED);
                when(local.relinquishStopped(eq(FIRST), any())).thenReturn(release);
                driver.reconcile();
                org.awaitility.Awaitility.await().untilAsserted(() -> verify(observationWrites).terminalHistory(
                        any(), org.mockito.ArgumentMatchers.nullable(ProcessObservationWriter.History.class)));
                org.awaitility.Awaitility.await().until(() -> !workers.stopping("orders"));
                int fullReads = intents.loads;
                history.set(TerminalPublication.Status.COMPLETE);
                nanoTime.set(Duration.ofSeconds(31).toNanos());
                driver.schedule();
                org.awaitility.Awaitility.await().until(() -> !rechecks.isEmpty() && !workers.stopping("orders"));
                verify(local, never()).relinquishStopped(any(), any());
                alerts.set(TerminalPublication.Status.COMPLETE);
                driver.schedule();
                org.awaitility.Awaitility.await().untilAsserted(() -> verify(local).relinquishStopped(eq(FIRST), any()));
                org.awaitility.Awaitility.await().until(() -> !workers.stopping("orders"));
                nanoTime.addAndGet(Duration.ofSeconds(31).toNanos());
                driver.schedule();
                org.awaitility.Awaitility.await().until(() -> rechecks.contains(StoppedTerminalRecheck.Tenure.ENDED) && !workers.stopping("orders"));
                assertThat(intents.loads).isEqualTo(fullReads);
                verify(observationWrites, times(1)).terminalHistory(any(), org.mockito.ArgumentMatchers.nullable(ProcessObservationWriter.History.class));
                // A new explicit Stop is a new intent even when its desired state stays STOPPED.
                intents.snapshot = new PipelineIntentSnapshot(FIRST, 5, new DesiredState("orders", PipelineState.STOPPED, "original"));
                driver.schedule();
                org.awaitility.Awaitility.await().until(() -> intents.loads > fullReads && !workers.stopping("orders"));
                intents.snapshot = new PipelineIntentSnapshot(FIRST, 6, new DesiredState("orders", PipelineState.RUNNING, "original"));
                scheduledPass(driver);
                assertThat(carrying).isTrue();
            }
        }
    }

    @Test void terminalRecheckExpiresButSynchronousPassesAlwaysExecute() throws Exception {
        intents.snapshot = new PipelineIntentSnapshot(FIRST, 4,
                new DesiredState("orders", PipelineState.STOPPED, "original"));
        try (var driver = driver()) {
            driver.reconcile();
            int reads = intents.loads;
            nanoTime.set(Duration.ofSeconds(29).toNanos()); scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads);
            nanoTime.set(Duration.ofSeconds(30).toNanos()); scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads + 1);
            driver.reconcile();
            assertThat(intents.loads).isEqualTo(reads + 2);
            verify(ownership, org.mockito.Mockito.atLeastOnce()).retain(org.mockito.ArgumentMatchers.argThat(ids -> ids.size() == 1 && ids.contains("orders")));
        }
    }

    @Test void aChangedPhysicalScopeCannotReuseTerminalCadence() throws Exception {
        intents.snapshot = new PipelineIntentSnapshot(FIRST, 4,
                new DesiredState("orders", PipelineState.STOPPED, "original"));
        try (var driver = driver()) {
            driver.reconcile(); int reads = intents.loads;
            views.current = scope(2);
            intents.snapshot = new PipelineIntentSnapshot(scope(2), 4, intents.snapshot.desired());
            scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads + 1);
            assertThat(views.observations(scope(2)).read("orders")).get()
                    .extracting(Observation::state).isEqualTo(PipelineState.STOPPED);
        }
    }

    @Test void absentSchedulingHintsNeverSuppressActuationReads() throws Exception {
        intents.snapshot = new PipelineIntentSnapshot(FIRST, 4,
                new DesiredState("orders", PipelineState.STOPPED, "original"));
        try (var driver = driver()) {
            driver.reconcile(); int reads = intents.loads;
            intents.hints = false; scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads + 1);
        }
    }

    @Test void failedPublicationCannotDeferTheNextPass() throws Exception {
        intents.snapshot = new PipelineIntentSnapshot(FIRST, 4,
                new DesiredState("orders", PipelineState.STOPPED, "original"));
        try (var driver = driver()) {
            views.rejectPublication = true; driver.reconcile(); int reads = intents.loads;
            views.rejectPublication = false; scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads + 1);
            assertThat(views.observations(FIRST).read("orders")).isPresent();
        }
    }

    @Test void aStoppedCheckpointWithALocalJobCannotBeDeferred() throws Exception {
        intents.snapshot = new PipelineIntentSnapshot(FIRST, 4,
                new DesiredState("orders", PipelineState.STOPPED, "original"));
        views.state(FIRST).create("orders", StateJson.of(PipelineState.STOPPED), T0);
        carrying.set(true);
        try (var driver = driver()) {
            driver.reconcile(); int reads = intents.loads; scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads + 1);
        }
    }

    @Test void pendingPurgeAndReassemblyRemainOnEveryTick() throws Exception {
        intents.snapshot = new PipelineIntentSnapshot(FIRST, 4,
                new DesiredState("orders", PipelineState.STOPPED, "original", true));
        try (var driver = driver()) {
            driver.reconcile(); int reads = intents.loads; scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads + 1);
            intents.snapshot = new PipelineIntentSnapshot(FIRST, 5,
                    new DesiredState("orders", PipelineState.STOPPED, "original", false, "original", true));
            driver.reconcile(); reads = intents.loads; scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads + 1);
        }
    }

    @Test void failureThatCannotAutomaticallyRetryIsDeferred() throws Exception {
        views.state(FIRST).create("orders", StateJson.of(PipelineState.FAILED), T0);
        try (var driver = driver()) {
            scheduledPass(driver); int reads = intents.loads; scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads);
        }
    }

    @Test void dynamicRecoveryIsNotDeferredAndCanBecomeAdmissibleWithoutANewIntent() throws Exception {
        var admitted = new AtomicBoolean(); rebuildPolicy = id -> admitted.get();
        views.state(FIRST).create("orders", StateJson.of(PipelineState.FAILED), T0);
        try (var driver = driver()) {
            scheduledPass(driver); int reads = intents.loads;
            admitted.set(true); scheduledPass(driver);
            assertThat(intents.loads).isGreaterThan(reads);
            assertThat(carrying).isTrue();
        }
    }

    @Test void exhaustedRecoveryIsDeferredOnlyForTheMatchingIntent() throws Exception {
        rebuildPolicy = id -> false;
        views.state(FIRST).create("orders", StateJson.of(PipelineState.FAILED), T0);
        views.recovery = new CheckpointDoc.LeaseRecovery(4, 3, 3, T0);
        try (var driver = driver()) {
            scheduledPass(driver); int reads = intents.loads; scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads);
            intents.snapshot = new PipelineIntentSnapshot(FIRST, 5, intents.snapshot.desired());
            scheduledPass(driver); reads = intents.loads; scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads + 1);
        }
    }

    @Test void lifecycleOnlyAlertsDoNotWakeSettledStoppedPipelines() throws Exception {
        var metric = new AtomicBoolean();
        alertFeed = new PipelineAlertFeed() {
            public void expecting(PipelineConvergencePass p) { }
            public void published(PipelineConvergencePass p, Observation o, Throwable t) { }
            public void unavailable(PipelineConvergencePass p) { }
            public void retain(java.util.Collection<String> ids) { }
            public boolean needsFrequentTerminalSamples(PipelineIntentScope scope) { return true; }
            public boolean needsFrequentTerminalSamples(PipelineIntentScope scope, PipelineState actual) {
                return metric.get() || actual != PipelineState.STOPPED;
            }
        };
        intents.snapshot = new PipelineIntentSnapshot(FIRST, 4,
                new DesiredState("orders", PipelineState.STOPPED, "original"));
        try (var driver = driver()) {
            scheduledPass(driver); int reads = intents.loads; scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads);
            metric.set(true); scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads + 1);
        }
    }

    @Test void activatingAPublicationBasedAlertWakesATerminalPipeline() throws Exception {
        var frequent = new AtomicBoolean();
        var delegate = PipelineAlertFeed.none();
        alertFeed = new PipelineAlertFeed() {
            public void expecting(PipelineConvergencePass p) { delegate.expecting(p); }
            public void published(PipelineConvergencePass p, Observation o, Throwable t) { delegate.published(p, o, t); }
            public void unavailable(PipelineConvergencePass p) { delegate.unavailable(p); }
            public void retain(java.util.Collection<String> ids) { delegate.retain(ids); }
            public boolean needsFrequentTerminalSamples(PipelineIntentScope scope) { return frequent.get(); }
        };
        views.state(FIRST).create("orders", StateJson.of(PipelineState.FAILED), T0);
        try (var driver = driver()) {
            scheduledPass(driver); int reads = intents.loads; scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads);
            frequent.set(true); scheduledPass(driver); scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads + 2);
        }
    }

    @Test void optionalTerminalProofFailureDoesNotOverwriteSuccessfulPublication() throws Exception {
        alertFeed = mock(PipelineAlertFeed.class);
        intents.snapshot = new PipelineIntentSnapshot(FIRST, 4,
                new DesiredState("orders", PipelineState.STOPPED, "original"));
        try (var driver = driver()) {
            views.rejectPostPublicationRead = true; driver.reconcile(); int reads = intents.loads;
            var published = views.observations(FIRST).read("orders").orElseThrow();
            assertThat(published.state()).isEqualTo(PipelineState.STOPPED);
            assertThat(published.metrics()).doesNotContainKey("reconcileFailuresInARow");
            verify(alertFeed, never()).unavailable(any());
            views.rejectPostPublicationRead = false; scheduledPass(driver);
            assertThat(intents.loads).isEqualTo(reads + 1);
        }
    }

    private static void scheduledPass(ConvergenceDriver driver) throws Exception {
        driver.schedule();
        awaitPasses(driver);
    }

    private static void awaitPasses(ConvergenceDriver driver) throws Exception {
        var field = ConvergenceDriver.class.getDeclaredField("passes");
        field.setAccessible(true);
        var executor = (PipelinePassExecutor) field.get(driver);
        org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3))
                .until(() -> executor.activePipelines().isEmpty());
    }

    @Test void aNewStartPassRunsWhileThreePeriodicPassesAreBlocked() throws Exception {
        assertStartAdmitted(false);
    }

    @Test void aRefusedStartPermitDoesNotGiveAwayTheReservedSlotOnRetry() throws Exception {
        assertStartAdmitted(true);
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void aPreparedStartContinuesInTheSamePassOnlyForItsOriginalIntent(boolean replaced) {
        var original = snapshot(FIRST);
        intents.snapshot = null;
        intents.pending = new PipelineIntentStore.PendingStart(original,
                mock(io.tapstate.spi.store.PipelinePreparationInputs.class));
        deferredStart = pending -> {
            assertThat(pending.intent()).isEqualTo(original);
            intents.snapshot = replaced ? new PipelineIntentSnapshot(FIRST, original.intentRevision() + 1,
                    new DesiredState("orders", PipelineState.STOPPED, "original")) : original;
        };
        try (var driver = driver()) {
            driver.reconcile();
            assertThat(carrying.get()).isEqualTo(!replaced);
            assertThat(passes).hasSize(replaced ? 0 : 1);
        }
    }

    @Test void pendingStartsCannotTakeEverySlotFromPeriodicWork() throws Exception {
        var outcomes = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var store = mock(PipelineIntentStore.class);
        when(store.scanCandidates()).thenReturn(java.util.stream.Stream.of("a", "b", "c", "d", "periodic")
                .map(id -> new PipelineIntentStore.ScanCandidate(id, id.equals("periodic") ? 0 : 1)).toList());
        when(store.loadForActuation(any())).thenAnswer(call -> {
            if (call.<String>getArgument(0).equals("periodic")) outcomes.countDown();
            else {
                try {
                    if (!release.await(8, TimeUnit.SECONDS)) throw new AssertionError("start barrier timed out");
                } catch (InterruptedException stopping) {
                    Thread.currentThread().interrupt();
                }
            }
            return Optional.empty();
        });
        var port = new ConvergenceDriver.ScopedPorts(store, views, () -> {},
                pass -> { throw new AssertionError("empty intent cannot actuate"); },
                pass -> RebuildAdmission.never(), ids -> {});
        try (var driver = ConvergenceDriver.scoped(null, mock(ObservationPublisher.class), null,
                MetricsExport.none(), () -> true, ownership, port)) {
            try {
                driver.schedule();
                assertThat(outcomes.await(2, TimeUnit.SECONDS))
                        .as("sustained start probes do not take the last periodic slot").isTrue();
            } finally { release.countDown(); }
        }
    }

    @Test void aSaturatedPeriodicScanPreservesTheNextPipelineTurn() throws Exception {
        var occupied = new java.util.concurrent.CountDownLatch(3);
        var release = new java.util.concurrent.CountDownLatch(1);
        var next = new java.util.concurrent.CountDownLatch(1);
        var store = mock(PipelineIntentStore.class);
        when(store.scanCandidates()).thenReturn(List.of("old-a", "old-b", "old-c", "pending-stop").stream()
                .map(id -> new PipelineIntentStore.ScanCandidate(id, 0)).toList());
        when(store.loadForActuation(any())).thenAnswer(call -> {
            if (call.<String>getArgument(0).equals("pending-stop")) {
                next.countDown();
            } else {
                occupied.countDown();
                if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("periodic barrier timed out");
            }
            return Optional.empty();
        });
        var port = new ConvergenceDriver.ScopedPorts(store, views, () -> {},
                pass -> { throw new AssertionError("empty intent cannot actuate"); },
                pass -> RebuildAdmission.never(), ids -> {});
        try (var driver = ConvergenceDriver.scoped(null, mock(ObservationPublisher.class), null,
                MetricsExport.none(), () -> true, ownership, port)) {
            try {
                driver.schedule();
                assertThat(occupied.await(2, TimeUnit.SECONDS)).isTrue();
                for (int i = 0; i < 3; i++) driver.schedule();
                assertThat(next.getCount()).isEqualTo(1);
            } finally { release.countDown(); }
            awaitPasses(driver);
            scheduledPass(driver);
            assertThat(next.getCount()).as("the first free scan must admit the deferred Stop").isZero();
        }
    }

    @Test void aFailedPreActuationProofKeepsTheOriginalStartPriority() throws Exception {
        assertStartAdmitted(StartProbe.FAILED_PROOF);
    }

    private enum StartProbe { LOAD, DENIED_PERMIT, FAILED_PROOF }

    private void assertStartAdmitted(boolean retryPermit) throws Exception {
        assertStartAdmitted(retryPermit ? StartProbe.DENIED_PERMIT : StartProbe.LOAD);
    }

    private void assertStartAdmitted(StartProbe probe) throws Exception {
        boolean retryPermit = probe != StartProbe.LOAD;
        var old = List.of("old-a", "old-b", "old-c", "old-d");
        var current = new java.util.concurrent.atomic.AtomicReference<>(old);
        var periodic = new java.util.concurrent.CountDownLatch(3);
        var fourth = new java.util.concurrent.atomic.AtomicBoolean();
        var started = new java.util.concurrent.CountDownLatch(1);
        var denied = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var fresh = new PipelineIntentScope("cluster", 1, "cache", "fresh", 1);
        var tries = new java.util.concurrent.atomic.AtomicInteger();
        when(ownership.permit(fresh)).thenAnswer(call -> {
            if (tries.incrementAndGet() == 1) {
                if (probe == StartProbe.FAILED_PROOF) {
                    var key = new WorkloadClaimKey(fresh.clusterId(), WorkloadClaimType.PIPELINE_ACTUATION, fresh.pipelineId());
                    return new PipelineActuationOwnership.Permit(true, claims.acquire(key, OWNER, 1, Duration.ofSeconds(30)).claim());
                }
                denied.countDown();
            } else started.countDown();
            return PipelineActuationOwnership.Permit.denied();
        });
        var store = mock(PipelineIntentStore.class);
        when(store.pipelineIds()).thenAnswer(call -> current.get());
        when(store.scanCandidates()).thenAnswer(call -> current.get().stream()
                .map(id -> new PipelineIntentStore.ScanCandidate(id, id.equals("fresh") ? 1L : 0L)).toList());
        when(store.loadForActuation(any())).thenAnswer(call -> {
            String id = call.getArgument(0);
            if (id.equals("fresh")) {
                if (retryPermit) return Optional.of(new PipelineIntentSnapshot(fresh, 1,
                        new DesiredState("fresh", PipelineState.RUNNING, "original", false)));
                started.countDown();
            } else {
                if (id.equals("old-d")) fourth.set(true);
                periodic.countDown();
                try {
                    if (!release.await(8, TimeUnit.SECONDS)) throw new AssertionError("periodic barrier timed out");
                } catch (InterruptedException stopping) {
                    Thread.currentThread().interrupt();
                }
            }
            return Optional.empty();
        });
        when(ownership.enterPass(fresh)).thenReturn(mock(PipelineActuationOwnership.Pass.class));
        var port = new ConvergenceDriver.ScopedPorts(store, views, () -> {},
                pass -> {
                    if (probe == StartProbe.FAILED_PROOF) {
                        denied.countDown();
                        throw new io.tapstate.core.common.TapstateException(IoError.STORE_UNAVAILABLE,
                                Map.of("detail", "initial claim proof unavailable"), null);
                    }
                    throw new AssertionError("empty intent cannot actuate");
                },
                pass -> RebuildAdmission.never(), ids -> {});
        try (var driver = ConvergenceDriver.scoped(null, mock(ObservationPublisher.class), null,
                MetricsExport.none(), () -> true, ownership, port)) {
            try {
                driver.schedule();
                assertThat(periodic.await(3, TimeUnit.SECONDS)).isTrue();
                current.set(List.of("old-a", "old-b", "old-c", "old-d", "fresh"));
                driver.schedule();
                if (retryPermit) {
                    assertThat(denied.await(2, TimeUnit.SECONDS)).isTrue();
                    org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(2)).until(() -> {
                        driver.schedule();
                        return started.getCount() == 0;
                    });
                }
                assertThat(started.await(2, TimeUnit.SECONDS))
                        .as("an unchanged periodic pass must not occupy the reserved start slot").isTrue();
                assertThat(fourth).isFalse();
            } finally { release.countDown(); }
        }
    }

    @Test void originalScopedPublicationReachesAlertsBeforeARefusingExporter() {
        sampleMetrics = true;
        var sequence = new ArrayList<String>();
        var received = new ArrayList<Observation>();
        alertFeed = new PipelineAlertFeed() {
            public void expecting(PipelineConvergencePass pass) { sequence.add("expecting"); }
            public void published(PipelineConvergencePass pass, Observation value, Throwable cause) {
                sequence.add("alert"); received.add(value);
                assertThat(pass.scope()).isEqualTo(FIRST);
                assertThat(pass.claim()).isEqualTo(WorkloadClaimFence.from(claims.read(KEY).orElseThrow().claim()));
            }
            public void unavailable(PipelineConvergencePass pass) { throw new AssertionError("export failure is not publication failure"); }
            public void retain(java.util.Collection<String> ids) { }
        };
        metricsExport = new MetricsExport() {
            public void offer(String id, PipelineState state, Instant at, List<io.tapstate.core.lifecycle.MetricFact> facts) {
                sequence.add("export"); assertThat(facts).isSameAs(received.getFirst().facts());
                throw new IllegalStateException("export backend unavailable");
            }
            public void forgetPipelinesOutside(java.util.Collection<String> ids) { }
        };
        driver().reconcile();
        assertThat(sequence).containsExactly("expecting", "expecting", "alert", "export");
        assertThat(received).hasSize(1);
        assertThat(received.getFirst()).isSameAs(views.observations(FIRST).read("orders").orElseThrow());
        assertThat(received.getFirst().facts()).isNotEmpty();
        assertThat(received.getFirst().metrics()).doesNotContainKey("reconcileFailuresInARow");
    }

    @Test void anAlertInputRefusalCannotBreakTheOriginalObservationOrExport() {
        var exported = new ArrayList<Observation>();
        alertFeed = new PipelineAlertFeed() {
            public void expecting(PipelineConvergencePass pass) { }
            public void published(PipelineConvergencePass pass, Observation value, Throwable failure) { throw fenced(); }
            public void unavailable(PipelineConvergencePass pass) { }
            public void retain(java.util.Collection<String> ids) { }
        };
        metricsExport = new MetricsExport() {
            public void offer(String id, PipelineState state, Instant at, List<io.tapstate.core.lifecycle.MetricFact> facts) {
                exported.add(new Observation(id,state,Map.of(),Map.of(),Map.of(),null,at,facts));
            }
            public void forgetPipelinesOutside(java.util.Collection<String> ids) { }
        };
        driver().reconcile();
        assertThat(exported).hasSize(1);
        assertThat(views.observations(FIRST).read("orders").orElseThrow().state()).isEqualTo(PipelineState.RUNNING);
    }
    @Test void theActualScopedDriverFeedsTheEvaluationWorkersWithNoExporterConfigured() {
        var harness=new PipelineAlertRuntimeTest();
        harness.fixture.now.set(Instant.now().plusSeconds(10));
        harness.intents.put("orders",intents.snapshot);
        try(var runtime=harness.runtime()){
            runtime.tick();org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->!runtime.health().degraded());
            alertFeed=runtime;driver().reconcile();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->!harness.rows.isEmpty());
            var head=harness.rows.values().iterator().next().value();
            assertThat(head.snapshot().status()).isEqualTo(io.tapstate.core.alerting.AlertSnapshot.Status.CLEAR);
            assertThat(head.snapshot().quality()).isEqualTo(io.tapstate.core.alerting.AlertSnapshot.Quality.VALID);
            assertThat(head.snapshot().lastObservedAt()).isEqualTo(views.observations(FIRST).read("orders").orElseThrow().observedAt());
            assertThat(head.snapshot().rule().key().resource().incarnation()).isEqualTo(Long.toString(FIRST.stateGeneration()));
        }
    }
    @Test void aRealUnreachableOtlpBackendCannotBlockOrRefreshTheAlertInput() throws Exception {
        int unusedPort;
        try(var socket=new java.net.ServerSocket(0,1,java.net.InetAddress.getLoopbackAddress())){unusedPort=socket.getLocalPort();}
        var settings=io.tapstate.adapters.otel.ExportSettings.otlpTo("http://127.0.0.1:"+unusedPort+"/v1/metrics",
                io.tapstate.adapters.otel.ExportSettings.Protocol.HTTP_PROTOBUF,Duration.ofHours(1));
        var captured=new ArrayList<Observation>();
        alertFeed=new PipelineAlertFeed(){
            public void expecting(PipelineConvergencePass pass){}
            public void published(PipelineConvergencePass pass,Observation value,Throwable failure){captured.add(value);}
            public void unavailable(PipelineConvergencePass pass){throw new AssertionError("OTLP must remain independent");}
            public void retain(java.util.Collection<String> ids){}
        };
        try(var exporter=io.tapstate.adapters.otel.OtelMetricsExport.start(settings)){
            metricsExport=exporter;sampleMetrics=true;var driver=driver();driver.reconcile();
            assertThat(captured).hasSize(1);var original=captured.getFirst();
            assertThat(exporter.flush(Duration.ofSeconds(2))).isFalse();
            assertThat(captured.getFirst()).isSameAs(original);
            assertThat(original).isSameAs(views.observations(FIRST).read("orders").orElseThrow());
            driver.reconcile();assertThat(captured).hasSize(2);
            assertThat(captured.getLast().state()).isEqualTo(PipelineState.RUNNING);
        }
    }
    @Test void rejectedExportCannotPreventEvaluationOrTheIndependentFreshnessClock() throws Exception {
        var collector = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var response = new CompletableFuture<OtlpRejection>();
        collector.createContext("/v1/metrics", exchange -> {
            try (exchange) {
                int size = exchange.getRequestBody().readAllBytes().length;
                exchange.sendResponseHeaders(400, -1);
                response.complete(new OtlpRejection(exchange.getRequestMethod(),
                        exchange.getRequestHeaders().getFirst("Content-Type"), size, exchange.getResponseCode()));
            }
        });
        collector.start();
        try {
            verifyAlertFreshnessWithRejectingCollector(collector.getAddress().getPort(), response);
        } finally {
            collector.stop(0);
        }
    }

    private record OtlpRejection(String method, String contentType, int size, int status) {}

    private void verifyAlertFreshnessWithRejectingCollector(int port,
            CompletableFuture<OtlpRejection> response) throws Exception {
        var settings=io.tapstate.adapters.otel.ExportSettings.otlpTo("http://127.0.0.1:"+port+"/v1/metrics",
                io.tapstate.adapters.otel.ExportSettings.Protocol.HTTP_PROTOBUF,Duration.ofHours(1));
        var harness=new PipelineAlertRuntimeTest();var rule=AlertEvaluationHostTest.definition("pipeline.observation-stale");
        org.mockito.Mockito.when(harness.fixture.store.evaluationRules(org.mockito.ArgumentMatchers.any(),org.mockito.ArgumentMatchers.anyInt()))
                .thenReturn(new io.tapstate.spi.store.AlertStore.DefinitionPage(List.of(rule),null));
        harness.fixture.now.set(Instant.now().plusSeconds(10));harness.intents.put("orders",intents.snapshot);
        try(var exporter=io.tapstate.adapters.otel.OtelMetricsExport.start(settings);var runtime=harness.runtime()){
            runtime.tick();org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->!runtime.health().degraded());
            metricsExport=exporter;sampleMetrics=true;alertFeed=runtime;driver().reconcile();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->runtime.health().queuedInputs()==0);
            runtime.tick();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->!harness.rows.isEmpty());
            var initial=harness.rows.values().iterator().next().value().snapshot();
            assertThat(initial.status()).isEqualTo(io.tapstate.core.alerting.AlertSnapshot.Status.CLEAR);
            var original=views.observations(FIRST).read("orders").orElseThrow();
            // Flush completion is not the collector's verdict. Require an actual rejected export.
            exporter.flush(Duration.ofSeconds(5));
            var rejected = response.get(5, TimeUnit.SECONDS);
            assertThat(rejected.method()).isEqualTo("POST");
            assertThat(rejected.contentType()).isEqualTo("application/x-protobuf");
            assertThat(rejected.size()).isPositive();
            assertThat(rejected.status()).isEqualTo(400);
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).until(()->runtime.health().queuedInputs()==0);
            harness.fixture.now.set(initial.lastObservedAt().plusSeconds(121));runtime.tick();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(3)).untilAsserted(()->{
                var stale=harness.rows.values().iterator().next().value().snapshot();
                assertThat(stale.status()).isEqualTo(io.tapstate.core.alerting.AlertSnapshot.Status.FIRING);
                assertThat(stale.lastObservedAt()).isEqualTo(harness.fixture.now.get());
            });
            assertThat(views.observations(FIRST).read("orders").orElseThrow()).isSameAs(original);
        }
    }

    private void advance(PipelineConvergencePass pass) {
        assertThat(pass.claim()).isEqualTo(WorkloadClaimFence.from(claims.read(KEY).orElseThrow().claim()));
        if (realOwnership) pass.advanced(ownership.beginExecution(pass.scope(), pass.claim()).claimFence());
        else {
            var actual = claims.advanceExecution(claims.read(KEY).orElseThrow().claim(), 7, Set.of("node")).orElseThrow();
            pass.advanced(WorkloadClaimFence.from(actual));
        }
        if (startFailure != null) throw startFailure;
        carrying.set(true);
    }

    @Test void theFirstRejectedPipelineKeepsPriorityAcrossQueueFullTicks() throws Exception {
        var firstEntered = new java.util.concurrent.CountDownLatch(1);
        var firstRelease = new java.util.concurrent.CountDownLatch(1);
        var secondEntered = new java.util.concurrent.CountDownLatch(1);
        var secondRelease = new java.util.concurrent.CountDownLatch(1);
        var tailEntered = new java.util.concurrent.CountDownLatch(1);
        var input = mock(PipelineIntentStore.class);
        when(input.pipelineIds()).thenReturn(List.of("a", "b", "c"));
        when(input.scanCandidates()).thenReturn(List.of("a", "b", "c").stream()
                .map(id -> new PipelineIntentStore.ScanCandidate(id, 0)).toList());
        when(input.loadForActuation(any())).thenAnswer(call -> {
            String id = call.getArgument(0);
            if (id.equals("a")) { firstEntered.countDown(); assertThat(firstRelease.await(5, TimeUnit.SECONDS)).isTrue(); }
            if (id.equals("b")) { secondEntered.countDown(); assertThat(secondRelease.await(5, TimeUnit.SECONDS)).isTrue(); }
            if (id.equals("c")) tailEntered.countDown();
            return Optional.empty();
        });
        try (var work = new PipelineConvergences(1, 1)) {
            var ports = new ConvergenceDriver.ScopedPorts(input, mock(PipelineStoreViews.class), () -> {},
                    ignored -> { throw new AssertionError("no prepared intent"); }, ignored -> RebuildAdmission.never(), ignored -> {});
            var driver = ConvergenceDriver.scoped(mock(PipelineConverger.class), mock(ObservationPublisher.class),
                    null, MetricsExport.none(), () -> true, mock(PipelineActuationOwnership.class), ports, null, work);
            try {
                driver.reconcile();
                assertThat(firstEntered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(work.pipelineIds()).containsExactlyInAnyOrder("a", "b");
                driver.reconcile();
                driver.reconcile();
                assertThat(work.health().rejectedRequests()).isPositive();
                firstRelease.countDown();
                assertThat(secondEntered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(work.health().queuedPipelines()).isZero();
                driver.reconcile();
                assertThat(work.pipelineIds()).containsExactlyInAnyOrder("b", "c");
                secondRelease.countDown();
                assertThat(tailEntered.await(2, TimeUnit.SECONDS)).isTrue();
            } finally { firstRelease.countDown(); secondRelease.countDown(); }
        }
    }

    @Test void aQueuedPeriodicAlertIsCurrentUntilItsOriginalCheckpointChanges() throws Exception {
        var guard = new CompletableFuture<BooleanSupplier>();
        alertFeed = mock(PipelineAlertFeed.class);
        org.mockito.Mockito.doAnswer(call -> {
            guard.complete(call.getArgument(2));
            return null;
        }).when(alertFeed).publishedPeriodic(any(), any(), any());
        when(ownership.proveObservationClaim(any(), any())).thenReturn(true);
        try (var observations = new PeriodicObservations(1, 1)) {
            periodicObservations = observations;
            var driver = driver();
            driver.reconcile();
            driver.reconcile();

            var queued = guard.get(5, TimeUnit.SECONDS);
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> observations.pipelineIds().isEmpty());
            assertThat(queued.getAsBoolean()).isTrue();

            observations.discard("orders");
            var state = views.state(FIRST);
            var running = state.read("orders").orElseThrow();
            assertThat(state.compareAndSwap("orders", running.epoch(), StateJson.of(PipelineState.COMPLETED), T0))
                    .isInstanceOf(CasOutcome.Applied.class);
            assertThat(queued.getAsBoolean()).isFalse();
        }
    }

    @Test void revokingAFrameBeforeTheStoreDispatchPreventsTheWrite() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        when(ownership.proveObservationClaim(any(), any())).thenReturn(true);
        try (var observations = new PeriodicObservations(1, 1)) {
            periodicObservations = observations;
            var driver = driver();
            driver.reconcile();
            org.awaitility.Awaitility.await("first observation finishes before taking a write baseline")
                    .atMost(Duration.ofSeconds(5)).until(() -> observations.idle("orders"));
            long before = views.writes.stream().filter(write -> write.kind().equals("observation")).count();
            views.beforeObservationWrite = () -> {
                entered.countDown();
                try { assertThat(release.await(5, TimeUnit.SECONDS)).isTrue(); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            };
            try {
                driver.reconcile();
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                observations.discard("orders");
            } finally {
                release.countDown();
            }
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> observations.pipelineIds().isEmpty());
            assertThat(views.writes.stream().filter(write -> write.kind().equals("observation")).count()).isEqualTo(before);
        }
    }

    @Test void aStagedFallbackCannotPublishAfterOnlyItsOriginalEngineExecutionEnds() {
        var execution = new java.util.concurrent.atomic.AtomicReference<ExecutionFence>();
        onStart = pass -> {
            advance(pass);
            execution.set(new ExecutionFence(pass.scope(), pass.claim()));
        };
        nativeSampling = new NativeObservationSampling(null, null, null,
                id -> Optional.ofNullable(execution.get()), id -> null, "orders", Clock.fixed(T0, ZoneOffset.UTC));
        when(ownership.proveObservationClaim(any(), any())).thenReturn(true);
        var cache = mock(io.tapstate.adapters.mongostore.MongoCacheDomain.class);
        when(cache.writeObservations(org.mockito.ArgumentMatchers.anyList())).thenAnswer(call -> {
            List<io.tapstate.adapters.mongostore.MongoCacheDomain.ObservationWrite> batch = call.getArgument(0);
            var write = batch.getFirst();
            assertThat(batch).hasSize(1);
            try {
                write.active().run();
                views.pipelineObservations(write.scope(), write.original(), write.active()).save(write.observation());
                return java.util.Collections.singletonList(null);
            } catch (RuntimeException refused) {
                return List.of(refused);
            }
        });
        try (var observations = new PeriodicObservations(1, 1); var writes = new ProcessObservationWriter(cache, false)) {
            periodicObservations = observations;
            observationWrites = writes;
            var driver = driver();
            driver.reconcile();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> writes.usage().pending() == 1);
            int prior = views.writes.size();
            assertThat(execution.get()).isNotNull();
            execution.set(null);
            writes.flush();
            assertThat(views.writes.subList(prior, views.writes.size())).noneMatch(write -> write.kind().equals("observation"));
        }
    }

    @Test void aHeldWorkerKeepsItsClaimAndBudgetUntilTheOriginalPassEnds() throws Exception {
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        try (var work = new PipelineConvergences(1, 2)) {
            pipelineConvergences = work;
            onStart = pass -> {
                entered.countDown();
                try { assertThat(release.await(5, TimeUnit.SECONDS)).isTrue(); }
                catch (InterruptedException stopped) { Thread.currentThread().interrupt(); throw new AssertionError(stopped); }
                advance(pass);
            };
            var driver = driver();
            try {
                driver.reconcile();
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                driver.reconcile();
                intents.ids = List.of();
                driver.reconcile();
                assertThat(retained).allSatisfy(ids -> assertThat(ids).contains("orders"));
                verify(ownership, org.mockito.Mockito.atLeastOnce()).retain(List.of("orders"));
            } finally { release.countDown(); }
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> work.pipelineIds().isEmpty());
            driver.reconcile();
            assertThat(passes).hasSize(1);
            assertThat(retained.getLast()).isEmpty();
            verify(ownership).retain(List.of());
        }
    }

    @Test void completionKeepsItsPublicationAndFencedCleanupOrderOnTheLifecycleWorker() {
        try (var work = new PipelineConvergences(1, 2)) {
            pipelineConvergences = work;
            var driver = driver();
            driver.reconcile();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> work.pipelineIds().isEmpty());
            views.writes.clear(); readyToComplete = true;
            driver.reconcile();
            org.awaitility.Awaitility.await().atMost(Duration.ofSeconds(5)).until(() -> work.pipelineIds().isEmpty());
            assertThat(completionTrace).containsExactly("final-observation", "completion-cas", "cleanup");
            assertThat(views.observations(FIRST).read("orders").orElseThrow().state()).isEqualTo(PipelineState.COMPLETED);
        }
    }

    @Test void scopedCompletionPublishesBeforeItsFencedCasAndRetainsTheFinalReadingAfterCleanup() {
        var driver=driver(); driver.reconcile(); views.writes.clear(); readyToComplete=true;
        driver.reconcile();
        assertThat(completionTrace).containsExactly("final-observation","completion-cas","cleanup");
        assertThat(views.writes.stream().filter(write -> write.kind().equals("state")).map(Write::claim))
                .containsExactly(WorkloadClaimFence.from(claims.read(KEY).orElseThrow().claim()));
        var completed=views.observations(FIRST).read("orders").orElseThrow();
        assertThat(completed.state()).isEqualTo(PipelineState.COMPLETED);
        assertThat(completed.snapshot()).containsEntry("orders",new io.tapstate.core.lifecycle.TableSnapshot(8,8L,100,true));
        driver.reconcile();
        assertThat(views.observations(FIRST).read("orders").orElseThrow().snapshot()).isEqualTo(completed.snapshot());
        assertThat(intents.loads).isEqualTo(4);
    }

    @Test void aNewIntentRevisionAfterFinalPublicationCannotBeCompletedByTheOldPass() {
        var driver=driver(); driver.reconcile(); views.writes.clear(); readyToComplete=true;
        onFinalObservation=() -> intents.snapshot=new PipelineIntentSnapshot(FIRST,5,intents.snapshot.desired());
        driver.reconcile();
        assertThat(completionTrace).containsExactly("final-observation");
        assertThat(intents.loads).isEqualTo(3);
        assertThat(views.writes).noneMatch(write -> write.kind().equals("state"));
        assertThat(views.state(FIRST).read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(PipelineState.RUNNING));
        assertThat(views.observations(FIRST).read("orders").orElseThrow().metrics()).doesNotContainKey("reconcileFailuresInARow");
    }

    @Test void aPhysicalReplacementDuringFinalPublicationNeverReceivesTheOldCompletion() {
        var driver=driver(); driver.reconcile(); views.writes.clear(); readyToComplete=true;
        onFinalObservation=() -> {intents.snapshot=snapshot(scope(2));views.current=scope(2);};
        driver.reconcile();
        assertThat(completionTrace).containsExactly("final-observation");
        assertThat(views.writes).noneMatch(write -> write.kind().equals("state"));
        assertThat(views.requested).containsOnly(FIRST);
        assertThat(views.observations(scope(2)).read("orders")).isEmpty();
        verify(ownership,never()).permit(scope(2));
    }

    @Test void failedCompletedCleanupKeepsItsFinalLoadAndRetriesWithoutAnotherTransition() {
        var driver=driver(); driver.reconcile(); views.writes.clear(); readyToComplete=true; rejectCleanup=true;
        driver.reconcile();
        var checkpoint=views.state(FIRST).read("orders").orElseThrow();
        var failedCleanup=views.observations(FIRST).read("orders").orElseThrow();
        assertThat(failedCleanup.state()).isEqualTo(PipelineState.COMPLETED);
        assertThat(failedCleanup.snapshot()).containsEntry("orders",new io.tapstate.core.lifecycle.TableSnapshot(8,8L,100,true));
        rejectCleanup=false; driver.reconcile();
        assertThat(views.state(FIRST).read("orders")).contains(checkpoint);
        assertThat(views.observations(FIRST).read("orders").orElseThrow().snapshot()).isEqualTo(failedCleanup.snapshot());
        assertThat(views.observations(FIRST).read("orders").orElseThrow().metrics()).doesNotContainKey("reconcileFailuresInARow");
        assertThat(completionTrace).containsExactly("final-observation","completion-cas","cleanup","cleanup");
    }

    @Test void thePublishedObservationIsSampledOnlyThroughItsSamePassAndActuallyAdvancedClaim() {
        var legacy = new InMemoryRateHistoryStore(); sampler = new io.tapstate.runtime.scheduler.RateSampler(legacy, Duration.ofSeconds(60)); sampleMetrics = true;
        driver().reconcile();
        assertThat(legacy.all()).isEmpty(); assertThat(views.rates.all()).hasSize(1);
        Observation published = views.observations(FIRST).read("orders").orElseThrow();
        assertThat(views.rates.all().getFirst().observedAt()).isEqualTo(published.observedAt());
        assertThat(views.rates.all().getFirst().counters()).containsEntry("recordCount", 5L);
        assertThat(views.writes.stream().filter(w -> w.kind().equals("rate")).map(w -> w.claim().executionGeneration())).containsExactly(1L);
        assertThat(views.requested).containsOnly(FIRST);
    }

    @Test void oneSnapshotAndPermitDriveThePassAndItsActualAdvancedPublication() {
        var driver = driver();
        driver.reconcile();
        assertThat(intents.loads).isEqualTo(1);
        verify(ownership).permit(FIRST);
        verify(ownership, never()).permit("orders");
        assertThat(passes).hasSize(1);
        assertThat(passes.getFirst().scope()).isEqualTo(FIRST);
        assertThat(views.writes.stream().filter(w -> w.kind().equals("state")).map(w -> w.claim().executionGeneration()))
                .containsExactly(0L);
        assertThat(views.writes.stream().filter(w -> w.kind().equals("observation")).map(w -> w.claim().executionGeneration()))
                .containsExactly(1L);
        assertThat(views.observations(FIRST).read("orders").orElseThrow().state()).isEqualTo(PipelineState.RUNNING);
    }

    @Test void aControlCannotEnterWhileAnAdvancedConvergencePassHasNotReturned() {
        var membership = mock(ClusterMembershipGate.class);
        when(membership.businessEligible()).thenReturn(true);
        when(membership.committed()).thenReturn(new io.tapstate.spi.store.ClusterMembership("cluster", 7, Set.of("node")));
        var scopedClaims = mock(io.tapstate.spi.store.WorkloadClaimStore.class);
        when(scopedClaims.acquirePipeline(any(), any(), org.mockito.ArgumentMatchers.anyLong(), any()))
                .thenAnswer(call -> claims.acquire(KEY, call.getArgument(1), call.getArgument(2), call.getArgument(3)));
        when(scopedClaims.readPipeline(any())).thenAnswer(call -> claims.read(KEY));
        when(scopedClaims.advanceExecution(any(), org.mockito.ArgumentMatchers.anyLong(), any()))
                .thenAnswer(call -> claims.advanceExecution(call.getArgument(0), call.getArgument(1), call.getArgument(2)));
        ownership = PipelineActuationOwnership.scoped(new io.tapstate.spi.store.CacheBusinessScope("cluster", 3, "cache-a"), OWNER,
                membership, new ClusterWorkloadClaims(scopedClaims, membership), Duration.ofMinutes(1), Duration.ofSeconds(10), System::nanoTime);
        realOwnership = true;
        onStart = pass -> {
            advance(pass);
            org.assertj.core.api.Assertions.assertThatThrownBy(() -> ownership.control(FIRST)).isInstanceOf(TapstateException.class);
        };
        var driver = driver(); driver.reconcile();
        try (var control = ownership.control(FIRST)) { control.requireActive(); }
    }

    @Test void aCodedFailureAfterAdvancePublishesUnderThatSameActualClaim() {
        startFailure = fenced();
        driver().reconcile();
        assertThat(intents.loads).isEqualTo(1);
        assertThat(views.writes.stream().filter(w -> w.kind().equals("state")).map(w -> w.claim().executionGeneration()))
                .containsExactly(0L, 1L);
        assertThat(views.writes.stream().filter(w -> w.kind().equals("observation")).map(w -> w.claim().executionGeneration()))
                .containsExactly(1L);
        Observation published = views.observations(FIRST).read("orders").orElseThrow();
        assertThat(published.state()).isEqualTo(PipelineState.FAILED);
        assertThat(published.failure().code()).isEqualTo(IoError.WORKLOAD_CLAIM_FENCED.code());
    }

    @Test void anUncodedFailureAfterAdvanceStillUsesTheSamePassForErrorProjection() {
        startFailure = new IllegalStateException("injected invariant failure");
        driver().reconcile();
        assertThat(intents.loads).isEqualTo(1);
        assertThat(views.writes.stream().filter(w -> w.kind().equals("observation")).map(w -> w.claim().executionGeneration()))
                .containsExactly(1L);
        assertThat(views.observations(FIRST).read("orders").orElseThrow().metrics())
                .containsEntry("reconcileFailuresInARow", 1L);
    }

    @Test void aPhysicalReplacementDuringStartIsNeverAdoptedByFailurePublication() {
        onStart = pass -> {
            advance(pass);
            intents.snapshot = snapshot(scope(2));
            views.current = scope(2);
        };
        driver().reconcile();
        assertThat(intents.loads).isEqualTo(1);
        verify(ownership, never()).permit(scope(2));
        assertThat(passes).hasSize(1);
        assertThat(passes.getFirst().scope()).isEqualTo(FIRST);
        assertThat(views.requested).containsOnly(FIRST);
        assertThat(views.observations(FIRST).read("orders")).isEmpty();
        assertThat(views.observations(scope(2)).read("orders")).isEmpty();
    }

    @Test void aRefusedPermitCannotPublishUnderAnUnscopedView() {
        deny = true;
        driver().reconcile();
        assertThat(intents.loads).isEqualTo(1);
        assertThat(passes).isEmpty();
        assertThat(views.requested).isEmpty();
        assertThat(views.writes).isEmpty();
    }

    @Test void aConfigReadFailureHasNoAuthorityToPublishByPipelineId() {
        intents.loadFailure = fenced();
        driver().reconcile();
        assertThat(intents.loads).isEqualTo(1);
        verify(ownership, never()).permit(any(PipelineIntentScope.class));
        assertThat(passes).isEmpty();
        assertThat(views.requested).isEmpty();
    }

    @Test void aDeletedOrRemovedIntentBetweenListingAndLoadOpensNoPass() {
        var driver = driver();
        intents.snapshot = null;
        driver.reconcile();
        intents.snapshot = new PipelineIntentSnapshot(FIRST, 5, null);
        driver.reconcile();
        assertThat(intents.loads).isEqualTo(2);
        verify(ownership, never()).permit(any(PipelineIntentScope.class));
        assertThat(passes).isEmpty();
        assertThat(views.requested).isEmpty();
    }

    @Test void anIneligibleMemberDoesNotEvenLoadTheIntent() {
        eligible.set(false);
        driver().reconcile();
        assertThat(intents.loads).isZero();
        assertThat(passes).isEmpty();
        assertThat(views.requested).isEmpty();
    }

    @Test void scopedCleanupCannotInvokeTheConvergersDefaultRebuildPolicy() {
        driver().reconcile();
        assertThat(retained).containsExactly(List.of("orders"));
    }

    @Test void cleanupWithoutAnAdmittedPassCannotInvokeTheDefaultRebuildPolicy() {
        deny = true;
        driver().reconcile();
        assertThat(passes).isEmpty();
        assertThat(retained).containsExactly(List.of("orders"));
    }

    @Test void consecutiveFailureCountsDoNotCrossPhysicalStateGenerations() {
        startFailure = new IllegalStateException("injected invariant failure");
        var driver = driver();
        driver.reconcile(); driver.reconcile();
        assertThat(views.observations(FIRST).read("orders").orElseThrow().metrics())
                .containsEntry("reconcileFailuresInARow", 2L);
        assertThat(claims.release(claims.read(KEY).orElseThrow().claim())).isTrue();
        claims.acquire(KEY, OWNER, 7, Duration.ofMinutes(1));
        intents.snapshot = snapshot(scope(2)); views.current = scope(2);
        driver.reconcile();
        assertThat(views.observations(scope(2)).read("orders").orElseThrow().metrics())
                .containsEntry("reconcileFailuresInARow", 1L);
        assertThat(intents.loads).isEqualTo(3);
    }

    private static PipelineIntentScope scope(long generation) { return new PipelineIntentScope("cluster", 3, "cache-a", "orders", generation); }
    private static PipelineIntentSnapshot snapshot(PipelineIntentScope scope) {
        return new PipelineIntentSnapshot(scope, 4, new DesiredState("orders", PipelineState.RUNNING, "original"));
    }
    private static TapstateException fenced() { return new TapstateException(IoError.WORKLOAD_CLAIM_FENCED, Map.of(), null); }

    private static final class Intents implements PipelineIntentStore {
        private PipelineIntentSnapshot snapshot = snapshot(FIRST);
        private List<String> ids = List.of("orders");
        private RuntimeException loadFailure;
        private PendingStart pending;
        @Override public Optional<PendingStart> pendingStart(String id) { return Optional.ofNullable(pending); }
        private int loads;
        private boolean hints = true;
        private ScanCandidate candidateOverride;
        @Override public Optional<PipelineIntentSnapshot> load(String id) { throw new AssertionError("authoring load does not prove preparation readiness"); }
        @Override public Optional<PipelineIntentSnapshot> loadForActuation(String id) { loads++; if (loadFailure != null) throw loadFailure; return Optional.ofNullable(snapshot); }
        @Override public List<String> pipelineIds() { return ids; }
        @Override public List<ScanCandidate> scanCandidates() {
            if (candidateOverride != null) return List.of(candidateOverride);
            return ids.stream().map(id -> snapshot == null || !hints || !id.equals(snapshot.scope().pipelineId())
                    ? new ScanCandidate(id, 0) : new ScanCandidate(id,
                            snapshot.desired().targetState() == PipelineState.RUNNING ? snapshot.intentRevision() : 0,
                            snapshot.scope(), snapshot.intentRevision(), snapshot.desired().targetState())).toList();
        }
        @Override public PipelineIntentStore guardedBy(Runnable active) { throw new AssertionError("read-only driver"); }
        @Override public boolean compareAndSet(PipelineIntentScope scope, long revision, DesiredState desired) { throw new AssertionError("read-only driver"); }
        @Override public boolean remove(PipelineIntentScope scope, long revision) { throw new AssertionError("read-only driver"); }
    }
    private record Write(String kind, PipelineIntentScope scope, WorkloadClaimFence claim) {}
    private final class Views implements PipelineStoreViews {
        private PipelineIntentScope current = FIRST;
        private boolean rejectPublication;
        private boolean rejectPostPublicationRead;
        private CheckpointDoc.LeaseRecovery recovery;
        private final Map<PipelineIntentScope, InMemoryStateStore> states = new HashMap<>();
        private final Map<PipelineIntentScope, InMemoryObservationStore> projections = new HashMap<>();
        private Runnable beforeObservationWrite = () -> {};
        private final InMemoryRateHistoryStore rates = new InMemoryRateHistoryStore();
        private final List<PipelineIntentScope> requested = new ArrayList<>();
        private final List<Write> writes = new ArrayList<>();
        private InMemoryStateStore state(PipelineIntentScope scope) {
            return states.computeIfAbsent(scope, ignored -> { var store = new InMemoryStateStore(); store.create("orders", StateJson.of(PipelineState.NEW), T0); return store; });
        }
        private InMemoryObservationStore observations(PipelineIntentScope scope) {
            return projections.computeIfAbsent(scope, ignored -> new InMemoryObservationStore());
        }
        private void check(PipelineIntentScope scope) { requested.add(scope); if (!current.equals(scope)) throw fenced(); }
        private void check(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active) {
            active.run(); check(scope);
            if (!original.equals(WorkloadClaimFence.from(claims.read(KEY).orElseThrow().claim()))) throw fenced();
        }
        @Override public io.tapstate.spi.store.RateHistoryStore pipelineRateHistory(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active) {
            var writer = mock(io.tapstate.spi.store.RateHistoryStore.class);
            org.mockito.Mockito.doCallRealMethod().when(writer).appendWithin(any(io.tapstate.core.lifecycle.RateSample.class), any(Duration.class));
            org.mockito.Mockito.doAnswer(call -> { check(scope, original, active); writes.add(new Write("rate", scope, original)); rates.append(call.getArgument(0)); return null; })
                    .when(writer).append(any(io.tapstate.core.lifecycle.RateSample.class)); return writer;
        }
        @Override public StateStore pipelineStates(PipelineIntentScope scope) {
            check(scope);
            if (rejectPostPublicationRead && !writes.isEmpty()
                    && writes.getLast().kind().equals("observation")) throw fenced();
            if (recovery == null) return state(scope);
            var reader = mock(StateStore.class);
            when(reader.read(any())).thenAnswer(call -> state(scope).read(call.getArgument(0))
                    .map(doc -> new CheckpointDoc(doc.pipelineId(), doc.stateJson(), doc.epoch(), doc.touchTime(), recovery)));
            return reader;
        }
        @Override public StateStore pipelineStates(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active) {
            return new StateStore() {
                @Override public Optional<CheckpointDoc> read(String id) {
                    check(scope);
                    return state(scope).read(id).map(doc -> recovery == null ? doc : new CheckpointDoc(
                            doc.pipelineId(), doc.stateJson(), doc.epoch(), doc.touchTime(), recovery));
                }
                @Override public CasOutcome compareAndSwap(String id, long epoch, String next, Instant at) {
                    check(scope, original, active); writes.add(new Write("state", scope, original));
                    if(next.equals(StateJson.of(PipelineState.COMPLETED)))completionTrace.add("completion-cas");
                    return state(scope).compareAndSwap(id, epoch, next, at);
                }
                @Override public void create(String id, String next, Instant at) { throw new AssertionError("no ordinary seed"); }
                @Override public void delete(String id) { throw new AssertionError("no ordinary delete"); }
            };
        }
        @Override public ObservationStore pipelineObservations(PipelineIntentScope scope) { check(scope); return observations(scope); }
        @Override public ObservationStore pipelineObservations(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active) {
            return new ObservationStore() {
                @Override public Optional<Observation> read(String id) { check(scope); return observations(scope).read(id); }
                @Override public Optional<Observation> refreshTerminal(Observation expected, long epoch, Instant at) {
                    check(scope, original, active);
                    var checkpoint = state(scope).read(scope.pipelineId()).orElseThrow();
                    if (checkpoint.epoch() != epoch || StateJson.parse(checkpoint.stateJson()) != expected.state()
                            || !read(scope.pipelineId()).filter(expected::equals).isPresent()) return Optional.empty();
                    var refreshed = new Observation(expected.pipelineId(), expected.state(), expected.metrics(), expected.snapshot(),
                            expected.positions(), expected.failure(), at, expected.facts(), expected.initialLoad());
                    save(refreshed);
                    return Optional.of(refreshed);
                }
                @Override public void save(Observation value) {
                    if (rejectPublication) throw fenced();
                    beforeObservationWrite.run();
                    check(scope, original, active); writes.add(new Write("observation", scope, original)); observations(scope).save(value);
                    if(readyToComplete && !completionObserved && value.state()==PipelineState.RUNNING) {
                        completionObserved=true;completionTrace.add("final-observation");onFinalObservation.run();
                    }
                }
                @Override public void delete(String id) { throw new AssertionError("no ordinary delete"); }
            };
        }
    }

    @Test void onlyTheFirstVerifiedStartClaimsTheInitialAdmissionTurn() throws Exception {
        try (var driver = driver()) {
            scheduledPass(driver);
            verify(ownership, times(1)).permit(FIRST, true);
            scheduledPass(driver);
            verify(ownership, times(1)).permit(FIRST, true);
            verify(ownership, times(2)).permit(FIRST);
            assertThat(carrying).isTrue();
        }
    }

    @Test void aNewStopIntentLoadedAfterAnOldStartHintCannotClaimInitialAdmission() throws Exception {
        intents.candidateOverride = new PipelineIntentStore.ScanCandidate("orders", 4, FIRST, 4, PipelineState.RUNNING);
        intents.snapshot = new PipelineIntentSnapshot(FIRST, 5, new DesiredState("orders", PipelineState.STOPPED, "original"));
        try (var driver = driver()) {
            scheduledPass(driver);
            verify(ownership, never()).permit(FIRST, true);
            verify(ownership).permit(FIRST);
            assertThat(views.state(FIRST).read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(PipelineState.STOPPED));
        }
    }
}
