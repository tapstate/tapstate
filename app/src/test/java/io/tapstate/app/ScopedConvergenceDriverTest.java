package io.tapstate.app;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
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

    private ConvergenceDriver driver() {
        claims.acquire(KEY, OWNER, 7, Duration.ofMinutes(1));
        if (!realOwnership) when(ownership.permit(any(PipelineIntentScope.class))).thenAnswer(call -> deny
                ? PipelineActuationOwnership.Permit.denied()
                : new PipelineActuationOwnership.Permit(true, claims.read(KEY).orElseThrow().claim()));
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
        }, pass -> RebuildAdmission.never(), ids -> retained.add(List.copyOf(ids)), alertFeed);
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
                sampler, metricsExport, eligible::get, ownership, bindings);
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
    @Test void unreachableExportCannotPreventEvaluationOrTheIndependentFreshnessClock() throws Exception {
        int unusedPort;
        try(var socket=new java.net.ServerSocket(0,1,java.net.InetAddress.getLoopbackAddress())){unusedPort=socket.getLocalPort();}
        var settings=io.tapstate.adapters.otel.ExportSettings.otlpTo("http://127.0.0.1:"+unusedPort+"/v1/metrics",
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
            assertThat(exporter.flush(Duration.ofSeconds(2))).isFalse();
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
        private RuntimeException loadFailure;
        private int loads;
        @Override public Optional<PipelineIntentSnapshot> load(String id) { throw new AssertionError("authoring load does not prove preparation readiness"); }
        @Override public Optional<PipelineIntentSnapshot> loadForActuation(String id) { loads++; if (loadFailure != null) throw loadFailure; return Optional.ofNullable(snapshot); }
        @Override public List<String> pipelineIds() { return List.of("orders"); }
        @Override public PipelineIntentStore guardedBy(Runnable active) { throw new AssertionError("read-only driver"); }
        @Override public boolean compareAndSet(PipelineIntentScope scope, long revision, DesiredState desired) { throw new AssertionError("read-only driver"); }
        @Override public boolean remove(PipelineIntentScope scope, long revision) { throw new AssertionError("read-only driver"); }
    }
    private record Write(String kind, PipelineIntentScope scope, WorkloadClaimFence claim) {}
    private final class Views implements PipelineStoreViews {
        private PipelineIntentScope current = FIRST;
        private final Map<PipelineIntentScope, InMemoryStateStore> states = new HashMap<>();
        private final Map<PipelineIntentScope, InMemoryObservationStore> projections = new HashMap<>();
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
            org.mockito.Mockito.doAnswer(call -> { check(scope, original, active); writes.add(new Write("rate", scope, original)); rates.append(call.getArgument(0)); return null; })
                    .when(writer).append(any(io.tapstate.core.lifecycle.RateSample.class)); return writer;
        }
        @Override public StateStore pipelineStates(PipelineIntentScope scope) { check(scope); return state(scope); }
        @Override public StateStore pipelineStates(PipelineIntentScope scope, WorkloadClaimFence original, Runnable active) {
            return new StateStore() {
                @Override public Optional<CheckpointDoc> read(String id) { check(scope); return state(scope).read(id); }
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
                @Override public void save(Observation value) {
                    check(scope, original, active); writes.add(new Write("observation", scope, original)); observations(scope).save(value);
                    if(readyToComplete && !completionObserved && value.state()==PipelineState.RUNNING) {
                        completionObserved=true;completionTrace.add("final-observation");onFinalObservation.run();
                    }
                }
                @Override public void delete(String id) { throw new AssertionError("no ordinary delete"); }
            };
        }
    }
}
