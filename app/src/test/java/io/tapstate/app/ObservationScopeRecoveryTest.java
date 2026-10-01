package io.tapstate.app;

import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.EpochCas;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.ObservationFailure;
import io.tapstate.core.lifecycle.PipelineEvent;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.lifecycle.CaptureReading;
import io.tapstate.core.lifecycle.DeliveryReading;
import io.tapstate.core.lifecycle.SnapshotReading;
import io.tapstate.core.lifecycle.FrontierStall;
import io.tapstate.core.lifecycle.FrontierStallPressure;
import io.tapstate.core.lifecycle.NestColdLayerPressure;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.FrontierStallAlert;
import io.tapstate.runtime.scheduler.FrontierStallWatch;
import io.tapstate.runtime.scheduler.NestColdLayerAlert;
import io.tapstate.runtime.scheduler.NestColdLayerWatch;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.PipelineConverger;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

class ObservationScopeRecoveryTest {
    private static final String PIPELINE = "orders";
    private static final Instant AT = Instant.parse("2026-09-28T10:00:00Z");
    private static final ObservationStore.Scope OWNER = new ObservationStore.Scope("inc-a", 41);
    private static final ObservationFailure FAILURE = new ObservationFailure(
            LifecycleError.PAUSED_JOB_MISSING.code(), Map.of("pipeline", PIPELINE));

    @Test
    void aWatchdogBeforeDeferredReplayPreservesTheFirstColdFailureTime() throws Exception {
        var scopes = org.mockito.Mockito.spy(new ObservationScopeRegistry());
        Fixture fixture = new Fixture(PipelineState.PAUSED, scopes);
        CountDownLatch currentVisible = new CountDownLatch(1);
        CountDownLatch permitReplay = new CountDownLatch(1);
        org.mockito.Mockito.doAnswer(invocation -> {
            boolean restored = (boolean) invocation.callRealMethod();
            if (restored) {
                currentVisible.countDown();
                if (!permitReplay.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("deferred replay was not released");
                }
            }
            return restored;
        }).when(scopes).restore(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any());
        Instant firstFailed;
        try (TelemetryDispatcher dispatcher = fixture.dispatcher(java.time.Duration.ofMillis(250))) {
            fixture.latest.failWrite.set(true);
            dispatcher.offerScopeRecovery(PIPELINE, null, null, () -> true);
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).failures() == 1);
            firstFailed = Instant.now();
            assertThat(fixture.scopes.current(PIPELINE)).isEmpty();

            dispatcher.offerScopeRecovery(PIPELINE, null, null, () -> true);
            assertThat(currentVisible.await(5, TimeUnit.SECONDS)).isTrue();
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).timeouts() == 1);
            await(() -> fixture.events.size() == 1);
            assertThat(fixture.events.getFirst().kind()).isEqualTo(PipelineEvent.Kind.TELEMETRY_DEGRADED);
            assertThat(fixture.events.getFirst().occurredAt()).isBeforeOrEqualTo(firstFailed);

            permitReplay.countDown();
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0);
            await(() -> {
                dispatcher.offer(fixture.publisher.prepareScoped(PIPELINE, null, OWNER).orElseThrow(), OWNER);
                return fixture.events.size() == 2;
            });
        } finally {
            permitReplay.countDown();
        }
        assertThat(fixture.events).extracting(PipelineEvent::kind).containsExactly(
                PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_RESTORED);
        assertThat(fixture.events.getFirst().occurredAt()).isBeforeOrEqualTo(firstFailed);
    }

    @Test
    void aCapturedClusterGrantCannotRestoreAnotherKeyOrExecution() throws Exception {
        var owner = new WorkloadOwner("node-a", "boot-a");
        var key = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE);
        var accepted = new ObservationScopeRecovery.Owner(key, owner, 1, 41, 3);
        List<ObservationScopeRecovery.Owner> denied = List.of(
                new ObservationScopeRecovery.Owner(key, owner, 1, 40, 3),
                new ObservationScopeRecovery.Owner(new WorkloadClaimKey("cluster-b",
                        WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE), owner, 1, 41, 3),
                new ObservationScopeRecovery.Owner(new WorkloadClaimKey("cluster-a",
                        WorkloadClaimType.PIPELINE_ACTUATION, "another-pipeline"), owner, 1, 41, 3),
                new ObservationScopeRecovery.Owner(new WorkloadClaimKey("cluster-a",
                        WorkloadClaimType.CAPTURE, PIPELINE), owner, 1, 41, 3));
        for (var captured : denied) {
            Fixture fixture = new Fixture(PipelineState.PAUSED);
            try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
                dispatcher.offerScopeRecovery(PIPELINE, null, null, captured, () -> true);
                await(() -> fixture.latest.reads.get() > 0
                        && dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0);
                assertThat(fixture.latest.writes.get()).isZero();
                assertThat(fixture.scopes.current(PIPELINE)).isEmpty();
                assertThat(fixture.generations.advances.get()).isZero();
                assertThat(fixture.events).isEmpty();
            }
        }
        Fixture fixture = new Fixture(PipelineState.PAUSED);
        try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
            dispatcher.offerScopeRecovery(PIPELINE, null, null, accepted, () -> true);
            await(() -> fixture.scopes.current(PIPELINE).isPresent());
            assertThat(fixture.scopes.current(PIPELINE)).contains(OWNER);
            assertThat(fixture.latest.writes.get()).isEqualTo(1);
        }
    }

    @Test
    void repeatedColdLatestWriteFailuresReplayOneBoundaryOnlyAfterQualifiedRecovery() throws Exception {
        Fixture fixture = new Fixture(PipelineState.PAUSED);
        Instant before = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        Instant firstFailed = null;
        try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
            for (int attempt = 1; attempt <= 2; attempt++) {
                fixture.latest.failWrite.set(true);
                dispatcher.offerScopeRecovery(PIPELINE, null, null, () -> true);
                long expected = attempt;
                await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).failures() == expected);
                assertThat(fixture.scopes.current(PIPELINE)).isEmpty();
                assertThat(fixture.events).isEmpty();
                if (attempt == 1) {
                    assertThat(fixture.latest.writes.get()).isZero();
                    firstFailed = Instant.now();
                }
            }
            dispatcher.offerScopeRecovery(PIPELINE, null, null, () -> true);
            await(() -> fixture.scopes.current(PIPELINE).isPresent());
            await(() -> fixture.events.size() == 2);
            dispatcher.offer(fixture.publisher.prepareScoped(PIPELINE, null, OWNER).orElseThrow(), OWNER);
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).successes() >= 2);
        }
        assertThat(fixture.events).extracting(PipelineEvent::kind).containsExactly(
                PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_RESTORED);
        assertThat(fixture.events.getFirst().occurredAt()).isBetween(before, firstFailed);
        assertThat(fixture.events.getLast().occurredAt()).isAfterOrEqualTo(fixture.events.getFirst().occurredAt());
        assertThat(fixture.events).allSatisfy(event -> {
            assertThat(event.pipelineIncarnationId()).isEqualTo(OWNER.pipelineIncarnationId());
            assertThat(event.executionGeneration()).isEqualTo(OWNER.executionGeneration());
            assertThat(event.reason()).isEqualTo("latest observation write");
        });
    }

    @Test
    void aColdWriteDeadlineRetainsTheActualDegradedTimeBeforeScopeRestoration() throws Exception {
        Fixture fixture = new Fixture(PipelineState.PAUSED);
        fixture.latest.blockWrite = true;
        Instant before = Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS);
        Instant observedTimeout;
        try (TelemetryDispatcher dispatcher = fixture.dispatcher(java.time.Duration.ofMillis(80))) {
            dispatcher.offerScopeRecovery(PIPELINE, null, null, () -> true);
            assertThat(fixture.latest.writeEntered.await(5, TimeUnit.SECONDS)).isTrue();
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).timeouts() == 1);
            observedTimeout = Instant.now();
            assertThat(fixture.scopes.current(PIPELINE)).isEmpty();
            assertThat(fixture.events).isEmpty();
            fixture.latest.writeRelease.countDown();
            await(() -> fixture.scopes.current(PIPELINE).isPresent()
                    && dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0);
            await(() -> {
                dispatcher.offer(fixture.publisher.prepareScoped(PIPELINE, null, OWNER).orElseThrow(), OWNER);
                return fixture.events.size() == 2;
            });
        } finally {
            fixture.latest.writeRelease.countDown();
        }
        assertThat(fixture.events).extracting(PipelineEvent::kind).containsExactly(
                PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_RESTORED);
        assertThat(fixture.events.getFirst().occurredAt()).isBetween(before, observedTimeout);
    }

    @Test
    void aColdPausedJobPublishesItsRealFailureWithoutAdvancingOrChangingIntent() throws Exception {
        Fixture fixture = new Fixture(PipelineState.PAUSED);
        DesiredState paused = new DesiredState(PIPELINE, PipelineState.PAUSED, "rev-a");
        fixture.desired.save(paused);
        try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
            ConvergenceDriver driver = fixture.driver(dispatcher);
            driver.reconcile();
            await(() -> fixture.latest.read(PIPELINE).orElseThrow().state() == PipelineState.FAILED
                    && fixture.latest.read(PIPELINE).orElseThrow().failure() != null);
            for (int pass = 0; pass < 4; pass++) {
                driver.reconcile();
            }
            await(() -> fixture.events.size() == 2);
            Observation failed = fixture.latest.read(PIPELINE).orElseThrow();
            assertThat(failed.failure()).isEqualTo(FAILURE);
            assertThat(failed.facts()).extracting(MetricFact::name)
                    .doesNotContain("tapstate.pipeline.snapshot.rows");
            assertThat(fixture.scopes.current(PIPELINE)).contains(OWNER);
            assertThat(fixture.desired.read(PIPELINE)).contains(paused);
            assertThat(fixture.state.read(PIPELINE).orElseThrow().epoch()).isEqualTo(1);
            assertThat(fixture.generations.value).isEqualTo(41);
            assertThat(fixture.generations.advances.get()).isZero();
            assertThat(fixture.starts.get()).isZero();
            assertThat(fixture.events).extracting(PipelineEvent::kind)
                    .containsExactly(PipelineEvent.Kind.STATE_CHANGED, PipelineEvent.Kind.FAILURE);
            assertThat(fixture.events).allSatisfy(event -> {
                assertThat(event.pipelineIncarnationId()).isEqualTo("inc-a");
                assertThat(event.executionGeneration()).isEqualTo(41);
                assertThat(event.occurredAt()).isEqualTo(AT.plusSeconds(1));
            });
            assertThat(failed.facts().stream().filter(fact -> fact.name().equals("tapstate.pipeline.errors"))
                    .flatMap(fact -> fact.points().stream()).mapToLong(MetricPoint::value).sum()).isEqualTo(1);
        }
    }

    @Test
    void anAlreadyFailedColdExecutionKeepsItsStoredCauseWithoutCountingAnotherDeath() throws Exception {
        Fixture fixture = new Fixture(PipelineState.FAILED);
        fixture.latest.put(saved(PipelineState.FAILED, FAILURE), Optional.of(OWNER));
        try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
            dispatcher.offerScopeRecovery(PIPELINE, null, null, () -> true);
            await(() -> fixture.latest.writes.get() == 1);
            Observation failed = fixture.latest.read(PIPELINE).orElseThrow();
            assertThat(failed.failure()).isEqualTo(FAILURE);
            assertThat(failed.facts()).extracting(MetricFact::name)
                    .doesNotContain("tapstate.pipeline.errors", "tapstate.pipeline.snapshot.rows");
            assertThat(fixture.events).isEmpty();
            assertThat(fixture.generations.advances.get()).isZero();
        }
    }

    @Test
    void retainedGenerationCannotAssociateOldOrLegacyDataWithARecreatedOrUnstartedResource() {
        Fixture fixture = new Fixture(PipelineState.PAUSED);
        fixture.artifacts.incarnation = "inc-b";
        assertThat(fixture.recovery.resolve(PIPELINE)).isEmpty();
        fixture.latest.put(saved(PipelineState.PAUSED, null), Optional.empty());
        assertThat(fixture.recovery.resolve(PIPELINE)).isEmpty();
        fixture.latest.put(saved(PipelineState.PAUSED, null), Optional.of(new ObservationStore.Scope("inc-b", 40)));
        assertThat(fixture.recovery.resolve(PIPELINE)).isEmpty();
        fixture.latest.put(saved(PipelineState.PAUSED, null), Optional.of(new ObservationStore.Scope("inc-b", 41)));
        fixture.state.delete(PIPELINE);
        fixture.state.create(PIPELINE, StateJson.of(PipelineState.NEW), AT);
        assertThat(fixture.recovery.resolve(PIPELINE)).isEmpty();
        fixture.state.compareAndSwap(PIPELINE, 0, StateJson.of(PipelineState.PAUSED), AT.plusSeconds(1));
        fixture.generations.value = 0;
        assertThat(fixture.recovery.resolve(PIPELINE)).isEmpty();
        fixture.generations.value = 41;
        fixture.artifacts.incarnation = null;
        assertThat(fixture.recovery.resolve(PIPELINE)).isEmpty();
        fixture.artifacts.incarnation = "inc-b";
        fixture.artifacts.pipeline = false;
        assertThat(fixture.recovery.resolve(PIPELINE)).isEmpty();
        assertThat(fixture.latest.writes.get()).isZero();
        assertThat(fixture.scopes.current(PIPELINE)).isEmpty();
        assertThat(fixture.generations.advances.get()).isZero();
    }

    @Test
    void slowLatestReadsCoalesceTheFirstFailureAndLeaveAnotherPipelineConverging() throws Exception {
        Fixture fixture = new Fixture(PipelineState.PAUSED);
        fixture.latest.block();
        fixture.desired.save(new DesiredState(PIPELINE, PipelineState.PAUSED, "rev-a"));
        fixture.state.create("fast", StateJson.of(PipelineState.RUNNING), AT);
        fixture.desired.save(new DesiredState("fast", PipelineState.PAUSED, "rev-fast"));
        try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
            ConvergenceDriver driver = fixture.driver(dispatcher);
            driver.reconcile();
            assertThat(fixture.latest.entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(fixture.state.read("fast").orElseThrow().stateJson())
                    .isEqualTo(StateJson.of(PipelineState.PAUSED));
            for (int pass = 0; pass < 20; pass++) {
                driver.reconcile();
            }
            assertThat(fixture.scopes.current(PIPELINE)).isEmpty();
            assertThat(fixture.latest.reads.get()).isEqualTo(1);
            assertThat(dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).coalesced()).isPositive();
            assertThat(fixture.latest.readerThread).startsWith("tapstate-telemetry-latest-");
            fixture.latest.release.countDown();
            await(() -> fixture.latest.read(PIPELINE).orElseThrow().failure() != null);
            await(() -> fixture.events.size() == 2);
            assertThat(fixture.latest.read(PIPELINE).orElseThrow().failure()).isEqualTo(FAILURE);
            assertThat(fixture.generations.advances.get()).isZero();
        } finally {
            fixture.latest.release.countDown();
        }
    }

    @Test
    void aLaterCheckpointDoesNotReceiveTheDelayedFailureButItsOriginalTraceSurvives() throws Exception {
        Fixture fixture = new Fixture(PipelineState.PAUSED);
        fixture.latest.block();
        fixture.desired.save(new DesiredState(PIPELINE, PipelineState.PAUSED, "rev-a"));
        try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
            fixture.driver(dispatcher).reconcile();
            assertThat(fixture.latest.entered.await(5, TimeUnit.SECONDS)).isTrue();
            CheckpointDoc failed = fixture.state.read(PIPELINE).orElseThrow();
            fixture.state.compareAndSwap(PIPELINE, failed.epoch(), StateJson.of(PipelineState.STOPPED),
                    AT.plusSeconds(2));
            fixture.latest.release.countDown();
            await(() -> fixture.latest.read(PIPELINE).orElseThrow().state() == PipelineState.STOPPED);
            await(() -> fixture.events.size() == 2);
            Observation stopped = fixture.latest.read(PIPELINE).orElseThrow();
            assertThat(stopped.failure()).isNull();
            assertThat(stopped.facts().stream().filter(fact -> fact.name().equals("tapstate.pipeline.snapshot.rows"))
                    .flatMap(fact -> fact.points().stream()).mapToLong(MetricPoint::value).sum()).isEqualTo(7);
            assertThat(fixture.events.get(1).occurredAt()).isEqualTo(failed.touchTime());
            assertThat(fixture.events.get(1).failure()).isEqualTo(FAILURE);
        } finally {
            fixture.latest.release.countDown();
        }
    }

    @Test
    void authorityOrOwnerChangesDuringAColdReadCannotInstallOrPublishTheOldScope() throws Exception {
        for (boolean changeOwner : List.of(false, true)) {
            Fixture fixture = new Fixture(PipelineState.PAUSED);
            fixture.latest.block();
            AtomicBoolean owner = new AtomicBoolean(true);
            try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
                dispatcher.offerScopeRecovery(PIPELINE, null, null, owner::get);
                assertThat(fixture.latest.entered.await(5, TimeUnit.SECONDS)).isTrue();
                if (changeOwner) {
                    owner.set(false);
                    fixture.scopes.cancelRestoration(PIPELINE);
                } else {
                    fixture.artifacts.incarnation = "inc-b";
                }
                fixture.latest.release.countDown();
                await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0);
                assertThat(fixture.scopes.current(PIPELINE)).isEmpty();
                assertThat(fixture.latest.writes.get()).isZero();
                assertThat(fixture.events).isEmpty();
            } finally {
                fixture.latest.release.countDown();
            }
        }
    }

    @Test
    void aFailedColdStateReadRetainsTheFirstCauseForTheNextColdAttempt() throws Exception {
        Fixture fixture = new Fixture(PipelineState.PAUSED);
        fixture.state.failColdRead.set(true);
        fixture.desired.save(new DesiredState(PIPELINE, PipelineState.PAUSED, "rev-a"));
        try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
            ConvergenceDriver driver = fixture.driver(dispatcher);
            driver.reconcile();
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).failures() == 1);
            assertThat(fixture.scopes.current(PIPELINE)).isEmpty();
            await(() -> {
                driver.reconcile();
                return fixture.latest.read(PIPELINE).orElseThrow().failure() != null;
            });
            assertThat(fixture.latest.read(PIPELINE).orElseThrow().failure()).isEqualTo(FAILURE);
            assertThat(errorCount(fixture.latest.read(PIPELINE).orElseThrow())).isEqualTo(1);
            await(() -> fixture.events.size() == 2);
        }
    }

    @Test
    void aPreparationFailureAfterCountingRetainsTheCauseWithoutCountingTheRetry() throws Exception {
        Fixture fixture = new Fixture(PipelineState.PAUSED);
        fixture.failMeasurement.set(true);
        fixture.desired.save(new DesiredState(PIPELINE, PipelineState.PAUSED, "rev-a"));
        try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
            ConvergenceDriver driver = fixture.driver(dispatcher);
            driver.reconcile();
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).failures() == 1);
            assertThat(fixture.scopes.current(PIPELINE)).isEmpty();
            await(() -> {
                driver.reconcile();
                return fixture.latest.read(PIPELINE).orElseThrow().failure() != null;
            });
            assertThat(fixture.latest.read(PIPELINE).orElseThrow().failure()).isEqualTo(FAILURE);
            assertThat(errorCount(fixture.latest.read(PIPELINE).orElseThrow())).isEqualTo(1);
            await(() -> fixture.events.size() == 4);
            assertThat(fixture.events).extracting(PipelineEvent::kind).containsExactly(
                    PipelineEvent.Kind.STATE_CHANGED, PipelineEvent.Kind.FAILURE,
                    PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_RESTORED);
        }
    }

    @Test
    void aFailedWriteReusesThePreparedFailureWithoutOpeningAScopeOrCountingTwice() throws Exception {
        Fixture fixture = new Fixture(PipelineState.PAUSED);
        fixture.latest.failWrite.set(true);
        fixture.desired.save(new DesiredState(PIPELINE, PipelineState.PAUSED, "rev-a"));
        try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
            ConvergenceDriver driver = fixture.driver(dispatcher);
            driver.reconcile();
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).failures() == 1);
            assertThat(fixture.scopes.current(PIPELINE)).isEmpty();
            assertThat(fixture.latest.read(PIPELINE).orElseThrow().failure()).isNull();
            await(() -> {
                driver.reconcile();
                return fixture.latest.read(PIPELINE).orElseThrow().failure() != null;
            });
            assertThat(fixture.latest.read(PIPELINE).orElseThrow().failure()).isEqualTo(FAILURE);
            assertThat(errorCount(fixture.latest.read(PIPELINE).orElseThrow())).isEqualTo(1);
            await(() -> fixture.events.size() == 4);
            assertThat(fixture.events).extracting(PipelineEvent::kind).containsExactly(
                    PipelineEvent.Kind.STATE_CHANGED, PipelineEvent.Kind.FAILURE,
                    PipelineEvent.Kind.TELEMETRY_DEGRADED, PipelineEvent.Kind.TELEMETRY_RESTORED);
        }
    }

    @Test
    void ownerLossDuringAnInFlightWriteCannotOpenLocalPublicationOnTheLateCallback() throws Exception {
        Fixture fixture = new Fixture(PipelineState.PAUSED);
        fixture.latest.blockWrite = true;
        AtomicBoolean owner = new AtomicBoolean(true);
        try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
            dispatcher.offerScopeRecovery(PIPELINE, null, null, owner::get);
            assertThat(fixture.latest.writeEntered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(fixture.scopes.current(PIPELINE)).isEmpty();
            owner.set(false);
            fixture.scopes.cancelRestoration(PIPELINE);
            fixture.latest.writeRelease.countDown();
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0);
            assertThat(fixture.scopes.current(PIPELINE)).isEmpty();
            assertThat(fixture.events).isEmpty();
        } finally {
            fixture.latest.writeRelease.countDown();
        }
    }

    @Test
    void anInvalidatedColdInheritedFailureReadCannotSaveOrUpdateWatches() throws Exception {
        for (boolean loseOwner : new boolean[] {true, false}) {
            Fixture fixture = new Fixture(PipelineState.FAILED);
            Observation before = saved(PipelineState.FAILED, FAILURE);
            fixture.latest.put(before, Optional.of(OWNER));
            fixture.latest.blockInheritedFailure = true;
            AtomicBoolean owner = new AtomicBoolean(true);
            AtomicInteger watched = new AtomicInteger();
            FrontierStallAlert alert = new FrontierStallAlert() {
                @Override public void crossed(String id, FrontierStall stall) { watched.incrementAndGet(); }
                @Override public void cleared(String id, FrontierStall stall) { watched.incrementAndGet(); }
            };
            ObservationPublisher publisher = new ObservationPublisher(fixture.state, fixture.latest,
                    id -> OptionalLong.empty(), id -> Map.of(), id -> SnapshotReading.NONE,
                    id -> Map.of(), id -> Map.of(),
                    new NestColdLayerWatch(NestColdLayerPressure.DEFAULT, NestColdLayerAlert.NONE),
                    id -> Map.of("chain", 90_000L),
                    new FrontierStallWatch(new FrontierStallPressure(java.time.Duration.ofMinutes(1)), alert),
                    id -> Map.of(), id -> Map.of(), id -> Map.of(), id -> CaptureReading.NONE,
                    id -> DeliveryReading.NONE, Clock.systemUTC());
            try (TelemetryDispatcher dispatcher = new TelemetryDispatcher(publisher, null,
                    MetricsExport.none(), fixture.scopes, TelemetryBoundaryDispatchTest.eventStore(fixture.events),
                    fixture.recovery, 1, 2)) {
                dispatcher.offerScopeRecovery(PIPELINE, null, null, owner::get);
                assertThat(fixture.latest.inheritedEntered.await(5, TimeUnit.SECONDS)).isTrue();
                assertThat(fixture.latest.reads.get()).as("resolve read, then the inherited FAILED cause read")
                        .isEqualTo(2);
                assertThat(fixture.latest.writes.get()).isZero();
                assertThat(watched.get()).isZero();
                assertThat(fixture.scopes.current(PIPELINE)).isEmpty();
                if (loseOwner) {
                    owner.set(false);
                } else {
                    fixture.scopes.cancelRestoration(PIPELINE);
                }
                fixture.latest.inheritedRelease.countDown();
                await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0);
                assertThat(fixture.latest.writes.get())
                        .as("qualification was lost before the physical save was called").isZero();
                assertThat(fixture.latest.read(PIPELINE).orElseThrow()).isSameAs(before);
                assertThat(watched.get()).as("an invalidated recovery cannot update local alert windows").isZero();
                assertThat(fixture.scopes.current(PIPELINE)).isEmpty();
                assertThat(fixture.events).isEmpty();
            } finally {
                fixture.latest.inheritedRelease.countDown();
            }
        }
    }

    private static long errorCount(Observation observation) {
        return observation.facts().stream().filter(fact -> fact.name().equals("tapstate.pipeline.errors"))
                .flatMap(fact -> fact.points().stream()).mapToLong(MetricPoint::value).sum();
    }

    private static Observation saved(PipelineState state, ObservationFailure failure) {
        MetricFact rows = MetricFact.single("tapstate.pipeline.snapshot.rows", MetricType.COUNTER, "{row}",
                MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                        MetricAttributes.TABLE_ID, "orders"), AT.minusSeconds(60), AT, 7));
        return new Observation(PIPELINE, state, Map.of(), Map.of(), Map.of(), failure, AT,
                List.of(rows));
    }

    private static void await(BooleanSupplier complete) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!complete.getAsBoolean() && System.nanoTime() - deadline < 0) {
            TimeUnit.MILLISECONDS.sleep(5);
        }
        assertThat(complete.getAsBoolean()).isTrue();
    }

    private static final class Fixture {
        private final MemoryState state = new MemoryState();
        private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
        private final MemoryLatest latest = new MemoryLatest();
        private final MemoryArtifacts artifacts = new MemoryArtifacts();
        private final MemoryGenerations generations = new MemoryGenerations();
        private final ObservationScopeRegistry scopes;
        private final List<PipelineEvent> events = new CopyOnWriteArrayList<>();
        private final AtomicInteger starts = new AtomicInteger();
        private final AtomicBoolean failMeasurement = new AtomicBoolean();
        private final ObservationPublisher publisher = new ObservationPublisher(state, latest, id -> {
            if (failMeasurement.compareAndSet(true, false)) {
                throw new IllegalStateException("native measure unavailable");
            }
            return OptionalLong.empty();
        }, id -> Map.of());
        private final ObservationScopeRecovery recovery = new ObservationScopeRecovery(
                artifacts, generations, latest, state, "cluster-a");

        private Fixture(PipelineState initial) {
            this(initial, new ObservationScopeRegistry());
        }

        private Fixture(PipelineState initial, ObservationScopeRegistry scopes) {
            this.scopes = scopes;
            state.create(PIPELINE, StateJson.of(initial), AT);
            latest.put(saved(initial, null), Optional.of(OWNER));
        }

        private TelemetryDispatcher dispatcher() {
            return new TelemetryDispatcher(publisher, null, MetricsExport.none(), scopes,
                    TelemetryBoundaryDispatchTest.eventStore(events), recovery, 2, 4);
        }

        private TelemetryDispatcher dispatcher(java.time.Duration deadline) {
            return new TelemetryDispatcher(publisher, null, MetricsExport.none(), scopes,
                    TelemetryBoundaryDispatchTest.eventStore(events), recovery, 2, 4, deadline);
        }

        private ConvergenceDriver driver(TelemetryDispatcher dispatcher) {
            LifecycleActuator actuator = new LifecycleActuator() {
                @Override public void start(String id) { starts.incrementAndGet(); }
                @Override public void pause(String id) { }
                @Override public void resume(String id) { }
                @Override public void stop(String id, boolean purgeState) { }
                @Override public Optional<Throwable> failure(String id) { return Optional.empty(); }
                @Override public boolean isCarryingAJob(String id) { return !PIPELINE.equals(id); }
            };
            return new ConvergenceDriver(new PipelineConverger(desired, state, actuator,
                    Clock.fixed(AT.plusSeconds(1), ZoneOffset.UTC)), desired, publisher, null,
                    MetricsExport.none(), () -> true, PipelineActuationOwnership.single(),
                    LifecycleWorkDispatcher.inline(), scopes, dispatcher);
        }
    }

    private static final class MemoryArtifacts implements ArtifactStore {
        private volatile String incarnation = "inc-a";
        private boolean pipeline = true;
        @Override public void saveAll(List<Resource> resources) { throw new AssertionError("unexpected write"); }
        @Override public Optional<Resource> get(String id) {
            if (!PIPELINE.equals(id)) {
                return Optional.empty();
            }
            return Optional.of(pipeline ? new PipelineResource(id, null, List.of(), null,
                    null, null, null, null) : new SourceResource(id, null, "mysql", Map.of(),
                            null, null, null, null));
        }
        @Override public List<Resource> list() { return List.of(); }
        @Override public Optional<String> pipelineIncarnationId(String id) {
            return Optional.ofNullable(incarnation);
        }
    }

    private static final class MemoryGenerations implements ExecutionGenerationStore {
        private volatile long value = 41;
        private final AtomicInteger advances = new AtomicInteger();
        @Override public Optional<WorkloadClaim> advanceUnderClaim(WorkloadClaim expected, long revision) {
            advances.incrementAndGet();
            throw new AssertionError("recovery must not advance");
        }
        @Override public OptionalLong advanceStandalone(String clusterId, String id) {
            advances.incrementAndGet();
            throw new AssertionError("recovery must not advance");
        }
        @Override public OptionalLong currentGeneration(String clusterId, String id) {
            assertThat(clusterId).isEqualTo("cluster-a");
            return value == 0 ? OptionalLong.empty() : OptionalLong.of(value);
        }
    }

    private static final class MemoryState implements StateStore {
        private final Map<String, CheckpointDoc> docs = new ConcurrentHashMap<>();
        private final AtomicBoolean failColdRead = new AtomicBoolean();
        @Override public Optional<CheckpointDoc> read(String id) {
            if (Thread.currentThread().getName().startsWith("tapstate-telemetry-latest-")
                    && failColdRead.compareAndSet(true, false)) {
                throw new IllegalStateException("cold state read unavailable");
            }
            return Optional.ofNullable(docs.get(id));
        }
        @Override public void create(String id, String json, Instant at) {
            docs.put(id, CheckpointDoc.initial(id, json, at));
        }
        @Override public synchronized CasOutcome compareAndSwap(String id, long epoch, String json, Instant at) {
            CasOutcome outcome = EpochCas.swap(docs.get(id), epoch, json, at);
            if (outcome instanceof CasOutcome.Applied applied) {
                docs.put(id, applied.next());
            }
            return outcome;
        }
        @Override public void delete(String id) { docs.remove(id); }
    }

    private static final class MemoryLatest implements ObservationStore {
        private final Map<String, Stored> rows = new ConcurrentHashMap<>();
        private final AtomicInteger writes = new AtomicInteger();
        private final AtomicInteger reads = new AtomicInteger();
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final CountDownLatch writeEntered = new CountDownLatch(1);
        private final CountDownLatch writeRelease = new CountDownLatch(1);
        private final CountDownLatch inheritedEntered = new CountDownLatch(1);
        private final CountDownLatch inheritedRelease = new CountDownLatch(1);
        private volatile boolean blockInheritedFailure;
        private final AtomicBoolean failWrite = new AtomicBoolean();
        private volatile boolean blocked;
        private volatile boolean blockWrite;
        private volatile String readerThread;
        private void block() { blocked = true; }
        private void put(Observation observation, Optional<Scope> scope) {
            rows.put(observation.pipelineId(), new Stored(observation, scope));
        }
        @Override public void save(Observation observation) { throw new AssertionError("unscoped write"); }
        @Override public synchronized boolean saveScoped(Observation observation, Scope scope) {
            if (failWrite.compareAndSet(true, false)) {
                throw new IllegalStateException("latest store unavailable");
            }
            if (blockWrite) {
                writeEntered.countDown();
                try {
                    if (!writeRelease.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("cold write was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
            }
            Stored before = rows.get(observation.pipelineId());
            if (before != null && before.scope().isPresent()) {
                Scope old = before.scope().orElseThrow();
                if (scope.executionGeneration() < old.executionGeneration()
                        || (scope.executionGeneration() == old.executionGeneration() && !scope.equals(old))) {
                    return false;
                }
            }
            put(observation, Optional.of(scope));
            writes.incrementAndGet();
            return true;
        }
        @Override public Optional<Observation> read(String id) {
            return Optional.ofNullable(rows.get(id)).map(Stored::observation);
        }
        @Override public Optional<Stored> readStored(String id) {
            int read = reads.incrementAndGet();
            readerThread = Thread.currentThread().getName();
            if (blockInheritedFailure && PIPELINE.equals(id) && read == 2) {
                inheritedEntered.countDown();
                try {
                    if (!inheritedRelease.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("inherited failure read was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
            }
            if (blocked && PIPELINE.equals(id)) {
                entered.countDown();
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) {
                        throw new AssertionError("cold read was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
            }
            return Optional.ofNullable(rows.get(id));
        }
        @Override public void delete(String id) { rows.remove(id); }
    }
}
