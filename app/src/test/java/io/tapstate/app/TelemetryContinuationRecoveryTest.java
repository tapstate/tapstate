package io.tapstate.app;

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
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.runtime.engine.Engine;
import io.tapstate.runtime.scheduler.ConvergeResult;
import io.tapstate.runtime.scheduler.LifecycleActuator;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ArtifactStore;
import io.tapstate.spi.store.ExecutionGenerationStore;
import io.tapstate.spi.store.ObservationContinuation;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Native identity is an explicit double; the real publisher, registry and bounded workers execute. */
class TelemetryContinuationRecoveryTest {
    private static final String PIPELINE = "orders";
    private static final Instant AT = Instant.parse("2026-10-01T00:00:00Z");
    private static final ObservationStore.Scope SOURCE = new ObservationStore.Scope("inc-a", 41);
    private static final ObservationStore.Scope TARGET = new ObservationStore.Scope("inc-a", 42);
    private static final StopReservation.JobIdentity JOB = new StopReservation.JobIdentity("single", 77, "boot-target");
    private static final ObservationFailure FAILURE = new ObservationFailure(
            LifecycleError.PAUSED_JOB_MISSING.code(), Map.of("pipeline", PIPELINE));

    @Test
    void anUnsubmittedAdmissionPinsItsKnownFloorBeforePublishingItsStoppedScope() {
        var scopes = new ObservationScopeRegistry();
        var store = new MemoryContinuationStore();
        MetricFact records = store.privateState.get().continuation().baselineFacts().getFirst();
        Map<String, String> durationAttributes = Map.of(MetricAttributes.PIPELINE_ID, PIPELINE, MetricAttributes.TABLE_ID, "orders");
        MetricFact duration = MetricFact.single("tapstate.pipeline.record.delivery.duration", MetricType.HISTOGRAM, "s",
                MetricPoint.distribution(durationAttributes, AT.minusSeconds(60), AT,
                        quarterSecondHistogram(7)));
        var original = new ObservationContinuation("resumed-42", SOURCE, Optional.empty(), Optional.empty(),
                List.of(records, duration), List.of());
        store.privateState.set(new ObservationStore.StoredContinuation(original,
                MemoryContinuationStore.receipt(original, "source-only")));

        var state = mock(StateStore.class);
        var desired = new InMemoryDesiredStore();
        var actuator = mock(LifecycleActuator.class);
        var artifacts = mock(ArtifactStore.class);
        var engine = mock(Engine.class);
        var authority = new AtomicReference<>(StopAuthority.standalone("single", TARGET.executionGeneration()));
        var intent = new DesiredState(PIPELINE, PipelineState.RUNNING, "rev-1");
        desired.save(intent);
        var source = new StopReservation.Source("single", SOURCE,
                new StopReservation.JobIdentity("single", 70, "boot-source"));
        var marker = new AtomicReference<>(new StopReservation(PIPELINE, original.token(), 0, 5, intent, source,
                StopReservation.Phase.SUCCESSOR_ADMITTED, StopReservation.CounterPolicy.CONTINUE, authority.get(),
                new StopReservation.Successor(TARGET, "boot-never-submitted", null), StopReservation.CURRENT_FORMAT));
        var actual = new AtomicReference<>(new CheckpointDoc(PIPELINE, StateJson.of(PipelineState.STOPPED), 5, AT));
        var actualJob = new AtomicReference<Engine.ExecutionJob>();
        when(state.supportsStopReservations()).thenReturn(true);
        when(state.readStopReservation(PIPELINE)).thenAnswer(call -> Optional.of(marker.get()));
        when(state.read(PIPELINE)).thenAnswer(call -> Optional.of(actual.get()));
        when(actuator.stopAuthority(PIPELINE)).thenAnswer(call -> Optional.of(authority.get()));
        when(artifacts.pipelineIncarnationId(PIPELINE)).thenReturn(Optional.of("inc-a"));
        when(engine.executionJob(PIPELINE)).thenAnswer(call -> Optional.ofNullable(actualJob.get()));
        when(engine.noUnfinishedJob(PIPELINE)).thenAnswer(call -> actualJob.get() == null);
        var recovery = new ObservationContinuationRecovery(scopes, store, state, desired, artifacts, actuator, engine);
        scopes.begin(PIPELINE, "inc-a", TARGET.executionGeneration());

        assertThat(recovery.prepareHandoff(PIPELINE, TARGET, () -> true)).isTrue();
        var admittedCarrier = store.privateState.get();
        assertThat(admittedCarrier.continuation().target())
                .as("the source-only floor is pinned to the admitted scope before its STOPPED publication can supersede it")
                .contains(new ObservationContinuation.Target(TARGET, Optional.empty()));
        assertThat(admittedCarrier.continuation().baselineFacts()).isEqualTo(original.baselineFacts());
        assertThat(admittedCarrier.continuation().producerStates()).isEmpty();
        assertThat(scopes.activeContinuationTarget(PIPELINE)).as("admission is not a real native Job").isEmpty();

        marker.set(new StopReservation(PIPELINE, original.token(), 0, 6, intent, source,
                StopReservation.Phase.REPLACEMENT_PENDING, StopReservation.CounterPolicy.CONTINUE, authority.get(),
                null, StopReservation.CURRENT_FORMAT));
        actual.set(new CheckpointDoc(PIPELINE, StateJson.of(PipelineState.STOPPED), 6, AT));
        assertThat(recovery.prepareHandoff(PIPELINE, TARGET, () -> true)).isTrue();
        assertThat(store.privateState.get()).as("retiring an unsubmitted slot preserves its exact carrier until the next admission")
                .isEqualTo(admittedCarrier);

        var nextScope = new ObservationStore.Scope("inc-a", 43);
        var nextJob = new StopReservation.JobIdentity("single", 88, "boot-next");
        authority.set(StopAuthority.standalone("single", nextScope.executionGeneration()));
        marker.set(new StopReservation(PIPELINE, original.token(), 0, 6, intent, source,
                StopReservation.Phase.SUCCESSOR_BOUND, StopReservation.CounterPolicy.CONTINUE, authority.get(),
                new StopReservation.Successor(nextScope, nextJob.bootId(), nextJob), StopReservation.CURRENT_FORMAT));
        actual.set(new CheckpointDoc(PIPELINE, StateJson.of(PipelineState.RUNNING), 6, AT));
        actualJob.set(new Engine.ExecutionJob(nextJob, nextScope));
        scopes.begin(PIPELINE, "inc-a", nextScope.executionGeneration());
        assertThat(recovery.prepareHandoff(PIPELINE, nextScope, () -> true)).isTrue();
        assertThat(store.privateState.get().continuation().baselineFacts()).isEqualTo(original.baselineFacts());
        for (Instant sample : List.of(AT.plusSeconds(1), AT.plusSeconds(2))) {
            var raw = new ObservationPublisher.Prepared(new Observation(PIPELINE, PipelineState.RUNNING,
                    Map.of("records.out", 2L), Map.of(), Map.of(), null, sample, List.of(
                            new MetricFact(records.name(), records.type(), records.unit(), List.of(MetricPoint.accumulated(records.points().getFirst().attributes(),
                                    AT.plusSeconds(1), sample, 2))),
                            new MetricFact(duration.name(), duration.type(), duration.unit(), List.of(MetricPoint.distribution(durationAttributes, AT.plusSeconds(1), sample,
                                    quarterSecondHistogram(2)))))),
                    false, Map.of(), Map.of(), Map.of());
            var packet = scopes.prepareContinuationPublication(raw,
                    new ObservationScopeRegistry.ActualTarget(nextScope, nextJob), () -> true).orElseThrow();
            assertThat(packet.projected().observation().facts()).filteredOn(fact -> fact.type() == MetricType.COUNTER)
                    .singleElement().satisfies(fact -> assertThat(fact.points()).singleElement().satisfies(point -> {
                        assertThat(point.value()).isEqualTo(9);
                        assertThat(point.startTime()).isEqualTo(AT.minusSeconds(60));
                    }));
            assertThat(packet.projected().observation().facts()).filteredOn(fact -> fact.type() == MetricType.HISTOGRAM)
                    .singleElement().satisfies(fact -> assertThat(fact.points()).singleElement().satisfies(point -> {
                        assertThat(point.histogram()).isEqualTo(quarterSecondHistogram(9));
                        assertThat(point.startTime()).isEqualTo(AT.minusSeconds(60));
                    }));
        }
    }

    @Test
    void aRetiredUnsubmittedSlotKeepsItsPreviouslyBoundFloorAcrossAnotherAdmission() {
        var fixture = new UnsubmittedOriginFixture(false);
        var original = fixture.store.privateState.get();

        assertThat(fixture.recovery.prepareHandoff(PIPELINE, fixture.unsubmittedScope, () -> true))
                .as("a known unbound carrier keeps the real origin from the previous bound target").isTrue();
        assertThat(fixture.store.privateState.get()).isEqualTo(original);
        assertThat(fixture.store.saveAttempts).hasValue(0);
        assertThat(fixture.scopes.activeContinuationTarget(PIPELINE)).isEmpty();
        verifyNoInteractions(fixture.engine);

        var nextScope = new ObservationStore.Scope("inc-a", 44);
        var nextJob = new StopReservation.JobIdentity("single", 88, "boot-next");
        fixture.authority.set(StopAuthority.standalone("single", nextScope.executionGeneration()));
        fixture.marker.set(new StopReservation(PIPELINE, original.continuation().token(), 0, 7, fixture.intent,
                fixture.source, StopReservation.Phase.SUCCESSOR_BOUND, StopReservation.CounterPolicy.CONTINUE,
                fixture.authority.get(), new StopReservation.Successor(nextScope, nextJob.bootId(), nextJob),
                StopReservation.CURRENT_FORMAT));
        fixture.actual.set(new CheckpointDoc(PIPELINE, StateJson.of(PipelineState.RUNNING), 7, AT));
        fixture.actualJob.set(new Engine.ExecutionJob(nextJob, nextScope));
        fixture.scopes.begin(PIPELINE, "inc-a", nextScope.executionGeneration());
        assertThat(fixture.recovery.prepareHandoff(PIPELINE, nextScope, () -> true)).isTrue();
        var retained = fixture.store.privateState.get().continuation();
        assertThat(retained.target()).contains(new ObservationContinuation.Target(nextScope, Optional.of(nextJob)));
        assertThat(retained.baselineOrigin()).isEqualTo(original.continuation().baselineOrigin());
        assertThat(retained.baselineFacts()).isEqualTo(original.continuation().baselineFacts());

        MetricFact records = retained.baselineFacts().getFirst();
        MetricFact duration = retained.baselineFacts().get(1);
        for (Instant sample : List.of(AT.plusSeconds(1), AT.plusSeconds(2))) {
            var raw = new ObservationPublisher.Prepared(new Observation(PIPELINE, PipelineState.RUNNING,
                    Map.of("records.out", 2L), Map.of(), Map.of(), null, sample, List.of(
                            new MetricFact(records.name(), records.type(), records.unit(), List.of(
                                    MetricPoint.accumulated(records.points().getFirst().attributes(), AT.plusSeconds(1), sample, 2))),
                            new MetricFact(duration.name(), duration.type(), duration.unit(), List.of(
                                    MetricPoint.distribution(duration.points().getFirst().attributes(), AT.plusSeconds(1), sample,
                                            quarterSecondHistogram(2)))))), false, Map.of(), Map.of(), Map.of());
            var packet = fixture.scopes.prepareContinuationPublication(raw,
                    new ObservationScopeRegistry.ActualTarget(nextScope, nextJob), () -> true).orElseThrow();
            assertThat(packet.projected().observation().facts()).filteredOn(fact -> fact.type() == MetricType.COUNTER)
                    .singleElement().satisfies(fact -> assertThat(fact.points()).singleElement().satisfies(point -> {
                        assertThat(point.value()).isEqualTo(9);
                        assertThat(point.startTime()).isEqualTo(AT.minusSeconds(60));
                    }));
            assertThat(packet.projected().observation().facts()).filteredOn(fact -> fact.type() == MetricType.HISTOGRAM)
                    .singleElement().satisfies(fact -> assertThat(fact.points()).singleElement().satisfies(point -> {
                        assertThat(point.histogram()).isEqualTo(quarterSecondHistogram(9));
                        assertThat(point.startTime()).isEqualTo(AT.minusSeconds(60));
                    }));
        }
    }

    @Test
    void aClaimBootAndGenerationChangeDuringTheSameExecutionReadRejectsTheOldPendingCarrier() throws Exception {
        var fixture = new UnsubmittedOriginFixture(true);
        var original = fixture.store.privateState.get().continuation();
        // This control uses the original source role so the unchanged-authority read is otherwise admissible.
        var sourceRole = new ObservationContinuation(original.token(), original.sourceScope(), original.target(),
                Optional.empty(), original.baselineFacts(), original.producerStates());
        fixture.store.privateState.set(new ObservationStore.StoredContinuation(sourceRole,
                MemoryContinuationStore.receipt(sourceRole, "guarded-source-role")));
        var expected = fixture.store.privateState.get();
        fixture.store.blockRead.set(true);
        try (var worker = Executors.newSingleThreadExecutor()) {
            var pending = worker.submit(() -> fixture.recovery.prepareHandoff(PIPELINE, fixture.unsubmittedScope, () -> true));
            try {
                assertThat(fixture.store.readEntered.await(5, TimeUnit.SECONDS)).isTrue();
                StopAuthority old = fixture.authority.get();
                StopAuthority next = StopAuthority.claimed(new WorkloadClaimFence(old.claim().key(),
                        new WorkloadOwner("owner-a", "boot-owner-a-restarted"), old.claim().claimGeneration() + 1,
                        old.executionGeneration(), old.claim().topologyRevision()));
                assertThat(next.executionGeneration()).isEqualTo(old.executionGeneration());
                assertThat(next.claim().owner().bootId()).isNotEqualTo(old.claim().owner().bootId());
                fixture.authority.set(next);
                fixture.store.readRelease.countDown();
                assertThat(pending.get(5, TimeUnit.SECONDS)).isFalse();
                assertThat(fixture.store.saveAttempts).hasValue(0);
                assertThat(fixture.store.committed).hasValue(0);
                assertThat(fixture.store.privateState.get()).isEqualTo(expected);
                assertThat(fixture.scopes.activeContinuationTarget(PIPELINE)).isEmpty();
                verifyNoInteractions(fixture.engine);
            } finally {
                fixture.store.readRelease.countDown();
            }
        }
    }

    /** Controlled native and authority doubles; all continuation selection is the real recovery and registry. */
    private static final class UnsubmittedOriginFixture {
        private final ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        private final MemoryContinuationStore store = new MemoryContinuationStore();
        private final Engine engine = mock(Engine.class);
        private final ObservationStore.Scope unsubmittedScope = new ObservationStore.Scope("inc-a", 43);
        private final DesiredState intent = new DesiredState(PIPELINE, PipelineState.RUNNING, "rev-1");
        private final StopReservation.Source source = new StopReservation.Source("single", SOURCE,
                new StopReservation.JobIdentity("single", 70, "boot-source"));
        private final AtomicReference<StopAuthority> authority = new AtomicReference<>();
        private final AtomicReference<StopReservation> marker;
        private final AtomicReference<CheckpointDoc> actual = new AtomicReference<>(
                new CheckpointDoc(PIPELINE, StateJson.of(PipelineState.STOPPED), 6, AT));
        private final AtomicReference<Engine.ExecutionJob> actualJob = new AtomicReference<>();
        private final ObservationContinuationRecovery recovery;

        private UnsubmittedOriginFixture(boolean claimed) {
            authority.set(claimed ? StopAuthority.claimed(new WorkloadClaimFence(
                    new WorkloadClaimKey("single", WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE),
                    new WorkloadOwner("owner-a", "boot-owner-a"), 5, unsubmittedScope.executionGeneration(), 7))
                    : StopAuthority.standalone("single", unsubmittedScope.executionGeneration()));
            MetricFact records = store.privateState.get().continuation().baselineFacts().getFirst();
            MetricFact duration = MetricFact.single("tapstate.pipeline.record.delivery.duration", MetricType.HISTOGRAM, "s",
                    MetricPoint.distribution(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE, MetricAttributes.TABLE_ID, "orders"),
                            AT.minusSeconds(60), AT, quarterSecondHistogram(7)));
            var floor = new ObservationContinuation("resumed-42", SOURCE,
                    Optional.of(new ObservationContinuation.Target(unsubmittedScope, Optional.empty())),
                    Optional.of(new ObservationContinuation.Target(TARGET, Optional.of(JOB))), List.of(records, duration), List.of());
            store.privateState.set(new ObservationStore.StoredContinuation(floor,
                    MemoryContinuationStore.receipt(floor, "previous-bound-floor")));
            marker = new AtomicReference<>(new StopReservation(PIPELINE, floor.token(), 0, 6, intent, source,
                    StopReservation.Phase.REPLACEMENT_PENDING, StopReservation.CounterPolicy.CONTINUE, authority.get(),
                    null, StopReservation.CURRENT_FORMAT));
            StateStore state = mock(StateStore.class);
            InMemoryDesiredStore desired = new InMemoryDesiredStore();
            desired.save(intent);
            LifecycleActuator actuator = mock(LifecycleActuator.class);
            ArtifactStore artifacts = mock(ArtifactStore.class);
            when(state.supportsStopReservations()).thenReturn(true);
            when(state.readStopReservation(PIPELINE)).thenAnswer(call -> Optional.of(marker.get()));
            when(state.read(PIPELINE)).thenAnswer(call -> Optional.of(actual.get()));
            when(actuator.stopAuthority(PIPELINE)).thenAnswer(call -> Optional.of(authority.get()));
            when(artifacts.pipelineIncarnationId(PIPELINE)).thenReturn(Optional.of("inc-a"));
            when(engine.executionJob(PIPELINE)).thenAnswer(call -> Optional.ofNullable(actualJob.get()));
            when(engine.noUnfinishedJob(PIPELINE)).thenAnswer(call -> actualJob.get() == null);
            recovery = new ObservationContinuationRecovery(scopes, store, state, desired, artifacts, actuator, engine);
            scopes.begin(PIPELINE, "inc-a", unsubmittedScope.executionGeneration());
        }
    }

    private static HistogramValue quarterSecondHistogram(long count) {
        List<Long> buckets = new ArrayList<>(Collections.nCopies(HistogramBounds.RECORD_DELIVERY_DURATION.buckets(), 0L));
        buckets.set(5, count);
        return new HistogramValue(count, count * 0.25, HistogramBounds.RECORD_DELIVERY_DURATION.bounds(), buckets);
    }

    @Test
    void aConcludedBoundTargetWithoutNativeHistoryKeepsKnownNineAndAcknowledgesTheExactReceipt() throws Exception {
        for (PipelineState actual : List.of(PipelineState.FAILED, PipelineState.COMPLETED)) {
            HistoricalFixture fixture = new HistoricalFixture(actual);
            try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
                dispatcher.offerScopeRecovery(PIPELINE, fixture.result(),
                        actual == PipelineState.FAILED ? FAILURE : null, () -> true);
                await(() -> fixture.store.committed.get() == 1);
                await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0);
                assertThat(fixture.scopes.current(PIPELINE)).contains(TARGET);
                Observation finalProjection = fixture.store.current.get().observation();
                assertThat(finalProjection.state()).isEqualTo(actual);
                assertThat(finalProjection.facts()).filteredOn(fact -> fact.name().equals("tapstate.pipeline.records"))
                        .singleElement().satisfies(fact -> assertThat(fact.points()).singleElement().satisfies(point -> {
                            assertThat(point.value()).isEqualTo(9);
                            assertThat(point.startTime()).isEqualTo(AT.minusSeconds(60));
                        }));
                var accepted = fixture.store.privateState.get();
                assertThat(accepted.continuation().target()).contains(new ObservationContinuation.Target(TARGET, Optional.of(JOB)));
                assertThat(accepted.continuation().producerStates()).filteredOn(producer -> producer.name().equals("tapstate.pipeline.records"))
                        .singleElement().satisfies(producer -> {
                            assertThat(producer.nativeStart()).isEqualTo(fixture.nativeStart);
                            assertThat(producer.published()).singleElement().satisfies(point -> assertThat(point.value()).isEqualTo(9));
                        });
                assertThat(fixture.scopes.durableReceipt(PIPELINE, fixture.marker.handoffIdentity())).contains(accepted.receipt());
                assertThat(fixture.actualJob.get()).isNull();
                verifyNoInteractions(fixture.generations);
            }
        }
    }

    @Test
    void aChangedOwnerOrForeignLiveJobCannotRestoreAConcludedHistoricalTarget() throws Exception {
        for (boolean changeOwner : List.of(true, false)) {
            HistoricalFixture fixture = new HistoricalFixture(PipelineState.FAILED);
            var saved = fixture.store.privateState.get();
            fixture.store.blockRead.set(true);
            try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
                dispatcher.offerScopeRecovery(PIPELINE, fixture.result(), FAILURE, () -> true);
                assertThat(fixture.store.readEntered.await(5, TimeUnit.SECONDS)).isTrue();
                if (changeOwner) {
                    fixture.authority.set(HistoricalFixture.authority("owner-b", 2));
                } else {
                    fixture.actualJob.set(new Engine.ExecutionJob(new StopReservation.JobIdentity("single", 78, "boot-foreign"), TARGET));
                }
                fixture.store.readRelease.countDown();
                await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0);
                assertThat(fixture.scopes.current(PIPELINE)).isEmpty();
                assertThat(fixture.store.committed.get()).isZero();
                assertThat(fixture.scopes.durableReceipt(PIPELINE, fixture.marker.handoffIdentity())).isEmpty();
                assertThat(fixture.store.privateState.get()).isEqualTo(saved);
                assertThat(fixture.events).noneMatch(event -> event.kind() == PipelineEvent.Kind.FAILURE
                        || event.kind() == PipelineEvent.Kind.STATE_CHANGED);
            } finally { fixture.store.readRelease.countDown(); }
        }
    }

    @Test
    void anAdmissionRestartCannotRevertTheKnownPreviousTargetNineToTheOriginalBaseSeven() {
        var scopes = new ObservationScopeRegistry();
        var store = new MemoryContinuationStore();
        MetricFact base = store.privateState.get().continuation().baselineFacts().getFirst();
        MetricPoint nine = MetricPoint.accumulated(base.points().getFirst().attributes(), AT.minusSeconds(60), AT, 9);
        var previousState = new ObservationContinuation.ProducerState(base.name(), base.type(), base.unit(), "out", "",
                AT.minusSeconds(10), List.of(), List.of(nine));
        var previous = new ObservationContinuation("resumed-42", SOURCE,
                Optional.of(new ObservationContinuation.Target(TARGET, Optional.of(JOB))), Optional.empty(), List.of(base),
                List.of(previousState));
        store.privateState.set(new ObservationStore.StoredContinuation(previous,
                MemoryContinuationStore.receipt(previous, "previous-nine")));
        store.current.set(new ObservationStore.Stored(new Observation(PIPELINE, PipelineState.RUNNING,
                Map.of("records.out", 9L), Map.of(), Map.of(), null, AT,
                List.of(new MetricFact(base.name(), base.type(), base.unit(), List.of(nine)))), Optional.of(TARGET)));

        var state = mock(StateStore.class);
        var desired = new InMemoryDesiredStore();
        var actuator = mock(LifecycleActuator.class);
        var artifacts = mock(ArtifactStore.class);
        var engine = mock(Engine.class);
        var newScope = new ObservationStore.Scope("inc-a", 43);
        var newJob = new StopReservation.JobIdentity("single", 88, "boot-next");
        var authority = StopAuthority.standalone("single", 43);
        var intent = new DesiredState(PIPELINE, PipelineState.RUNNING, "rev-1");
        desired.save(intent);
        var marker = new StopReservation(PIPELINE, "resumed-42", 0, 5, intent,
                new StopReservation.Source("single", SOURCE, new StopReservation.JobIdentity("single", 70, "boot-source")),
                StopReservation.Phase.SUCCESSOR_BOUND, StopReservation.CounterPolicy.CONTINUE, authority,
                new StopReservation.Successor(newScope, "boot-next", newJob), StopReservation.CURRENT_FORMAT);
        when(state.supportsStopReservations()).thenReturn(true);
        when(state.readStopReservation(PIPELINE)).thenReturn(Optional.of(marker));
        when(state.read(PIPELINE)).thenReturn(Optional.of(new CheckpointDoc(PIPELINE, StateJson.of(PipelineState.RUNNING), 5, AT)));
        when(actuator.stopAuthority(PIPELINE)).thenReturn(Optional.of(authority));
        when(artifacts.pipelineIncarnationId(PIPELINE)).thenReturn(Optional.of("inc-a"));
        when(engine.executionJob(PIPELINE)).thenReturn(Optional.of(new Engine.ExecutionJob(newJob, newScope)));
        var recovery = new ObservationContinuationRecovery(scopes, store, state, desired, artifacts, actuator, engine);

        // The durable state left by a failed pending-floor attach is the bound previous target's nine.
        // Its slot was retired and the next admission completed; the original base of seven is incomplete.
        boolean ready = recovery.prepareHandoff(PIPELINE, newScope, () -> true);
        if (ready) {
            MetricPoint fresh = MetricPoint.accumulated(base.points().getFirst().attributes(), AT.plusSeconds(1), AT.plusSeconds(1), 2);
            var raw = new ObservationPublisher.Prepared(new Observation(PIPELINE, PipelineState.RUNNING,
                    Map.of("records.out", 2L), Map.of(), Map.of(), null, AT.plusSeconds(1),
                    List.of(new MetricFact(base.name(), base.type(), base.unit(), List.of(fresh)))), false,
                    Map.of(), Map.of(), Map.of());
            var packet = scopes.prepareContinuationPublication(raw,
                    new ObservationScopeRegistry.ActualTarget(newScope, newJob), () -> true).orElseThrow();
            var known = packet.projected().observation().facts().stream()
                    .filter(fact -> fact.name().equals("tapstate.pipeline.records")).flatMap(fact -> fact.points().stream())
                    .filter(point -> "out".equals(point.attributes().get(MetricAttributes.DIRECTION))).toList();
            assertThat(known.isEmpty() || known.stream().mapToLong(MetricPoint::value).sum() == 11)
                    .as("recovery preserves previous logical nine, or remains unknown; it cannot fall back to seven plus two")
                    .isTrue();
        }
        var retained = store.privateState.get().continuation();
        long retainedKnown = retained.target().filter(target -> target.scope().equals(TARGET)).isPresent()
                ? retained.producerStates().stream().flatMap(producer -> producer.published().stream()).mapToLong(MetricPoint::value).sum()
                : retained.baselineFacts().stream().flatMap(fact -> fact.points().stream()).mapToLong(MetricPoint::value).sum();
        assertThat(retainedKnown).as("a known durable prefix is not replaced with the incomplete original floor").isGreaterThanOrEqualTo(9);
    }

    @Test
    void aQualifiedColdFailureReachesTheHealthyEventSinkEvenWhenLatestRejectsAfterAdoption() throws Exception {
        Fixture fixture = new Fixture();
        fixture.store.rejectNext.set(true);
        var checkpoint = fixture.state.read(PIPELINE).orElseThrow();
        var failed = new ConvergeResult(io.tapstate.runtime.scheduler.ConvergeStatus.FAILED,
                Optional.of(checkpoint), Optional.empty(), Optional.of(PipelineState.RUNNING));
        try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
            dispatcher.offerScopeRecovery(PIPELINE, failed, FAILURE, () -> true);
            assertThat(fixture.store.rejected.await(5, TimeUnit.SECONDS)).isTrue();
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0);
            assertThat(fixture.scopes.current(PIPELINE)).contains(TARGET);
            assertThat(fixture.store.committed.get()).isZero();
            await(() -> fixture.events.stream().filter(event -> event.kind() == PipelineEvent.Kind.FAILURE
                    || event.kind() == PipelineEvent.Kind.STATE_CHANGED).count() == 2);
            assertThat(fixture.events).filteredOn(event -> event.kind() == PipelineEvent.Kind.FAILURE)
                    .as("latest refusal cannot discard an already qualified cold failure")
                    .singleElement().satisfies(event -> {
                        assertThat(event.failure()).isEqualTo(FAILURE);
                        assertThat(event.pipelineIncarnationId()).isEqualTo("inc-a");
                        assertThat(event.executionGeneration()).isEqualTo(42);
                    });
            assertThat(fixture.events).filteredOn(event -> event.kind() == PipelineEvent.Kind.STATE_CHANGED)
                    .singleElement().satisfies(event -> {
                        assertThat(event.beforeState()).isEqualTo(PipelineState.RUNNING);
                        assertThat(event.afterState()).isEqualTo(PipelineState.FAILED);
                    });

            dispatcher.offerPreparation(PIPELINE, null, TARGET, () -> true);
            await(() -> fixture.store.committed.get() == 1);
            assertThat(errorCount(fixture.store.current.get().observation())).isEqualTo(1);
            assertThat(fixture.events).filteredOn(event -> event.kind() == PipelineEvent.Kind.FAILURE).hasSize(1);
        }
    }

    @Test
    void aChangedActualJobWhilePrivateReadIsBlockedCannotInstallTheOldTargetOrReplayItsSignals() throws Exception {
        Fixture fixture = new Fixture();
        fixture.store.blockRead.set(true);
        var checkpoint = fixture.state.read(PIPELINE).orElseThrow();
        var failed = new ConvergeResult(io.tapstate.runtime.scheduler.ConvergeStatus.FAILED,
                Optional.of(checkpoint), Optional.empty(), Optional.of(PipelineState.RUNNING));
        try (TelemetryDispatcher dispatcher = fixture.dispatcher()) {
            dispatcher.offerScopeRecovery(PIPELINE, failed, FAILURE, () -> true);
            assertThat(fixture.store.readEntered.await(5, TimeUnit.SECONDS)).isTrue();
            fixture.actualJob.set(new Engine.ExecutionJob(new StopReservation.JobIdentity("single", 78, "boot-new"),
                    new ObservationStore.Scope("inc-a", 43)));
            fixture.store.readRelease.countDown();
            await(() -> dispatcher.health().get(TelemetryDispatcher.Sink.LATEST).inFlight() == 0);
            assertThat(fixture.scopes.current(PIPELINE)).isEmpty();
            assertThat(fixture.store.committed.get()).isZero();
            assertThat(fixture.events).noneMatch(event -> event.kind() == PipelineEvent.Kind.FAILURE
                    || event.kind() == PipelineEvent.Kind.STATE_CHANGED);
        } finally { fixture.store.readRelease.countDown(); }
    }

    private static long errorCount(Observation observation) {
        return observation.facts().stream().filter(fact -> fact.name().equals("tapstate.pipeline.errors"))
                .flatMap(fact -> fact.points().stream()).filter(point -> FAILURE.code().equals(point.attributes().get(MetricAttributes.CODE)))
                .mapToLong(MetricPoint::value).sum();
    }

    private static void await(BooleanSupplier done) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!done.getAsBoolean() && System.nanoTime() - deadline < 0) { TimeUnit.MILLISECONDS.sleep(5); }
        assertThat(done.getAsBoolean()).isTrue();
    }

    private static final class HistoricalFixture {
        private final StateStore state = mock(StateStore.class);
        private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
        private final ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        private final MemoryContinuationStore store = new MemoryContinuationStore();
        private final List<PipelineEvent> events = new CopyOnWriteArrayList<>();
        private final Engine engine = mock(Engine.class);
        private final LifecycleActuator actuator = mock(LifecycleActuator.class);
        private final ArtifactStore artifacts = mock(ArtifactStore.class);
        private final ExecutionGenerationStore generations = mock(ExecutionGenerationStore.class);
        private final AtomicReference<Engine.ExecutionJob> actualJob = new AtomicReference<>();
        private final AtomicReference<StopAuthority> authority = new AtomicReference<>(authority("owner-a", 1));
        private final Instant nativeStart = AT.minusSeconds(10).plusNanos(123);
        private final CheckpointDoc checkpoint;
        private final StopReservation marker;

        private HistoricalFixture(PipelineState actual) {
            var intent = new DesiredState(PIPELINE, PipelineState.RUNNING, "rev-1");
            desired.save(intent);
            checkpoint = new CheckpointDoc(PIPELINE, StateJson.of(actual), 6, AT);
            marker = new StopReservation(PIPELINE, "resumed-42", 0, 5, intent,
                    new StopReservation.Source("single", SOURCE, new StopReservation.JobIdentity("single", 70, "boot-source")),
                    StopReservation.Phase.SUCCESSOR_BOUND, StopReservation.CounterPolicy.CONTINUE, authority.get(),
                    new StopReservation.Successor(TARGET, JOB.bootId(), JOB), StopReservation.CURRENT_FORMAT);
            when(state.supportsStopReservations()).thenReturn(true);
            when(state.readStopReservation(PIPELINE)).thenReturn(Optional.of(marker));
            when(state.read(PIPELINE)).thenReturn(Optional.of(checkpoint));
            when(actuator.stopAuthority(PIPELINE)).thenAnswer(invocation -> Optional.of(authority.get()));
            when(artifacts.pipelineIncarnationId(PIPELINE)).thenReturn(Optional.of("inc-a"));
            when(artifacts.get(PIPELINE)).thenReturn(Optional.of(new PipelineResource(PIPELINE, null, List.of(),
                    null, null, null, null, null)));
            when(engine.executionJob(PIPELINE)).thenAnswer(invocation -> Optional.ofNullable(actualJob.get()));
            when(engine.noUnfinishedJob(PIPELINE)).thenAnswer(invocation -> actualJob.get() == null);
            when(generations.currentGeneration("single", PIPELINE)).thenReturn(OptionalLong.of(42));
            MetricFact base = store.privateState.get().continuation().baselineFacts().getFirst();
            MetricPoint nine = MetricPoint.accumulated(base.points().getFirst().attributes(), AT.minusSeconds(60), AT, 9);
            var producer = new ObservationContinuation.ProducerState(base.name(), base.type(), base.unit(), "out", "",
                    nativeStart, List.of(), List.of(nine));
            var continuation = new ObservationContinuation(marker.token(), SOURCE,
                    Optional.of(new ObservationContinuation.Target(TARGET, Optional.of(JOB))), Optional.empty(), List.of(base), List.of(producer));
            store.privateState.set(new ObservationStore.StoredContinuation(continuation,
                    MemoryContinuationStore.receipt(continuation, "concluded-nine")));
        }

        private ConvergeResult result() {
            return new ConvergeResult(StateJson.parse(checkpoint.stateJson()) == PipelineState.FAILED
                    ? io.tapstate.runtime.scheduler.ConvergeStatus.FAILED : io.tapstate.runtime.scheduler.ConvergeStatus.CONVERGED,
                    Optional.of(checkpoint), Optional.empty(), Optional.of(PipelineState.RUNNING));
        }

        private TelemetryDispatcher dispatcher() {
            // Native history is gone: metric ports return absence and the final known facts come only from the bound receipt.
            var publisher = new ObservationPublisher(state, store, id -> OptionalLong.empty(), id -> Map.of());
            var scopeRecovery = new ObservationScopeRecovery(artifacts, generations, store, state, "single");
            var continuationRecovery = new ObservationContinuationRecovery(scopes, store, state, desired, artifacts, actuator, engine);
            return new TelemetryDispatcher(publisher, null, MetricsExport.none(), scopes,
                    TelemetryBoundaryDispatchTest.eventStore(events), scopeRecovery, continuationRecovery, 1, 4, Duration.ofSeconds(30));
        }

        private static StopAuthority authority(String node, long claimGeneration) {
            return StopAuthority.claimed(new WorkloadClaimFence(new WorkloadClaimKey("single", WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE),
                    new WorkloadOwner(node, "boot-" + node), claimGeneration, TARGET.executionGeneration(), 1));
        }
    }

    private static final class Fixture {
        private final InMemoryStateStore state = new InMemoryStateStore();
        private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
        private final ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        private final MemoryContinuationStore store = new MemoryContinuationStore();
        private final List<PipelineEvent> events = new CopyOnWriteArrayList<>();
        private final Engine engine = mock(Engine.class);
        private final LifecycleActuator actuator = mock(LifecycleActuator.class);
        private final ArtifactStore artifacts = mock(ArtifactStore.class);
        private final ExecutionGenerationStore generations = mock(ExecutionGenerationStore.class);
        private final AtomicReference<Engine.ExecutionJob> actualJob = new AtomicReference<>(new Engine.ExecutionJob(JOB, TARGET));
        private final ObservationPublisher publisher;
        private final ObservationScopeRecovery scopeRecovery;
        private final ObservationContinuationRecovery continuationRecovery;

        private Fixture() {
            state.create(PIPELINE, StateJson.of(PipelineState.FAILED), AT);
            desired.save(new DesiredState(PIPELINE, PipelineState.RUNNING, "rev-1"));
            when(engine.executionJob(PIPELINE)).thenAnswer(invocation -> Optional.ofNullable(actualJob.get()));
            when(actuator.stopAuthority(PIPELINE)).thenReturn(Optional.of(StopAuthority.standalone("single", 42)));
            when(artifacts.pipelineIncarnationId(PIPELINE)).thenReturn(Optional.of("inc-a"));
            when(artifacts.get(PIPELINE)).thenReturn(Optional.of(new PipelineResource(PIPELINE, null, List.of(),
                    null, null, null, null, null)));
            when(generations.currentGeneration("single", PIPELINE)).thenReturn(OptionalLong.of(42));
            publisher = new ObservationPublisher(state, store, id -> OptionalLong.empty(), id -> Map.of());
            scopeRecovery = new ObservationScopeRecovery(artifacts, generations, store, state, "single");
            continuationRecovery = new ObservationContinuationRecovery(scopes, store, state, desired, artifacts, actuator, engine);
        }

        private TelemetryDispatcher dispatcher() {
            return new TelemetryDispatcher(publisher, null, MetricsExport.none(), scopes,
                    TelemetryBoundaryDispatchTest.eventStore(events), scopeRecovery, continuationRecovery, 1, 4, Duration.ofSeconds(30));
        }
    }

    private static final class MemoryContinuationStore implements ObservationStore {
        private final AtomicBoolean rejectNext = new AtomicBoolean();
        private final AtomicBoolean blockRead = new AtomicBoolean();
        private final CountDownLatch rejected = new CountDownLatch(1);
        private final CountDownLatch readEntered = new CountDownLatch(1);
        private final CountDownLatch readRelease = new CountDownLatch(1);
        private final AtomicInteger committed = new AtomicInteger();
        private final AtomicInteger saveAttempts = new AtomicInteger();
        private final AtomicInteger revisions = new AtomicInteger();
        private final AtomicReference<Stored> current = new AtomicReference<>();
        private final AtomicReference<StoredContinuation> privateState;

        private MemoryContinuationStore() {
            MetricFact floor = MetricFact.single("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                    MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE, MetricAttributes.TABLE_ID, "orders",
                            MetricAttributes.DIRECTION, "out", MetricAttributes.OP, "insert"), AT.minusSeconds(60), AT, 7));
            var continuation = new ObservationContinuation("resumed-42", SOURCE,
                    Optional.of(new ObservationContinuation.Target(TARGET, Optional.of(JOB))), Optional.empty(), List.of(floor), List.of());
            privateState = new AtomicReference<>(new StoredContinuation(continuation, receipt(continuation, "initial")));
        }

        @Override public void save(Observation observation) { throw new AssertionError("scoped write required"); }
        @Override public Optional<Observation> read(String id) { return readStored(id).map(Stored::observation); }
        @Override public Optional<Stored> readStored(String id) { return Optional.ofNullable(current.get()); }
        @Override public Optional<StoredContinuation> readContinuation(String id) {
            if (blockRead.compareAndSet(true, false)) {
                readEntered.countDown();
                try { assertThat(readRelease.await(5, TimeUnit.SECONDS)).isTrue(); }
                catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
            }
            return Optional.of(privateState.get());
        }
        @Override public synchronized Optional<ContinuationReceipt> saveContinuation(String id, StopReservation marker,
                Optional<ContinuationReceipt> expected, ObservationContinuation next) {
            saveAttempts.incrementAndGet();
            if (!expected.equals(Optional.of(privateState.get().receipt()))) { return Optional.empty(); }
            assertThat(marker.pipelineId()).isEqualTo(id);
            assertThat(next.token()).isEqualTo(marker.token());
            var accepted = new StoredContinuation(next, receipt(next, "attached-" + revisions.incrementAndGet()));
            privateState.set(accepted);
            return Optional.of(accepted.receipt());
        }
        @Override public synchronized PublicationResult saveScoped(Observation observation, Scope scope, ContinuationWrite write) {
            if (rejectNext.compareAndSet(true, false)) {
                rejected.countDown();
                return new PublicationResult(false, Optional.empty());
            }
            assertThat(write).isInstanceOf(ContinuationWrite.Store.class);
            ContinuationWrite.Store replacement = (ContinuationWrite.Store) write;
            assertThat(replacement.expectedReceipt()).contains(privateState.get().receipt());
            assertThat(replacement.next().target().orElseThrow().scope()).isEqualTo(scope);
            var accepted = new StoredContinuation(replacement.next(), receipt(replacement.next(), "revision-" + revisions.incrementAndGet()));
            privateState.set(accepted);
            current.set(new Stored(observation, Optional.of(scope)));
            committed.incrementAndGet();
            return new PublicationResult(true, Optional.of(accepted.receipt()));
        }
        @Override public void delete(String id) { throw new AssertionError("unexpected delete"); }
        private static ContinuationReceipt receipt(ObservationContinuation value, String revision) {
            return new ContinuationReceipt(PIPELINE, revision, "a".repeat(64), 1, value.token(), value.sourceScope(),
                    value.target(), value.baselineOrigin(), value.knownBaseline());
        }
    }
}
