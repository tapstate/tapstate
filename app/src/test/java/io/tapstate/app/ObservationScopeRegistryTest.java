package io.tapstate.app;

import io.tapstate.core.lifecycle.MetricAttributes;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.MetricFact;
import io.tapstate.core.lifecycle.MetricPoint;
import io.tapstate.core.lifecycle.MetricType;
import io.tapstate.core.lifecycle.Observation;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.ObservationContinuation;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.HandoffIdentity;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.BiConsumer;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

class ObservationScopeRegistryTest {

    @Test
    void aPinnedBoundSourcesFrozenBaseRemainsKnownBeforeItsFirstNativeCheckpoint() {
        for (boolean measured : List.of(false, true)) {
            var scopes = new ObservationScopeRegistry();
            var key = continuationKey(42);
            var previous = continuationTarget(42, 77);
            var facts = frame(START, START, 7).observation().facts();
            List<ObservationContinuation.ProducerState> producers = measured
                    ? List.of(new ObservationContinuation.ProducerState(facts.getFirst().name(), MetricType.COUNTER,
                            "{row}", "", "", START.plusSeconds(1), List.of(),
                            frame(START.plusSeconds(1), START, 9).observation().facts().getFirst().points())) : List.of();
            var stored = new ObservationContinuation(key.token(), key.sourceScope(), Optional.of(
                    new ObservationContinuation.Target(previous.scope(), Optional.of(previous.job()))), Optional.empty(), facts, producers);
            var source = scopes.beginSourceContinuation(PIPELINE, key, Optional.of(previous), () -> true).orElseThrow();
            var frozen = scopes.prepareSourceContinuation(source, Optional.empty(), Optional.of(stored),
                    ObservationScopeRegistry.SourceReadStatus.READABLE);
            assertThat(frozen.status()).isEqualTo(ObservationScopeRegistry.PreparationStatus.KNOWN);
            assertThat(frozen.snapshot().orElseThrow().baselineFacts()).singleElement().satisfies(fact -> {
                assertThat(fact.points().getFirst().value()).isEqualTo(measured ? 9 : 7);
                assertThat(fact.points().getFirst().startTime()).isEqualTo(START);
            });
            assertThat(frozen.snapshot().orElseThrow().baselineOrigin()).contains(previous);
            assertThat(frozen.snapshot().orElseThrow().continuation().producerStates()).isEmpty();
        }
    }

    @Test
    void receiptConflictRefreshUsesTheNewerSameOwnerNativeEpochWithoutRegressingTwelveToNine() {
        var key = continuationKey(42);
        var target = continuationTarget(42, 77);
        var local = knownContinuationRegistry();
        var first = local.prepareContinuationPublication(frame(START.plusSeconds(1), START.plusSeconds(1), 2), target,
                () -> true).orElseThrow();
        var firstReceipt = continuationReceipt(first.snapshot().orElseThrow(), "r1");
        assertThat(local.publicationAccepted(first.ticket(), firstReceipt)).isTrue();
        var acceptedWriter = new ObservationScopeRegistry();
        var initial = acceptedWriter.beginTargetContinuation(PIPELINE, key, target, () -> true).orElseThrow();
        acceptedWriter.adoptTargetContinuation(initial, first.snapshot(), Optional.empty());
        var newer = acceptedWriter.prepareContinuationPublication(frame(START.plusSeconds(2), START.plusSeconds(2), 3), target,
                () -> true).orElseThrow();
        assertThat(counter(newer.projected())).isEqualTo(12);
        var newerReceipt = continuationReceipt(newer.snapshot().orElseThrow(), "r2");

        // This is the read performed after the current writer's expected private receipt was refused.
        var refreshed = local.beginTargetContinuation(PIPELINE, key, target, () -> true).orElseThrow();
        assertThat(local.adoptTargetContinuation(refreshed, newer.snapshot(), Optional.empty()).status())
                .isEqualTo(ObservationScopeRegistry.PreparationStatus.KNOWN);
        assertThat(local.continuationRead(refreshed,
                new ObservationStore.StoredContinuation(newer.snapshot().orElseThrow(), newerReceipt))).isTrue();
        var late = local.prepareContinuationPublication(frame(START.plusSeconds(3), START.plusSeconds(1), 2), target,
                () -> true).orElseThrow();
        assertThat(late.expectedReceipt()).contains(newerReceipt);
        assertThat(counter(late.projected())).as("the accepted T2 checkpoint fences a later-observed T1 reading")
                .isEqualTo(12);
        assertThat(late.snapshot().orElseThrow().producerStates()).singleElement()
                .satisfies(state -> assertThat(state.nativeStart()).isEqualTo(START.plusSeconds(2)));
    }

    @Test
    void sourceFreezingOmitsUnknownHistogramStartsWhilePreservingKnownCounterAndHistogramPoints() {
        var scopes = new ObservationScopeRegistry();
        var key = continuationKey(41);
        var ticket = scopes.beginSourceContinuation(PIPELINE, key, () -> true).orElseThrow();
        var buckets = new ArrayList<Long>(java.util.Collections.nCopies(HistogramBounds.RECORD_DELIVERY_DURATION.buckets(), 0L));
        buckets.set(0, 3L);
        MetricPoint unknown = MetricPoint.distribution(TABLE, null, START,
                HistogramBounds.RECORD_DELIVERY_DURATION.value(3, 3.0, buckets));
        var knownBuckets = new ArrayList<>(buckets);
        knownBuckets.set(0, 4L);
        MetricPoint known = MetricPoint.distribution(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                MetricAttributes.TABLE_ID, "known-table"), START.minusSeconds(60), START,
                HistogramBounds.RECORD_DELIVERY_DURATION.value(4, 4.0, knownBuckets));
        MetricFact histogram = new MetricFact("tapstate.pipeline.record.delivery.duration", MetricType.HISTOGRAM,
                HistogramBounds.UNIT, List.of(unknown, known));
        var counterFact = frame(START, START.minusSeconds(60), 7).observation().facts().getFirst();
        var saved = new Observation(PIPELINE, PipelineState.PAUSED, Map.of(), Map.of(), Map.of(), null, START,
                List.of(counterFact, histogram));
        var result = scopes.prepareSourceContinuation(ticket,
                Optional.of(new ObservationStore.Stored(saved, Optional.of(key.sourceScope()))), Optional.empty(),
                ObservationScopeRegistry.SourceReadStatus.READABLE);
        assertThat(result.status()).isEqualTo(ObservationScopeRegistry.PreparationStatus.KNOWN);
        var snapshot = result.snapshot().orElseThrow();
        assertThatCode(snapshot::continuation).as("unknown starts are omitted before constructing a private carrier")
                .doesNotThrowAnyException();
        var retained = snapshot.continuation();
        assertThat(retained.baselineFacts()).filteredOn(fact -> fact.name().equals("tapstate.pipeline.snapshot.rows"))
                .singleElement().satisfies(fact -> assertThat(fact.points().getFirst().value()).isEqualTo(7));
        assertThat(retained.baselineFacts()).filteredOn(fact -> fact.type() == MetricType.HISTOGRAM)
                .singleElement().satisfies(fact -> {
                    assertThat(fact.points()).containsExactly(known);
                    assertThat(fact.points().getFirst().histogram().count()).isEqualTo(4);
                });
        assertThat(scopes.current(PIPELINE)).isEmpty();
    }

    @Test
    void aQualifiedDurableSourceSurvivesAdmissionWithoutInstallingItsOldNativeScope() {
        var scopes = new ObservationScopeRegistry();
        var source = new ObservationStore.Scope("inc-a", 41);
        var key = continuationKey(41);
        var ticket = scopes.beginSourceContinuation(PIPELINE, key, () -> true).orElseThrow();
        var prepared = scopes.prepareSourceContinuation(ticket,
                Optional.of(new ObservationStore.Stored(frame(START, START.minusSeconds(60), 7).observation(), Optional.of(source))),
                Optional.empty(), ObservationScopeRegistry.SourceReadStatus.READABLE);
        assertThat(prepared.status()).isEqualTo(ObservationScopeRegistry.PreparationStatus.KNOWN);
        assertThat(scopes.current(PIPELINE)).isEmpty();
        assertThat(prepared.snapshot().orElseThrow().continuation().target()).isEmpty();
        scopes.begin(PIPELINE, "inc-a", 42);
        var target = continuationTarget(42, 77);
        var bound = scopes.beginTargetContinuation(PIPELINE, continuationKey(42), target, () -> true).orElseThrow();
        assertThat(scopes.adoptTargetContinuation(bound, Optional.empty(), Optional.empty()).status())
                .isEqualTo(ObservationScopeRegistry.PreparationStatus.KNOWN);
        var packet = scopes.prepareContinuationPublication(frame(START.plusSeconds(1), START.plusSeconds(1), 2), target, () -> true)
                .orElseThrow();
        assertThat(counter(packet.projected())).isEqualTo(9);
        assertThat(start(packet.projected())).isEqualTo(START.minusSeconds(60));
        assertThat(packet.snapshot().orElseThrow().producerStates()).singleElement()
                .satisfies(state -> assertThat(state.nativeStart()).isEqualTo(START.plusSeconds(1)));
    }

    @Test
    void coldAdoptionRestoresPrivateBaseAndEpochInsteadOfAddingPublishedNineAgain() {
        var live = knownContinuationRegistry();
        var target = continuationTarget(42, 77);
        Instant nativeStart = START.plusSeconds(1).plusNanos(123456789);
        var first = live.prepareContinuationPublication(frame(nativeStart, nativeStart, 2), target, () -> true).orElseThrow();
        assertThat(counter(first.projected())).isEqualTo(9);
        var cold = new ObservationScopeRegistry();
        var ticket = cold.beginTargetContinuation(PIPELINE, continuationKey(42), target, () -> true).orElseThrow();
        assertThat(cold.adoptTargetContinuation(ticket, first.snapshot(), Optional.empty()).status())
                .isEqualTo(ObservationScopeRegistry.PreparationStatus.KNOWN);
        var same = cold.prepareContinuationPublication(frame(nativeStart.plusSeconds(1), nativeStart, 2), target, () -> true)
                .orElseThrow();
        assertThat(counter(same.projected())).isEqualTo(9);
        assertThat(start(same.projected())).isEqualTo(START);
        var second = cold.prepareContinuationPublication(frame(nativeStart.plusSeconds(2), nativeStart.plusSeconds(2), 3), target,
                () -> true).orElseThrow();
        assertThat(counter(second.projected())).isEqualTo(12);
    }

    @Test
    void aReadableUnknownCarrierAfterHandoffRetirementProvidesProofWithoutInventingZero() {
        var scopes = new ObservationScopeRegistry();
        var target = continuationTarget(42, 77);
        var key = continuationKey(42);
        var unknown = new ObservationContinuation(key.token(), key.sourceScope(), Optional.of(
                new ObservationContinuation.Target(target.scope(), Optional.of(target.job()))), Optional.empty(), List.of(), List.of());
        var ticket = scopes.beginTargetContinuation(PIPELINE, key, target, () -> true).orElseThrow();
        assertThat(scopes.adoptTargetContinuation(ticket, Optional.of(unknown), Optional.empty()).status())
                .isEqualTo(ObservationScopeRegistry.PreparationStatus.UNKNOWN);
        var actualReceipt = continuationReceipt(unknown, "unknown-readable");
        assertThat(scopes.continuationRead(ticket, new ObservationStore.StoredContinuation(unknown, actualReceipt))).isTrue();
        assertThat(scopes.durableReceipt(PIPELINE, handoff(key, target))).contains(actualReceipt);
        var packet = scopes.prepareContinuationPublication(frame(START.plusSeconds(1), START.plusSeconds(1), 2), target, () -> true)
                .orElseThrow();
        assertThat(packet.projected().observation().facts()).isEmpty();
        assertThat(packet.projected().observation().metrics()).isEmpty();
        assertThat(packet.unknownProven()).isTrue();
        assertThat(packet.expectedReceipt()).contains(actualReceipt);
    }

    @Test
    void unavailableSourceReadDoesNotProveAbsenceAndKnownRetryDoesNotLoseItsFloor() {
        var scopes = new ObservationScopeRegistry();
        var key = continuationKey(41);
        var ticket = scopes.beginSourceContinuation(PIPELINE, key, () -> true).orElseThrow();
        var absent = scopes.prepareSourceContinuation(ticket, Optional.empty(), Optional.empty(),
                ObservationScopeRegistry.SourceReadStatus.UNAVAILABLE);
        assertThat(absent.status()).isEqualTo(ObservationScopeRegistry.PreparationStatus.UNKNOWN);
        assertThat(absent.snapshot().orElseThrow().unknownProven()).isFalse();
        var known = scopes.prepareSourceContinuation(ticket, Optional.of(new ObservationStore.Stored(
                frame(START, START, 7).observation(), Optional.of(key.sourceScope()))), Optional.empty(),
                ObservationScopeRegistry.SourceReadStatus.READABLE);
        assertThat(known.status()).isEqualTo(ObservationScopeRegistry.PreparationStatus.KNOWN);
        assertThat(scopes.prepareSourceContinuation(ticket, Optional.empty(), Optional.empty(),
                ObservationScopeRegistry.SourceReadStatus.UNAVAILABLE).snapshot().orElseThrow().baselineFacts())
                .isEqualTo(known.snapshot().orElseThrow().baselineFacts());
    }

    @Test
    void unknownTargetCanLaterAcquireItsExactSourceAndRepeatedAdoptKeepsTheNativeCheckpoint() {
        var scopes = new ObservationScopeRegistry();
        var key = continuationKey(42);
        var target = continuationTarget(42, 77);
        var first = scopes.beginTargetContinuation(PIPELINE, key, target, () -> true).orElseThrow();
        assertThat(scopes.adoptTargetContinuation(first, Optional.empty(), Optional.empty()).status())
                .isEqualTo(ObservationScopeRegistry.PreparationStatus.UNKNOWN);
        var source = scopes.beginSourceContinuation(PIPELINE, key, () -> true).orElseThrow();
        var known = scopes.prepareSourceContinuation(source, Optional.of(new ObservationStore.Stored(
                frame(START, START, 7).observation(), Optional.of(key.sourceScope()))), Optional.empty(),
                ObservationScopeRegistry.SourceReadStatus.READABLE);
        var adopt = scopes.beginTargetContinuation(PIPELINE, key, target, () -> true).orElseThrow();
        scopes.adoptTargetContinuation(adopt, Optional.empty(), known.snapshot());
        var packet = scopes.prepareContinuationPublication(frame(START.plusSeconds(1), START.plusSeconds(1), 2), target, () -> true)
                .orElseThrow();
        var receipt = continuationReceipt(packet.snapshot().orElseThrow(), "first-known");
        assertThat(scopes.publicationAccepted(packet.ticket(), receipt)).isTrue();
        var repeated = scopes.beginTargetContinuation(PIPELINE, key, target, () -> true).orElseThrow();
        assertThat(scopes.adoptTargetContinuation(repeated, Optional.empty(), Optional.empty()).status())
                .isEqualTo(ObservationScopeRegistry.PreparationStatus.KNOWN);
        assertThat(scopes.durableReceipt(PIPELINE, handoff(key, target))).contains(receipt);
        var next = scopes.prepareContinuationPublication(frame(START.plusSeconds(2), START.plusSeconds(1), 3), target, () -> true)
                .orElseThrow();
        assertThat(counter(next.projected())).isEqualTo(10);
        assertThat(next.snapshot().orElseThrow().producerStates()).singleElement()
                .satisfies(state -> assertThat(state.nativeStart()).isEqualTo(START.plusSeconds(1)));
    }

    @Test
    void aNewOwnerUsesItsMatchingDurableNewEpochAndRejectsALateOldEpochAsLowerTotal() {
        var target = continuationTarget(42, 77);
        var oldOwner = claimedContinuationKey("node-a", 1);
        var newOwner = claimedContinuationKey("node-b", 2);
        var base = new ObservationContinuation(oldOwner.token(), oldOwner.sourceScope(), Optional.empty(), Optional.empty(),
                frame(START, START, 7).observation().facts(), List.of());
        var local = new ObservationScopeRegistry();
        var before = local.beginTargetContinuation(PIPELINE, oldOwner, target, () -> true).orElseThrow();
        local.adoptTargetContinuation(before, Optional.of(base), Optional.empty());
        var first = local.prepareContinuationPublication(frame(START.plusSeconds(1), START.plusSeconds(1), 2), target, () -> true)
                .orElseThrow();
        var durableWriter = new ObservationScopeRegistry();
        var cold = durableWriter.beginTargetContinuation(PIPELINE, oldOwner, target, () -> true).orElseThrow();
        durableWriter.adoptTargetContinuation(cold, first.snapshot(), Optional.empty());
        var durable = durableWriter.prepareContinuationPublication(frame(START.plusSeconds(2), START.plusSeconds(2), 3), target,
                () -> true).orElseThrow();
        assertThat(counter(durable.projected())).isEqualTo(12);
        var takeover = local.beginTargetContinuation(PIPELINE, newOwner, target, () -> true).orElseThrow();
        local.adoptTargetContinuation(takeover, durable.snapshot(), Optional.empty());
        var late = local.prepareContinuationPublication(frame(START.plusSeconds(3), START.plusSeconds(1), 2), target, () -> true)
                .orElseThrow();
        assertThat(counter(late.projected())).isEqualTo(12);
        assertThat(late.snapshot().orElseThrow().producerStates()).singleElement()
                .satisfies(state -> assertThat(state.nativeStart()).isEqualTo(START.plusSeconds(2)));
    }

    @Test
    void aPreviousBoundTargetIsExplicitlyTheNewFloorOriginAndItsEpochDoesNotFollowTheSuccessor() {
        var previous = continuationTarget(42, 77);
        var live = knownContinuationRegistry();
        var packet = live.prepareContinuationPublication(frame(START.plusSeconds(1), START.plusSeconds(1), 2), previous, () -> true)
                .orElseThrow();
        var replacement = new ObservationScopeRegistry();
        var source = replacement.beginSourceContinuation(PIPELINE, continuationKey(42), Optional.of(previous), () -> true)
                .orElseThrow();
        var frozen = replacement.prepareSourceContinuation(source, Optional.empty(), packet.snapshot(),
                ObservationScopeRegistry.SourceReadStatus.READABLE).snapshot().orElseThrow();
        assertThat(frozen.baselineFacts().getFirst().points().getFirst().value()).isEqualTo(9);
        assertThat(frozen.continuation().baselineOrigin().orElseThrow().realJob()).contains(previous.job());
        assertThat(frozen.continuation().producerStates()).isEmpty();
        var target = continuationTarget(43, 78);
        var ticket = replacement.beginTargetContinuation(PIPELINE, continuationKey(43), target, () -> true).orElseThrow();
        replacement.adoptTargetContinuation(ticket, Optional.empty(), Optional.of(frozen));
        var next = replacement.prepareContinuationPublication(frame(START.plusSeconds(2), START.plusSeconds(2), 2), target, () -> true)
                .orElseThrow();
        assertThat(counter(next.projected())).isEqualTo(11);
    }

    @Test
    void resetAndIncarnationDeletionClearActiveContinuationButAdmissionKeepsTheQualifiedSource() {
        for (boolean deletion : List.of(false, true)) {
            var scopes = knownContinuationRegistry();
            var target = continuationTarget(42, 77);
            var packet = scopes.prepareContinuationPublication(frame(START.plusSeconds(1), START.plusSeconds(1), 2), target, () -> true)
                    .orElseThrow();
            var receipt = continuationReceipt(packet.snapshot().orElseThrow(), "reset-proof");
            scopes.publicationAccepted(packet.ticket(), receipt);
            if (deletion) { scopes.forgetIncarnation(PIPELINE, "inc-a"); }
            var fresh = new ObservationStore.Scope(deletion ? "inc-b" : "inc-a", 43);
            scopes.beginResetExecution(PIPELINE, fresh);
            assertThat(scopes.activeContinuationTarget(PIPELINE)).isEmpty();
            assertThat(scopes.activeContinuationKey(PIPELINE)).isEmpty();
            assertThat(scopes.durableReceipt(PIPELINE, handoff(continuationKey(42), target))).isEmpty();
            var current = new ObservationScopeRegistry.ActualTarget(fresh,
                    new StopReservation.JobIdentity("single", 78, "boot-78"));
            var reset = scopes.prepareContinuationPublication(frame(START.plusSeconds(2), START.plusSeconds(2), 2), current, () -> true)
                    .orElseThrow();
            assertThat(counter(reset.projected())).isEqualTo(2);
            assertThat(start(reset.projected())).isEqualTo(START.plusSeconds(2));
            assertThat(reset.snapshot()).isEmpty();
        }
    }

    @Test
    void sourceAndTargetChecksRunOutsideTheEntryLockAndLateIOCannotReviveResetState() throws Exception {
        try (var executor = Executors.newSingleThreadExecutor()) {
            var scopes = new ObservationScopeRegistry();
            var key = continuationKey(42);
            var target = continuationTarget(42, 77);
            AtomicInteger checks = new AtomicInteger();
            var ticket = scopes.beginTargetContinuation(PIPELINE, key, target, () -> {
                if (checks.incrementAndGet() == 2) {
                    try { executor.submit(() -> scopes.beginResetExecution(PIPELINE, new ObservationStore.Scope("inc-a", 43)))
                            .get(5, TimeUnit.SECONDS); }
                    catch (Exception failed) { throw new AssertionError(failed); }
                }
                return true;
            }).orElseThrow();
            assertThat(scopes.adoptTargetContinuation(ticket, Optional.empty(), Optional.empty()).status())
                    .isEqualTo(ObservationScopeRegistry.PreparationStatus.INVALIDATED);
            assertThat(scopes.current(PIPELINE)).contains(new ObservationStore.Scope("inc-a", 43));
            assertThat(scopes.activeContinuationTarget(PIPELINE)).isEmpty();
        }
    }

    private static ObservationScopeRegistry knownContinuationRegistry() {
        var scopes = new ObservationScopeRegistry();
        var target = continuationTarget(42, 77);
        var key = continuationKey(42);
        var base = new ObservationContinuation(key.token(), key.sourceScope(), Optional.empty(), Optional.empty(),
                frame(START, START, 7).observation().facts(), List.of());
        var ticket = scopes.beginTargetContinuation(PIPELINE, key, target, () -> true).orElseThrow();
        scopes.adoptTargetContinuation(ticket, Optional.of(base), Optional.empty());
        return scopes;
    }

    private static ObservationScopeRegistry.ContinuationKey continuationKey(long generation) {
        return new ObservationScopeRegistry.ContinuationKey("durable-resume", new ObservationStore.Scope("inc-a", 41),
                StopReservation.CounterPolicy.CONTINUE, StopAuthority.standalone("single", generation));
    }

    private static ObservationScopeRegistry.ContinuationKey claimedContinuationKey(String owner, long claimGeneration) {
        var claim = new WorkloadClaim(new WorkloadClaimKey("single", WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE),
                new WorkloadOwner(owner, "boot-" + owner), claimGeneration, 42, 3, START.plusSeconds(60));
        return new ObservationScopeRegistry.ContinuationKey("durable-resume", new ObservationStore.Scope("inc-a", 41),
                StopReservation.CounterPolicy.CONTINUE, StopAuthority.claimed(WorkloadClaimFence.from(claim)));
    }

    private static ObservationScopeRegistry.ActualTarget continuationTarget(long generation, long job) {
        return new ObservationScopeRegistry.ActualTarget(new ObservationStore.Scope("inc-a", generation),
                new StopReservation.JobIdentity("single", job, "boot-" + job));
    }

    private static HandoffIdentity handoff(ObservationScopeRegistry.ContinuationKey key, ObservationScopeRegistry.ActualTarget target) {
        return new HandoffIdentity(PIPELINE, key.token(), key.policy(), key.sourceScope(), target.scope(), target.job());
    }

    private static ObservationStore.ContinuationReceipt continuationReceipt(ObservationContinuation snapshot, String revision) {
        return new ObservationStore.ContinuationReceipt(PIPELINE, revision, "a".repeat(64), 1, snapshot.token(), snapshot.sourceScope(),
                snapshot.target(), snapshot.baselineOrigin(), snapshot.knownBaseline());
    }

    @Test
    void aColdRebuildKeepsItsQualifiedStoredFloorWithoutInstallingAnObservationScope() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        ObservationStore.Scope existingJob = new ObservationStore.Scope("inc-a", 41);
        ObservationStore.Stored saved = new ObservationStore.Stored(
                frame(START, START.minusSeconds(60), 7).observation(), Optional.of(existingJob));

        assertThat(scopes.current(PIPELINE)).isEmpty();
        var ticket = scopes.beginColdRebuild(PIPELINE, existingJob, () -> true).orElseThrow();
        assertThat(scopes.prepareColdRebuildingResume(ticket, Optional.of(saved))).isTrue();
        assertThat(scopes.current(PIPELINE)).isEmpty();
        var unknownNative = scopes.continueFrame(gaugeFrame(START.plusMillis(500), 22), existingJob);
        assertThat(unknownNative.observation().facts()).allMatch(fact -> fact.type() == MetricType.GAUGE);

        ObservationStore.Scope next = scopes.begin(PIPELINE, "inc-a", 42);
        ObservationPublisher.Prepared continued = scopes.continueFrame(
                frame(START.plusSeconds(1), START.plusSeconds(1), 2), next);
        assertThat(counter(continued)).isEqualTo(9);
        assertThat(start(continued)).isEqualTo(START.minusSeconds(60));
    }

    @Test
    void unqualifiedOrUnavailableColdTelemetryIsUnknownAndDoesNotBlockAValidRebuild() {
        var existing = new ObservationStore.Scope("inc-a", 41);
        Observation known = frame(START, START.minusSeconds(60), 7).observation();
        Observation legacy = new Observation(PIPELINE, PipelineState.RUNNING,
                Map.of("records.out", 7L), Map.of(), Map.of(), null, START, List.of());
        Observation other = new Observation("other", known.state(), known.metrics(), known.snapshot(),
                known.positions(), null, START, known.facts());
        List<Optional<ObservationStore.Stored>> unavailable = List.of(Optional.empty(),
                Optional.of(new ObservationStore.Stored(known, Optional.empty())),
                Optional.of(new ObservationStore.Stored(known, Optional.of(new ObservationStore.Scope("inc-a", 40)))),
                Optional.of(new ObservationStore.Stored(known, Optional.of(new ObservationStore.Scope("inc-b", 41)))),
                Optional.of(new ObservationStore.Stored(other, Optional.of(existing))),
                Optional.of(new ObservationStore.Stored(legacy, Optional.of(existing))),
                Optional.of(new ObservationStore.Stored(gaugeFrame(START, 22).observation(), Optional.of(existing))));
        for (var stored : unavailable) {
            ObservationScopeRegistry scopes = new ObservationScopeRegistry();
            var ticket = scopes.beginColdRebuild(PIPELINE, existing, () -> true).orElseThrow();
            assertThat(scopes.prepareColdRebuildingResume(ticket, stored)).isTrue();
            assertThat(scopes.current(PIPELINE)).isEmpty();
            var next = scopes.begin(PIPELINE, "inc-a", 42);
            var fresh = scopes.continueFrame(frame(START.plusSeconds(1), START.plusSeconds(1), 2), next);
            assertThat(counter(fresh)).isEqualTo(2);
            assertThat(start(fresh)).isEqualTo(START.plusSeconds(1));
        }
    }

    @Test
    void aFailedStartAndAnUnavailableRetryKeepTheAlreadyQualifiedColdFloor() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var existing = new ObservationStore.Scope("inc-a", 41);
        var saved = new ObservationStore.Stored(frame(START, START.minusSeconds(60), 7).observation(),
                Optional.of(existing));
        var first = scopes.beginColdRebuild(PIPELINE, existing, () -> true).orElseThrow();
        assertThat(scopes.prepareColdRebuildingResume(first, Optional.of(saved))).isTrue();
        var rejected = scopes.begin(PIPELINE, "inc-a", 42);
        scopes.discard(PIPELINE, rejected);

        assertThat(scopes.prepareColdRebuildingResume(first, Optional.of(saved))).isFalse();
        scopes.cancelColdRebuild(first);
        var retry = scopes.beginColdRebuild(PIPELINE, existing, () -> true).orElseThrow();
        assertThat(scopes.prepareColdRebuildingResume(retry, Optional.empty())).isTrue();
        assertThat(scopes.prepareColdRebuildingResume(retry, Optional.of(new ObservationStore.Stored(
                frame(START, START, 999).observation(), Optional.of(new ObservationStore.Scope("inc-other", 99))))))
                .isTrue();
        var admitted = scopes.begin(PIPELINE, "inc-a", 43);
        var continued = scopes.continueFrame(frame(START.plusSeconds(2), START.plusSeconds(2), 2), admitted);
        assertThat(counter(continued)).isEqualTo(9);
        assertThat(start(continued)).isEqualTo(START.minusSeconds(60));
    }

    @Test
    void coldFloorsBelongOnlyToAHigherGenerationOfTheSameIncarnation() {
        for (var next : List.of(new ObservationStore.Scope("inc-b", 42), new ObservationStore.Scope("inc-a", 41))) {
            ObservationScopeRegistry scopes = new ObservationScopeRegistry();
            var existing = new ObservationStore.Scope("inc-a", 41);
            var ticket = scopes.beginColdRebuild(PIPELINE, existing, () -> true).orElseThrow();
            assertThat(scopes.prepareColdRebuildingResume(ticket, Optional.of(new ObservationStore.Stored(
                    frame(START, START, 7).observation(), Optional.of(existing))))).isTrue();
            var begun = scopes.begin(PIPELINE, next.pipelineIncarnationId(), next.executionGeneration());
            assertThat(scopes.prepareColdRebuildingResume(ticket, Optional.empty())).isFalse();
            scopes.cancelColdRebuild(ticket);
            assertThat(counter(scopes.continueFrame(frame(START.plusSeconds(1), START.plusSeconds(1), 2), begun)))
                    .isEqualTo(2);
        }
    }

    @Test
    void aColdStoredReadCannotCrossBeginDeleteDiscardRetainOrCancellation() {
        List<BiConsumer<ObservationScopeRegistry, ObservationScopeRegistry.ColdRebuildTicket>> changes = List.of(
                (scopes, ticket) -> scopes.begin(PIPELINE, "inc-a", 42),
                (scopes, ticket) -> scopes.discard(PIPELINE, scopes.begin(PIPELINE, "inc-a", 42)),
                (scopes, ticket) -> scopes.forgetIncarnation(PIPELINE, "inc-a"),
                (scopes, ticket) -> scopes.discard(PIPELINE, new ObservationStore.Scope("inc-a", 41)),
                (scopes, ticket) -> scopes.retain(List.of()),
                (scopes, ticket) -> scopes.clearContinuation(PIPELINE),
                (scopes, ticket) -> scopes.cancelColdRebuild(ticket),
                (scopes, ticket) -> scopes.cancelRestoration(PIPELINE));
        var existing = new ObservationStore.Scope("inc-a", 41);
        var saved = new ObservationStore.Stored(frame(START, START, 7).observation(), Optional.of(existing));
        for (var change : changes) {
            ObservationScopeRegistry scopes = new ObservationScopeRegistry();
            var ticket = scopes.beginColdRebuild(PIPELINE, existing, () -> true).orElseThrow();
            // The caller's stored read completes only after this registry transition.
            change.accept(scopes, ticket);
            assertThat(scopes.prepareColdRebuildingResume(ticket, Optional.of(saved))).isFalse();
            var next = scopes.begin(PIPELINE, "inc-a", 43);
            assertThat(counter(scopes.continueFrame(frame(START.plusSeconds(1), START.plusSeconds(1), 2), next)))
                    .isEqualTo(2);
        }
    }

    @Test
    void ownerLossCancelsAStagedFloorWhileAnOldDeletionCannotEraseANewIncarnationTicket() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var existing = new ObservationStore.Scope("inc-b", 42);
        var saved = new ObservationStore.Stored(frame(START, START, 7).observation(), Optional.of(existing));
        AtomicBoolean current = new AtomicBoolean(true);
        var ticket = scopes.beginColdRebuild(PIPELINE, existing, current::get).orElseThrow();
        scopes.forgetIncarnation(PIPELINE, "inc-a");
        assertThat(scopes.prepareColdRebuildingResume(ticket, Optional.of(saved))).isTrue();
        current.set(false);
        assertThat(scopes.prepareColdRebuildingResume(ticket, Optional.empty())).isFalse();
        var next = scopes.begin(PIPELINE, "inc-b", 43);
        assertThat(counter(scopes.continueFrame(frame(START.plusSeconds(1), START.plusSeconds(1), 2), next)))
                .isEqualTo(2);
        assertThat(scopes.beginColdRebuild(PIPELINE, existing, () -> true)).isEmpty();
    }

    @Test
    void coldAuthorityChecksRunOutsideTheEntryLockAndObserveRegistryChangesDuringBothChecks() throws Exception {
        var existing = new ObservationStore.Scope("inc-a", 41);
        try (var executor = Executors.newSingleThreadExecutor()) {
            ObservationScopeRegistry before = new ObservationScopeRegistry();
            assertThat(before.beginColdRebuild(PIPELINE, existing, () -> {
                try { executor.submit(() -> before.clearContinuation(PIPELINE)).get(5, TimeUnit.SECONDS); }
                catch (Exception failed) { throw new AssertionError(failed); }
                return true;
            })).isEmpty();

            ObservationScopeRegistry after = new ObservationScopeRegistry();
            AtomicInteger checks = new AtomicInteger();
            var ticket = after.beginColdRebuild(PIPELINE, existing, () -> {
                if (checks.incrementAndGet() == 2) {
                    try { executor.submit(() -> after.clearContinuation(PIPELINE)).get(5, TimeUnit.SECONDS); }
                    catch (Exception failed) { throw new AssertionError(failed); }
                }
                return true;
            }).orElseThrow();
            assertThat(after.prepareColdRebuildingResume(ticket, Optional.of(new ObservationStore.Stored(
                    frame(START, START, 7).observation(), Optional.of(existing))))).isFalse();
            assertThat(checks.get()).isEqualTo(2);
        }
    }

    @Test
    void theQualifiedTicketReusesWarmFramesAndNativeEpochContinuation() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var existing = scopes.begin(PIPELINE, "inc-a", 41);
        scopes.continueFrame(frame(START.plusSeconds(1), START, 7), existing);
        var restartedNative = scopes.continueFrame(frame(START.plusSeconds(2), START.plusSeconds(2), 2), existing);
        assertThat(counter(restartedNative)).isEqualTo(9);
        var ticket = scopes.beginColdRebuild(PIPELINE, existing, () -> true).orElseThrow();
        assertThat(scopes.prepareColdRebuildingResume(ticket, Optional.empty())).isTrue();
        assertThat(scopes.current(PIPELINE)).contains(existing);
        var next = scopes.begin(PIPELINE, "inc-a", 42);
        var continued = scopes.continueFrame(frame(START.plusSeconds(3), START.plusSeconds(3), 3), next);
        assertThat(counter(continued)).isEqualTo(12);
        assertThat(start(continued)).isEqualTo(START);
    }

    @Test
    void aFactsAbsentFrameRetainsItsLegacyMetricsWithoutInventingMeasurements() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var scope = scopes.begin(PIPELINE, "inc-a", 41);
        Observation legacy = new Observation(PIPELINE, PipelineState.RUNNING,
                Map.of("records.out", 7L, "lag.orders", 2L), Map.of(), Map.of(), null, START);
        var prepared = new ObservationPublisher.Prepared(legacy, false, Map.of(), Map.of(), Map.of());

        var continued = scopes.continueFrame(prepared, scope).observation();

        assertThat(continued.metrics()).containsExactlyInAnyOrderEntriesOf(legacy.metrics());
        assertThat(continued.facts()).isEmpty();
        InMemoryRateHistoryStore history = new InMemoryRateHistoryStore();
        assertThat(new io.tapstate.runtime.scheduler.RateSampler(history, java.time.Duration.ofMinutes(1))
                .appendIfDue(continued, scope)).isTrue();
        assertThat(history.readPage(PIPELINE, START, START.plusSeconds(1), null, 10).entries())
                .singleElement().satisfies(entry -> assertThat(entry.sample().countingSince()).isNull());

        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());
        var rebuilt = scopes.begin(PIPELINE, "inc-a", 42);
        Observation nextLegacy = new Observation(PIPELINE, PipelineState.RUNNING,
                legacy.metrics(), Map.of(), Map.of(), null, START.plusSeconds(1));
        var nextPrepared = new ObservationPublisher.Prepared(nextLegacy, false, Map.of(), Map.of(), Map.of());
        assertThat(scopes.continueFrame(nextPrepared, rebuilt).observation()).satisfies(next -> {
            assertThat(next.metrics()).containsExactlyInAnyOrderEntriesOf(legacy.metrics());
            assertThat(next.facts()).isEmpty();
        });
    }

    @Test
    void aTypedCounterWithMixedNativeStartsDoesNotRestoreItsLegacyFlatValue() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var scope = scopes.begin(PIPELINE, "inc-a", 41);
        MetricFact unknown = new MetricFact("tapstate.pipeline.records", MetricType.COUNTER, "{record}",
                List.of(MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                                MetricAttributes.DIRECTION, "out", MetricAttributes.TABLE_ID, "table-a"),
                                START, START.plusSeconds(1), 4),
                        MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                                MetricAttributes.DIRECTION, "out", MetricAttributes.TABLE_ID, "table-b"),
                                START.minusSeconds(1), START.plusSeconds(1), 3)));
        Observation observation = new Observation(PIPELINE, PipelineState.RUNNING,
                Map.of("records.out", 7L), Map.of(), Map.of(), null, START.plusSeconds(1), List.of(unknown));
        var prepared = new ObservationPublisher.Prepared(observation, false, Map.of(), Map.of(), Map.of());

        var continued = scopes.continueFrame(prepared, scope).observation();

        assertThat(continued.metrics()).doesNotContainKey("records.out");
        assertThat(continued.facts()).flatExtracting(MetricFact::points).isEmpty();
    }

    @Test
    void leaseRenewalKeepsOneTicketWhileAnotherOwnerOrFenceInvalidatesIt() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var key = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, PIPELINE);
        var owner = new WorkloadOwner("node-a", "boot-a");
        var first = new WorkloadClaim(key, owner, 1, 41, 3, START.plusSeconds(5));
        var renewed = new WorkloadClaim(key, owner, 1, 41, 3, START.plusSeconds(10));
        var ticket = scopes.restoration(PIPELINE, null, null, ObservationScopeRecovery.Owner.of(first))
                .orElseThrow();
        assertThat(scopes.restoration(PIPELINE, null, null, ObservationScopeRecovery.Owner.of(renewed))
                .orElseThrow()).isSameAs(ticket);
        var next = new WorkloadClaim(key, new WorkloadOwner("node-b", "boot-b"),
                2, 41, 3, START.plusSeconds(10));
        var replacement = scopes.restoration(PIPELINE, null, null, ObservationScopeRecovery.Owner.of(next))
                .orElseThrow();
        assertThat(scopes.awaiting(ticket)).isFalse();
        assertThat(scopes.awaiting(replacement)).isTrue();
    }

    @Test
    void aLateRestoreCannotCrossBeginDiscardDeleteOrRetainEvenWhenCurrentWasAbsent() {
        List<Consumer<ObservationScopeRegistry>> invalidations = List.of(
                scopes -> scopes.begin(PIPELINE, "inc-a", 42),
                scopes -> {
                    var next = scopes.begin(PIPELINE, "inc-a", 42);
                    scopes.discard(PIPELINE, next);
                },
                scopes -> scopes.discard(PIPELINE, new ObservationStore.Scope("inc-a", 41)),
                scopes -> scopes.forgetIncarnation(PIPELINE, "inc-a"),
                scopes -> scopes.retain(List.of()),
                scopes -> scopes.cancelRestoration(PIPELINE));
        var saved = new ObservationStore.Stored(frame(START, START, 7).observation(),
                Optional.of(new ObservationStore.Scope("inc-a", 41)));
        for (var invalidate : invalidations) {
            ObservationScopeRegistry scopes = new ObservationScopeRegistry();
            var ticket = scopes.restoration(PIPELINE, null, null).orElseThrow();
            assertThat(scopes.awaiting(ticket)).isTrue();
            invalidate.accept(scopes);
            assertThat(scopes.awaiting(ticket)).isFalse();
            assertThat(scopes.restore(ticket, saved)).isFalse();
            assertThat(scopes.current(PIPELINE)).isNotEqualTo(saved.scope());
        }
    }

    @Test
    void restoredHistoryIsNotANativeProducerButSuppliesAStoppedFinalAndExplicitRebuildFloor() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        var owner = new ObservationStore.Scope("inc-a", 41);
        var saved = new ObservationStore.Stored(frame(START, START.minusSeconds(60), 7).observation(),
                Optional.of(owner));
        var ticket = scopes.restoration(PIPELINE, null, null).orElseThrow();
        assertThat(scopes.restore(ticket, saved)).isTrue();
        scopes.restored(ticket);
        assertThat(scopes.continuing(PIPELINE, owner)).isFalse();
        assertThat(scopes.needsStoredFallback(PIPELINE)).isFalse();
        var failed = scopes.continueFrame(gaugeFrame(START.plusSeconds(1), 22, PipelineState.FAILED), owner);
        assertThat(failed.observation().facts()).allMatch(fact -> fact.type() == MetricType.GAUGE);

        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());
        var next = scopes.begin(PIPELINE, "inc-a", 42);
        var resumed = scopes.continueFrame(frame(START.plusSeconds(2), START.plusSeconds(2), 2), next);
        assertThat(counter(resumed)).isEqualTo(9);
        assertThat(start(resumed)).isEqualTo(START.minusSeconds(60));

        scopes.clearContinuation(PIPELINE);
        var fresh = scopes.begin(PIPELINE, "inc-a", 43);
        assertThat(counter(scopes.continueFrame(frame(START.plusSeconds(3), START.plusSeconds(3), 1), fresh)))
                .isEqualTo(1);

        ObservationScopeRegistry stoppedScopes = new ObservationScopeRegistry();
        var stoppedTicket = stoppedScopes.restoration(PIPELINE, null, null).orElseThrow();
        assertThat(stoppedScopes.restore(stoppedTicket, saved)).isTrue();
        stoppedScopes.restored(stoppedTicket);
        var stopped = stoppedScopes.continueFrame(gaugeFrame(START.plusSeconds(4), 22, PipelineState.STOPPED), owner);
        assertThat(counter(stopped)).isEqualTo(7);
        assertThat(start(stopped)).isEqualTo(START.minusSeconds(60));
    }

    @Test
    void removingAnOldIncarnationInvalidatesItsScopeButCannotEraseARecreatedOne() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        ObservationStore.Scope old = scopes.begin(PIPELINE, "inc-old", 41);

        scopes.forgetIncarnation(PIPELINE, "inc-old");
        assertThat(scopes.current(PIPELINE)).isEmpty();

        ObservationStore.Scope current = scopes.begin(PIPELINE, "inc-new", 42);
        scopes.forgetIncarnation(PIPELINE, "inc-old");
        assertThat(scopes.current(PIPELINE)).contains(current);
        assertThat(current).isNotEqualTo(old);
    }

    @Test
    void aRebuildKeepsNamedSeriesWithinTheDeclaredTableBudgetAndPreservesTheirTotal() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        ObservationStore.Scope old = scopes.begin(PIPELINE, "inc-a", 1);
        List<MetricPoint> many = new ArrayList<>();
        for (int table = 0; table < 1_000; table++) {
            many.add(MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                    MetricAttributes.TABLE_ID, "table-%04d".formatted(table)), START,
                    START.plusSeconds(1), 1));
        }
        MetricFact oldFact = new MetricFact("tapstate.pipeline.snapshot.rows", MetricType.COUNTER,
                "{row}", many);
        Observation oldObservation = new Observation(PIPELINE, PipelineState.RUNNING, Map.of(), Map.of(), Map.of(),
                null, START.plusSeconds(1), List.of(oldFact));
        scopes.continueFrame(new ObservationPublisher.Prepared(oldObservation, false,
                Map.of(), Map.of(), Map.of()), old);
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());

        ObservationStore.Scope resumed = scopes.begin(PIPELINE, "inc-a", 2);
        MetricPoint newcomer = MetricPoint.accumulated(Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
                MetricAttributes.TABLE_ID, "table-new"), START.plusSeconds(2), START.plusSeconds(2), 2);
        Observation fresh = new Observation(PIPELINE, PipelineState.RUNNING, Map.of(), Map.of(), Map.of(), null,
                START.plusSeconds(2), List.of(new MetricFact("tapstate.pipeline.snapshot.rows",
                        MetricType.COUNTER, "{row}", List.of(newcomer))));
        ObservationPublisher.Prepared continued = scopes.continueFrame(
                new ObservationPublisher.Prepared(fresh, false, Map.of(), Map.of(), Map.of()), resumed);
        List<MetricPoint> points = continued.observation().facts().getFirst().points();

        assertThat(points).hasSize(1_001);
        assertThat(points).anySatisfy(point -> {
            assertThat(point.attributes()).containsEntry(MetricAttributes.TABLE_ID, "table-0999");
            assertThat(point.value()).isEqualTo(1);
        });
        assertThat(points).anySatisfy(point -> {
            assertThat(point.attributes()).containsEntry(MetricAttributes.OVERFLOW, "true");
            assertThat(point.value()).isEqualTo(2);
        });
        assertThat(points.stream().mapToLong(MetricPoint::value).sum()).isEqualTo(1_002);
    }

    private static final String PIPELINE = "orders";
    private static final Instant START = Instant.parse("2026-09-27T00:00:00Z");
    private static final Map<String, String> TABLE = Map.of(MetricAttributes.PIPELINE_ID, PIPELINE,
            MetricAttributes.TABLE_ID, "orders");

    @Test
    void repeatedRebuildsCarryTheCorrectedBaselineWhileAPlainStopStartResetsIt() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        ObservationStore.Scope first = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(frame(START.plusSeconds(1), START, 7), first);
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());

        ObservationStore.Scope second = scopes.begin(PIPELINE, "inc-a", 2);
        ObservationPublisher.Prepared afterOne = scopes.continueFrame(
                frame(START.plusSeconds(2), START.plusSeconds(2), 2), second);
        assertThat(counter(afterOne)).isEqualTo(9);
        assertThat(start(afterOne)).isEqualTo(START);
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());

        ObservationStore.Scope third = scopes.begin(PIPELINE, "inc-a", 3);
        ObservationPublisher.Prepared afterTwo = scopes.continueFrame(
                frame(START.plusSeconds(3), START.plusSeconds(3), 3), third);
        assertThat(counter(afterTwo)).isEqualTo(12);
        assertThat(start(afterTwo)).isEqualTo(START);

        scopes.clearContinuation(PIPELINE);
        ObservationPublisher.Prepared stopped = scopes.continueFrame(
                gaugeFrame(START.plusSeconds(3).plusMillis(500), 22, PipelineState.STOPPED), third);
        assertThat(counter(stopped)).isEqualTo(12);
        assertThat(start(stopped)).isEqualTo(START);
        assertThat(stopped.observation().state()).isEqualTo(PipelineState.STOPPED);
        ObservationStore.Scope fresh = scopes.begin(PIPELINE, "inc-a", 4);
        ObservationPublisher.Prepared restarted = scopes.continueFrame(
                frame(START.plusSeconds(4), START.plusSeconds(4), 1), fresh);
        assertThat(counter(restarted)).isEqualTo(1);
        assertThat(start(restarted)).isEqualTo(START.plusSeconds(4));
        assertThat(scopes.continueFrame(frame(START.plusSeconds(5), START, 99), second).observation().facts())
                .isEqualTo(frame(START.plusSeconds(5), START, 99).observation().facts());
        assertThat(scopes.current(PIPELINE)).contains(fresh);
    }

    @Test
    void aRejectedNewExecutionKeepsTheFrozenBaselineForTheNextSubmission() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        ObservationStore.Scope old = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.continueFrame(frame(START.plusSeconds(1), START, 7), old);
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());
        ObservationStore.Scope rejected = scopes.begin(PIPELINE, "inc-a", 2);
        scopes.discard(PIPELINE, rejected);

        ObservationStore.Scope admitted = scopes.begin(PIPELINE, "inc-a", 3);
        ObservationPublisher.Prepared continued = scopes.continueFrame(
                frame(START.plusSeconds(3), START.plusSeconds(3), 2), admitted);

        assertThat(counter(continued)).isEqualTo(9);
        assertThat(start(continued)).isEqualTo(START);
    }

    @Test
    void anUnknownBaselineStaysUnknownAndAStoredMatchingScopeCanSupplyAKnownOne() {
        ObservationScopeRegistry scopes = new ObservationScopeRegistry();
        ObservationStore.Scope old = scopes.begin(PIPELINE, "inc-a", 1);
        scopes.prepareRebuildingResume(PIPELINE, Optional.empty());
        ObservationStore.Scope noBaseline = scopes.begin(PIPELINE, "inc-a", 2);
        ObservationPublisher.Prepared unknown = scopes.continueFrame(gaugeFrame(START.plusSeconds(2), 22), noBaseline);
        assertThat(unknown.observation().facts()).allMatch(fact -> fact.type() == MetricType.GAUGE);
        assertThat(unknown.observation().metrics()).containsEntry("lag.orders", 22L)
                .doesNotContainKey("records.out");

        scopes.clearContinuation(PIPELINE);
        ObservationStore.Scope storedOwner = scopes.begin(PIPELINE, "inc-a", 3);
        Observation saved = frame(START.plusSeconds(3), START, 7).observation();
        scopes.prepareRebuildingResume(PIPELINE,
                Optional.of(new ObservationStore.Stored(saved, Optional.of(storedOwner))));
        ObservationStore.Scope resumed = scopes.begin(PIPELINE, "inc-a", 4);
        ObservationPublisher.Prepared fromStored = scopes.continueFrame(
                frame(START.plusSeconds(4), START.plusSeconds(4), 2), resumed);
        assertThat(counter(fromStored)).isEqualTo(9);

        scopes.clearContinuation(PIPELINE);
        ObservationStore.Scope other = scopes.begin(PIPELINE, "inc-b", 5);
        scopes.prepareRebuildingResume(PIPELINE,
                Optional.of(new ObservationStore.Stored(saved, Optional.of(old))));
        ObservationStore.Scope newIncarnation = scopes.begin(PIPELINE, "inc-b", 6);
        assertThat(counter(scopes.continueFrame(frame(START.plusSeconds(6), START.plusSeconds(6), 2),
                newIncarnation))).isEqualTo(2);
        assertThat(other.pipelineIncarnationId()).isEqualTo("inc-b");
    }

    private static ObservationPublisher.Prepared frame(Instant at, Instant since, long rows) {
        MetricFact counter = MetricFact.single("tapstate.pipeline.snapshot.rows", MetricType.COUNTER, "{row}",
                MetricPoint.accumulated(TABLE, since, at, rows));
        Observation observation = new Observation(PIPELINE, PipelineState.RUNNING, Map.of(), Map.of(), Map.of(),
                null, at, List.of(counter));
        return new ObservationPublisher.Prepared(observation, false, Map.of(), Map.of(), Map.of());
    }

    private static ObservationPublisher.Prepared gaugeFrame(Instant at, long lag) {
        return gaugeFrame(at, lag, PipelineState.RUNNING);
    }

    private static ObservationPublisher.Prepared gaugeFrame(Instant at, long lag, PipelineState state) {
        MetricFact gauge = MetricFact.single("tapstate.pipeline.lag", MetricType.GAUGE, "s",
                MetricPoint.reading(TABLE, at, lag));
        Observation observation = new Observation(PIPELINE, state,
                Map.of("lag.orders", lag), Map.of(), Map.of(), null, at, List.of(gauge));
        return new ObservationPublisher.Prepared(observation, false, Map.of(), Map.of(), Map.of());
    }

    private static long counter(ObservationPublisher.Prepared frame) {
        return frame.observation().facts().stream()
                .filter(fact -> fact.name().equals("tapstate.pipeline.snapshot.rows"))
                .findFirst().orElseThrow().points().getFirst().value();
    }

    private static Instant start(ObservationPublisher.Prepared frame) {
        return frame.observation().facts().stream()
                .filter(fact -> fact.name().equals("tapstate.pipeline.snapshot.rows"))
                .findFirst().orElseThrow().points().getFirst().startTime();
    }
}
