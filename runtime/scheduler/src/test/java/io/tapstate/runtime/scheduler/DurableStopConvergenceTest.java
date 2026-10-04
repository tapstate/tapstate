package io.tapstate.runtime.scheduler;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.CasOutcome;
import io.tapstate.core.lifecycle.EpochCas;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.LifecycleError;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.SuccessorAdmission;
import io.tapstate.spi.store.SuccessorEnd;
import io.tapstate.spi.store.HandoffIdentity;
import io.tapstate.spi.store.ObservationStore;
import io.tapstate.spi.store.StateStore;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static io.tapstate.core.lifecycle.PipelineState.NEW;
import static io.tapstate.core.lifecycle.PipelineState.FAILED;
import static io.tapstate.core.lifecycle.PipelineState.COMPLETED;
import static io.tapstate.core.lifecycle.PipelineState.PAUSED;
import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static io.tapstate.core.lifecycle.PipelineState.STOPPED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Control-flow tests pin typed stop subjects; native identity is exercised by the engine binding. */
class DurableStopConvergenceTest {

    private static final Instant AT = Instant.parse("2026-10-01T00:00:00Z");
    private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
    private final HandoffState state = new HandoffState();
    private final StopActuator actuator = new StopActuator();

    private PipelineConverger loop() {
        state.enableStops(desired, id -> actuator.authority);
        return new PipelineConverger(desired, state, actuator, Clock.fixed(AT, ZoneOffset.UTC));
    }

    @Test
    void rebuildingResumeReportsTheOldStopBeforeItWaitsForNativeCompletion() {
        PipelineConverger loop = rebuildingResume();
        actuator.over.set(false);
        AtomicReference<PipelineConverger.PendingAction> latest = new AtomicReference<>();
        AtomicReference<PipelineConverger.PendingAction> duringStop = new AtomicReference<>();
        actuator.onFinish = () -> duringStop.set(latest.get());

        ConvergeResult waiting = loop.converge("orders", decision -> latest.set(decision.action()));

        assertThat(waiting.status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(PAUSED));
        assertThat(actuator.starts).hasValue(0);
        assertThat(duringStop.get()).as("the admitted resume is still ending its old physical job")
                .isEqualTo(PipelineConverger.PendingAction.STOP);
    }

    @Test
    void oneWorkerReportsStartAfterTheOldStopAndBeforeReplacementCapacityAdmission() {
        PipelineConverger loop = rebuildingResume();
        actuator.capacityUnavailable = true;
        AtomicReference<PipelineConverger.PendingAction> latest = new AtomicReference<>();
        List<PipelineConverger.PendingAction> actualActions = new ArrayList<>();
        actuator.onFinish = () -> actualActions.add(latest.get());
        actuator.onPrepareReplacement = () -> actualActions.add(latest.get());

        ConvergeResult waiting = loop.converge("orders", decision -> latest.set(decision.action()));

        assertThat(waiting.status()).isEqualTo(ConvergeStatus.START_CAPACITY);
        assertThat(state.readStopReservation("orders").orElseThrow().phase())
                .isEqualTo(StopReservation.Phase.REPLACEMENT_PENDING);
        assertThat(state.advances).isZero();
        assertThat(actuator.starts).hasValue(0);
        assertThat(actualActions).as("one accepted worker crosses from teardown to start admission")
                .containsExactly(PipelineConverger.PendingAction.STOP, PipelineConverger.PendingAction.START);
    }

    @Test
    void supersedingAReplacementPendingMarkerReportsItsActualRetirementAsStop() {
        PipelineConverger loop = rebuildingResume();
        actuator.capacityUnavailable = true;
        assertThat(loop.converge("orders").status()).isEqualTo(ConvergeStatus.START_CAPACITY);
        StopReservation marker = state.readStopReservation("orders").orElseThrow();
        desired.save(new DesiredState("orders", RUNNING, "rev-2"));
        actuator.over.set(false);
        AtomicReference<PipelineConverger.PendingAction> latest = new AtomicReference<>();
        AtomicReference<PipelineConverger.PendingAction> duringStop = new AtomicReference<>();
        actuator.onFinish = () -> duringStop.set(latest.get());

        ConvergeResult waiting = loop.converge("orders", decision -> latest.set(decision.action()));

        assertThat(waiting.status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        assertThat(state.readStopReservation("orders")).contains(marker);
        assertThat(actuator.retiring).hasValue(1);
        assertThat(state.advances).isZero();
        assertThat(duringStop.get()).as("the superseded phase does not classify the new worker's native action")
                .isEqualTo(PipelineConverger.PendingAction.STOP);
    }

    @Test
    void anAbsentBoundSuccessorReportsItsFloorAndCaptureRetirementAsStop() {
        PipelineConverger loop = rebuildingResume();
        actuator.telemetryReady = false;
        assertThat(loop.converge("orders").status()).isEqualTo(ConvergeStatus.CONVERGED);
        StopReservation marker = state.readStopReservation("orders").orElseThrow();
        assertThat(marker.phase()).isEqualTo(StopReservation.Phase.SUCCESSOR_BOUND);
        actuator.nativeJob = null;
        actuator.carrying = false;
        actuator.over.set(false);
        AtomicReference<PipelineConverger.PendingAction> latest = new AtomicReference<>();
        AtomicReference<PipelineConverger.PendingAction> duringStop = new AtomicReference<>();
        actuator.onFinish = () -> duringStop.set(latest.get());

        ConvergeResult waiting = loop.converge("orders", decision -> latest.set(decision.action()));

        assertThat(waiting.status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        assertThat(state.readStopReservation("orders")).contains(marker);
        assertThat(state.advances).isEqualTo(1);
        assertThat(actuator.starts).hasValue(1);
        assertThat(duringStop.get()).as("retiring a missing bound successor cannot be labeled as a fresh start")
                .isEqualTo(PipelineConverger.PendingAction.STOP);
    }

    @Test
    void anAlreadyRunningBoundJobWaitingOnlyForTelemetryDoesNotReportStart() {
        PipelineConverger loop = rebuildingResume();
        actuator.telemetryReady = false;
        assertThat(loop.converge("orders").status()).isEqualTo(ConvergeStatus.CONVERGED);
        StopReservation marker = state.readStopReservation("orders").orElseThrow();
        AtomicReference<PipelineConverger.PendingAction> latest = new AtomicReference<>();
        AtomicReference<PipelineConverger.PendingAction> duringAdoption = new AtomicReference<>();
        actuator.onAdopt = () -> duringAdoption.set(latest.get());

        ConvergeResult running = loop.converge("orders", decision -> latest.set(decision.action()));

        assertThat(running.status()).isEqualTo(ConvergeStatus.CONVERGED);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(state.readStopReservation("orders")).contains(marker);
        assertThat(state.advances).isEqualTo(1);
        assertThat(actuator.starts).hasValue(1);
        assertThat(duringAdoption.get()).as("telemetry completion cannot relabel an existing running job")
                .isEqualTo(PipelineConverger.PendingAction.NONE);
    }

    @Test
    void anOrdinaryNewRunReportsStartBeforeItsActualSubmission() {
        state.create("orders", StateJson.of(NEW), AT);
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        List<PipelineConverger.PendingAction> decisions = new ArrayList<>();

        assertThat(loop().converge("orders", decision -> decisions.add(decision.action())).status())
                .isEqualTo(ConvergeStatus.CONVERGED);

        assertThat(decisions).contains(PipelineConverger.PendingAction.START);
        assertThat(actuator.starts).hasValue(1);
    }

    @Test
    void anUnfinishedStopKeepsActualAndOneDurableTokenWithoutStartingAnything() {
        state.create("orders", StateJson.of(RUNNING), AT);
        desired.save(new DesiredState("orders", STOPPED, "rev-1", true));
        PipelineConverger loop = loop();

        ConvergeResult first = loop.converge("orders");
        StopReservation accepted = state.readStopReservation("orders").orElseThrow();
        ConvergeResult retry = loop.converge("orders");

        assertThat(first.status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        assertThat(retry.status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        assertThat(first.transitionFrom()).isEmpty();
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(state.readStopReservation("orders")).contains(accepted);
        assertThat(accepted.sourceEpoch()).isZero();
        assertThat(accepted.reservedEpoch()).isEqualTo(1);
        assertThat(state.stopReservations()).isEqualTo(1);
        assertThat(actuator.firstAttempts.get()).isEqualTo(1);
        assertThat(actuator.finishes.get()).isEqualTo(2);
        assertThat(actuator.starts.get()).isZero();
        assertThat(state.compareAndSwap("orders", accepted.reservedEpoch(), StateJson.of(STOPPED), AT))
                .isInstanceOf(io.tapstate.core.lifecycle.CasOutcome.Fenced.class);
    }

    @Test
    void actualCompletionClearsTheExactMarkerAndOnlyThenPublishesStopped() {
        state.create("orders", StateJson.of(RUNNING), AT);
        desired.save(new DesiredState("orders", STOPPED, "rev-1"));
        PipelineConverger loop = loop();
        loop.converge("orders");
        actuator.over.set(true);

        ConvergeResult done = loop.converge("orders");

        assertThat(done.status()).isEqualTo(ConvergeStatus.CONVERGED);
        assertThat(done.transitionFrom()).contains(RUNNING);
        assertThat(done.checkpoint().orElseThrow().epoch()).isEqualTo(2);
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(STOPPED));
        loop.converge("orders");
        assertThat(actuator.finishes.get()).isEqualTo(2);
    }

    @Test
    void aCompletionFencedByANewIntentDoesNotRebaseOrStartAReplacement() {
        state.create("orders", StateJson.of(RUNNING), AT);
        DesiredState original = new DesiredState("orders", STOPPED, "rev-1", true);
        desired.save(original);
        PipelineConverger loop = loop();
        loop.converge("orders");
        actuator.over.set(true);
        state.onBeforeComplete(() -> desired.save(new DesiredState("orders", RUNNING, "rev-2")));

        ConvergeResult fenced = loop.converge("orders");

        assertThat(fenced.status()).isEqualTo(ConvergeStatus.SUPERSEDED);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(state.read("orders").orElseThrow().epoch()).isEqualTo(1);
        assertThat(state.readStopReservation("orders").orElseThrow().originalDesired()).isEqualTo(original);
        assertThat(actuator.starts.get()).isZero();
    }

    @Test
    void aSupersedingStampedStartAtomicallyReplacesOldWorkBeforeOneFreshSubmission() {
        state.create("orders", StateJson.of(RUNNING), AT);
        desired.save(new DesiredState("orders", STOPPED, "rev-1", true));
        PipelineConverger loop = loop();
        loop.converge("orders");
        StopReservation old = state.readStopReservation("orders").orElseThrow();
        DesiredState successor = new DesiredState("orders", RUNNING, "rev-2", false, "assembly-2", true,
                old.reservedEpoch());
        desired.save(successor);
        actuator.over.set(true);
        state.onAfterStopSupersession(() -> actuator.over.set(false));

        assertThat(loop.converge("orders").status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        StopReservation replacement = state.readStopReservation("orders").orElseThrow();
        assertThat(replacement.token()).isNotEqualTo(old.token());
        assertThat(replacement.sourceEpoch()).isEqualTo(old.reservedEpoch());
        assertThat(replacement.reservedEpoch()).isEqualTo(old.reservedEpoch() + 1);
        assertThat(replacement.originalDesired()).isEqualTo(successor);
        assertThat(replacement.originalDesired().rebuiltAtStateEpoch()).isEqualTo(old.reservedEpoch());
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(state.completeStop(old, AT)).isEmpty();
        assertThat(actuator.retiring.get()).isEqualTo(1);
        assertThat(actuator.starts.get()).isZero();
        state.onAfterStopSupersession(() -> { });
        actuator.over.set(true);
        loop.converge("orders");
        loop.converge("orders");
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(actuator.starts.get()).isEqualTo(1);
        assertThat(state.stopReservations()).isEqualTo(2);
    }

    @Test
    void aRestartedConvergerUsesTheStoredOriginalStampAndStartsOnceAfterTheOldStop() {
        state.create("orders", StateJson.of(PAUSED), AT);
        DesiredState original = new DesiredState("orders", RUNNING, "rev-2", false, "assembly-2", true, 0L);
        desired.save(original);
        loop().converge("orders");
        StopReservation reserved = state.readStopReservation("orders").orElseThrow();
        assertThat(reserved.originalDesired().rebuiltAtStateEpoch()).isZero();
        assertThat(state.read("orders").orElseThrow().epoch()).isEqualTo(1);
        actuator.over.set(true);
        PipelineConverger restarted = new PipelineConverger(desired, state, actuator, Clock.fixed(AT, ZoneOffset.UTC));

        ConvergeResult resumed = restarted.converge("orders");
        restarted.converge("orders");

        assertThat(resumed.status()).isEqualTo(ConvergeStatus.CONVERGED);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(actuator.continuing.get()).isZero();
        assertThat(actuator.firstAttempts.get()).isEqualTo(1);
        assertThat(actuator.starts.get()).isEqualTo(1);
    }

    @Test
    void aMarkerBlocksAnOldCompletionCallbackAndAnUnrelatedPreparedStart() {
        state.create("orders", StateJson.of(RUNNING), AT);
        desired.save(new DesiredState("orders", STOPPED, "rev-1"));
        PipelineConverger loop = loop();
        loop.converge("orders");
        StopReservation marker = state.readStopReservation("orders").orElseThrow();

        assertThat(loop.markCompleted("orders").status()).isEqualTo(ConvergeStatus.SUPERSEDED);
        assertThat(state.readStopReservation("orders")).contains(marker);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(actuator.starts.get()).isZero();
        assertThat(actuator.finishes.get()).isEqualTo(1);
    }

    @Test
    void aCapableStoreCannotFallBackToASubjectlessLegacyStop() {
        state.create("orders", StateJson.of(NEW), AT);
        desired.save(new DesiredState("orders", STOPPED, "rev-1"));
        state.enableStops(desired, id -> null);
        RecordingActuator legacy = new RecordingActuator();

        assertThatThrownBy(() -> new PipelineConverger(desired, state, legacy, Clock.fixed(AT, ZoneOffset.UTC))
                .converge("orders")).isInstanceOf(IllegalStateException.class);
        assertThat(legacy.calls()).isEmpty();
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(NEW));
    }

    @Test
    void aSupersedingStartFromFailedKeepsItsStampedPurgeAfterAConvergerRestart() {
        assertSupersedingStartSurvivesRetirementRestart(FAILED);
    }

    @Test
    void aSupersedingStartFromCompletedKeepsItsStampedPurgeAfterAConvergerRestart() {
        assertSupersedingStartSurvivesRetirementRestart(COMPLETED);
    }

    @Test
    void aSupersedingStartFromRunningKeepsItsInheritedPurgeAfterAConvergerRestart() {
        assertSupersedingStartSurvivesRetirementRestart(RUNNING);
    }

    private void assertSupersedingStartSurvivesRetirementRestart(PipelineState originalActual) {
        state.create("orders", StateJson.of(originalActual), AT);
        actuator.authority = StopAuthority.standalone("cluster-a", 17);
        desired.save(new DesiredState("orders", STOPPED, "rev-1", true));
        PipelineConverger originalLoop = loop();
        assertThat(originalLoop.converge("orders").status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        StopReservation old = state.readStopReservation("orders").orElseThrow();
        DesiredState successor = new DesiredState("orders", RUNNING, "rev-2", true,
                "assembly-2", true, old.reservedEpoch());
        desired.save(successor);
        actuator.over.set(true);
        AssertionError processLost = new AssertionError("process lost after committing stop supersession");
        state.onAfterStopSupersession(() -> { throw processLost; });

        assertThatThrownBy(() -> originalLoop.converge("orders")).isSameAs(processLost);
        assertThat(state.read("orders").orElseThrow().epoch()).isGreaterThan(old.reservedEpoch());
        assertThat(desired.read("orders")).contains(successor);
        assertThat(successor.rebuiltAtStateEpoch()).isEqualTo(old.reservedEpoch());
        assertThat(actuator.starts.get()).isZero();
        assertThat(actuator.purges.get()).isZero();
        assertThat(state.completeStop(old, AT)).isEmpty();

        state.onAfterStopSupersession(() -> { });
        PipelineConverger restarted = new PipelineConverger(desired, state, actuator,
                Clock.fixed(AT, ZoneOffset.UTC));
        restarted.converge("orders");
        restarted.converge("orders");

        assertThat(actuator.starts.get())
                .as("the superseding start remains owed after process loss with actual %s", originalActual)
                .isEqualTo(1);
        assertThat(actuator.purges.get())
                .as("the exact successor carries the requested purge across the process boundary")
                .isEqualTo(1);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(desired.read("orders")).contains(successor);
        restarted.converge("orders");
        assertThat(actuator.starts.get()).isEqualTo(1);
        assertThat(actuator.purges.get()).isEqualTo(1);
    }

    @Test
    void aRestartedConvergerRebindsTheExactOldJobAndFencesItsPreviousOwnerCallback() {
        state.create("orders", StateJson.of(PAUSED), AT);
        WorkloadClaimKey key = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
        StopAuthority firstOwner = StopAuthority.claimed(new WorkloadClaimFence(
                key, new WorkloadOwner("node-a", "boot-a"), 5, 17, 1));
        StopAuthority nextOwner = StopAuthority.claimed(new WorkloadClaimFence(
                key, new WorkloadOwner("node-b", "boot-b"), 6, 17, 1));
        actuator.authority = firstOwner;
        actuator.existingJob = new StopReservation.JobIdentity("cluster-a", 99, "boot-a");
        DesiredState original = new DesiredState("orders", RUNNING, "rev-2", false,
                "assembly-2", true, 0L);
        desired.save(original);
        PipelineConverger firstLoop = loop();
        assertThat(firstLoop.converge("orders").status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        StopReservation accepted = state.readStopReservation("orders").orElseThrow();
        BooleanSupplier oldCompletionIsCurrent = actuator.lastCurrent;
        assertThat(oldCompletionIsCurrent.getAsBoolean()).isTrue();

        actuator.authority = nextOwner;
        PipelineConverger restarted = new PipelineConverger(desired, state, actuator,
                Clock.fixed(AT, ZoneOffset.UTC));
        assertThat(restarted.converge("orders").status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        StopReservation rebound = state.readStopReservation("orders").orElseThrow();

        assertThat(rebound.token()).isEqualTo(accepted.token());
        assertThat(rebound.sourceEpoch()).isEqualTo(accepted.sourceEpoch());
        assertThat(rebound.reservedEpoch()).isEqualTo(accepted.reservedEpoch() + 1);
        assertThat(rebound.originalDesired()).isEqualTo(original);
        assertThat(rebound.originalDesired().rebuiltAtStateEpoch()).isZero();
        assertThat(rebound.source()).isEqualTo(new StopReservation.Source(
                "cluster-a", new ObservationStore.Scope("inc-a", 17), actuator.existingJob));
        assertThat(rebound.writerAuthority()).isEqualTo(nextOwner);
        assertThat(nextOwner.executionGeneration()).isEqualTo(firstOwner.executionGeneration());
        assertThat(actuator.lastReservation).isEqualTo(rebound);
        assertThat(oldCompletionIsCurrent.getAsBoolean()).isFalse();
        assertThat(state.completeStop(accepted, AT)).isEmpty();
        assertThat(state.readStopReservation("orders")).contains(rebound);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(PAUSED));
        assertThat(state.read("orders").orElseThrow().epoch()).isEqualTo(rebound.reservedEpoch());
        assertThat(actuator.starts.get()).isZero();
        assertThat(actuator.purges.get()).isZero();

        actuator.over.set(true);
        PipelineConverger resumedAgain = new PipelineConverger(desired, state, actuator,
                Clock.fixed(AT, ZoneOffset.UTC));
        assertThat(resumedAgain.converge("orders").status()).isEqualTo(ConvergeStatus.CONVERGED);
        resumedAgain.converge("orders");

        assertThat(actuator.starts.get()).isEqualTo(1);
        assertThat(actuator.firstAttempts.get()).isEqualTo(1);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(oldCompletionIsCurrent.getAsBoolean()).isFalse();
        assertThat(state.stopReservations()).isEqualTo(1);
    }

    @Test
    void anUnstampedPausedResumeKeepsItsContinuationAcrossAConvergerRestart() {
        state.create("orders", StateJson.of(PAUSED), AT);
        actuator.authority = StopAuthority.standalone("cluster-a", 17);
        actuator.resumeNeedsRebuild = true;
        DesiredState original = new DesiredState("orders", RUNNING, "rev-1");
        desired.save(original);
        PipelineConverger firstLoop = loop();
        assertThat(firstLoop.converge("orders").status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        StopReservation accepted = state.readStopReservation("orders").orElseThrow();
        assertThat(accepted.originalDesired().rebuiltAtStateEpoch()).isNull();
        assertThat(accepted.originalDesired().purgeState()).isFalse();
        assertThat(actuator.continuing.get()).isEqualTo(1);

        actuator.over.set(true);
        PipelineConverger restarted = new PipelineConverger(desired, state, actuator,
                Clock.fixed(AT, ZoneOffset.UTC));
        assertThat(restarted.converge("orders").status()).isEqualTo(ConvergeStatus.CONVERGED);
        restarted.converge("orders");

        assertThat(actuator.continuing.get()).isEqualTo(2);
        assertThat(actuator.starts.get()).isEqualTo(1);
        assertThat(actuator.purges.get()).isZero();
        assertThat(desired.read("orders")).contains(original);
        assertThat(state.readStopReservation("orders")).isEmpty();
    }

    @Test
    void aPurgingPausedRebuildDoesNotCarryAResumeFloor() {
        state.create("orders", StateJson.of(PAUSED), AT);
        actuator.authority = StopAuthority.standalone("cluster-a", 17);
        actuator.resumeNeedsRebuild = true;
        DesiredState original = new DesiredState("orders", RUNNING, "rev-1", true);
        desired.save(original);
        PipelineConverger firstLoop = loop();
        assertThat(firstLoop.converge("orders").status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        assertThat(actuator.continuing.get()).isZero();

        actuator.over.set(true);
        assertThat(firstLoop.converge("orders").status()).isEqualTo(ConvergeStatus.CONVERGED);

        assertThat(actuator.continuing.get()).isZero();
        assertThat(actuator.purges.get()).isEqualTo(1);
        assertThat(actuator.starts.get()).isEqualTo(1);
        assertThat(desired.read("orders")).contains(original);
    }

    @Test
    void anUnfinishedOldTeardownCannotReplaceItsMarkerOrConsumeTheSuccessorStamp() {
        state.create("orders", StateJson.of(FAILED), AT);
        actuator.authority = StopAuthority.standalone("cluster-a", 17);
        actuator.existingJob = new StopReservation.JobIdentity("cluster-a", 99, "boot-a");
        desired.save(new DesiredState("orders", STOPPED, "rev-1", true));
        PipelineConverger originalLoop = loop();
        assertThat(originalLoop.converge("orders").status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        StopReservation old = state.readStopReservation("orders").orElseThrow();
        DesiredState successor = new DesiredState("orders", RUNNING, "rev-2", true,
                "assembly-2", true, old.reservedEpoch());
        desired.save(successor);
        PipelineConverger restarted = new PipelineConverger(desired, state, actuator,
                Clock.fixed(AT, ZoneOffset.UTC));

        assertThat(restarted.converge("orders").status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        assertThat(restarted.converge("orders").status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        assertThat(state.readStopReservation("orders")).contains(old);
        assertThat(state.read("orders").orElseThrow().epoch()).isEqualTo(old.reservedEpoch());
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
        assertThat(desired.read("orders")).contains(successor);
        assertThat(successor.rebuiltAtStateEpoch()).isEqualTo(old.reservedEpoch());
        assertThat(state.stopReservations()).isEqualTo(1);
        assertThat(actuator.starts.get()).isZero();
        assertThat(actuator.purges.get()).isZero();

        actuator.over.set(true);
        assertThat(restarted.converge("orders").status()).isEqualTo(ConvergeStatus.CONVERGED);
        restarted.converge("orders");

        assertThat(actuator.starts.get()).isEqualTo(1);
        assertThat(actuator.purges.get()).isEqualTo(1);
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(state.stopReservations()).isEqualTo(2);
        assertThat(state.completeStop(old, AT)).isEmpty();
    }

    @Test
    void aRetiringExistingJobCannotBeFinishedAfterItsCurrentOwnerProofIsLost() {
        state.create("orders", StateJson.of(PAUSED), AT);
        WorkloadClaimKey key = new WorkloadClaimKey("cluster-a", WorkloadClaimType.PIPELINE_ACTUATION, "orders");
        actuator.authority = StopAuthority.claimed(new WorkloadClaimFence(
                key, new WorkloadOwner("node-a", "boot-a"), 5, 17, 1));
        actuator.existingJob = new StopReservation.JobIdentity("cluster-a", 99, "boot-a");
        desired.save(new DesiredState("orders", STOPPED, "rev-1"));
        PipelineConverger originalLoop = loop();
        assertThat(originalLoop.converge("orders").status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        StopReservation old = state.readStopReservation("orders").orElseThrow();
        BooleanSupplier oldCompletionIsCurrent = actuator.lastCurrent;

        desired.save(new DesiredState("orders", STOPPED, "rev-2"));
        actuator.authority = null;
        actuator.over.set(true);
        assertThat(originalLoop.converge("orders").status()).isEqualTo(ConvergeStatus.SUPERSEDED);

        assertThat(actuator.finishes.get())
                .as("an unavailable current owner cannot authorize even the old job's native side effects")
                .isEqualTo(1);
        assertThat(oldCompletionIsCurrent.getAsBoolean()).isFalse();
        assertThat(state.readStopReservation("orders")).contains(old);
        assertThat(state.read("orders").orElseThrow().epoch()).isEqualTo(old.reservedEpoch());
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(PAUSED));
        assertThat(actuator.starts.get()).isZero();
        assertThat(actuator.purges.get()).isZero();
    }

    @Test
    void capacityWaitKeepsReplacementPendingAndAdvancesNoGeneration() {
        state.create("orders", StateJson.of(PAUSED), AT);
        actuator.authority = StopAuthority.standalone("cluster-a", 17);
        actuator.resumeNeedsRebuild = true;
        actuator.over.set(true);
        actuator.capacityUnavailable = true;
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        PipelineConverger loop = loop();

        assertThat(loop.converge("orders").status()).isEqualTo(ConvergeStatus.START_CAPACITY);
        StopReservation pending = state.readStopReservation("orders").orElseThrow();
        assertThat(pending.phase()).isEqualTo(StopReservation.Phase.REPLACEMENT_PENDING);
        assertThat(pending.counterPolicy()).isEqualTo(StopReservation.CounterPolicy.CONTINUE);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(STOPPED));
        assertThat(state.advances).isZero();
        assertThat(actuator.starts.get()).isZero();
        assertThat(loop.converge("orders").status()).isEqualTo(ConvergeStatus.START_CAPACITY);
        assertThat(state.readStopReservation("orders")).contains(pending);
        assertThat(state.advances).isZero();
    }

    @Test
    void aColdUnsubmittedAdmissionRetiresItsEmptySlotAndLeavesAGenerationHole() {
        state.create("orders", StateJson.of(PAUSED), AT);
        actuator.authority = StopAuthority.standalone("cluster-a", 17);
        actuator.resumeNeedsRebuild = true; actuator.over.set(true); actuator.crashAfterAdmission = true;
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        PipelineConverger first = loop();
        assertThatThrownBy(() -> first.converge("orders")).isInstanceOf(AssertionError.class)
                .hasMessage("process lost after durable admission");
        StopReservation admitted = state.readStopReservation("orders").orElseThrow();
        assertThat(admitted.phase()).isEqualTo(StopReservation.Phase.SUCCESSOR_ADMITTED);
        assertThat(admitted.successor().scope().executionGeneration()).isEqualTo(18);
        assertThat(actuator.starts.get()).isZero();

        actuator.crashAfterAdmission = false;
        PipelineConverger restarted = new PipelineConverger(desired, state, actuator, Clock.fixed(AT, ZoneOffset.UTC));
        assertThat(restarted.converge("orders").status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        StopReservation pending = state.readStopReservation("orders").orElseThrow();
        assertThat(pending.phase()).isEqualTo(StopReservation.Phase.REPLACEMENT_PENDING);
        assertThat(pending.source()).isEqualTo(admitted.source());
        assertThat(pending.token()).isEqualTo(admitted.token());
        assertThat(restarted.converge("orders").status()).isEqualTo(ConvergeStatus.CONVERGED);
        assertThat(state.advances).isEqualTo(2);
        assertThat(actuator.authority.executionGeneration()).isEqualTo(19);
        assertThat(actuator.starts.get()).isEqualTo(1);
    }

    @Test
    void aSubmittedUnboundSuccessorIsAdoptedWithoutAnotherAdvanceOrSubmission() {
        state.create("orders", StateJson.of(PAUSED), AT);
        actuator.authority = StopAuthority.standalone("cluster-a", 17);
        actuator.resumeNeedsRebuild = true; actuator.over.set(true); actuator.omitSubmittedIdentity = true;
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        assertThat(loop().converge("orders").status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        StopReservation admitted = state.readStopReservation("orders").orElseThrow();
        StopReservation.JobIdentity submitted = actuator.nativeJob;
        assertThat(admitted.phase()).isEqualTo(StopReservation.Phase.SUCCESSOR_ADMITTED);
        assertThat(state.advances).isEqualTo(1);
        assertThat(actuator.starts.get()).isEqualTo(1);

        PipelineConverger restarted = new PipelineConverger(desired, state, actuator, Clock.fixed(AT, ZoneOffset.UTC));
        assertThat(restarted.converge("orders").status()).isEqualTo(ConvergeStatus.CONVERGED);
        assertThat(actuator.nativeJob).isEqualTo(submitted);
        assertThat(actuator.adoptions.get()).isGreaterThanOrEqualTo(1);
        assertThat(state.advances).isEqualTo(1);
        assertThat(actuator.starts.get()).isEqualTo(1);
        assertThat(state.readStopReservation("orders")).isEmpty();
    }

    @Test
    void boundBusinessKeepsConvergingWhileItsReadableContinuationIsUnavailable() {
        state.create("orders", StateJson.of(PAUSED), AT);
        actuator.authority = StopAuthority.standalone("cluster-a", 17);
        actuator.resumeNeedsRebuild = true; actuator.over.set(true); actuator.telemetryReady = false;
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        PipelineConverger loop = loop();
        assertThat(loop.converge("orders").status()).isEqualTo(ConvergeStatus.CONVERGED);
        StopReservation bound = state.readStopReservation("orders").orElseThrow();
        assertThat(bound.phase()).isEqualTo(StopReservation.Phase.SUCCESSOR_BOUND);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(loop.converge("orders").status()).isEqualTo(ConvergeStatus.CONVERGED);
        assertThat(state.readStopReservation("orders")).contains(bound);
        assertThat(actuator.starts.get()).isEqualTo(1);
        assertThat(state.advances).isEqualTo(1);

        actuator.telemetryReady = true;
        assertThat(loop.converge("orders").status()).isEqualTo(ConvergeStatus.CONVERGED);
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(actuator.starts.get()).isEqualTo(1);
    }

    @Test
    void aNewStopPreemptsOnlyTheActualBoundSuccessorWhileTelemetryIsUnavailable() {
        state.create("orders", StateJson.of(PAUSED), AT);
        actuator.authority = StopAuthority.standalone("cluster-a", 17);
        actuator.existingJob = new StopReservation.JobIdentity("cluster-a", 99, "source-boot");
        actuator.resumeNeedsRebuild = true; actuator.over.set(true); actuator.telemetryReady = false;
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        PipelineConverger loop = loop(); loop.converge("orders");
        StopReservation bound = state.readStopReservation("orders").orElseThrow();
        StopReservation.JobIdentity target = bound.successor().job();
        actuator.finishedJobs.clear();
        desired.save(new DesiredState("orders", STOPPED, "rev-stop"));

        assertThat(loop.converge("orders").status()).isEqualTo(ConvergeStatus.SUPERSEDED);
        assertThat(actuator.finishedJobs).containsExactly(target);
        assertThat(actuator.finishedJobs).doesNotContain(bound.source().oldJob());
        assertThat(actuator.nativeJob).isNull();
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(actuator.starts.get()).isEqualTo(1);
        assertThat(state.advances).isEqualTo(1);
    }

    @Test
    void aLegacyMarkerIsExplicitlyPromotedUsingItsProtectedActualAndOriginalTuple() {
        state.create("orders", StateJson.of(PAUSED), AT);
        actuator.authority = StopAuthority.standalone("cluster-a", 17);
        DesiredState original = new DesiredState("orders", RUNNING, "rev-1"); desired.save(original);
        loop();
        StopReservation legacy = new StopReservation("orders", "legacy-work", 0, 1, original,
                new StopReservation.NoJob("cluster-a", actuator.authority));
        state.reserveStop(state.read("orders").orElseThrow(), legacy, AT).orElseThrow();

        PipelineConverger restarted = new PipelineConverger(desired, state, actuator, Clock.fixed(AT, ZoneOffset.UTC));
        assertThat(restarted.converge("orders").status()).isEqualTo(ConvergeStatus.STOP_PENDING);
        StopReservation promoted = state.readStopReservation("orders").orElseThrow();
        assertThat(state.promotions).isEqualTo(1);
        assertThat(promoted.legacy()).isFalse();
        assertThat(promoted.counterPolicy()).isEqualTo(StopReservation.CounterPolicy.CONTINUE);
        assertThat(promoted.originalDesired()).isEqualTo(original);
        assertThat(promoted.sourceEpoch()).isEqualTo(legacy.sourceEpoch());
        assertThat(promoted.token()).isEqualTo(legacy.token());
        assertThat(actuator.starts.get()).isZero();
    }

    @Test
    void aCompletedBoundSuccessorKeepsItsTerminalActualWhileTelemetryIsUnavailable() {
        terminalSuccessorKeepsItsActualAndFloor(COMPLETED);
    }

    @Test
    void aFailedBoundSuccessorKeepsItsTerminalActualWhileTelemetryIsUnavailable() {
        terminalSuccessorKeepsItsActualAndFloor(FAILED);
    }

    @Test
    void aCompletedBoundSuccessorKeepsItsTerminalActualAfterNativeHistoryDisappears() {
        terminalHistoryDisappearanceKeepsItsActualAndFloor(COMPLETED);
    }

    @Test
    void aFailedBoundSuccessorKeepsItsTerminalActualAfterNativeHistoryDisappears() {
        terminalHistoryDisappearanceKeepsItsActualAndFloor(FAILED);
    }

    @Test
    void aCodedReplacementRefusalBeforeAdmissionFailsOnceWithoutAdvancingGeneration() {
        PipelineConverger loop = rebuildingResume();
        DesiredState original = desired.read("orders").orElseThrow();
        TapstateException refusal = replacementRefusal();
        actuator.beforeAdmissionRefusal = refusal;

        ConvergeResult failed = convergeWithoutEscapingRefusal(loop);

        assertThat(failed.status()).isEqualTo(ConvergeStatus.FAILED);
        assertThat(failed.failure()).contains(refusal);
        assertThat(failed.transitionFrom()).contains(STOPPED);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(actuator.lastAdmission).isNull();
        assertThat(actuator.nativeJob).isNull();
        assertThat(state.advances).isZero();
        assertThat(actuator.authority.executionGeneration()).isEqualTo(17);

        loop.converge("orders");
        new PipelineConverger(desired, state, actuator, Clock.fixed(AT, ZoneOffset.UTC)).converge("orders");

        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
        assertThat(actuator.replacementPreparations.get()).isEqualTo(1);
        assertThat(state.advances).isZero();
        assertThat(actuator.starts.get()).isZero();
        assertThat(desired.read("orders")).contains(original);
    }

    @Test
    void aCodedDagRefusalAfterAdmissionFailsWithoutRetryingEmptyGenerationHoles() {
        PipelineConverger loop = rebuildingResume();
        DesiredState original = desired.read("orders").orElseThrow();
        TapstateException refusal = replacementRefusal();
        actuator.afterAdmissionRefusal = refusal;

        ConvergeResult failed = convergeWithoutEscapingRefusal(loop);

        assertThat(failed.status()).isEqualTo(ConvergeStatus.FAILED);
        assertThat(failed.failure()).contains(refusal);
        assertThat(failed.transitionFrom()).contains(STOPPED);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
        assertThat(actuator.lastAdmission.phase()).isEqualTo(StopReservation.Phase.SUCCESSOR_ADMITTED);
        assertThat(actuator.lastAdmission.originalDesired()).isEqualTo(original);
        assertThat(actuator.lastAdmission.successor().scope().executionGeneration()).isEqualTo(18);
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(actuator.nativeJob).isNull();
        assertThat(state.advances).isEqualTo(1);
        assertThat(actuator.starts.get()).isZero();

        loop.converge("orders");
        new PipelineConverger(desired, state, actuator, Clock.fixed(AT, ZoneOffset.UTC)).converge("orders");

        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(actuator.replacementPreparations.get()).isEqualTo(1);
        assertThat(state.advances).isEqualTo(1);
        assertThat(actuator.authority.executionGeneration()).isEqualTo(18);
        assertThat(actuator.starts.get()).isZero();
        assertThat(desired.read("orders")).contains(original);
    }

    @Test
    void aCodedActivationRefusalKeepsTheExactBoundFailedJobAndOriginalCause() {
        PipelineConverger loop = rebuildingResume();
        DesiredState original = desired.read("orders").orElseThrow();
        TapstateException refusal = replacementRefusal();
        actuator.activationRefusal = refusal;
        actuator.telemetryReady = false;

        ConvergeResult failed = convergeWithoutEscapingRefusal(loop);

        assertThat(failed.status()).isEqualTo(ConvergeStatus.FAILED);
        assertThat(failed.failure()).contains(refusal);
        assertThat(failed.transitionFrom()).contains(STOPPED);
        assertThat(actuator.failure("orders")).isEmpty();
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
        StopReservation bound = state.readStopReservation("orders").orElseThrow();
        assertThat(bound.phase()).isEqualTo(StopReservation.Phase.SUCCESSOR_BOUND);
        assertThat(bound.token()).isEqualTo(actuator.lastAdmission.token());
        assertThat(bound.source()).isEqualTo(actuator.lastAdmission.source());
        assertThat(bound.originalDesired()).isEqualTo(original);
        assertThat(bound.successor().scope()).isEqualTo(actuator.lastAdmission.successor().scope());
        assertThat(bound.successor().job()).isEqualTo(actuator.nativeJob).isNotEqualTo(bound.source().oldJob());
        assertThat(bound.reservedEpoch()).isGreaterThan(actuator.lastAdmission.reservedEpoch());
        assertThat(state.advances).isEqualTo(1);
        assertThat(actuator.starts.get()).isEqualTo(1);

        loop.converge("orders");
        PipelineConverger restarted = new PipelineConverger(desired, state, actuator, Clock.fixed(AT, ZoneOffset.UTC));
        restarted.converge("orders");

        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
        assertThat(state.readStopReservation("orders")).contains(bound);
        assertThat(actuator.replacementPreparations.get()).isEqualTo(1);
        assertThat(state.advances).isEqualTo(1);
        assertThat(actuator.starts.get()).isEqualTo(1);

        actuator.telemetryReady = true;
        restarted.converge("orders");

        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
        assertThat(actuator.nativeJob).isEqualTo(bound.successor().job());
        assertThat(state.advances).isEqualTo(1);
        assertThat(actuator.starts.get()).isEqualTo(1);
        assertThat(desired.read("orders")).contains(original);
    }

    private PipelineConverger rebuildingResume() {
        state.create("orders", StateJson.of(PAUSED), AT);
        actuator.authority = StopAuthority.standalone("cluster-a", 17);
        actuator.existingJob = new StopReservation.JobIdentity("cluster-a", 99, "source-boot");
        actuator.resumeNeedsRebuild = true;
        actuator.over.set(true);
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        return loop();
    }

    private static TapstateException replacementRefusal() {
        return new TapstateException(LifecycleError.PIPELINE_NOT_RUNNABLE, Map.of("pipeline", "orders"), null);
    }

    private static ConvergeResult convergeWithoutEscapingRefusal(PipelineConverger loop) {
        AtomicReference<ConvergeResult> result = new AtomicReference<>();
        assertThatCode(() -> result.set(loop.converge("orders")))
                .as("a coded replacement refusal must be returned as the pipeline failure")
                .doesNotThrowAnyException();
        return result.get();
    }

    private void terminalSuccessorKeepsItsActualAndFloor(PipelineState terminal) {
        state.create("orders", StateJson.of(PAUSED), AT);
        actuator.authority = StopAuthority.standalone("cluster-a", 17);
        actuator.resumeNeedsRebuild = true; actuator.over.set(true); actuator.telemetryReady = false;
        desired.save(new DesiredState("orders", RUNNING, "rev-1"));
        PipelineConverger loop = loop();
        assertThat(loop.converge("orders").status()).isEqualTo(ConvergeStatus.CONVERGED);
        StopReservation running = state.readStopReservation("orders").orElseThrow();
        StopReservation.Source source = running.source();
        StopReservation.JobIdentity target = running.successor().job();
        actuator.terminalState = terminal;

        loop.converge("orders");

        assertThat(state.read("orders").orElseThrow().stateJson())
                .as("a matching native %s job cannot remain a running actual", terminal).isEqualTo(StateJson.of(terminal));
        StopReservation ended = state.readStopReservation("orders").orElseThrow();
        assertThat(ended.phase()).isEqualTo(StopReservation.Phase.SUCCESSOR_BOUND);
        assertThat(ended.source()).isEqualTo(source);
        assertThat(ended.successor().job()).isEqualTo(target);
        assertThat(ended.token()).isEqualTo(running.token());
        assertThat(ended.reservedEpoch()).isGreaterThan(running.reservedEpoch());
        assertThat(actuator.starts.get()).isEqualTo(1);
        assertThat(state.advances).isEqualTo(1);
        loop.converge("orders");
        assertThat(state.readStopReservation("orders")).contains(ended);
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(terminal));

        actuator.telemetryReady = true;
        loop.converge("orders");

        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(state.read("orders").orElseThrow().stateJson())
                .as("a later durable continuation ACK cannot resurrect a terminal job").isEqualTo(StateJson.of(terminal));
        assertThat(actuator.nativeJob).isEqualTo(target);
        assertThat(actuator.starts.get()).isEqualTo(1);
        assertThat(state.advances).isEqualTo(1);
    }

    private void terminalHistoryDisappearanceKeepsItsActualAndFloor(PipelineState terminal) {
        PipelineConverger loop = rebuildingResume();
        DesiredState original = desired.read("orders").orElseThrow();
        actuator.telemetryReady = false;
        loop.converge("orders");
        StopReservation running = state.readStopReservation("orders").orElseThrow();
        actuator.terminalState = terminal;
        loop.converge("orders");
        CheckpointDoc concluded = state.read("orders").orElseThrow();
        StopReservation ended = state.readStopReservation("orders").orElseThrow();
        assertThat(concluded.stateJson()).isEqualTo(StateJson.of(terminal));
        assertThat(ended.phase()).isEqualTo(StopReservation.Phase.SUCCESSOR_BOUND);
        assertThat(ended.source()).isEqualTo(running.source());
        assertThat(ended.successor()).isEqualTo(running.successor());
        int finishes = actuator.finishes.get();
        int adoptions = actuator.adoptions.get();

        // Removing native history does not undo the already recorded business conclusion.
        actuator.nativeJob = null;
        actuator.terminalState = null;
        actuator.carrying = false;
        ConvergeResult absent = loop.converge("orders");

        assertThat(state.read("orders").orElseThrow().stateJson())
                .as("native history loss cannot retire a concluded %s execution into a fresh start", terminal)
                .isEqualTo(StateJson.of(terminal));
        assertThat(absent.status()).isEqualTo(ConvergeStatus.CONVERGED);
        assertThat(state.read("orders")).contains(concluded);
        assertThat(state.readStopReservation("orders")).contains(ended);
        PipelineConverger restarted = new PipelineConverger(desired, state, actuator, Clock.fixed(AT, ZoneOffset.UTC));
        assertThat(restarted.converge("orders").status()).isEqualTo(ConvergeStatus.CONVERGED);
        assertThat(state.read("orders")).contains(concluded);
        assertThat(state.readStopReservation("orders")).contains(ended);
        assertThat(actuator.finishes.get()).isEqualTo(finishes);
        assertThat(actuator.adoptions.get()).isEqualTo(adoptions);
        assertThat(actuator.replacementPreparations.get()).isEqualTo(1);
        assertThat(state.advances).isEqualTo(1);
        assertThat(actuator.starts.get()).isEqualTo(1);
        assertThat(actuator.authority.executionGeneration()).isEqualTo(18);

        actuator.telemetryReady = true;
        assertThat(restarted.converge("orders").status()).isEqualTo(ConvergeStatus.CONVERGED);

        assertThat(state.readStopReservation("orders")).isEmpty();
        assertThat(state.read("orders").orElseThrow().stateJson()).isEqualTo(StateJson.of(terminal));
        assertThat(actuator.nativeJob).isNull();
        assertThat(actuator.replacementPreparations.get()).isEqualTo(1);
        assertThat(state.advances).isEqualTo(1);
        assertThat(actuator.starts.get()).isEqualTo(1);
        assertThat(desired.read("orders")).contains(original);
    }

    /** Contract double with exact guarded marker writes; native/capacity facts stay in the actuator. */
    private final class HandoffState implements StateStore {
        private final Map<String, CheckpointDoc> docs = new HashMap<>();
        private final Map<String, StopReservation> markers = new HashMap<>();
        private DesiredStore intents;
        private Function<String, StopAuthority> authorities;
        private Runnable beforeComplete = () -> { }, afterSupersession = () -> { };
        private int reservations, advances, promotions;

        void enableStops(DesiredStore value, Function<String, StopAuthority> current) { intents = value; authorities = current; }
        @Override public boolean supportsStopReservations() { return intents != null; }
        @Override public Optional<CheckpointDoc> read(String id) { return Optional.ofNullable(docs.get(id)); }
        @Override public void create(String id, String json, Instant at) { docs.put(id, CheckpointDoc.initial(id, json, at)); }
        @Override public void delete(String id) { docs.remove(id); markers.remove(id); }
        @Override public CasOutcome compareAndSwap(String id, long epoch, String json, Instant at) {
            if (markers.containsKey(id)) { return new CasOutcome.Fenced(docs.get(id).epoch()); }
            CasOutcome result = EpochCas.swap(docs.get(id), epoch, json, at);
            if (result instanceof CasOutcome.Applied applied) { docs.put(id, applied.next()); }
            return result;
        }
        @Override public Optional<StopReservation> readStopReservation(String id) { return Optional.ofNullable(markers.get(id)); }
        @Override public Optional<StopReservation> reserveStop(CheckpointDoc expected, StopReservation proposal, Instant at) {
            if (!expected.equals(docs.get(expected.pipelineId())) || markers.containsKey(expected.pipelineId())
                    || !intent(proposal.originalDesired()) || !authority(proposal.pipelineId(), proposal.writerAuthority())) {
                return Optional.empty();
            }
            docs.put(expected.pipelineId(), ((CasOutcome.Applied) EpochCas.swap(expected, expected.epoch(), expected.stateJson(), at)).next());
            markers.put(expected.pipelineId(), proposal); reservations++; return Optional.of(proposal);
        }
        @Override public Optional<StopReservation> promoteStopReservation(StopReservation expected, DesiredState current,
                StopAuthority writer, Instant at) {
            if (!expected.legacy() || !exact(expected) || !intent(current) || !authority(expected.pipelineId(), writer)) {
                return Optional.empty();
            }
            StopReservation next = changed(expected, StopReservation.Phase.STOPPING,
                    StopReservation.CounterPolicy.freeze(docs.get(expected.pipelineId()), expected.originalDesired()), writer, null);
            promotions++; return Optional.of(write(expected, next, null, at));
        }
        @Override public Optional<StopReservation> rebindStop(StopReservation expected, StopAuthority writer, Instant at) {
            if (!exact(expected) || !intent(expected.originalDesired()) || !authority(expected.pipelineId(), writer)) {
                return Optional.empty();
            }
            StopReservation next = expected.rebind(writer, expected.reservedEpoch() + 1);
            return Optional.of(write(expected, next, null, at));
        }
        @Override public Optional<StopReservation> replaceStop(StopReservation expected, StopReservation next, Instant at) {
            if (!exact(expected) || expected.originalDesired().equals(next.originalDesired())
                    || expected.token().equals(next.token()) || !intent(next.originalDesired())
                    || !authority(next.pipelineId(), next.writerAuthority())) { return Optional.empty(); }
            write(expected, next, null, at); reservations++; afterSupersession.run(); return Optional.of(next);
        }
        @Override public Optional<CheckpointDoc> completeStop(StopReservation expected, Instant at) {
            beforeComplete.run();
            if (!guard(expected)) { return Optional.empty(); }
            CheckpointDoc done = advance(expected, StateJson.of(STOPPED), at);
            markers.remove(expected.pipelineId()); return Optional.of(done);
        }
        @Override public Optional<CheckpointDoc> retireStop(StopReservation expected, DesiredState next,
                StopAuthority writer, Instant at) {
            if (!exact(expected) || !intent(next) || !authority(expected.pipelineId(), writer)) { return Optional.empty(); }
            CheckpointDoc done = advance(expected, docs.get(expected.pipelineId()).stateJson(), at);
            markers.remove(expected.pipelineId()); afterSupersession.run(); return Optional.of(done);
        }
        @Override public Optional<StopReservation> markReplacementPending(StopReservation expected, Instant at) {
            if (!guard(expected) || expected.phase() != StopReservation.Phase.STOPPING) { return Optional.empty(); }
            return Optional.of(write(expected, changed(expected, StopReservation.Phase.REPLACEMENT_PENDING,
                    expected.counterPolicy(), expected.writerAuthority(), null), StateJson.of(STOPPED), at));
        }
        @Override public Optional<SuccessorAdmission> admitSuccessor(StopReservation expected, String incarnation,
                String boot, Set<String> executionMembers, Instant at) {
            if (!guard(expected) || expected.phase() != StopReservation.Phase.REPLACEMENT_PENDING) { return Optional.empty(); }
            long generation = expected.writerAuthority() == null ? 1 : expected.writerAuthority().executionGeneration() + 1;
            StopAuthority writer;
            Optional<WorkloadClaim> fullClaim = Optional.empty();
            if (expected.writerAuthority() != null && expected.writerAuthority().claim() != null) {
                if (executionMembers.isEmpty()) { throw new AssertionError("claimed fixture admission needs its planned members"); }
                WorkloadClaimFence old = expected.writerAuthority().claim();
                WorkloadClaim claim = new WorkloadClaim(old.key(), old.owner(), old.claimGeneration(), generation,
                        old.topologyRevision(), AT.plusSeconds(3600), generation, old.claimGeneration(),
                        Set.copyOf(executionMembers), 0, false);
                fullClaim = Optional.of(claim); writer = StopAuthority.claimed(WorkloadClaimFence.from(claim));
            } else { writer = StopAuthority.standalone(expected.source().clusterId(), generation); }
            StopReservation slot = changed(expected, StopReservation.Phase.SUCCESSOR_ADMITTED, expected.counterPolicy(), writer,
                    new StopReservation.Successor(new ObservationStore.Scope(incarnation, generation), boot, null));
            write(expected, slot, null, at); actuator.authority = writer; advances++;
            return Optional.of(new SuccessorAdmission(slot, fullClaim));
        }
        @Override public Optional<StopReservation> bindSuccessor(StopReservation expected, ObservationStore.Scope scope,
                StopReservation.JobIdentity job, Instant at) {
            if (!guard(expected) || expected.phase() != StopReservation.Phase.SUCCESSOR_ADMITTED
                    || !expected.successor().scope().equals(scope)
                    || !expected.successor().submissionBootId().equals(job.bootId())) { return Optional.empty(); }
            return Optional.of(write(expected, changed(expected, StopReservation.Phase.SUCCESSOR_BOUND,
                    expected.counterPolicy(), expected.writerAuthority(),
                    new StopReservation.Successor(scope, expected.successor().submissionBootId(), job)), StateJson.of(RUNNING), at));
        }
        @Override public Optional<StopReservation> retireSuccessor(StopReservation expected, SuccessorEnd end, Instant at) {
            if (!guard(expected) || expected.successor() == null) { return Optional.empty(); }
            boolean matching = switch (end) {
                case SuccessorEnd.Absent absent -> expected.phase() == StopReservation.Phase.SUCCESSOR_ADMITTED
                        && expected.successor().scope().equals(absent.scope())
                        && expected.successor().submissionBootId().equals(absent.submissionBootId());
                case SuccessorEnd.Terminal terminal -> expected.phase() == StopReservation.Phase.SUCCESSOR_BOUND
                        && expected.successor().scope().equals(terminal.scope()) && expected.successor().job().equals(terminal.job());
            };
            if (!matching) { return Optional.empty(); }
            return Optional.of(write(expected, changed(expected, StopReservation.Phase.REPLACEMENT_PENDING,
                    expected.counterPolicy(), expected.writerAuthority(), null), StateJson.of(STOPPED), at));
        }
        @Override public Optional<StopReservation> recordSuccessorTerminal(StopReservation expected,
                SuccessorEnd.Terminal end, PipelineState terminal, Instant at) {
            if (!guard(expected) || expected.phase() != StopReservation.Phase.SUCCESSOR_BOUND
                    || !expected.successor().scope().equals(end.scope()) || !expected.successor().job().equals(end.job())) {
                return Optional.empty();
            }
            String actual = docs.get(expected.pipelineId()).stateJson();
            if (actual.equals(StateJson.of(terminal))) { return Optional.of(expected); }
            if (!actual.equals(StateJson.of(RUNNING))) { return Optional.empty(); }
            return Optional.of(write(expected, changed(expected, StopReservation.Phase.SUCCESSOR_BOUND,
                    expected.counterPolicy(), expected.writerAuthority(), expected.successor()), StateJson.of(terminal), at));
        }
        @Override public Optional<CheckpointDoc> failReplacement(StopReservation expected,
                Optional<StopReservation.JobIdentity> factualJob, Instant at) {
            if (expected.legacy() || expected.phase() != StopReservation.Phase.REPLACEMENT_PENDING
                    && expected.phase() != StopReservation.Phase.SUCCESSOR_ADMITTED) {
                throw new IllegalArgumentException("replacement refusal requires the pending or admitted marker");
            }
            if (!guard(expected) || !docs.get(expected.pipelineId()).stateJson().equals(StateJson.of(STOPPED))) {
                return Optional.empty();
            }
            StopReservation.Successor slot = expected.successor();
            if (factualJob.isPresent() && (slot == null
                    || !slot.submissionBootId().equals(factualJob.orElseThrow().bootId())
                    || !expected.source().clusterId().equals(factualJob.orElseThrow().clusterId())
                    || factualJob.orElseThrow().equals(expected.source().oldJob()))) { return Optional.empty(); }
            if (factualJob.isEmpty()) {
                CheckpointDoc failed = advance(expected, StateJson.of(FAILED), at);
                markers.remove(expected.pipelineId()); return Optional.of(failed);
            }
            write(expected, changed(expected, StopReservation.Phase.SUCCESSOR_BOUND,
                    expected.counterPolicy(), expected.writerAuthority(),
                    new StopReservation.Successor(slot.scope(), slot.submissionBootId(), factualJob.orElseThrow())),
                    StateJson.of(FAILED), at);
            return Optional.of(docs.get(expected.pipelineId()));
        }
        @Override public Optional<CheckpointDoc> completeHandoff(StopReservation expected, HandoffIdentity ready, Instant at) {
            if (!guard(expected) || expected.phase() != StopReservation.Phase.SUCCESSOR_BOUND
                    || expected.counterPolicy() == StopReservation.CounterPolicy.CONTINUE && !expected.handoffIdentity().equals(ready)) {
                return Optional.empty();
            }
            CheckpointDoc done = advance(expected, docs.get(expected.pipelineId()).stateJson(), at);
            markers.remove(expected.pipelineId()); return Optional.of(done);
        }
        private StopReservation changed(StopReservation old, StopReservation.Phase phase,
                StopReservation.CounterPolicy policy, StopAuthority writer, StopReservation.Successor slot) {
            return new StopReservation(old.pipelineId(), old.token(), old.sourceEpoch(), old.reservedEpoch() + 1,
                    old.originalDesired(), old.source(), phase, policy, writer, slot, 16);
        }
        private StopReservation write(StopReservation old, StopReservation next, String json, Instant at) {
            advance(old, json == null ? docs.get(old.pipelineId()).stateJson() : json, at);
            markers.put(old.pipelineId(), next); return next;
        }
        private CheckpointDoc advance(StopReservation expected, String json, Instant at) {
            CheckpointDoc next = ((CasOutcome.Applied) EpochCas.swap(docs.get(expected.pipelineId()),
                    expected.reservedEpoch(), json, at)).next(); docs.put(expected.pipelineId(), next); return next;
        }
        private boolean exact(StopReservation value) {
            return value.equals(markers.get(value.pipelineId())) && docs.get(value.pipelineId()).epoch() == value.reservedEpoch();
        }
        private boolean guard(StopReservation value) {
            return exact(value) && intent(value.originalDesired()) && authority(value.pipelineId(), value.writerAuthority());
        }
        private boolean intent(DesiredState value) { return intents.read(value.pipelineId()).filter(value::equals).isPresent(); }
        private boolean authority(String id, StopAuthority value) { return java.util.Objects.equals(value, authorities.apply(id)); }
        void onBeforeComplete(Runnable hook) { beforeComplete = hook; }
        void onAfterStopSupersession(Runnable hook) { afterSupersession = hook; }
        int stopReservations() { return reservations; }
    }

    private static final class StopActuator implements LifecycleActuator {
        private final AtomicBoolean over = new AtomicBoolean();
        private final AtomicInteger starts = new AtomicInteger();
        private final AtomicInteger finishes = new AtomicInteger();
        private final AtomicInteger continuing = new AtomicInteger();
        private final AtomicInteger firstAttempts = new AtomicInteger();
        private final AtomicInteger retiring = new AtomicInteger();
        private final AtomicInteger purges = new AtomicInteger();
        private boolean carrying;
        private boolean resumeNeedsRebuild;
        private boolean capacityUnavailable;
        private boolean crashAfterAdmission;
        private boolean omitSubmittedIdentity;
        private boolean telemetryReady = true;
        private TapstateException beforeAdmissionRefusal;
        private TapstateException afterAdmissionRefusal;
        private TapstateException activationRefusal;
        private PipelineState terminalState;
        private StopReservation.JobIdentity nativeJob;
        private StopReservation lastAdmission;
        private final AtomicInteger replacementPreparations = new AtomicInteger();
        private final AtomicInteger adoptions = new AtomicInteger();
        private final List<StopReservation.JobIdentity> finishedJobs = new ArrayList<>();
        private StopAuthority authority;
        private StopReservation.JobIdentity existingJob;
        private BooleanSupplier lastCurrent;
        private StopReservation lastReservation;
        private Runnable onFinish = () -> { };
        private Runnable onPrepareReplacement = () -> { };
        private Runnable onAdopt = () -> { };

        @Override public void start(String id) { starts.incrementAndGet(); carrying = true; }
        @Override public void pause(String id) { throw new AssertionError("unexpected pause"); }
        @Override public void resume(String id) { throw new AssertionError("unexpected resume"); }
        @Override public void stop(String id, boolean purge) { throw new AssertionError("legacy stop was bypassed"); }
        @Override public Optional<Throwable> failure(String id) { return Optional.empty(); }
        @Override public Optional<Throwable> lost(String id) { return Optional.empty(); }
        @Override public boolean isCarryingAJob(String id) { return carrying; }
        @Override public boolean needsRebuildOnResume(String id) { return resumeNeedsRebuild; }
        @Override public Optional<StopReservation.Subject> stopSubject(String id) {
            return Optional.of(existingJob == null ? new StopReservation.NoJob("cluster-a", authority)
                    : new StopReservation.ExistingJob("inc-a", authority.executionGeneration(), existingJob, authority));
        }
        @Override public Optional<StopAuthority> stopAuthority(String id) { return Optional.ofNullable(authority); }
        @Override public PreparedReplacement prepareReplacement(StopReservation pending, ReplacementAdmission admission,
                Predicate<StopReservation> current) {
            onPrepareReplacement.run();
            replacementPreparations.incrementAndGet();
            if (capacityUnavailable) { throw new StartDeferred(StartDeferred.Reason.CAPACITY); }
            if (beforeAdmissionRefusal != null) { throw beforeAdmissionRefusal; }
            StopReservation admitted = admission.admit(authority, "inc-a", "submit-boot", java.util.Set.of("fixture-data-member"))
                    .orElseThrow(() -> new AssertionError("unexpected admission fence")).reservation();
            lastAdmission = admitted;
            if (crashAfterAdmission) { throw new AssertionError("process lost after durable admission"); }
            if (afterAdmissionRefusal != null) { throw afterAdmissionRefusal; }
            return new PreparedReplacement() {
                @Override public StopReservation admitted() { return admitted; }
                @Override public void submit() {
                    assertThat(current.test(admitted)).isTrue();
                    starts.incrementAndGet(); carrying = true;
                    nativeJob = new StopReservation.JobIdentity("cluster-a",
                            100 + admitted.successor().scope().executionGeneration(), admitted.successor().submissionBootId());
                    if (activationRefusal != null) {
                        // Native cancellation retains the matching terminal job, but not the start refusal.
                        carrying = false; terminalState = FAILED;
                        throw activationRefusal;
                    }
                }
                @Override public Optional<StopReservation.JobIdentity> submittedJob() {
                    return omitSubmittedIdentity ? Optional.empty() : Optional.ofNullable(nativeJob);
                }
                @Override public void close() { }
            };
        }
        @Override public Optional<SuccessorInspection> inspectSuccessor(StopReservation marker, BooleanSupplier current) {
            return current.getAsBoolean() ? Optional.of(new SuccessorInspection(Optional.ofNullable(nativeJob), Optional.ofNullable(terminalState)))
                    : Optional.empty();
        }
        @Override public boolean adoptSuccessor(StopReservation marker, BooleanSupplier current) {
            onAdopt.run();
            if (!current.getAsBoolean() || nativeJob == null || !nativeJob.equals(marker.successor().job())) { return false; }
            adoptions.incrementAndGet(); carrying = true; return true;
        }
        @Override public Optional<HandoffIdentity> continuationReady(StopReservation marker, BooleanSupplier current) {
            return telemetryReady && current.getAsBoolean() ? Optional.of(marker.handoffIdentity()) : Optional.empty();
        }

        @Override public boolean finishStop(StopReservation reservation, boolean carry, boolean first,
                boolean retire, BooleanSupplier current) {
            onFinish.run();
            finishes.incrementAndGet();
            if (carry) { continuing.incrementAndGet(); }
            if (first) { firstAttempts.incrementAndGet(); }
            if (retire) { retiring.incrementAndGet(); }
            lastCurrent = current;
            lastReservation = reservation;
            StopReservation.JobIdentity selected = reservation.successor() == null ? reservation.source().oldJob()
                    : reservation.successor().job() == null ? nativeJob : reservation.successor().job();
            if (selected != null) { finishedJobs.add(selected); }
            boolean completed = current.getAsBoolean() && over.get();
            if (completed && !retire && reservation.originalDesired().purgeState()) { purges.incrementAndGet(); }
            if (completed) {
                carrying = false;
                if (selected != null && selected.equals(nativeJob)) { nativeJob = null; }
            }
            return completed;
        }
    }
}
