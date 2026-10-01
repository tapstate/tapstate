package io.tapstate.runtime.scheduler;

import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.spi.store.StopAuthority;
import io.tapstate.spi.store.StopReservation;
import io.tapstate.spi.store.WorkloadClaimFence;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimType;
import io.tapstate.spi.store.WorkloadOwner;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static io.tapstate.core.lifecycle.PipelineState.NEW;
import static io.tapstate.core.lifecycle.PipelineState.FAILED;
import static io.tapstate.core.lifecycle.PipelineState.COMPLETED;
import static io.tapstate.core.lifecycle.PipelineState.PAUSED;
import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static io.tapstate.core.lifecycle.PipelineState.STOPPED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Control-flow tests pin typed stop subjects; native identity is exercised by the engine binding. */
class DurableStopConvergenceTest {

    private static final Instant AT = Instant.parse("2026-10-01T00:00:00Z");
    private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
    private final InMemoryStateStore state = new InMemoryStateStore();
    private final StopActuator actuator = new StopActuator();

    private PipelineConverger loop() {
        state.enableStops(desired, id -> actuator.authority);
        return new PipelineConverger(desired, state, actuator, Clock.fixed(AT, ZoneOffset.UTC));
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
        assertThat(rebound.subject()).isEqualTo(new StopReservation.ExistingJob(
                "inc-a", 17, actuator.existingJob, nextOwner));
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
        private StopAuthority authority;
        private StopReservation.JobIdentity existingJob;
        private BooleanSupplier lastCurrent;
        private StopReservation lastReservation;

        @Override public void start(String id) { starts.incrementAndGet(); carrying = true; }
        @Override public void pause(String id) { throw new AssertionError("unexpected pause"); }
        @Override public void resume(String id) { throw new AssertionError("unexpected resume"); }
        @Override public void stop(String id, boolean purge) { throw new AssertionError("legacy stop was bypassed"); }
        @Override public Optional<Throwable> failure(String id) { return Optional.empty(); }
        @Override public boolean isCarryingAJob(String id) { return carrying; }
        @Override public boolean needsRebuildOnResume(String id) { return resumeNeedsRebuild; }
        @Override public Optional<StopReservation.Subject> stopSubject(String id) {
            return Optional.of(existingJob == null ? new StopReservation.NoJob("cluster-a", authority)
                    : new StopReservation.ExistingJob("inc-a", authority.executionGeneration(), existingJob, authority));
        }
        @Override public Optional<StopAuthority> stopAuthority(String id) { return Optional.ofNullable(authority); }
        @Override public boolean finishStop(StopReservation reservation, boolean carry, boolean first,
                boolean retire, BooleanSupplier current) {
            finishes.incrementAndGet();
            if (carry) { continuing.incrementAndGet(); }
            if (first) { firstAttempts.incrementAndGet(); }
            if (retire) { retiring.incrementAndGet(); }
            lastCurrent = current;
            lastReservation = reservation;
            boolean completed = current.getAsBoolean() && over.get();
            if (completed && !retire && reservation.originalDesired().purgeState()) { purges.incrementAndGet(); }
            if (completed) { carrying = false; }
            return completed;
        }
    }
}
