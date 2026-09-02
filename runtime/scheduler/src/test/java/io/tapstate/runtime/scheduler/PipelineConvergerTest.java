package io.tapstate.runtime.scheduler;

import io.tapstate.core.common.Severity;
import io.tapstate.core.common.TapstateErrorCode;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.CheckpointDoc;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.StateJson;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.tapstate.core.lifecycle.PipelineState.COMPLETED;
import static io.tapstate.core.lifecycle.PipelineState.FAILED;
import static io.tapstate.core.lifecycle.PipelineState.NEW;
import static io.tapstate.core.lifecycle.PipelineState.PAUSED;
import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static io.tapstate.core.lifecycle.PipelineState.STOPPED;
import static io.tapstate.runtime.scheduler.ConvergeStatus.CONVERGED;
import static io.tapstate.runtime.scheduler.ConvergeStatus.NOTHING_TO_DO;
import static io.tapstate.runtime.scheduler.ConvergeStatus.SUPERSEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The convergence loop: it reads the desired intent, seeds the actual checkpoint when a pipeline
 * first appears, and drives the actual state toward the target through the fencing compare-and-swap,
 * rebasing on a fenced write. The artificial-failover cases stage a competing writer to prove the
 * single-node fencing contract — strictly-monotonic epoch, one winner per race — that a real
 * multi-node failover would otherwise be needed to witness.
 */
class PipelineConvergerTest {

    private static final Instant T0 = Instant.parse("2026-07-01T00:00:00Z");
    private static final String REV = "rev-1";

    private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
    private final InMemoryStateStore state = new InMemoryStateStore();
    private final RecordingActuator actuator = new RecordingActuator();
    private final PipelineConverger converger =
            new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC));

    @Test
    @DisplayName("with no desired intent there is nothing to converge and no checkpoint is written")
    void noDesiredIsANoOp() {
        ConvergeResult result = converger.converge("p1");

        assertThat(result.status()).isEqualTo(NOTHING_TO_DO);
        assertThat(state.read("p1")).isEmpty();
    }

    @Test
    @DisplayName("a first convergence seeds the checkpoint and drives it to the desired state")
    void firstConvergenceSeedsAndDrives() {
        desired.save(new DesiredState("p1", RUNNING, REV));

        ConvergeResult result = converger.converge("p1");

        assertThat(result.status()).isEqualTo(CONVERGED);
        CheckpointDoc actual = state.read("p1").orElseThrow();
        assertThat(actual.stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(actual.epoch()).isEqualTo(1); // 0 = NEW seed, 1 = first transition
    }

    @Test
    @DisplayName("converging an already-converged pipeline writes nothing and leaves the epoch alone")
    void convergenceIsIdempotent() {
        desired.save(new DesiredState("p1", RUNNING, REV));
        converger.converge("p1");
        long epochAfterFirst = state.read("p1").orElseThrow().epoch();

        ConvergeResult again = converger.converge("p1");

        assertThat(again.status()).isEqualTo(CONVERGED);
        assertThat(state.read("p1").orElseThrow().epoch()).isEqualTo(epochAfterFirst);
    }

    @Test
    @DisplayName("the epoch advances strictly monotonically across a start/pause/resume/stop sequence")
    void epochIsMonotonicAcrossTheVerbSequence() {
        converge(RUNNING);
        converge(PAUSED);
        converge(RUNNING);
        converge(STOPPED);

        CheckpointDoc actual = state.read("p1").orElseThrow();
        assertThat(actual.stateJson()).isEqualTo(StateJson.of(STOPPED));
        assertThat(actual.epoch()).isEqualTo(4); // seed 0 -> RUNNING 1 -> PAUSED 2 -> RUNNING 3 -> STOPPED 4
    }

    @Test
    @DisplayName("a fenced converger re-reads the fresh epoch and its retry wins; the epoch advances once per write")
    void artificialFailoverRebasesAndWins() {
        state.create("p1", StateJson.of(NEW), T0); // seed at epoch 0
        desired.save(new DesiredState("p1", RUNNING, REV));

        // A competing owner writes PAUSED at epoch 0 just before the converger's first swap, fencing it.
        AtomicBoolean fired = new AtomicBoolean(false);
        state.onBeforeSwap(() -> {
            if (fired.compareAndSet(false, true)) {
                state.applySwap("p1", 0L, StateJson.of(PAUSED), T0);
            }
        });

        ConvergeResult result = converger.converge("p1");

        assertThat(result.status()).isEqualTo(CONVERGED);
        CheckpointDoc actual = state.read("p1").orElseThrow();
        assertThat(actual.stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(actual.epoch()).isEqualTo(2); // 0 seed -> 1 competitor(PAUSED) -> 2 converger(RUNNING)
        assertThat(state.swapAttempts()).isEqualTo(2); // one fenced, one applied
        // The rebased write it won was PAUSED -> RUNNING, so the actuation it drove is a resume.
        assertThat(actuator.calls()).containsExactly("resume:p1");
    }

    @Test
    @DisplayName("a fenced converger concedes without re-swapping when a competitor already reached its target")
    void fencedConvergerConcedesWhenACompetitorReachedTheTarget() {
        state.create("p1", StateJson.of(NEW), T0); // seed at epoch 0
        desired.save(new DesiredState("p1", RUNNING, REV));

        // A competitor writes the converger's OWN target (RUNNING) at epoch 0, just before its swap.
        AtomicBoolean fired = new AtomicBoolean(false);
        state.onBeforeSwap(() -> {
            if (fired.compareAndSet(false, true)) {
                state.applySwap("p1", 0L, StateJson.of(RUNNING), T0);
            }
        });

        ConvergeResult result = converger.converge("p1");

        assertThat(result.status()).isEqualTo(CONVERGED);
        CheckpointDoc actual = state.read("p1").orElseThrow();
        assertThat(actual.stateJson()).isEqualTo(StateJson.of(RUNNING));
        assertThat(actual.epoch()).isEqualTo(1); // the competitor's write; the converger did not bump it again
        assertThat(state.swapAttempts()).isEqualTo(1); // one fenced attempt, then it conceded — no redundant re-swap
        // It conceded the transition, so it must not drive the job side: actuation fires only on a won CAS.
        assertThat(actuator.calls()).isEmpty();
    }

    @Test
    @DisplayName("a relentlessly fenced converger gives up after a bounded number of retries rather than spinning")
    void boundedRetriesGiveUpWhenPersistentlyFenced() {
        state.create("p1", StateJson.of(NEW), T0);
        desired.save(new DesiredState("p1", RUNNING, REV));

        // A competitor bumps the epoch before every converger swap, so the converger is always stale.
        state.onBeforeSwap(() -> {
            long current = state.read("p1").orElseThrow().epoch();
            state.applySwap("p1", current, StateJson.of(PAUSED), T0);
        });

        ConvergeResult result = converger.converge("p1");

        assertThat(result.status()).isEqualTo(SUPERSEDED);
        assertThat(state.swapAttempts()).isEqualTo(PipelineConverger.MAX_CAS_ATTEMPTS);
        // It never won a swap, so it never actuated: no job side is driven for a superseded pass.
        assertThat(actuator.calls()).isEmpty();
    }

    @Test
    @DisplayName("markCompleted drives a running pipeline to the terminal COMPLETED state")
    void markCompletedDrivesToCompleted() {
        converge(RUNNING); // actual now RUNNING

        ConvergeResult result = converger.markCompleted("p1");

        assertThat(result.status()).isEqualTo(CONVERGED);
        assertThat(state.read("p1").orElseThrow().stateJson()).isEqualTo(StateJson.of(COMPLETED));
    }

    @Test
    @DisplayName("markCompleted on a pipeline that has never run is a no-op")
    void markCompletedWithoutACheckpointIsANoOp() {
        ConvergeResult result = converger.markCompleted("p1");

        assertThat(result.status()).isEqualTo(NOTHING_TO_DO);
        assertThat(state.read("p1")).isEmpty();
    }

    @Test
    @DisplayName("driving a fresh pipeline to RUNNING starts its data-plane job")
    void startingActuatesStart() {
        desired.save(new DesiredState("p1", RUNNING, REV));

        converger.converge("p1");

        assertThat(actuator.calls()).containsExactly("start:p1");
    }

    @Test
    @DisplayName("pausing a running pipeline suspends its job, and resuming it resumes the job")
    void pauseThenResumeActuatesSuspendThenResume() {
        converge(RUNNING);
        converge(PAUSED);
        converge(RUNNING);

        assertThat(actuator.calls()).containsExactly("start:p1", "pause:p1", "resume:p1");
    }

    @Test
    @DisplayName("stopping a pipeline cancels its job")
    void stoppingActuatesStop() {
        converge(RUNNING);
        converge(STOPPED);

        assertThat(actuator.calls()).containsExactly("start:p1", "stop:p1:keep");
    }

    @Test
    @DisplayName("a re-dig — stop then start — cancels the job then submits a fresh one")
    void rewindActuatesStopThenStart() {
        converge(RUNNING);
        converge(STOPPED);
        converge(RUNNING);

        assertThat(actuator.calls()).containsExactly("start:p1", "stop:p1:keep", "start:p1");
    }

    @Test
    @DisplayName("a converge pass that changes no state actuates nothing")
    void idempotentConvergeActuatesNothing() {
        converge(RUNNING);
        actuator.reset();

        converger.converge("p1"); // already RUNNING: no transition, no actuation

        assertThat(actuator.calls()).isEmpty();
    }

    @Test
    @DisplayName("marking a pipeline completed stops its job")
    void markCompletedActuatesStop() {
        converge(RUNNING);
        actuator.reset();

        converger.markCompleted("p1");

        // :keep, and that is the assertion. A source running out is not somebody asking for the
        // pipeline's progress to be thrown away, and the completed pipeline is startable again.
        assertThat(actuator.calls()).containsExactly("stop:p1:keep");
    }

    @Test
    @DisplayName("a start refused with a coded reason converges to FAILED and carries that reason")
    void aCodedRefusalToStartConvergesToFailed() {
        // A job that never started is not the same event as a job that died, and it used to be handled
        // as no event at all: the throw escaped the pass, so nothing was published, and the pipeline sat
        // at the last state anyone had observed while the loop retried it every tick. Observed against a
        // real unreachable store, where the diagnosis existed and reached only the server log.
        TapstateException refusal = new TapstateException(
                new StubCode("actuation.view-store-unreachable"), Map.of("store", "views"), null);
        actuator.refuseStartWith(refusal);
        desired.save(new DesiredState("p1", RUNNING, REV));

        ConvergeResult result = converger.converge("p1");

        assertThat(result.status()).isEqualTo(ConvergeStatus.FAILED);
        // The cause has to travel, not just the status: what the publisher renders as the observation's
        // coded failure is this object, so a pass that reported FAILED with nothing attached would leave
        // the read face saying the pipeline is broken and not saying why -- which is the whole defect.
        assertThat(result.failure()).contains(refusal);
        assertThat(state.read("p1").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
    }

    @Test
    @DisplayName("a start that throws an uncoded fault is left to escape, not laundered into FAILED")
    void anUncodedFaultStillEscapes() {
        // Coded refusals are conditions an operator acts on; an uncoded throw is a defect in this
        // process, and recording it as the pipeline's own failure would file a bug report under the
        // user's name. It keeps crashing the pass, which is what makes it visible as a bug.
        actuator.refuseStartWith(new IllegalStateException("a bug in the builder"));
        desired.save(new DesiredState("p1", RUNNING, REV));

        assertThatThrownBy(() -> converger.converge("p1"))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    @DisplayName("a running pipeline whose job has died converges to FAILED, carries the cause, and stops it")
    void aDeadJobConvergesToFailed() {
        converge(RUNNING); // actual now RUNNING
        actuator.reset();
        RuntimeException cause = new RuntimeException("sink write failed");
        actuator.failWith(cause);

        ConvergeResult result = converger.converge("p1");

        assertThat(result.status()).isEqualTo(ConvergeStatus.FAILED);
        assertThat(result.failure()).contains(cause);
        assertThat(state.read("p1").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
        // The dead job's capture is torn down: driving to FAILED actuates a stop over it -- and with
        // :keep, so the position it died at is still there for the run that recovers from this.
        assertThat(actuator.calls()).containsExactly("stop:p1:keep");
    }

    /**
     * A process that comes up to a checkpoint saying RUNNING has to put a job behind it. This is the
     * restart case, and every signal the loop had before this one reads healthy in it: the desired
     * intent says RUNNING, the recorded state already says RUNNING, and nothing has failed - because
     * from this process's point of view nothing has happened at all. It never ran the job, so it has no
     * failure to report about one.
     *
     * <p>What it costs to miss: the loop converges, the read faces answer RUNNING with no errors, and
     * no job reads the source or writes a sink. The pipeline is stopped in every way except the one a
     * user or an operator can see. Measured in an end-to-end restart, where the second process
     * published observations for three minutes over a data plane that never started.
     */
    @Test
    @DisplayName("a process that comes up to a RUNNING checkpoint with no job behind it starts one")
    void aRunningCheckpointWithNoJobBehindItIsActuated() {
        converge(RUNNING); // a previous process left the checkpoint at RUNNING
        actuator.reset();
        actuator.carryingNothing(); // this process has never run it: no job, and no failure to report

        ConvergeResult result = converger.converge("p1");

        assertThat(result.status()).isEqualTo(CONVERGED);
        assertThat(state.read("p1").orElseThrow().stateJson())
                .as("the pipeline is still meant to be running, so its recorded state does not move")
                .isEqualTo(StateJson.of(RUNNING));
        assertThat(actuator.calls())
                .as("a job has to be put behind the checkpoint, or nothing is carrying this pipeline")
                .containsExactly("start:p1");
    }

    /**
     * The restart road reaches the same refusal as the CAS road, and it used to escape here. A store
     * that is unreachable when a process comes up is exactly the condition this refusal exists for --
     * the previous process left a RUNNING checkpoint, this one cannot put a job behind it, and the
     * pipeline is not going to run. Letting the throw out leaves the checkpoint saying RUNNING while
     * the loop retries every tick, which is the same state the coded refusal was introduced to remove
     * on the other road: the read face says healthy and the reason lives in the server log alone.
     */
    @Test
    @DisplayName("a restart whose start is refused with a coded reason converges to FAILED, not RUNNING")
    void aCodedRefusalOnTheRestartRoadConvergesToFailed() {
        converge(RUNNING); // a previous process left the checkpoint at RUNNING
        actuator.carryingNothing(); // and this process has no job behind it
        TapstateException refusal = new TapstateException(
                new StubCode("actuation.view-store-unreachable"), Map.of("store", "views"), null);
        actuator.refuseStartWith(refusal);

        ConvergeResult result = converger.converge("p1");

        assertThat(result.status()).isEqualTo(ConvergeStatus.FAILED);
        assertThat(result.failure()).contains(refusal);
        // The recorded state is the half that matters most here: a checkpoint left at RUNNING is what
        // makes every read face keep answering healthy over a data plane that does not exist.
        assertThat(state.read("p1").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
    }

    /**
     * An uncoded throw on the restart road stays a defect, same as on the other one. Recording it as
     * the pipeline's failure would file a bug in this process under the user's name.
     */
    @Test
    @DisplayName("an uncoded fault on the restart road still escapes")
    void anUncodedFaultOnTheRestartRoadStillEscapes() {
        converge(RUNNING);
        actuator.carryingNothing();
        actuator.refuseStartWith(new IllegalStateException("a bug in the builder"));

        assertThatThrownBy(() -> converger.converge("p1"))
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * And the tick after that one does not submit a second time. The guard is "no job is carrying it",
     * not "this process did not start it": once one is, the loop has nothing left to do, or every tick
     * would re-submit for the life of the pipeline.
     */
    @Test
    @DisplayName("once a job is carrying it again, later ticks actuate nothing")
    void aRestartedJobIsNotSubmittedOnEveryTick() {
        converge(RUNNING);
        actuator.carryingNothing();
        converger.converge("p1"); // puts a job behind it
        actuator.reset();

        ConvergeResult again = converger.converge("p1");

        assertThat(again.status()).isEqualTo(CONVERGED);
        assertThat(actuator.calls()).isEmpty();
    }

    @Test
    @DisplayName("a failed pipeline is not re-driven toward a still-RUNNING target, so a dead job is not restarted")
    void aFailedPipelineIsNotRestarted() {
        converge(RUNNING);
        actuator.failWith(new RuntimeException("sink write failed"));
        converger.converge("p1"); // drives to FAILED
        actuator.reset();

        ConvergeResult again = converger.converge("p1"); // desired still RUNNING, job still failed

        assertThat(again.status()).isEqualTo(CONVERGED);
        assertThat(state.read("p1").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
        assertThat(actuator.calls()).isEmpty(); // no restart of the dead job on every tick
    }

    @Test
    @DisplayName("stopping a failed pipeline clears it to STOPPED so a fresh start can run it again")
    void stoppingAFailedPipelineRecoversIt() {
        converge(RUNNING);
        actuator.failWith(new RuntimeException("sink write failed"));
        converger.converge("p1"); // drives to FAILED
        actuator.reset();

        converge(STOPPED); // user stop: desired -> STOPPED

        assertThat(state.read("p1").orElseThrow().stateJson()).isEqualTo(StateJson.of(STOPPED));
        assertThat(actuator.calls()).containsExactly("stop:p1:keep");
    }

    @Test
    @DisplayName("a stop that asked to clear the pipeline's state drives the stop with that answer")
    void aStopAskedToPurgeDrivesItThrough() {
        converge(RUNNING);
        actuator.reset();

        convergeStopping(true);

        assertThat(actuator.calls()).containsExactly("stop:p1:purge");
    }

    @Test
    @DisplayName("what the user asked reaches the actuator, and nothing else decides it")
    void aStopAskedToKeepDrivesTheKeepingAnswer() {
        converge(RUNNING);
        actuator.reset();

        convergeStopping(false);

        // The pair is the point. Both land the pipeline in STOPPED and both actuate a stop, so a case
        // asserting one alone would pass over an actuator that ignored the answer and always did one.
        assertThat(actuator.calls()).containsExactly("stop:p1:keep");
    }

    @Test
    @DisplayName("a job dying under a stop that asked to clear does not carry that answer into the failure")
    void aDeadJobIsDrivenToFailedWithoutPurgingWhateverTheLastIntentSaid() {
        converge(RUNNING);
        // The intent now says RUNNING with nothing to purge, but a stop-with-purge earlier in this
        // pipeline's life must not leak into the converge-side transition below.
        desired.save(new DesiredState("p1", RUNNING, REV, true));
        actuator.reset();
        actuator.failWith(new RuntimeException("sink write failed"));

        converger.converge("p1");

        assertThat(state.read("p1").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
        assertThat(actuator.calls()).containsExactly("stop:p1:keep");
    }

    private void converge(io.tapstate.core.lifecycle.PipelineState target) {
        desired.save(new DesiredState("p1", target, REV));
        converger.converge("p1");
    }

    private void convergeStopping(boolean purgeState) {
        desired.save(new DesiredState("p1", STOPPED, REV, purgeState));
        converger.converge("p1");
    }
    /** A code whose only job is to carry a name: this test is about the path, not the catalog. */
    private record StubCode(String code) implements TapstateErrorCode {

        @Override
        public Severity severity() {
            return Severity.ERROR;
        }

        @Override
        public Set<String> placeholders() {
            return Set.of("store");
        }
    }

}
