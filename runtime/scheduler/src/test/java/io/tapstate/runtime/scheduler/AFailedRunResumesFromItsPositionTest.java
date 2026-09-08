package io.tapstate.runtime.scheduler;

import static io.tapstate.core.lifecycle.PipelineState.FAILED;
import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static io.tapstate.core.lifecycle.PipelineState.STOPPED;
import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

/**
 * A run that died is recovered from where it got to, not from the beginning.
 *
 * <p>The pipeline here reaches {@code FAILED} the way a real one does -- the converge side observing
 * that the job behind a pipeline believed to be running has died -- rather than by a desired-state
 * double being handed the word. That distinction is the whole reason this sits in the scheduler: only
 * the actual ledger ever carries {@code FAILED}, so a case that wrote it into a desired stub would be
 * green over a path production does not have.
 *
 * <p>What is asserted is the argument, not the outcome. Recovery through a clearing stop also ends with
 * the pipeline running -- and ends with it reading its whole source, because the position went with the
 * clearing. The two are indistinguishable from outside until the run is well under way, so "it came
 * back up" is not a check. Every stop across the whole sequence has to be the keeping one.
 */
class AFailedRunResumesFromItsPositionTest {

    private static final Instant T0 = Instant.parse("2026-07-01T00:00:00Z");
    private static final String REV = "rev-1";

    private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
    private final InMemoryStateStore state = new InMemoryStateStore();
    private final RecordingActuator actuator = new RecordingActuator();
    private final PipelineConverger converger =
            new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC));

    @Test
    void aRunThatDiedIsRecoveredWithoutClearingWhereItGotTo() {
        drive(RUNNING);
        // The job dies on its own; the converge side notices and drives the pipeline to FAILED. This is
        // the transition the case is about, and it is the one no desired-state stub can produce.
        actuator.failWith(new RuntimeException("sink write failed"));
        converger.converge("p1");
        assertThat(state.read("p1").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));

        // Recovery, said in the two verbs the state machine offers for it: stopping is legal from
        // failed, starting is not, so this is deliberately two steps and not a re-drive of a dead job.
        driveStopping(false);
        drive(RUNNING);

        assertThat(state.read("p1").orElseThrow().stateJson())
                .as("the failed run really is running again, not merely un-failed")
                .isEqualTo(StateJson.of(RUNNING));
        assertThat(actuator.calls())
                .as("it is started again after the recovery, so there is a run to resume with")
                .endsWith("start:p1");
        // The assertion this case exists for. A single purging stop anywhere in the sequence -- the one
        // that drives to FAILED included -- throws away the position, and every other assertion here
        // stays green when it does.
        assertThat(actuator.calls())
                .as("nothing in a recovery may clear the position the failed run reached")
                .noneMatch(call -> call.endsWith(":purge"));
    }

    @Test
    void aRunThatDiedStaysFailedUntilSomebodySaysOtherwise() {
        drive(RUNNING);
        actuator.failWith(new RuntimeException("sink write failed"));
        converger.converge("p1");
        actuator.reset();

        // Desired is still RUNNING from the drive above, so this is the loop ticking rather than anybody
        // asking for anything. Re-driving here would restart the dead job on every pass.
        converger.converge("p1");

        assertThat(state.read("p1").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
        assertThat(actuator.calls())
                .as("a failed run is not brought back by the loop noticing it again")
                .isEmpty();
    }

    private void drive(PipelineState target) {
        desired.save(new DesiredState("p1", target, REV));
        converger.converge("p1");
    }

    private void driveStopping(boolean purgeState) {
        desired.save(new DesiredState("p1", STOPPED, REV, purgeState));
        converger.converge("p1");
    }
}
