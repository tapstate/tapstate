package io.tapstate.runtime.scheduler;

import static io.tapstate.core.lifecycle.PipelineState.FAILED;
import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;

import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.StateJson;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import org.junit.jupiter.api.Test;

/**
 * Stopping a pipeline and starting it again is one instruction, and this side carries out both halves.
 *
 * <p>It cannot be two intents. This side reads the latest intent rather than consuming a queue of them,
 * and a pipeline is asked to stop and start again within milliseconds -- far inside one pass of the loop
 * -- so the stop is overwritten before anything ever reads it. Both halves then go missing at once: the
 * run is never torn down, nothing is ever cleared, and the pass that finally looks sees a running
 * pipeline that is already what the surviving intent asks for, so it does nothing and reports success.
 * Nothing about that is visible from outside: the pipeline really is running, because it never stopped.
 *
 * <p>So the instruction rides on the intent, and this is where it is honoured. Every case here asserts
 * the actuated verbs rather than the state that follows them, because the state is the half that looks
 * right either way -- a pipeline that was never touched and one that was torn down and rebuilt both end
 * up running.
 */
class AStopAndAStartWrittenTogetherAreBothCarriedOutTest {

    private static final Instant T0 = Instant.parse("2026-09-04T00:00:00Z");
    private static final String REV = "rev-1";

    private final InMemoryDesiredStore desired = new InMemoryDesiredStore();
    private final InMemoryStateStore state = new InMemoryStateStore();
    private final RecordingActuator actuator = new RecordingActuator();
    private final PipelineConverger converger =
            new PipelineConverger(desired, state, actuator, Clock.fixed(T0, ZoneOffset.UTC));

    @Test
    void aRunningPipelineIsTornDownAndBuiltAgain() {
        running();

        desired.save(rebuild(false));
        converger.converge("p1");

        assertThat(actuator.calls())
                .as("the pipeline is already running, so nothing but the instruction can produce these")
                .containsExactly("stop:p1:keep", "start:p1");
    }

    @Test
    void theClearingItWasAskedForRidesOnTheTearDownHalf() {
        running();

        desired.save(rebuild(true));
        converger.converge("p1");

        assertThat(actuator.calls())
                .as("a rebuild asked to clear has to clear, or it re-reads nothing and says it did")
                .containsExactly("stop:p1:purge", "start:p1");
    }

    @Test
    void aRunThatDiedIsBuiltAgainRatherThanLeftFailed() {
        running();
        actuator.failWith(new RuntimeException("sink write failed"));
        converger.converge("p1");
        assertThat(state.read("p1").orElseThrow().stateJson()).isEqualTo(StateJson.of(FAILED));
        actuator.reset();

        desired.save(rebuild(false));
        converger.converge("p1");

        assertThat(actuator.calls())
                .as("a failed run is left alone by a plain start on purpose; an explicit rebuild is how it "
                        + "is recovered, and it must not clear where the run got to")
                .containsExactly("stop:p1:keep", "start:p1");
    }

    @Test
    void theInstructionIsCarriedOutOnceAndNotOnEveryLaterTick() {
        running();
        desired.save(rebuild(false));
        converger.converge("p1");
        actuator.reset();

        converger.converge("p1");
        converger.converge("p1");

        assertThat(actuator.calls())
                .as("a loop that read the instruction again would tear the pipeline down once a second, "
                        + "for ever, with every read face reporting it healthy")
                .isEmpty();
        assertThat(state.read("p1").orElseThrow().stateJson())
                .as("and it is left running, not left wherever the repeat happened to stop it")
                .isEqualTo(StateJson.of(RUNNING));
    }

    /** Drives the pipeline to a running job and forgets the verbs that took it there. */
    private void running() {
        desired.save(new DesiredState("p1", RUNNING, REV));
        converger.converge("p1");
        actuator.reset();
    }

    /**
     * The intent a start written straight after a stop produces: run, having first torn down -- stamped
     * with where the actual state stands right now, which is what a start superseding an unread stop
     * sees. Carrying it out moves that epoch on, and that is the whole of how it is spent.
     */
    private DesiredState rebuild(boolean purgeState) {
        long epoch = state.read("p1").orElseThrow().epoch();
        return new DesiredState("p1", RUNNING, REV, purgeState, null, true, epoch);
    }
}
