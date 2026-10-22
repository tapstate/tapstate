package io.tapstate.core.lifecycle;

import io.tapstate.core.common.TapstateException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.Set;

import static io.tapstate.core.lifecycle.LifecycleVerb.PAUSE;
import static io.tapstate.core.lifecycle.LifecycleVerb.RESUME;
import static io.tapstate.core.lifecycle.LifecycleVerb.START;
import static io.tapstate.core.lifecycle.LifecycleVerb.STOP;
import static io.tapstate.core.lifecycle.PipelineState.COMPLETED;
import static io.tapstate.core.lifecycle.PipelineState.FAILED;
import static io.tapstate.core.lifecycle.PipelineState.NEW;
import static io.tapstate.core.lifecycle.PipelineState.PAUSED;
import static io.tapstate.core.lifecycle.PipelineState.RUNNING;
import static io.tapstate.core.lifecycle.PipelineState.STOPPED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * The pipeline lifecycle state machine: the verb-keyed transition table, its preconditions, and
 * the coded error a rejected transition raises. Pure functions, so every case is a direct call.
 */
class LifecycleMachineTest {

    @Test
    @DisplayName("start enters RUNNING from each of its three legal origins")
    void startFromNewStoppedOrCompleted() {
        assertThat(LifecycleMachine.transition(NEW, START)).isEqualTo(RUNNING);
        assertThat(LifecycleMachine.transition(STOPPED, START)).isEqualTo(RUNNING);
        assertThat(LifecycleMachine.transition(COMPLETED, START)).isEqualTo(RUNNING);
    }

    @Test
    @DisplayName("pause suspends a running pipeline")
    void pauseFromRunning() {
        assertThat(LifecycleMachine.transition(RUNNING, PAUSE)).isEqualTo(PAUSED);
    }

    @Test
    @DisplayName("resume returns a paused pipeline to running")
    void resumeFromPaused() {
        assertThat(LifecycleMachine.transition(PAUSED, RESUME)).isEqualTo(RUNNING);
    }

    @Test
    @DisplayName("stop accepts running, paused, and naturally completed runs")
    void stopFromRunningPausedOrCompleted() {
        assertThat(LifecycleMachine.transition(RUNNING, STOP)).isEqualTo(STOPPED);
        assertThat(LifecycleMachine.transition(PAUSED, STOP)).isEqualTo(STOPPED);
        assertThat(LifecycleMachine.transition(COMPLETED, STOP)).isEqualTo(STOPPED);
    }

    @Test
    @DisplayName("a paused pipeline cannot be started (only resumed or stopped)")
    void startFromPausedIsRejected() {
        assertThatThrownBy(() -> LifecycleMachine.transition(PAUSED, START))
                .isInstanceOf(TapstateException.class);
    }

    @Test
    @DisplayName("a failed pipeline is recovered by stopping it, not by starting it directly")
    void stopClearsAFailedPipelineButStartDoesNot() {
        // Stop clears a failed run to STOPPED, from where a fresh start runs it again. Start is not legal
        // straight from FAILED: it would leave the desired intent already RUNNING unchanged, so the
        // converge side could not tell a fresh start from the stale intent it is deliberately not re-driving.
        assertThat(LifecycleMachine.transition(FAILED, STOP)).isEqualTo(STOPPED);
        assertThatThrownBy(() -> LifecycleMachine.transition(FAILED, START))
                .isInstanceOf(TapstateException.class);
        assertThatThrownBy(() -> LifecycleMachine.transition(FAILED, PAUSE))
                .isInstanceOf(TapstateException.class);
    }

    @Test
    @DisplayName("an illegal transition carries the lifecycle.illegal-transition code and {from, verb} args")
    void illegalTransitionIsCoded() {
        TapstateException ex = catchThrowableOfType(
                () -> LifecycleMachine.transition(RUNNING, START), TapstateException.class);

        assertThat(ex.code()).isSameAs(LifecycleError.ILLEGAL_TRANSITION);
        assertThat(ex.code().code()).isEqualTo("lifecycle.illegal-transition");
        assertThat(ex.args()).containsEntry("from", RUNNING).containsEntry("verb", "start");
        assertThat(ex.args().keySet()).containsExactlyInAnyOrder("from", "verb");
    }

    @Test
    @DisplayName("isLegal agrees with transition on every state/verb pair")
    void isLegalMatchesTransition() {
        for (PipelineState from : PipelineState.values()) {
            for (LifecycleVerb verb : LifecycleVerb.values()) {
                boolean legal = LifecycleMachine.isLegal(from, verb);
                if (legal) {
                    assertThat(LifecycleMachine.transition(from, verb)).isNotNull();
                } else {
                    assertThatThrownBy(() -> LifecycleMachine.transition(from, verb))
                            .isInstanceOf(TapstateException.class);
                }
            }
        }
    }

    @Test
    @DisplayName("legalVerbs lists exactly the actions available from each state")
    void legalVerbsPerState() {
        assertThat(LifecycleMachine.legalVerbs(NEW)).isEqualTo(EnumSet.of(START));
        assertThat(LifecycleMachine.legalVerbs(RUNNING)).isEqualTo(EnumSet.of(PAUSE, STOP));
        assertThat(LifecycleMachine.legalVerbs(PAUSED)).isEqualTo(EnumSet.of(RESUME, STOP));
        assertThat(LifecycleMachine.legalVerbs(STOPPED)).isEqualTo(EnumSet.of(START));
        assertThat(LifecycleMachine.legalVerbs(COMPLETED)).isEqualTo(EnumSet.of(START, STOP));
        assertThat(LifecycleMachine.legalVerbs(FAILED)).isEqualTo(EnumSet.of(STOP));
    }

    @Test
    @DisplayName("legalVerbs is consistent with isLegal for every pair")
    void legalVerbsConsistentWithIsLegal() {
        for (PipelineState from : PipelineState.values()) {
            Set<LifecycleVerb> legal = LifecycleMachine.legalVerbs(from);
            for (LifecycleVerb verb : LifecycleVerb.values()) {
                assertThat(legal.contains(verb)).isEqualTo(LifecycleMachine.isLegal(from, verb));
            }
        }
    }
}
