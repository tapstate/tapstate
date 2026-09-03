package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.model.SourceRef;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.core.model.Srs;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The one reading of whether a pipeline is at rest, and the refusal that reads it: a source may not
 * change whether it buffers through the shared replay store while a pipeline reading it is still up.
 */
class LivePipelinesTest {

    private final TestLifecycleStores.Desired desired = new TestLifecycleStores.Desired();
    private final TestLifecycleStores.State state = new TestLifecycleStores.State();
    private final LivePipelines live = new LivePipelines(desired, state);

    /**
     * Paused is not at rest, and this is the case the refusal exists for.
     *
     * <p>Pausing suspends the engine and nothing else -- the capture keeps reading and the source
     * connection stays held -- so a paused pipeline is still running the thing the flag decides. It is
     * also the one state that looks stopped from the outside, which makes it the state an author is
     * most likely to make this change from, and the one a guard reading "is it stopped" would wave
     * through.
     */
    @Test
    void aPausedPipelineIsNotAtRest() {
        state.put("p1", PipelineState.PAUSED);
        desired.put("p1", PipelineState.PAUSED);

        assertThat(live.isAtRest("p1")).isFalse();
    }

    @Test
    void aStoppedPipelineNobodyHasAskedToRunIsAtRest() {
        state.put("p1", PipelineState.STOPPED);
        desired.put("p1", PipelineState.STOPPED);

        assertThat(live.isAtRest("p1")).isTrue();
    }

    /**
     * Both halves are read, so a pipeline that is down but has been asked back up is not at rest: the
     * next convergence would raise it onto whatever was changed underneath it in the meantime.
     */
    @Test
    void aStoppedPipelineAlreadyAskedToRunAgainIsNotAtRest() {
        state.put("p1", PipelineState.STOPPED);
        desired.put("p1", PipelineState.RUNNING);

        assertThat(live.isAtRest("p1")).isFalse();
    }

    @Test
    void aPipelineNothingHasEverRunIsAtRest() {
        assertThat(live.isAtRest("never-applied")).isTrue();
    }

    @Test
    void refusesTurningTheReplayStoreOffWhileAPipelineReadingItIsUp() {
        state.put("p1", PipelineState.RUNNING);
        desired.put("p1", PipelineState.RUNNING);
        List<Resource> stored = List.of(source("orders", true), pipeline("p1", "orders"));

        TapstateException refused = (TapstateException) org.assertj.core.api.Assertions
                .catchThrowable(() -> live.refuseBufferingChangeWhileLive(
                        source("orders", true), source("orders", false), stored));

        assertThat(refused).isNotNull();
        assertThat(refused.code()).isEqualTo(SourceError.SRS_CHANGE_WHILE_RUNNING);
        assertThat(refused.args())
                .as("the refusal names which pipelines are in the way, so the author need not go looking")
                .containsEntry("id", "orders")
                .containsEntry("pipelines", List.of("p1"));
    }

    /**
     * Paused is let through, and this is the case the change exists for.
     *
     * <p>The guard does not ask "is it stopped" -- that is {@link LivePipelines#isAtRest}, and paused is
     * not. It asks whether an edit would take effect with nothing reporting the gap, and on a paused
     * pipeline something does. Its only two exits both re-check: a resume is refused while the revision
     * it paused against is no longer the latest, and a stop followed by a start assembles the run afresh
     * from whatever is stored now. Neither can carry on against a definition it did not read.
     *
     * <p>Refusing here left an author who wanted to edit a paused pipeline and then carry on from its
     * position with no route at all: the edit was refused, the resume was refused, and re-reading the
     * whole source was the only way forward.
     */
    @Test
    void allowsTheChangeWhileTheOnlyReaderIsMerelyPaused() {
        state.put("p1", PipelineState.PAUSED);
        desired.put("p1", PipelineState.PAUSED);
        List<Resource> stored = List.of(source("orders", true), pipeline("p1", "orders"));

        live.refuseBufferingChangeWhileLive(source("orders", true), source("orders", false), stored);
    }

    /**
     * A resume already asked for is refused, even though the pipeline is still sitting at paused.
     *
     * <p>The discriminating case for the rule above. Reading the actual state alone -- "it says paused,
     * let it through" -- passes exactly this pipeline, which is on its way back up and will raise the
     * held job onto whatever was changed underneath it. What makes paused safe is that its exits
     * re-check, and this pipeline has already taken one.
     */
    @Test
    void refusesTheChangeWhenAResumeHasAlreadyBeenAskedFor() {
        state.put("p1", PipelineState.PAUSED);
        desired.put("p1", PipelineState.RUNNING);
        List<Resource> stored = List.of(source("orders", true), pipeline("p1", "orders"));

        assertThatThrownBy(() -> live.refuseBufferingChangeWhileLive(
                source("orders", true), source("orders", false), stored))
                .isInstanceOf(TapstateException.class);
    }

    /** The pipeline-side peer of the two above: paused is let through. */
    @Test
    void allowsAPipelineSideSwitchChangeWhileThePipelineIsMerelyPaused() {
        state.put("p1", PipelineState.PAUSED);
        desired.put("p1", PipelineState.PAUSED);

        live.refuseBufferingChangeWhileLive(pipelineWithSwitch("p1", "orders", true),
                pipelineWithSwitch("p1", "orders", false));
    }

    /** And the pipeline-side peer still refuses while the pipeline is actually running. */
    @Test
    void stillRefusesAPipelineSideSwitchChangeWhileThePipelineIsRunning() {
        state.put("p1", PipelineState.RUNNING);
        desired.put("p1", PipelineState.RUNNING);

        assertThatThrownBy(() -> live.refuseBufferingChangeWhileLive(
                pipelineWithSwitch("p1", "orders", true), pipelineWithSwitch("p1", "orders", false)))
                .isInstanceOf(TapstateException.class);
    }

    @Test
    void allowsTheChangeOnceEveryPipelineReadingItIsStopped() {
        state.put("p1", PipelineState.STOPPED);
        desired.put("p1", PipelineState.STOPPED);
        List<Resource> stored = List.of(source("orders", true), pipeline("p1", "orders"));

        live.refuseBufferingChangeWhileLive(source("orders", true), source("orders", false), stored);
    }

    /**
     * The guard is about this one field, not about editing a source at all. A running pipeline is an
     * ordinary thing to edit a description on, and a refusal that fired for every edit would be read as
     * "sources are frozen while anything runs" and worked around rather than obeyed.
     */
    @Test
    void allowsAnEditThatLeavesTheBufferingAloneWhileAReaderIsUp() {
        state.put("p1", PipelineState.RUNNING);
        desired.put("p1", PipelineState.RUNNING);
        List<Resource> stored = List.of(source("orders", true), pipeline("p1", "orders"));

        live.refuseBufferingChangeWhileLive(source("orders", true), source("orders", true), stored);
    }

    /**
     * A source with no srs block and one whose block leaves the flag unset are both buffered, so moving
     * between them is not a change. Comparing the field as written rather than what it means would
     * refuse an edit that changes nothing about how the source runs.
     */
    @Test
    void treatsAnUnsetFlagAndNoBlockAtAllAsTheSameAnswer() {
        state.put("p1", PipelineState.RUNNING);
        desired.put("p1", PipelineState.RUNNING);
        SourceResource noBlock = new SourceResource(
                "orders", null, "mysql", Map.of(), null, null, null, null, null);
        SourceResource unsetFlag = new SourceResource(
                "orders", null, "mysql", Map.of(), null, null, null,
                new Srs(null, null, null, null, null), null);
        List<Resource> stored = List.of(noBlock, pipeline("p1", "orders"));

        live.refuseBufferingChangeWhileLive(noBlock, unsetFlag, stored);
    }

    /** A source nothing reads has nothing to disturb, however the flag moves. */
    @Test
    void allowsTheChangeWhenNoPipelineReadsTheSource() {
        state.put("p1", PipelineState.RUNNING);
        desired.put("p1", PipelineState.RUNNING);
        List<Resource> stored = List.of(source("orders", true), pipeline("p1", "customers"));

        live.refuseBufferingChangeWhileLive(source("orders", true), source("orders", false), stored);
    }

    private static SourceResource source(String id, boolean srsEnabled) {
        return new SourceResource(id, null, "mysql", Map.of(), null, null, null,
                new Srs(null, null, null, null, srsEnabled), null);
    }

    private static PipelineResource pipeline(String id, String sourceId) {
        return new PipelineResource(id, null, List.of(SourceRef.bare(sourceId)), null, null, null, null, null);
    }

    private static PipelineResource pipelineWithSwitch(String id, String sourceId, boolean srs) {
        return new PipelineResource(
                id, null, List.of(SourceRef.spec(sourceId, srs)), null, null, null, null, null);
    }

}
