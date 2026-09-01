package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.PipelineState;
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

    /** The same edit, refused for the state T12's own wording would have allowed. */
    @Test
    void refusesTheChangeWhileTheOnlyReaderIsMerelyPaused() {
        state.put("p1", PipelineState.PAUSED);
        desired.put("p1", PipelineState.PAUSED);
        List<Resource> stored = List.of(source("orders", true), pipeline("p1", "orders"));

        assertThatThrownBy(() -> live.refuseBufferingChangeWhileLive(
                source("orders", true), source("orders", false), stored))
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
        return new PipelineResource(id, null, List.of(sourceId), null, null, null, null, null);
    }

}
