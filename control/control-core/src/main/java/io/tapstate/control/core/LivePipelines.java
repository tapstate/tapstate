package io.tapstate.control.core;

import io.tapstate.core.common.TapstateException;
import io.tapstate.core.dsl.ReferenceGraph;
import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.lifecycle.PipelineState;
import io.tapstate.core.lifecycle.StateJson;
import io.tapstate.core.model.PipelineResource;
import io.tapstate.core.model.Resource;
import io.tapstate.core.model.SourceResource;
import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.StateStore;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Whether a pipeline is at rest, and what that permits to be changed underneath it.
 *
 * <p>One reading, shared by every guard that asks the question. A guard that judged "still stopped"
 * differently from another would pass exactly the pipelines the other exists to catch, and the two
 * would only ever disagree in the states nobody checks by hand.
 *
 * <p>Both halves of the lifecycle are read, because either alone lets a live pipeline through: the
 * desired state alone passes one whose stop has been requested but not yet reached, which is still
 * executing; the actual state alone passes one whose start has been requested but not yet reached,
 * which the next convergence would raise onto whatever was changed underneath it.
 */
public final class LivePipelines {

    /** Actual states a pipeline is at rest in; any other means it is still executing. */
    private static final Set<PipelineState> RESTING = Set.of(
            PipelineState.NEW, PipelineState.STOPPED, PipelineState.COMPLETED, PipelineState.FAILED);

    /** Desired states that will drive a pipeline back up, whatever it is doing right now. */
    private static final Set<PipelineState> HEADED_UP = Set.of(
            PipelineState.RUNNING, PipelineState.PAUSED);

    private final DesiredStore desired;
    private final StateStore state;

    public LivePipelines(DesiredStore desired, StateStore state) {
        this.desired = Objects.requireNonNull(desired, "desired");
        this.state = Objects.requireNonNull(state, "state");
    }

    /** Whether the pipeline is at rest right now: not executing, and not headed back up. */
    public boolean isAtRest(String pipelineId) {
        return isAtRest(actualStateOf(pipelineId), intentOf(pipelineId));
    }

    /** Whether those two readings together mean at rest -- the one definition every caller shares. */
    public static boolean isAtRest(PipelineState actual, PipelineState intent) {
        return RESTING.contains(actual) && !HEADED_UP.contains(intent);
    }

    /** What the pipeline is doing. A pipeline with no checkpoint has never run. */
    public PipelineState actualStateOf(String pipelineId) {
        return state.read(pipelineId)
                .map(checkpoint -> StateJson.parse(checkpoint.stateJson()))
                .orElse(PipelineState.NEW);
    }

    /** Where the pipeline has been asked to be. A pipeline with no intent has been asked for nothing. */
    public PipelineState intentOf(String pipelineId) {
        return desired.read(pipelineId).map(DesiredState::targetState).orElse(PipelineState.NEW);
    }

    /**
     * Refuses a change to whether a source buffers through the shared replay store while a pipeline
     * reading it is still up.
     *
     * <p>The flag decides how a run is assembled, and a run is assembled once, when it starts. Changing
     * it under a live pipeline therefore changes nothing now and everything at the next start -- the
     * artifact and the run that is executing disagree from that moment on, and nothing reports it. The
     * refusal is what turns that into something the author is told about while they can still act on it.
     *
     * <p><strong>Paused counts as up, and that is the case this exists for.</strong> Pausing suspends
     * the engine and nothing else: the capture keeps reading, the source connection stays held. It is
     * the one state that looks stopped from the outside while the thing this guards is still running,
     * so it is also the state an author is most likely to make this change from.
     *
     * <p>Only pipelines are considered, and only ones that reference this source. A source nothing reads
     * has nothing to disturb, and a change from a source that did not previously exist is not a change.
     */
    public void refuseBufferingChangeWhileLive(
            SourceResource stored, SourceResource replacement, List<Resource> allStored) {
        Objects.requireNonNull(replacement, "replacement");
        Objects.requireNonNull(allStored, "allStored");
        if (stored == null || stored.srsEnabled() == replacement.srsEnabled()) {
            return;
        }
        Set<String> pipelines = allStored.stream()
                .filter(PipelineResource.class::isInstance)
                .map(Resource::id)
                .collect(Collectors.toSet());
        List<String> live = ReferenceGraph.of(allStored).referencedBy(stored.id()).stream()
                .map(ReferenceGraph.Edge::id)
                .filter(pipelines::contains)
                .filter(id -> !isAtRest(id))
                .sorted()
                .toList();
        if (!live.isEmpty()) {
            throw new TapstateException(
                    SourceError.SRS_CHANGE_WHILE_RUNNING,
                    Map.of("id", stored.id(), "pipelines", live),
                    null);
        }
    }
}
