package io.tapstate.app;

import com.hazelcast.jet.core.DAG;
import io.tapstate.core.lifecycle.PipelineStateHolding;
import io.tapstate.runtime.engine.nest.NestSettings;

import java.util.List;
import java.util.Set;

/**
 * Supplies the Jet topology a pipeline runs, and the namespaces that topology keeps state in. The actuator
 * asks for both as it starts the pipeline's job — the namespaces to write down where this run will keep
 * state, while the pipeline being asked is still the one the run is built from. Kept a seam so the topology
 * a pipeline runs can vary — the store-backed builder in production, an idle stand-in in a lifecycle test —
 * without the actuator, which drives the job by pipeline id alone, having to change.
 *
 * <p>Both are answered here rather than the second one somewhere else, because they have to come from the
 * same compiled tree: a topology built one way and dropped by names worked out another way would leave
 * state behind under names nothing would ever name again.
 */
interface DagSource {

    /**
     * Validates the pipeline's start preconditions before the actuator starts capture or submits a job.
     * Store-backed implementations use this to reject a sync whose source model has not been discovered;
     * lightweight test sources have no store-backed preconditions and keep the no-op default.
     */
    default void validateStart(String pipelineId) {
    }

    /** The topology to run for {@code pipelineId}. */
    DAG dagFor(String pipelineId);

    /**
     * What {@code pipelineId}'s topology keeps state in — for each component that keeps any, what to call
     * it, whose it is, and the namespaces it is kept under. Empty where the pipeline keeps none.
     *
     * <p>Deliberately not a defaulted method. An implementation that quietly answered "none" would leave
     * every namespace it owns unrecorded, and nothing about that announces itself: the pipeline stops, the
     * state stays, and the next run reads it as its own.
     *
     * <p>One answer rather than two, because a stop reads both halves of it: what it clears is the
     * namespaces named here, and what it says it is about to clear is the labels named here. A second list
     * written out by hand would agree with this one only for as long as somebody kept checking, and the
     * disagreement it eventually produces is a stop reporting it cleared everything while leaving
     * something behind. It is also what lets a component that starts keeping state arrive as a
     * declaration and nothing else: named here, it is dropped and spoken about without an edit anywhere.
     */
    List<PipelineStateHolding> stateHeldBy(String pipelineId);

    /**
     * What {@code pipelineId}'s nests are held to, and the map namespaces those numbers apply to.
     *
     * <p>Both together rather than one each, for the reason the two above are: they come from the same
     * compiled tree. Numbers applied to namespaces worked out separately would hold maps nothing writes to
     * and leave the ones that are written to on somebody else's number.
     *
     * <p>The namespaces here are the ones that are maps, which is fewer than {@link #stateHeldBy}
     * returns - that one also names where the tree's shape is written down, which lives in the store alone
     * and has no map to configure.
     */
    NestCapacity capacityOf(String pipelineId);

    /** A pipeline's nest settings, and the state map namespaces they apply to. */
    record NestCapacity(Set<String> mapNamespaces, NestSettings settings) {

        /** A pipeline with no nest in it: no namespaces to hold, and nothing asking to be held. */
        static NestCapacity none() {
            return new NestCapacity(Set.of(), NestSettings.defaults());
        }
    }
}
