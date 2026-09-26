package io.tapstate.app;

import com.hazelcast.jet.core.DAG;
import io.tapstate.core.lifecycle.PipelineStateHolding;
import io.tapstate.core.model.BatchSpec;
import io.tapstate.runtime.engine.ExecutionShape;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.spi.store.ArtifactStore;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

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
     * Captures and validates the pipeline revision this start will use, then returns a deferred builder.
     * The deferment lets an outstanding teardown finish before DAG construction reads or records shape
     * state, while the captured artifact revision remains fixed across every answer below.
     */
    default StartPreparation prepareStart(String pipelineId, String defaultDatabase) {
        validateStart(pipelineId);
        NestCapacity capacity = capacityOf(pipelineId);
        Set<OperatorStateLocation> locations = stateLocations(pipelineId, defaultDatabase);
        return new StartPreparation(
                capacity, locations, Optional.empty(), fence -> plannedDagFor(pipelineId, fence));
    }

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
     * The topology to run for {@code pipelineId}, with every external effect in it held to {@code fence}'s
     * run — so a member still carrying a piece of an earlier run stops writing rather than writing beside
     * the current one.
     *
     * <p>Defaulted to the unfenced topology for the stand-ins a lifecycle test drives, whose topologies
     * reach nothing outside the process and therefore have nothing to fence. The store-backed builder
     * overrides it; a member of a cluster only ever reaches this one.
     */
    default DAG dagFor(String pipelineId, ExecutionFence fence) {
        return dagFor(pipelineId);
    }

    /**
     * The topology to run, held to {@code fence}'s run as {@link #dagFor(String, ExecutionFence)} is, together
     * with how wide it was planned to run: the width each node was worked out to run at, the members that was
     * worked out for, and the batch each node takes its input in.
     *
     * <p>Defaulted, for the stand-ins a lifecycle test drives, to a topology planned over nothing: every node
     * runs as one processor for the cluster, and there is no plan to say so.
     */
    default PlannedDag plannedDagFor(String pipelineId, ExecutionFence fence) {
        return new PlannedDag(dagFor(pipelineId, fence), ExecutionShape.totalOne(), List.of(), Map.of());
    }

    /**
     * A topology and the plan it was drawn from: {@code shape} holds each node's width, worked out for
     * {@code members} - by stable id - and {@code batches} the batch each node takes its input in.
     */
    record PlannedDag(DAG dag, ExecutionShape shape, List<String> members, Map<String, BatchSpec> batches) {

        public PlannedDag {
            Objects.requireNonNull(dag, "dag");
            Objects.requireNonNull(shape, "shape");
            members = List.copyOf(Objects.requireNonNull(members, "members"));
            batches = Map.copyOf(Objects.requireNonNull(batches, "batches"));
        }
    }

    /**
     * What {@code pipelineId}'s topology keeps state in — for each component that keeps any, what to call
     * it, whose it is, and the namespaces it is kept under. Empty where the pipeline keeps none.
     *
     * <p>Every component that keeps some, not one of them: a pipeline that both nests and joins is
     * answered with both sets, and one that only joins is still answered.
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
     * The physical locations behind {@link #stateHeldBy}. The generic answer resolves every declared
     * namespace through the deployment default supplied by the store-owning actuator; the store-backed
     * source overrides it to route Nest namespaces to their per-operator databases.
     */
    default Set<OperatorStateLocation> stateLocations(String pipelineId, String defaultDatabase) {
        Objects.requireNonNull(defaultDatabase, "defaultDatabase");
        java.util.LinkedHashSet<OperatorStateLocation> locations = new java.util.LinkedHashSet<>();
        stateHeldBy(pipelineId).forEach(holding -> holding.namespaces().forEach(namespace ->
                locations.add(new OperatorStateLocation(defaultDatabase, namespace))));
        return Set.copyOf(locations);
    }

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
    record NestCapacity(Map<String, String> mapDatabases, NestSettings settings) {

        Set<String> mapNamespaces() {
            return mapDatabases.keySet();
        }

        /** A pipeline with no nest in it: no namespaces to hold, and nothing asking to be held. */
        static NestCapacity none() {
            return new NestCapacity(Map.of(), NestSettings.defaults());
        }
    }

    /**
     * The validated, artifact-derived inputs available before placement is configured. DAG construction is
     * deferred until after placement and pending teardown because it may inspect and record operator shape.
     */
    record StartPreparation(
            NestCapacity capacity,
            Set<OperatorStateLocation> stateLocations,
            Optional<ArtifactStore> artifactSnapshot,
            Function<ExecutionFence, PlannedDag> dagBuilder) {

        public StartPreparation {
            Objects.requireNonNull(capacity, "capacity");
            stateLocations = Set.copyOf(Objects.requireNonNull(stateLocations, "stateLocations"));
            Objects.requireNonNull(artifactSnapshot, "artifactSnapshot");
            Objects.requireNonNull(dagBuilder, "dagBuilder");
        }

        /**
         * Builds the topology, with every external effect in it held to {@code fence}'s run. Taking the
         * fence here rather than at preparation is what keeps the two in step: the run is fenced before
         * the first side effect of a start, and the topology is built after placement and teardown, so
         * the generation the build is held to is the one this member has just been granted.
         *
         * <p>A null fence is the single-member path, where there is one run of anything and so nothing
         * for a second one to be held against. Deliberately the only way to get an unfenced topology
         * from a preparation: a no-argument build would let a clustered start drop its fence without
         * anything in the call saying so.
         */
        StartPlan build(ExecutionFence fence) {
            return new StartPlan(dagBuilder.apply(fence), capacity, stateLocations, artifactSnapshot);
        }
    }

    /** Every artifact-derived input to one start, resolved from one immutable snapshot. */
    record StartPlan(
            PlannedDag planned,
            NestCapacity capacity,
            Set<OperatorStateLocation> stateLocations,
            Optional<ArtifactStore> artifactSnapshot) {

        public StartPlan {
            Objects.requireNonNull(planned, "planned");
            Objects.requireNonNull(capacity, "capacity");
            stateLocations = Set.copyOf(Objects.requireNonNull(stateLocations, "stateLocations"));
            Objects.requireNonNull(artifactSnapshot, "artifactSnapshot");
        }

        /** The topology to submit. */
        DAG dag() {
            return planned.dag();
        }
    }
}
