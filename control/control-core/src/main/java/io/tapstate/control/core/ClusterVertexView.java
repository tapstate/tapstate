package io.tapstate.control.core;

import java.util.List;
import java.util.Objects;

/**
 * One vertex of a running pipeline, and where its work is actually happening.
 *
 * <p>{@code requested} and {@code computedLocal} are part of the shape before anything fills them, and
 * that is deliberate: they are what a reader will compare against {@code effective} to answer "why is
 * this running as wide as it is", and a shape that gained them later would make every reader written
 * against this one wrong about what {@code effective} meant.
 *
 * @param name          the vertex's name in the run's plan
 * @param requested     the target its node was given, read from the plan the run was submitted on, or null
 *                      where the vertex does not run at its node's width - one gathering several producers
 *                      into one - or the run has no plan recorded for this execution
 * @param effective     how many processors are actually doing this vertex's work in this execution, or
 *                      null when nothing has been measured. Placeholder instances are not counted --
 *                      see {@link #processors()}
 * @param computedLocal the per-member count the plan worked out for its node, or null where the node runs as
 *                      one processor for the cluster, or for the same reasons {@code requested} is
 * @param executionId   the engine's identity for the execution these processors belong to
 * @param processors    the working processors, by index. A vertex the plan pins to one member still has
 *                      an instance on every other member and that instance does nothing; listing those
 *                      would report a pinned vertex as running everywhere
 */
public record ClusterVertexView(
        String name,
        Integer requested,
        Integer effective,
        Integer computedLocal,
        String executionId,
        List<ClusterProcessorView> processors) {

    public ClusterVertexView {
        name = Objects.requireNonNull(name, "name");
        processors = List.copyOf(Objects.requireNonNull(processors, "processors"));
    }
}
