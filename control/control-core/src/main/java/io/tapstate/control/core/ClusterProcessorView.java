package io.tapstate.control.core;

/**
 * One working processor instance of one vertex, and the member running it.
 *
 * <p>The pair {@code (vertex, index)} is the identity that survives the work being spread out: once a
 * vertex runs on several members at once, a single member named against the vertex could not say which
 * part of it ran where, and there would be no way to add that later without taking a field away.
 *
 * @param index      the processor's index within this execution of its vertex
 * @param memberUuid the engine identity of the member running it
 * @param nodeId     that member's stable id, or null when it carries no Tapstate identity
 */
public record ClusterProcessorView(int index, String memberUuid, String nodeId) {
}
