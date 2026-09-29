package io.tapstate.control.core;

/**
 * One processor instance of one vertex, as the engine currently reports it.
 *
 * <p>{@code index} is the index the engine gives the instance across the whole execution, not within its
 * member. It is the half of the identity that stays meaningful once a vertex runs on several members at
 * once: two members each numbering their own instances from zero would produce two processors that
 * cannot be told apart.
 *
 * @param index      the processor's index within this execution of its vertex
 * @param memberUuid the engine identity of the member running it
 * @param working    whether this instance does the vertex's work. A vertex the plan pins to one member
 *                   still has an instance on every other member, and that instance does nothing. Reporting
 *                   those as workers would say a pinned vertex runs everywhere, which is the opposite of
 *                   what pinning it means
 */
public record LivePipelineProcessor(int index, String memberUuid, boolean working) {
}
