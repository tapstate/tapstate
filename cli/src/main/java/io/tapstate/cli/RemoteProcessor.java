package io.tapstate.cli;

/**
 * One processor instance of a vertex, as read back from the server (rule R6: mirrored, not shared).
 *
 * @param index      the processor's index within this execution of its vertex
 * @param memberUuid the engine identity of the member running it
 * @param nodeId     that member's stable id, or null when it carries none
 */
record RemoteProcessor(Integer index, String memberUuid, String nodeId) {
}
