package io.tapstate.cli;

import java.util.List;

/**
 * One vertex of a running pipeline, as read back from the server (rule R6: mirrored, not shared).
 *
 * @param name          the vertex's name in the run's plan
 * @param requested     the parallelism the plan asked for, or null when it asked for none
 * @param effective     how many processors are doing its work, or null when the server did not say
 * @param computedLocal the per-member parallelism a planner worked out, or null when none did
 * @param executionId   the execution these processors belong to
 * @param processors    the working processors, by index
 */
record RemoteVertex(
        String name,
        Integer requested,
        Integer effective,
        Integer computedLocal,
        String executionId,
        List<RemoteProcessor> processors) {

    RemoteVertex {
        processors = List.copyOf(processors);
    }
}
