package io.tapstate.core.lifecycle;

import java.util.List;

/**
 * Answers, from what a pipeline's writers have durably recorded, which of a set of initial loads are still
 * landing. Resolved on the member a source runs on, from the same record its sinks write their progress to.
 */
@FunctionalInterface
public interface LoadLandings {

    /**
     * Those of {@code awaited} not yet landed: some writer named with the load has not durably landed every row
     * of it in this run. A load the pipeline recorded as landed in an earlier run is landed, and so is a table
     * whose load the pipeline does not read at all - nothing of either is left to overtake.
     */
    List<AwaitedLoad> stillLanding(List<AwaitedLoad> awaited);
}
