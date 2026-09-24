package io.tapstate.control.core;

import java.util.List;

/**
 * The pipeline runs the engine is carrying right now. A port, for the same reason the member list is one:
 * what is executing is visible only to the engine layer, and this ring does not link it.
 *
 * <p>Live, and therefore never stored. Every member can answer it, because a run belongs to the cluster
 * rather than to the member that submitted it -- which is what lets the read face keep its promise that
 * any node answers the same.
 */
@FunctionalInterface
public interface LivePipelineRuns {

    /** Every run the engine is carrying, in whatever order it reports them. */
    List<LivePipelineRun> runs();

    /** A build with no engine behind it, which carries nothing rather than claiming an empty cluster. */
    static LivePipelineRuns none() {
        return List::of;
    }
}
