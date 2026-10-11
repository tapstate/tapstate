package io.tapstate.app;

import java.util.Set;

/**
 * Whether every member a pipeline's run would take part on can load the connectors its sinks open, asked before
 * the run starts anything.
 *
 * <p>A sink opens its connector on whichever member runs it, and that can be any member: a sink running on every
 * member opens one on each, and one running as a single processor for the cluster is placed on a member nobody
 * chooses. A member that cannot load the connector finds out only then - with the run already reading - and the
 * reason stays on that member. Asking every member first turns that into a start refused with the member, the
 * connector and the reason named, before any capture is opened or any job exists.
 */
interface ConnectorReadiness {

    /** Asks no member, and so knows of no connector certified to be shared. */
    ConnectorReadiness NONE = (pipelineId, connectors) -> Set.of();

    /**
     * Returns once every member the run would take part on has loaded each of {@code connectors}, the same
     * artifact on every member, with those of them every member loaded as an artifact certified to be shared by
     * the writers on one member; otherwise refuses the start with a code naming the member and connector that
     * could not, and why.
     */
    Set<String> requireEveryMemberCanLoad(String pipelineId, Set<String> connectors);
}
