package io.tapstate.control.core;

/**
 * What a member is to the cluster as a whole, as opposed to what it is to any one pipeline.
 *
 * <p>The distinction is the point of having the field at all: a node that is up, reachable and doing
 * nothing looks identical from outside to a node that is up, reachable and broken. Saying which of the
 * two it is, in the same answer that lists it, is what lets a reader stop guessing.
 */
public enum ClusterMemberState {

    /** Committed by a majority as part of this cluster, and therefore eligible for work. */
    ACTIVE,

    /**
     * Seen by the engine but not yet in the committed member set. Ordinary and brief -- committing a
     * change in membership is a round trip on its own cadence -- and worth naming, because a node that
     * stays here is a node whose commit is not happening.
     */
    JOINING
}
