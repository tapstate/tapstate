package io.tapstate.e2e;

/**
 * One member as the cluster's read face describes it, carrying what says which boot of that node it is.
 *
 * <p>The stable node id alone cannot say. It is the same across every restart of a node, which is the
 * whole point of it; the three fields beside it are the ones that change when a process is replaced,
 * and a case about a replacement has to be able to watch them change while the id does not.
 */
record ClusterMemberFacts(
        String nodeId, String memberUuid, String bootId, String controlUrl, String state) {
}
