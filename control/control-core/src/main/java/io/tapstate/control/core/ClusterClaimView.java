package io.tapstate.control.core;

/**
 * One workload claim, as a control-plane reader sees it.
 *
 * <p>The generations are here rather than beside the pipeline because every claim has its own pair and
 * they fence different things: the controller's execution generation is what tells one actuation of a
 * pipeline from the next, and a capture's is what tells one reader of a source from the next. A single
 * pair hung off the pipeline would have to mean the controller's, and would read as the pipeline's.
 *
 * @param resourceId         what this claim is over -- the pipeline for a controller claim, the capture
 *                           contract for a capture claim
 * @param ownerNodeId        the stable node that holds it; this is what makes a claim readable after the
 *                           process that took it is gone
 * @param ownerBootId        the boot of that node which took it, so a node that restarted is not read as
 *                           the one that was holding this
 * @param claimGeneration    increases every time ownership changes hands
 * @param executionGeneration increases every time the owner starts a new run under the same ownership
 * @param topologyRevision   the committed membership this claim was taken under, which is what says
 *                           whether it predates the cluster as it now stands
 * @param leased             whether the store still considered it owned at the moment it answered. A
 *                           claim whose lease has lapsed still has a record and still names its last
 *                           owner, and reading the record alone cannot tell the two apart
 */
public record ClusterClaimView(
        String resourceId,
        String ownerNodeId,
        String ownerBootId,
        long claimGeneration,
        long executionGeneration,
        long topologyRevision,
        boolean leased) {
}
