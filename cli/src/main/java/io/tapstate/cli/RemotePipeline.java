package io.tapstate.cli;

import java.util.List;

/**
 * One pipeline as the cluster holds it, read back from the server (rule R6: mirrored, not shared).
 *
 * @param pipelineId      the pipeline
 * @param controllerClaim who actuates it, or null when nobody has filed a claim
 * @param captureClaims   one per capture it reads through that somebody has claimed
 * @param measuredAt      when the readings behind {@code vertices} were taken, in the server's own
 *                        words, or null when none have been
 * @param measuredFrom    the members those readings came from; against the member list it says whether
 *                        the picture is complete
 * @param awaitingRebalance the members of this cluster carrying no part of this run, by stable id --
 *                          a member that arrived after it started is given nothing until somebody asks
 *                          for a rebalance
 * @param vertices        the vertices of the current execution, empty when it has no live run
 */
record RemotePipeline(
        String pipelineId,
        RemoteClaim controllerClaim,
        List<RemoteClaim> captureClaims,
        String measuredAt,
        List<String> measuredFrom,
        List<String> awaitingRebalance,
        List<RemoteVertex> vertices) {

    RemotePipeline {
        captureClaims = List.copyOf(captureClaims);
        measuredFrom = List.copyOf(measuredFrom);
        awaitingRebalance = List.copyOf(awaitingRebalance);
        vertices = List.copyOf(vertices);
    }
}
