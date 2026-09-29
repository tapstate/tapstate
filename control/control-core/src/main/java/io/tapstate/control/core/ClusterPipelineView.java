package io.tapstate.control.core;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * One pipeline, as the cluster holds it: who owns it, who reads its sources, and where its work runs.
 *
 * <p>Every pipeline the cluster has been asked to run is listed, whether or not it is running. A list of
 * what happens to be executing would leave out the pipeline that is supposed to be executing and is not,
 * which is the first one anybody looks for.
 *
 * @param pipelineId      the pipeline
 * @param controllerClaim who currently actuates it, or null when nobody has filed a claim -- a single-node
 *                        build fences nothing, and a pipeline nobody has started yet has no owner
 * @param captureClaims   one per capture this pipeline reads through, in a stable order. A capture with no
 *                        claim filed is absent rather than listed as unowned: its identity is derived from
 *                        the stored contract, so "nobody has taken it" and "it does not exist" would
 *                        otherwise look the same
 * @param measuredAt      when the readings behind {@code vertices} were taken, or null when none have
 *                        been. A run is known to exist before its processors are, and an answer with no
 *                        timestamp is one nobody has measured rather than a run with no processors
 * @param measuredFrom    the members those readings came from, by engine identity. Compared against the
 *                        cluster's members it says whether this picture is complete -- for the first
 *                        seconds of a run it is assembled from some members and not yet others, and
 *                        without this a half-measured run and a genuinely narrow one read the same
 * @param awaitingRebalance the members that are part of this cluster but carry no part of this run,
 *                          by stable id. A run keeps the members it was planned over, so a member that
 *                          arrived afterwards is given nothing until somebody asks for a rebalance --
 *                          and "I added a machine and nothing happened" is the question this answers.
 *                          <br/>Empty when nothing has been measured. For the first seconds of a run a
 *                          member that has not reported yet cannot be told from one carrying nothing,
 *                          which is what {@code measuredAt} and {@code measuredFrom} are there to say;
 *                          a member that joined a run already going is exact, and that is the case this
 *                          is read for
 * @param vertices        the vertices of the current execution, empty when it has no live run
 */
public record ClusterPipelineView(
        String pipelineId,
        ClusterClaimView controllerClaim,
        List<ClusterClaimView> captureClaims,
        Instant measuredAt,
        List<String> measuredFrom,
        List<String> awaitingRebalance,
        List<ClusterVertexView> vertices) {

    public ClusterPipelineView {
        pipelineId = Objects.requireNonNull(pipelineId, "pipelineId");
        captureClaims = List.copyOf(Objects.requireNonNull(captureClaims, "captureClaims"));
        measuredFrom = List.copyOf(Objects.requireNonNull(measuredFrom, "measuredFrom"));
        awaitingRebalance = List.copyOf(Objects.requireNonNull(awaitingRebalance, "awaitingRebalance"));
        vertices = List.copyOf(Objects.requireNonNull(vertices, "vertices"));
    }
}
