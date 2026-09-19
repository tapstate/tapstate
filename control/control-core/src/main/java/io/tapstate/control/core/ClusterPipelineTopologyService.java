package io.tapstate.control.core;

import io.tapstate.spi.store.DesiredStore;
import io.tapstate.spi.store.WorkloadClaim;
import io.tapstate.spi.store.WorkloadClaimKey;
import io.tapstate.spi.store.WorkloadClaimReading;
import io.tapstate.spi.store.WorkloadClaimStore;
import io.tapstate.spi.store.WorkloadClaimType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeSet;

/**
 * The pipeline half of the topology: for each pipeline the cluster has been asked to run, who owns it,
 * who reads its sources, and where the work of its current run is happening.
 *
 * <p>The pipelines come from the desired intent rather than from what is executing, so a pipeline that is
 * supposed to be running and is not still appears -- with a claim, perhaps, and no vertices. That is the
 * first case anybody reads this face for, and a list built from live runs is exactly the list it is
 * missing from.
 *
 * <p>Ownership is read from the durable claims, so every node answers the same without asking anyone.
 * The vertices are asked of the engine, which every member can ask equally.
 */
public final class ClusterPipelineTopologyService {

    private final LivePipelineRuns runs;
    private final PipelineCaptures captures;
    private final WorkloadClaimStore claims;
    private final DesiredStore desired;
    private final String clusterId;

    /**
     * @param claims    the workload-claim store, or null on a build that fences nothing -- a single node
     *                  owns everything it runs, and there is nobody to take it from
     * @param desired   the desired-intent store, or null when there is none to enumerate
     * @param clusterId the cluster claims are filed under; with none, no claim can be looked up
     */
    public ClusterPipelineTopologyService(
            LivePipelineRuns runs,
            PipelineCaptures captures,
            WorkloadClaimStore claims,
            DesiredStore desired,
            String clusterId) {
        this.runs = Objects.requireNonNull(runs, "runs");
        this.captures = Objects.requireNonNull(captures, "captures");
        this.claims = claims;
        this.desired = desired;
        this.clusterId = clusterId;
    }

    /**
     * Every pipeline the cluster holds, ordered by id.
     *
     * @param members the member half's answer. A processor is reported under the engine's identity for
     *                this run of a member, and a claim names the stable one, so the pairing has to come
     *                from here for the two halves to be joined at all. It is also what says which
     *                members a run is carrying no part of
     */
    public List<ClusterPipelineView> pipelines(List<ClusterMemberView> members) {
        if (desired == null) {
            return List.of();
        }
        Map<String, String> nodeIdByMemberUuid = new HashMap<>();
        for (ClusterMemberView member : members) {
            if (member.memberUuid() != null && member.nodeId() != null) {
                nodeIdByMemberUuid.put(member.memberUuid(), member.nodeId());
            }
        }
        Map<String, LivePipelineRun> runById = new HashMap<>();
        for (LivePipelineRun run : runs.runs()) {
            runById.put(run.pipelineId(), run);
        }
        List<ClusterPipelineView> views = new ArrayList<>();
        // Sorted, for the same reason the members are: a list that reshuffles between reads cannot be
        // diffed by anybody.
        for (String pipelineId : new TreeSet<>(desired.pipelineIds())) {
            LivePipelineRun run = runById.get(pipelineId);
            views.add(new ClusterPipelineView(
                    pipelineId,
                    claimOver(WorkloadClaimType.PIPELINE_ACTUATION, pipelineId),
                    captureClaims(pipelineId),
                    run == null ? null : run.measuredAt(),
                    run == null ? List.of() : List.copyOf(new TreeSet<>(run.measuredFrom())),
                    run == null ? List.of() : awaitingRebalance(run, members),
                    run == null ? List.of() : vertices(run, nodeIdByMemberUuid)));
        }
        return views;
    }

    /**
     * The members of this cluster that carry no part of this run.
     *
     * <p>Read off the run itself rather than off anything the owner remembers, because only the owner
     * remembers it and every node has to answer this the same. The engine gives a run an instance of
     * every vertex on every member it planned the run over -- a working one, or a placeholder where the
     * vertex is pinned elsewhere -- so a member with no instance at all is a member the run was not
     * planned over. That is what a member which joined after the run started looks like.
     *
     * <p>Only members the cluster has committed are judged: one that is still joining has not been given
     * work because it is not yet entitled to any, which is a different thing and the member half says it.
     */
    private static List<String> awaitingRebalance(
            LivePipelineRun run, List<ClusterMemberView> members) {
        if (run.measuredAt() == null) {
            return List.of();
        }
        Set<String> carrying = new HashSet<>();
        for (LivePipelineVertex vertex : run.vertices()) {
            for (LivePipelineProcessor processor : vertex.processors()) {
                carrying.add(processor.memberUuid());
            }
        }
        List<String> awaiting = new ArrayList<>();
        for (ClusterMemberView member : members) {
            if (member.state() == ClusterMemberState.ACTIVE
                    && member.nodeId() != null
                    && !carrying.contains(member.memberUuid())) {
                awaiting.add(member.nodeId());
            }
        }
        awaiting.sort(Comparator.naturalOrder());
        return List.copyOf(awaiting);
    }

    private List<ClusterClaimView> captureClaims(String pipelineId) {
        List<ClusterClaimView> views = new ArrayList<>();
        for (String captureId : new TreeSet<>(captures.captureIds(pipelineId))) {
            ClusterClaimView claim = claimOver(WorkloadClaimType.CAPTURE, captureId);
            if (claim != null) {
                views.add(claim);
            }
        }
        return views;
    }

    /** The claim over one resource, or null when nothing fences it and when nobody has taken it. */
    private ClusterClaimView claimOver(WorkloadClaimType type, String resourceId) {
        if (claims == null || clusterId == null) {
            return null;
        }
        Optional<WorkloadClaimReading> reading =
                claims.read(new WorkloadClaimKey(clusterId, type, resourceId));
        if (reading.isEmpty()) {
            return null;
        }
        WorkloadClaim claim = reading.get().claim();
        return new ClusterClaimView(
                claim.key().resourceId(),
                claim.owner().nodeId(),
                claim.owner().bootId(),
                claim.claimGeneration(),
                claim.executionGeneration(),
                claim.topologyRevision(),
                reading.get().leased());
    }

    private static List<ClusterVertexView> vertices(
            LivePipelineRun run, Map<String, String> nodeIdByMemberUuid) {
        List<ClusterVertexView> views = new ArrayList<>();
        for (LivePipelineVertex vertex : run.vertices()) {
            List<ClusterProcessorView> processors = new ArrayList<>();
            for (LivePipelineProcessor processor : vertex.processors()) {
                if (!processor.working()) {
                    continue;
                }
                processors.add(new ClusterProcessorView(
                        processor.index(),
                        processor.memberUuid(),
                        nodeIdByMemberUuid.get(processor.memberUuid())));
            }
            processors.sort((left, right) -> Integer.compare(left.index(), right.index()));
            views.add(new ClusterVertexView(
                    vertex.name(),
                    // Nothing in the plan pins a vertex's parallelism yet, and nothing computes a
                    // per-member one, so both are absent rather than reported as whatever ran.
                    null,
                    processors.size(),
                    null,
                    run.executionId(),
                    processors));
        }
        return views;
    }
}
