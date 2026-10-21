package io.tapstate.control.core;

import io.tapstate.core.lifecycle.ExecutionPlans;
import io.tapstate.core.lifecycle.ExecutionPlan;
import io.tapstate.core.lifecycle.ProcessorRuntimeContext;
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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Objects;
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
    private final ExecutionPlans plans;

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
        this(runs, captures, claims, desired, clusterId, ExecutionPlans.NONE);
    }

    /**
     * As above, reading how wide each run was planned to run from {@code plans}: a vertex that runs at its node's
     * width then says the target that node was given and the per-member count worked out for it.
     */
    public ClusterPipelineTopologyService(
            LivePipelineRuns runs,
            PipelineCaptures captures,
            WorkloadClaimStore claims,
            DesiredStore desired,
            String clusterId,
            ExecutionPlans plans) {
        this.runs = Objects.requireNonNull(runs, "runs");
        this.captures = Objects.requireNonNull(captures, "captures");
        this.claims = claims;
        this.desired = desired;
        this.clusterId = clusterId;
        this.plans = Objects.requireNonNull(plans, "plans");
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
        Set<String> currentMemberUuids = members.stream().map(ClusterMemberView::memberUuid)
                .filter(Objects::nonNull).collect(java.util.stream.Collectors.toUnmodifiableSet());
        Map<String, LivePipelineRun> runById = new HashMap<>();
        for (LivePipelineRun run : runs.runs()) {
            runById.put(run.pipelineId(), run);
        }
        // Sorted, for the same reason the members are: a list that reshuffles between reads cannot be
        // diffed by anybody.
        Set<String> pipelineIds = new TreeSet<>(desired.pipelineIds());
        Map<String, Set<String>> captureIds = captureIds(pipelineIds);
        Map<WorkloadClaimKey, WorkloadClaimReading> readings = readings(pipelineIds, captureIds);
        // Every pipeline's plan in one read, for the reason the claims are: a face listing every pipeline must
        // not ask the cluster once per pipeline.
        Map<String, ExecutionPlan> planById = runById.isEmpty() ? Map.of() : plans.current(runById.keySet());
        List<ClusterPipelineView> views = new ArrayList<>();
        for (String pipelineId : pipelineIds) {
            LivePipelineRun run = runById.get(pipelineId);
            ClusterClaimView controller = claimOver(readings, WorkloadClaimType.PIPELINE_ACTUATION, pipelineId);
            Map<ProcessorSlot, ProcessorRuntimeContext> nativeSlots = run == null ? Map.of()
                    : nativeSlots(run, members, controller);
            views.add(new ClusterPipelineView(
                    pipelineId,
                    controller,
                    captureClaims(readings, captureIds.getOrDefault(pipelineId, Set.of())),
                    run == null ? null : run.measuredAt(),
                    run == null ? List.of() : List.copyOf(new TreeSet<>(run.measuredFrom())),
                    run == null ? List.of() : awaitingRebalance(run, members, currentMemberUuids, nativeSlots),
                    run == null ? List.of()
                            : vertices(run, nodeIdByMemberUuid, currentMemberUuids, nativeSlots,
                                    plannedNodes(planById.get(pipelineId), controller, run.processorContexts()))));
        }
        return views;
    }

    /**
     * The node each vertex of a run runs the width of, from the plan the run was submitted on - and only that
     * plan. One written for an earlier execution than the claim now names says nothing about the run executing:
     * its widths were worked out for another run, and reporting them would put an old answer beside a new run.
     */
    private static Map<String, ExecutionPlan.Node> plannedNodes(ExecutionPlan plan, ClusterClaimView controller,
            List<ProcessorRuntimeContext> contexts) {
        if (plan == null) {
            return Map.of();
        }
        if (controller != null && !Objects.equals(plan.executionGeneration(), controller.executionGeneration())) {
            return Map.of();
        }
        if (contexts.stream().anyMatch(context -> !Objects.equals(plan.executionGeneration(), context.executionGeneration()))) {
            return Map.of();
        }
        Map<String, ExecutionPlan.Node> byVertex = new HashMap<>();
        for (ExecutionPlan.Node node : plan.nodes()) {
            for (String vertex : node.vertices()) {
                byVertex.put(vertex, node);
            }
        }
        return byVertex;
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
            LivePipelineRun run, List<ClusterMemberView> members, Set<String> currentMemberUuids,
            Map<ProcessorSlot, ProcessorRuntimeContext> nativeSlots) {
        if (run.measuredAt() == null) {
            return List.of();
        }
        Set<String> carrying = new HashSet<>();
        for (LivePipelineVertex vertex : run.vertices()) {
            for (LivePipelineProcessor processor : vertex.processors()) {
                carrying.add(processor.memberUuid());
                ProcessorRuntimeContext context = contextFor(run, vertex.name(), processor, currentMemberUuids, nativeSlots);
                if (context != null) { carrying.add(context.memberUuid()); }
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

    /** Whether there is anything to look a claim up in: a claim store, and a cluster to name claims in. */
    private boolean fenced() {
        return claims != null && clusterId != null;
    }

    /**
     * The captures of every pipeline listed, sorted, asked of the port as one question so that the sources
     * the pipelines share are read once rather than once for every pipeline naming them.
     *
     * <p>Not worked out at all when nothing fences them. A capture is reported by its claim, so with no
     * claim to look up every one of them would be read off the store only to be dropped.
     */
    private Map<String, Set<String>> captureIds(Set<String> pipelineIds) {
        if (!fenced()) {
            return Map.of();
        }
        Map<String, Set<String>> sorted = new HashMap<>();
        captures.captureIdsByPipeline(pipelineIds)
                .forEach((pipelineId, ids) -> sorted.put(pipelineId, new TreeSet<>(ids)));
        return sorted;
    }

    /**
     * Every claim this answer reports -- each pipeline's controller and each of its captures -- asked of the
     * store as one question. The face answers for the whole cluster, from a store that is also carrying
     * every renewal in it: a round trip per claim, one after the other, is a cost that grows with the
     * cluster and that nothing would report but the clock.
     */
    private Map<WorkloadClaimKey, WorkloadClaimReading> readings(
            Set<String> pipelineIds, Map<String, Set<String>> captureIds) {
        if (!fenced()) {
            return Map.of();
        }
        Set<WorkloadClaimKey> keys = new LinkedHashSet<>();
        for (String pipelineId : pipelineIds) {
            keys.add(new WorkloadClaimKey(clusterId, WorkloadClaimType.PIPELINE_ACTUATION, pipelineId));
        }
        for (Set<String> ids : captureIds.values()) {
            for (String captureId : ids) {
                keys.add(new WorkloadClaimKey(clusterId, WorkloadClaimType.CAPTURE, captureId));
            }
        }
        return claims.readAll(keys);
    }

    private List<ClusterClaimView> captureClaims(
            Map<WorkloadClaimKey, WorkloadClaimReading> readings, Set<String> captureIds) {
        List<ClusterClaimView> views = new ArrayList<>();
        for (String captureId : captureIds) {
            ClusterClaimView claim = claimOver(readings, WorkloadClaimType.CAPTURE, captureId);
            if (claim != null) {
                views.add(claim);
            }
        }
        return views;
    }

    /** The claim over one resource, or null when nothing fences it and when nobody has taken it. */
    private ClusterClaimView claimOver(
            Map<WorkloadClaimKey, WorkloadClaimReading> readings, WorkloadClaimType type, String resourceId) {
        if (!fenced()) {
            return null;
        }
        WorkloadClaimReading reading = readings.get(new WorkloadClaimKey(clusterId, type, resourceId));
        if (reading == null) {
            return null;
        }
        WorkloadClaim claim = reading.claim();
        return new ClusterClaimView(
                claim.key().resourceId(),
                claim.owner().nodeId(),
                claim.owner().bootId(),
                claim.claimGeneration(),
                claim.executionGeneration(),
                claim.topologyRevision(),
                reading.leased(), claim.leaseUntil(), reading.leaseRemaining().toMillis(),
                claim.profileGeneration() == 0 ? null : claim.profileGeneration(),
                claim.contextExecutionGeneration() == 0 ? null : claim.contextExecutionGeneration(),
                claim.executionClaimGeneration() == 0 ? null : claim.executionClaimGeneration(),
                claim.executionIncarnation(), claim.executionRevision(), claim.executionTopologyRevision(),
                claim.executionProfile() == null ? null : claim.executionProfile().generation(),
                claim.executionProfile() == null ? null : claim.executionProfile().profile().hash(),
                claim.executionMembers().values().stream().sorted(java.util.Comparator.comparing(io.tapstate.spi.store.ClusterExecutionMember::nodeId))
                        .map(value -> new ClusterClaimView.Member(value.nodeId(), value.bootId(), value.memberUuid())).toList(),
                claim.failureClaimGeneration() == 0 ? null : claim.failureClaimGeneration(),
                claim.failureClaimGeneration() == 0 ? null : claim.failureAfterMemberLoss(),
                claim.contextExecutionGeneration() == 0 ? null : claim.contextExecutionGeneration() == claim.executionGeneration());
    }

    private record ProcessorSlot(String vertex, int index) { }

    /** A changed metric identity can be joined only through the current execution's unique initialized slot. */
    private static Map<ProcessorSlot, ProcessorRuntimeContext> nativeSlots(LivePipelineRun run,
            List<ClusterMemberView> members, ClusterClaimView controller) {
        LivePipelineRun.NativeExecution scope = run.nativeExecution();
        if (scope == null || !scope.measuredExecutionId().equals(run.executionId()) || controller == null
                || !Boolean.TRUE.equals(controller.executionContextCurrent())
                || !Objects.equals(controller.contextExecutionGeneration(), scope.executionGeneration())
                || !Objects.equals(controller.executionClaimGeneration(), scope.claimGeneration())
                || !Objects.equals(controller.executionProfileGeneration(), scope.profileGeneration())
                || !Objects.equals(controller.profileGeneration(), scope.profileGeneration())
                || controller.executionProfileHash() == null || controller.executionMembers().isEmpty()) {
            return Map.of();
        }
        Map<String, ClusterClaimView.Member> cohort = new HashMap<>();
        Set<String> cohortUuids = new HashSet<>();
        for (ClusterClaimView.Member member : controller.executionMembers()) {
            if (cohort.putIfAbsent(member.nodeId(), member) != null || !cohortUuids.add(member.memberUuid())) {
                return Map.of();
            }
        }
        Set<ProcessorSlot> measured = new HashSet<>();
        for (LivePipelineVertex vertex : run.vertices()) {
            for (LivePipelineProcessor processor : vertex.processors()) {
                if (!measured.add(new ProcessorSlot(vertex.name(), processor.index()))) { return Map.of(); }
            }
        }
        Map<ProcessorSlot, ProcessorRuntimeContext> slots = new HashMap<>();
        for (ProcessorRuntimeContext context : run.processorContexts()) {
            ClusterClaimView.Member original = cohort.get(context.nodeId());
            if (!run.pipelineId().equals(context.pipelineId()) || !scope.matches(context)
                    || context.memberCount() != cohort.size() || original == null
                    || !original.bootId().equals(context.bootId()) || !original.memberUuid().equals(context.memberUuid())) {
                return Map.of();
            }
            List<ClusterMemberView> current = members.stream().filter(member ->
                    Objects.equals(member.nodeId(), context.nodeId())).toList();
            if (current.size() != 1 || !currentMember(current.get(0), context, controller.executionProfileHash())) {
                return Map.of();
            }
            if (slots.putIfAbsent(new ProcessorSlot(context.vertex(), context.globalProcessorIndex()), context) != null) {
                return Map.of();
            }
        }
        return Map.copyOf(slots);
    }

    private static boolean currentMember(ClusterMemberView member, ProcessorRuntimeContext context, String profileHash) {
        return member.state() == ClusterMemberState.ACTIVE && Boolean.TRUE.equals(member.live())
                && Boolean.TRUE.equals(member.sessionLeased()) && Boolean.TRUE.equals(member.joined())
                && Objects.equals(member.profileGeneration(), context.profileGeneration())
                && Objects.equals(member.profileHash(), profileHash)
                && Objects.equals(member.bootId(), context.bootId())
                && Objects.equals(member.sessionBootId(), context.bootId())
                && Objects.equals(member.memberUuid(), context.memberUuid())
                && Objects.equals(member.joinedMemberUuid(), context.memberUuid())
                && Objects.equals(member.hzAddress(), context.memberAddress())
                && Objects.equals(member.joinedMemberAddress(), context.memberAddress());
    }

    private static ProcessorRuntimeContext contextFor(LivePipelineRun run, String vertex,
            LivePipelineProcessor processor, Set<String> currentMemberUuids,
            Map<ProcessorSlot, ProcessorRuntimeContext> nativeSlots) {
        if (run.nativeExecution() == null) {
            List<ProcessorRuntimeContext> direct = run.processorContexts().stream().filter(context ->
                    context.vertex().equals(vertex) && context.globalProcessorIndex() == processor.index()
                            && context.memberUuid().equals(processor.memberUuid())).toList();
            return direct.size() == 1 ? direct.get(0) : null;
        }
        ProcessorRuntimeContext context = nativeSlots.get(new ProcessorSlot(vertex, processor.index()));
        if (context == null || processor.memberUuid() == null || processor.memberUuid().isBlank()
                || (!context.memberUuid().equals(processor.memberUuid())
                        && currentMemberUuids.contains(processor.memberUuid()))) {
            return null;
        }
        return context;
    }

    private static List<ClusterVertexView> vertices(
            LivePipelineRun run, Map<String, String> nodeIdByMemberUuid, Set<String> currentMemberUuids,
            Map<ProcessorSlot, ProcessorRuntimeContext> nativeSlots, Map<String, ExecutionPlan.Node> planned) {
        List<ClusterVertexView> views = new ArrayList<>();
        for (LivePipelineVertex vertex : run.vertices()) {
            List<LivePipelineProcessor> working = vertex.processors().stream()
                    .filter(LivePipelineProcessor::working)
                    .sorted((left, right) -> Integer.compare(left.index(), right.index()))
                    .toList();
            List<ClusterProcessorView> processors = new ArrayList<>();
            for (LivePipelineProcessor processor : working) {
                ProcessorRuntimeContext context = contextFor(run, vertex.name(), processor,
                        currentMemberUuids, nativeSlots);
                processors.add(new ClusterProcessorView(
                        processor.index(),
                        context == null ? null : context.localProcessorIndex(),
                        processor.memberUuid(),
                        context == null ? nodeIdByMemberUuid.get(processor.memberUuid()) : context.nodeId(),
                        processor.backlog(),
                        processor.frontierGaps(),
                        processor.frontierStalledMillis(),
                        processor.queuedByStream(),
                        processor.inFlightByTable(), context));
            }
            // What the plan says of the vertex's node, where the vertex runs at its node's width: a vertex the plan
            // does not name - one gathering several producers into one, or any vertex of a run with no plan
            // recorded - asked for nothing, and says so by leaving both absent.
            ExecutionPlan.Node node = planned.get(vertex.name());
            views.add(new ClusterVertexView(
                    vertex.name(),
                    node == null ? null : node.requested(),
                    processors.size(),
                    node == null ? null : node.computedLocal(),
                    run.executionId(),
                    processors));
        }
        return views;
    }
}
