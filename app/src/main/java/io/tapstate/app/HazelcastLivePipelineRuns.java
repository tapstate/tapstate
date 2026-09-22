package io.tapstate.app;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricTags;
import io.tapstate.control.core.ClusterError;
import io.tapstate.control.core.LivePipelineProcessor;
import io.tapstate.control.core.LivePipelineRun;
import io.tapstate.control.core.LivePipelineRuns;
import io.tapstate.control.core.LivePipelineVertex;
import io.tapstate.core.common.TapstateException;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Where each pipeline's work is running, read off the engine's own readings of its running jobs.
 *
 * <p>The readings are the only place the engine publishes this. Each one is tagged with the vertex, the
 * processor's index across the whole execution, the member it ran on and the execution it belongs to --
 * which is the whole of what the topology needs, and is reported identically to every member, because a
 * job belongs to the cluster rather than to whoever submitted it.
 *
 * <p>Two things about them are not obvious and both would produce a confident wrong answer.
 *
 * <p>A vertex the plan pins to one member still has an instance on every other member, and that instance
 * is a placeholder that does nothing. Counting those would report a pinned vertex as running everywhere,
 * which is the opposite of what pinning it means. They are told apart by the type the engine reports for
 * them, and the two-member case over this asserts that a pinned vertex has exactly one worker -- so if
 * the engine ever stops naming them this way, that case says so rather than this quietly over-reporting.
 *
 * <p>The readings are collected periodically and per member, so for the first seconds of a run they have
 * arrived from some members and not yet others -- measured at about five seconds on a two-member
 * cluster. Naming only the processors that had reported would be wrong rather than merely stale, so
 * which members the picture was assembled from is reported with it.
 */
final class HazelcastLivePipelineRuns implements LivePipelineRuns {

    /**
     * The types the engine gives the instances that stand in for a pinned vertex on the members that are
     * not running it. Neither does any work, and neither is ours.
     */
    private static final Set<String> PLACEHOLDER_TYPES = Set.of("ExpectNothingP", "NoopP");

    private final HazelcastInstance member;

    HazelcastLivePipelineRuns(HazelcastInstance member) {
        this.member = Objects.requireNonNull(member, "member");
    }

    @Override
    public List<LivePipelineRun> runs() {
        List<LivePipelineRun> live = new ArrayList<>();
        try {
            for (Job job : member.getJet().getJobs()) {
                // A terminal job is kept under its name; reporting one would say a stopped pipeline is
                // running on the members that last ran it.
                if (job.getName() == null || job.getStatus().isTerminal()) {
                    continue;
                }
                live.add(runOf(job));
            }
        } catch (HazelcastInstanceNotActiveException notActive) {
            // The same window the member half refuses in, and the same code: one cause does not become
            // two answers because the read has two halves. An empty list would be worse than either --
            // it reads as this member having looked and found the cluster running nothing.
            throw new TapstateException(ClusterError.MEMBERSHIP_UNREADABLE, Map.of(), notActive);
        }
        return List.copyOf(live);
    }

    private static LivePipelineRun runOf(Job job) {
        // Ordered by first appearance, which is the order the engine names the vertices in.
        Map<String, Map<Integer, LivePipelineProcessor>> byVertex = new LinkedHashMap<>();
        Set<String> measuredFrom = new HashSet<>();
        String executionId = null;
        Instant measuredAt = null;
        JobMetrics collected = job.getMetrics();
        for (String metric : collected.metrics()) {
            for (Measurement measurement : collected.get(metric)) {
                String vertex = measurement.tag(MetricTags.VERTEX);
                String index = measurement.tag(MetricTags.PROCESSOR);
                String memberUuid = measurement.tag(MetricTags.MEMBER);
                if (memberUuid != null) {
                    measuredFrom.add(memberUuid);
                }
                if (executionId == null) {
                    executionId = measurement.tag(MetricTags.EXECUTION);
                }
                Instant taken = Instant.ofEpochMilli(measurement.timestamp());
                if (measuredAt == null || taken.isAfter(measuredAt)) {
                    measuredAt = taken;
                }
                if (vertex == null || index == null) {
                    // A reading about the vertex as a whole rather than about one of its processors.
                    continue;
                }
                byVertex.computeIfAbsent(vertex, ignored -> new LinkedHashMap<>())
                        .putIfAbsent(
                                Integer.parseInt(index),
                                new LivePipelineProcessor(
                                        Integer.parseInt(index),
                                        memberUuid,
                                        !PLACEHOLDER_TYPES.contains(
                                                measurement.tag(MetricTags.PROCESSOR_TYPE))));
            }
        }
        List<LivePipelineVertex> vertices = new ArrayList<>();
        byVertex.forEach((name, processors) ->
                vertices.add(new LivePipelineVertex(name, List.copyOf(processors.values()))));
        return new LivePipelineRun(job.getName(), executionId, measuredAt, measuredFrom, vertices);
    }
}
