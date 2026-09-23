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

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    /**
     * How long this member waits for the engine to list its jobs before saying it cannot answer.
     *
     * <p>The listing is a call to every member and to the one coordinating the cluster, and it waits for all
     * of them. When a member dies the coordinating role moves, and a listing caught in that move can be
     * dropped outright -- measured on a two-member cluster, the survivor's engine logged the call as a
     * duplicate it would not run and nothing answered it again. Unbounded, the read then waits out the
     * engine's own call timeout, a minute, and so does whoever asked: at the moment a member has just been
     * lost, which is the moment someone most wants to know who is left. A listing that is answered at all
     * takes milliseconds, so this is a bound on how long a stuck one is waited for, not on a slow one.
     */
    private static final Duration LISTING_BOUND = Duration.ofSeconds(5);

    private final HazelcastInstance member;
    private final Duration listingBound;

    /**
     * Where listings run, so that giving up on one is real: the engine waits on its calls interruptibly, and
     * a listing cancelled on its own thread stops rather than staying parked for the rest of its minute.
     *
     * <p>Kept for the life of this reader rather than made per read, because a per-read pool is a resource
     * each read would have to close, and closing one waits for what it is running -- which is the one wait
     * this exists to bound. Its threads are daemons and let go of when idle, so nothing here outlives the
     * process or holds a thread no read is using.
     */
    private final ExecutorService listings = Executors.newCachedThreadPool(runnable -> {
        Thread thread = new Thread(runnable, "cluster-topology-runs");
        thread.setDaemon(true);
        return thread;
    });

    HazelcastLivePipelineRuns(HazelcastInstance member) {
        this(member, LISTING_BOUND);
    }

    HazelcastLivePipelineRuns(HazelcastInstance member, Duration listingBound) {
        this.member = Objects.requireNonNull(member, "member");
        this.listingBound = Objects.requireNonNull(listingBound, "listingBound");
    }

    /** The runs, or the same refusal the member half gives when the engine could not list them in time. */
    @Override
    public List<LivePipelineRun> runs() {
        Future<List<LivePipelineRun>> answer = listings.submit(this::listed);
        try {
            return answer.get(listingBound.toMillis(), TimeUnit.MILLISECONDS);
        } catch (TimeoutException unanswered) {
            answer.cancel(true);
            // The same code as the engine not being active, for the same reason: the cluster is changing
            // under this read, and "ask again in a moment" is the whole of the answer.
            throw new TapstateException(ClusterError.MEMBERSHIP_UNREADABLE, Map.of(), unanswered);
        } catch (InterruptedException interrupted) {
            answer.cancel(true);
            Thread.currentThread().interrupt();
            // The caller was interrupted, not the cluster: nothing was learned about it to report.
            throw new IllegalStateException("interrupted while listing the engine's jobs", interrupted);
        } catch (ExecutionException failed) {
            if (failed.getCause() instanceof RuntimeException runtime) {
                throw runtime;
            }
            if (failed.getCause() instanceof Error error) {
                throw error;
            }
            throw new IllegalStateException("listing the engine's jobs failed", failed.getCause());
        }
    }

    private List<LivePipelineRun> listed() {
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
