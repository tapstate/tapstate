package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.core.metrics.JobMetrics;
import com.hazelcast.jet.core.metrics.Measurement;
import com.hazelcast.jet.core.metrics.MetricNames;
import com.hazelcast.jet.core.metrics.MetricTags;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.runtime.engine.join.JoinRecomputeMetricNames;
import io.tapstate.runtime.engine.nest.NestDeadLetterMetricNames;
import io.tapstate.runtime.engine.nest.NestMemoryBudget;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStateMetricNames;
import io.tapstate.spi.store.KeyedStateStore;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * The data-plane execution engine: the lifecycle actuator that maps a pipeline's lifecycle to Jet
 * job operations. A pipeline runs as exactly one Jet job named by the pipeline id, so the actuator
 * can find that job again to suspend / resume / cancel it. The topology (the {@link DAG}) is built
 * elsewhere and handed in; this engine only submits and controls the job.
 *
 * <p>Jet is a subordinate execution layer, not the source of truth: jobs carry no durable offset
 * snapshot ({@link ProcessingGuarantee#NONE}) and may be discarded and re-submitted at any time.
 * Continuation truth lives in the store, so a resumed job re-reads its start position from there
 * rather than from a Jet snapshot. Rule R4: this depends on the kernel and Hazelcast only.
 */
public final class Engine {

    private final HazelcastInstance member;

    /**
     * The layer behind the nest state maps, asked how much a namespace holds altogether. Absent on a run
     * that keeps its state in memory alone, where what is in memory is all there is.
     */
    private final KeyedStateStore nestState;

    public Engine(HazelcastInstance member) {
        this(member, null);
    }

    /** An engine that can also say how much of a nest's state is on the layer behind its memory. */
    public Engine(HazelcastInstance member, KeyedStateStore nestState) {
        this.member = Objects.requireNonNull(member, "member");
        this.nestState = nestState;
    }

    /**
     * Submits the pipeline's topology as a Jet job named by the pipeline id. Idempotent: if a job
     * under that name is already active it is left running and returned, so a repeated convergence
     * pass does not start a second job for the same pipeline.
     *
     * <p>Clears any failure {@link JobFailureRegistry} recorded for this pipeline id first: a fresh
     * submission means a fresh run (the caller only submits after driving the pipeline back toward
     * RUNNING), so a cause an earlier run recorded must not be mistaken for this one's while it is
     * still healthy. A submission that finds the job already active clears a registry entry that was
     * never set, which is a no-op.
     */
    public void submit(String pipelineId, DAG dag) {
        submit(pipelineId, dag, Set.of(), NestSettings.defaults());
    }

    /**
     * As above, first holding {@code stateNamespaces} to what {@code settings} asks them to keep in memory.
     *
     * <p>Before the job and not after: a state map is created the first moment a vertex asks for it, and
     * what a map holds is fixed as it is created. Applied afterwards the numbers would be accepted, change
     * nothing, and report nothing - the run would sit on the process-wide figure while its own artifact
     * named another.
     *
     * <p>A submission naming no namespaces configures none, which is what a pipeline with no nest in it
     * does.
     */
    public void submit(String pipelineId, DAG dag, Set<String> stateNamespaces, NestSettings settings) {
        NestMemoryBudget.applyTo(member, stateNamespaces, settings);
        JobFailureRegistry.of(member).clear(pipelineId);
        JobConfig config = new JobConfig()
                .setName(pipelineId)
                .setProcessingGuarantee(ProcessingGuarantee.NONE);
        member.getJet().newJobIfAbsent(dag, config);
    }

    /** Pauses the pipeline's running job. The job is kept so it can be resumed. */
    public void suspend(String pipelineId) {
        requireJob(pipelineId).suspend();
    }

    /**
     * Resumes the pipeline's suspended job. Under the no-snapshot guarantee this re-runs the
     * topology, so the source re-reads its start position from the store rather than a Jet snapshot.
     */
    public void resume(String pipelineId) {
        requireJob(pipelineId).resume();
    }

    /**
     * Cancels the pipeline's job. Idempotent: a pipeline with no running job is already stopped, so
     * the job side is a no-op. The pipeline-private continuation the store holds (its consumer cursor,
     * private operator state and sink watermark) is cleared separately when the source store is
     * present; this actuator owns the job side.
     *
     * <p>Always releases any failure the {@link JobFailureRegistry} recorded for this pipeline, live job
     * or not: a stop is the terminal verb for a failed pipeline, and its job is already terminal by then —
     * so the release must not hide behind the live-job guard, or the recorded exception (and its whole
     * cause graph) would stay referenced for the rest of the member's life. By the time a stop is driven,
     * the converge side has already read the failure and moved the pipeline to the observable FAILED
     * state, so nothing is lost by forgetting it here.
     */
    public void cancel(String pipelineId) {
        Job job = liveJob(pipelineId);
        if (job != null) {
            job.cancel();
        }
        JobFailureRegistry.of(member).clear(pipelineId);
    }

    /**
     * Waits for the pipeline's job to be over, up to {@code budget}: true once it is, false if the budget
     * ran out with the job still running. A pipeline with no job has nothing to wait for and is over by
     * definition.
     *
     * <p>A cancel only asks. The job goes on running while it winds down, and a processor writing state
     * as it closes writes it after the cancel has returned - so anything that lets go of what the job was
     * writing to has to wait here first, or it lets go of it while the job is still filling it back in.
     * How the job ended does not matter to the caller: cancelled, failed and completed are equally over.
     *
     * <p>The budget is what keeps a stuck job from holding up the caller indefinitely, and a false answer
     * is a real answer rather than a failure - the caller is expected to have a way of finishing later
     * what it could not safely do now.
     */
    /**
     * Whether a job for this pipeline exists here and has not ended. A member that has never run the
     * pipeline answers false, which is the whole reason this is asked: a process that has just come up
     * cannot tell "running elsewhere in my own past" from "not running" through any failure it holds,
     * because it holds none.
     *
     * <p>A suspended job counts as carrying the pipeline. It is being held on purpose and a resume
     * continues it; treating it as absent would submit a second job over a paused one.
     */
    public boolean hasLiveJob(String pipelineId) {
        Job job = member.getJet().getJob(pipelineId);
        if (job == null) {
            return false;
        }
        JobStatus status = job.getStatus();
        return status != JobStatus.FAILED && status != JobStatus.COMPLETED;
    }

    public boolean awaitTerminal(String pipelineId, Duration budget) {
        Job job = member.getJet().getJob(pipelineId);
        if (job == null) {
            return true;
        }
        try {
            job.getFuture().toCompletableFuture().get(budget.toMillis(), TimeUnit.MILLISECONDS);
            return true;
        } catch (CancellationException | ExecutionException over) {
            return true;
        } catch (TimeoutException stillRunning) {
            return false;
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * The failure of the pipeline's job if it died on its own, or empty while it runs, has no job, or
     * was ended by a cancel. A Jet job reaches FAILED both when it throws and when it is cancelled, so a
     * bare status check cannot tell a real failure from a stop; the terminal future tells them apart —
     * a cancelled job completes with a {@link CancellationException}, a failed one with its own cause.
     * The job is terminal here, so joining its future returns or throws at once without blocking.
     *
     * <p>Checks the {@link JobFailureRegistry} first: once a job's terminal result is durable, Jet tears
     * down the live context backing {@code job.getFuture()} and later reconstructs a cause-free mock
     * throwable from the failure's stored text (measured: production loses the real cause this way the
     * large majority of the time, a race of a few milliseconds wide). A processor that caught the real
     * exception records it there before that teardown can happen, so a hit is always the real cause. Only
     * a job that failed for a reason no processor recorded — a fault inside Jet itself, for one — falls
     * through to asking Jet, which after that same teardown can answer with the degraded mock instead.
     */
    public Optional<Throwable> failureOf(String pipelineId) {
        Job job = member.getJet().getJob(pipelineId);
        if (job == null || job.getStatus() != JobStatus.FAILED) {
            return Optional.empty();
        }
        Optional<Throwable> recorded = JobFailureRegistry.of(member).get(pipelineId);
        if (recorded.isPresent()) {
            return recorded;
        }
        try {
            job.getFuture().join();
            return Optional.empty();
        } catch (CancellationException cancelled) {
            return Optional.empty();
        } catch (CompletionException failed) {
            Throwable cause = failed.getCause() != null ? failed.getCause() : failed;
            return cause instanceof CancellationException ? Optional.empty() : Optional.of(cause);
        }
    }

    /**
     * The number of records the pipeline's live job has driven to its output sinks, or empty when it
     * has no live job. The count is the received count summed over the output-sink vertices - serve
     * sinks and view materializations alike - so a
     * filter earlier in the chain is reflected in it. It reads the job's last collected metrics, so a
     * freshly submitted job reports a low or zero count until the first collection; a stopped pipeline
     * reports empty, matching the live-state projection the rest of the read face carries.
     */
    public OptionalLong recordCount(String pipelineId) {
        Job job = liveJob(pipelineId);
        if (job == null) {
            return OptionalLong.empty();
        }
        long reached = job.getMetrics().get(MetricNames.RECEIVED_COUNT).stream()
                .filter(Engine::isOutputSink)
                .mapToLong(Measurement::value)
                .sum();
        return OptionalLong.of(reached);
    }

    /**
     * How far each chain of the pipeline's live job trails the bound combined for it, keyed by chain;
     * empty when it has no live job and for a job whose sinks have no such distance to report.
     *
     * <p>A chain reported by more than one sink is kept at its widest reading. Two sinks fed the same
     * chain are two frontiers over it, and the pipeline has only got as far as whichever is furthest
     * behind; combining them any other way reports a pipeline healthier than any sink in it really is.
     *
     * <p>Like the record count this reads the job's last collected statistics, so a freshly submitted job
     * reports nothing until the first collection. Absence here is "not measured", never "measured at
     * zero" - the two call for opposite responses, and a zero standing in for an unwired reading is the
     * frontier-lag alarm's blind spot rather than its quiet state.
     */
    public Map<String, Long> frontierGaps(String pipelineId) {
        return byChain(pipelineId, JetFrontierGauge::chainOf);
    }

    /**
     * How long each chain of the pipeline has had its durable position pinned where it is, in
     * milliseconds, keyed by chain; empty when it has no live job and for a job whose chains are not
     * pinned.
     *
     * <p>This is what says a frontier has stopped at all. The distance beside it reads zero for a chain
     * held back by pending changes upstream and zero again for one keeping up, so on its own it cannot
     * raise anything; read together they say a chain is pinned, for how long, and which of the two pins
     * it is. The duration is also the one in the unit the consequence is measured in — a pinned position
     * is racing the source's retention window, and that window is kept in time, not in positions.
     *
     * <p>A chain reported by more than one sink is kept at its longest reading, for the same reason a
     * distance is kept at its widest: the pipeline is only as unpinned as its most stuck sink.
     */
    public Map<String, Long> frontierStalls(String pipelineId) {
        return byChain(pipelineId, JetFrontierGauge::stalledChainOf);
    }

    /**
     * How many changes each namespace of the pipeline could never place in a document, keyed by namespace;
     * empty when it has no live job and for a job whose nests have discarded nothing.
     *
     * <p>The one number about a nest that nothing else can stand in for. A pipeline discarding every row it
     * reads and one discarding none produce the same documents, run the same queues and report the same
     * throughput - the rows counted here were never going to appear in any document, so no assertion about
     * the output distinguishes the two. Absent rather than zero for a namespace that discarded nothing, for
     * the reason the state readings are: "not measured" and "measured at nothing" call for opposite
     * responses, and this is one where the second is the quiet state worth being able to trust.
     *
     * <p>Kept at its highest per namespace rather than summed, because every processor of a vertex on a
     * member reports that member's running total rather than its own share.
     */
    public Map<String, Long> nestDeadLetters(String pipelineId) {
        return byChain(pipelineId, NestDeadLetterMetricNames::namespaceOf);
    }

    /**
     * How many rows each large rebuild of the pipeline has sent so far, keyed by the namespace and
     * dimension key it is about; empty when it has no live job and while no rebuild large enough to
     * report is under way.
     *
     * <p>Read beside {@link #joinRecomputeExpected}, which says how many rows that rebuild has
     * altogether. The distance between the two is the whole of what this says: while it is open the
     * target holds half the old value of one dimension row and half the new one, and everything else
     * about the pipeline reads healthy - the job runs, the queues drain, the error count is zero.
     *
     * <p>Absent rather than zero, and the absence covers two different things on purpose: no rebuild is
     * running, or the one running is small enough that nobody needs telling. Both are the quiet state.
     * A rebuild that has finished keeps its last reading, which is the number it ended on.
     *
     * <p>Kept at its highest per subject rather than summed, because a rebuild belongs to whichever
     * processor owns the key's partition and every collection of it reports that same running total.
     */
    public Map<String, Long> joinRecomputeDone(String pipelineId) {
        return byChain(pipelineId, JoinRecomputeMetricNames::doneSubjectOf);
    }

    /**
     * About how many rows each large rebuild of the pipeline has altogether, keyed the same way and
     * empty in the same cases as {@link #joinRecomputeDone}.
     *
     * <p>An estimate read off the index rather than a count: counting it exactly would mean walking
     * every page of the bucket before walking them again to rebuild it, which is the cost the reading
     * exists to warn about.
     */
    public Map<String, Long> joinRecomputeExpected(String pipelineId) {
        return byChain(pipelineId, JoinRecomputeMetricNames::expectedSubjectOf);
    }

    /**
     * The pipeline's per-chain readings whose metric names {@code chainOf} recognises, each kept at its
     * highest across every sink that reported it. A metric name it does not recognise is skipped, so the
     * readings that share this shape stay separate despite riding the same collection.
     */
    private Map<String, Long> byChain(String pipelineId, Function<String, String> chainOf) {
        Job job = liveJob(pipelineId);
        if (job == null) {
            return Map.of();
        }
        JobMetrics collected = job.getMetrics();
        Map<String, Long> highest = new HashMap<>();
        for (String metric : collected.metrics()) {
            String chain = chainOf.apply(metric);
            if (chain == null) {
                continue;
            }
            for (Measurement measurement : collected.get(metric)) {
                highest.merge(chain, measurement.value(), Math::max);
            }
        }
        return highest;
    }

    /**
     * What each namespace of the pipeline's nest state looks like right now, keyed by namespace; empty
     * when it has no live job and for a job whose nests report nothing.
     *
     * <p>Every processor instance of one vertex reads the same shared counters and the same map, so they
     * all report the same numbers and a namespace is kept at its highest reading rather than summed - a sum
     * would multiply every count by however many instances the vertex happened to run as.
     *
     * <p>Like the record count this reads the job's last collected statistics, so a freshly submitted job
     * reports nothing until the first collection. A namespace that reports nothing is absent rather than
     * present at zero: "not measured" and "measured empty" call for opposite responses, and a state layer
     * that stopped reporting would otherwise read as one that had emptied.
     */
    public Map<String, NestStateReading> nestStateReadings(String pipelineId) {
        Job job = liveJob(pipelineId);
        if (job == null) {
            return Map.of();
        }
        JobMetrics collected = job.getMetrics();
        Map<String, Map<String, Long>> byNamespace = new HashMap<>();
        for (String metric : collected.metrics()) {
            NestStateMetricNames.Reading reading = NestStateMetricNames.readingOf(metric);
            if (reading == null) {
                continue;
            }
            for (Measurement measurement : collected.get(metric)) {
                byNamespace.computeIfAbsent(reading.namespace(), ignored -> new HashMap<>())
                        .merge(reading.kind(), measurement.value(), Math::max);
            }
        }
        Map<String, NestStateReading> readings = new HashMap<>();
        byNamespace.forEach((namespace, kinds) -> readings.put(namespace,
                NestStateMetricNames.readingFrom(kinds, stored(namespace))));
        return readings;
    }

    /**
     * How much the layer behind the memory holds for {@code namespace}, asked here rather than published
     * from the run: it is a question about a namespace as a whole, so its cost is what the namespace holds
     * rather than what an event touched, and asking it as state is written would put that cost on every
     * write. Here it is paid once per namespace by whoever is reporting.
     */
    private OptionalLong stored(String namespace) {
        return nestState == null ? OptionalLong.empty() : OptionalLong.of(nestState.count(namespace));
    }

    /**
     * Whether a measurement belongs to an output-sink vertex, which the builder names by one of two
     * prefixes: a serve sink delivering outward, or a view materializing into the state store. Both
     * are places records reach, and a pipeline may carry either alone - counting only the serve
     * prefix would report zero for a view-only pipeline whose data is flowing correctly.
     */
    private static boolean isOutputSink(Measurement measurement) {
        String vertex = measurement.tag(MetricTags.VERTEX);
        return vertex != null
                && (vertex.startsWith(PipelineDagBuilder.SERVE_VERTEX_PREFIX)
                        || vertex.startsWith(PipelineDagBuilder.VIEW_VERTEX_PREFIX));
    }

    /**
     * The pipeline's live job, or {@code null} when it has none. A completed or cancelled job is
     * kept under its name but is not live: Jet retains terminal jobs, so a bare presence check would
     * mistake a stopped pipeline for a running one.
     */
    private Job liveJob(String pipelineId) {
        Job job = member.getJet().getJob(pipelineId);
        return job == null || job.getStatus().isTerminal() ? null : job;
    }

    /** The pipeline's live job, or a coded {@code engine.no-such-job} when it has none to act on. */
    private Job requireJob(String pipelineId) {
        Job job = liveJob(pipelineId);
        if (job == null) {
            throw new TapstateException(EngineError.NO_SUCH_JOB, Map.of("pipeline", pipelineId), null);
        }
        return job;
    }
}
