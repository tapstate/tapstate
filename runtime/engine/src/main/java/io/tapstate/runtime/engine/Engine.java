package io.tapstate.runtime.engine;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
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
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.core.lifecycle.Stage;
import io.tapstate.core.lifecycle.StageReading;
import io.tapstate.core.lifecycle.NestStateReading;
import io.tapstate.runtime.engine.join.JoinRecomputeMetricNames;
import io.tapstate.runtime.engine.nest.NestDeadLetterMetricNames;
import io.tapstate.runtime.engine.nest.NestMemoryBudget;
import io.tapstate.runtime.engine.nest.NestSettings;
import io.tapstate.runtime.engine.nest.NestStatePlacement;
import io.tapstate.runtime.engine.nest.NestStateMetricNames;
import io.tapstate.spi.store.KeyedStateStore;
import io.tapstate.spi.store.OperatorStateStores;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
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
    private final OperatorStateStores operatorStateStores;

    public Engine(HazelcastInstance member) {
        this(member, (OperatorStateStores) null);
    }

    /** An engine that can also say how much of a nest's state is on the layer behind its memory. */
    public Engine(HazelcastInstance member, KeyedStateStore nestState) {
        this(member, nestState == null ? null : OperatorStateStores.stateOnly("default", nestState));
    }

    /** An engine resolving each nest namespace through the deployment's operator-state stores. */
    public Engine(HazelcastInstance member, OperatorStateStores operatorStateStores) {
        this.member = Objects.requireNonNull(member, "member");
        this.operatorStateStores = operatorStateStores;
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
        refuseIfLost(pipelineId);
        NestMemoryBudget.applyTo(member, stateNamespaces, settings);
        JobFailureRegistry.of(member).clear(pipelineId);
        JobConfig config = new JobConfig()
                .setName(pipelineId)
                .setProcessingGuarantee(ProcessingGuarantee.NONE);
        member.getJet().newJobIfAbsent(dag, config);
    }

    /** Submits after pinning every nest namespace to the database the compiled artifact resolved. */
    public void submit(String pipelineId, DAG dag, Map<String, String> stateDatabases,
            NestSettings settings) {
        refuseIfLost(pipelineId);
        configureNestState(stateDatabases, settings);
        JobFailureRegistry.of(member).clear(pipelineId);
        JobConfig config = new JobConfig()
                .setName(pipelineId)
                .setProcessingGuarantee(ProcessingGuarantee.NONE);
        member.getJet().newJobIfAbsent(dag, config);
    }

    /** Validates and pins placement before capture or graph construction performs a side effect. */
    public void configureNestState(Map<String, String> stateDatabases, NestSettings settings) {
        NestStatePlacement.applyTo(member, stateDatabases, settings);
    }

    /**
     * Refuses, with the coded reason, once this engine's member has been shut down for want of memory; does
     * nothing while it runs. Asked before any job operation that would need the member, including by a caller
     * with work to do before it gets as far as this engine, so that nothing is started for a job that cannot
     * be.
     */
    public void refuseIfLost(String pipelineId) {
        lost(pipelineId).ifPresent(refusal -> {
            throw refusal;
        });
    }

    /**
     * Whether this engine's member has been shut down for want of memory, from the moment its out-of-memory
     * handling starts taking it down. Nothing on the member can be reached from then on, and for as long as that
     * shutdown lasts a job on it can still be ending where no lookup sees it any more.
     */
    public boolean isLost() {
        return MemberOutOfMemory.of(member).isPresent();
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
        Job job = jobNamed(pipelineId);
        if (job == null) {
            return false;
        }
        JobStatus status = job.getStatus();
        return status != JobStatus.FAILED && status != JobStatus.COMPLETED;
    }

    public boolean awaitTerminal(String pipelineId, Duration budget) {
        Job job = jobNamed(pipelineId);
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
     *
     * <p>A member shut down for want of memory is answered before any of that: every job it ran died with it,
     * and the member is the one thing that can no longer be asked, so the loss is the failure.
     */
    public Optional<Throwable> failureOf(String pipelineId) {
        Optional<Throwable> lost = lost(pipelineId).map(Throwable.class::cast);
        if (lost.isPresent()) {
            return lost;
        }
        Job job = jobNamed(pipelineId);
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
     * How many rows the large rebuilds of the pipeline have sent so far, keyed by the namespace the
     * rebuilt dimension lives in; empty when it has no live job and while no rebuild large enough to
     * report is under way. Two rebuilds under way in one namespace are added together: the key each one
     * is about is a value out of a row, and a reading that leaves the engine is not keyed by one.
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
     * <p>Kept at its highest per rebuild before namespaces are added up, because a rebuild belongs to
     * whichever processor owns the key's partition and every collection of it reports that same running
     * total.
     */
    public Map<String, Long> joinRecomputeDone(String pipelineId) {
        return byNamespace(pipelineId, JoinRecomputeMetricNames::doneSubjectOf);
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
        return byNamespace(pipelineId, JoinRecomputeMetricNames::expectedSubjectOf);
    }

    /**
     * The rebuild readings {@code subjectOf} names, at their highest per rebuild and then added per
     * namespace. Two steps because the two are different facts: one rebuild is reported by every
     * processor at the same running total, so the highest of those is the reading; two rebuilds in one
     * namespace are two pieces of work, so their readings add.
     */
    private Map<String, Long> byNamespace(String pipelineId, Function<String, String> subjectOf) {
        Map<String, Long> perNamespace = new HashMap<>();
        byChain(pipelineId, subjectOf).forEach((subject, reading) ->
                perNamespace.merge(JoinRecomputeMetricNames.namespaceOf(subject), reading, Long::sum));
        return perNamespace;
    }

    /**
     * How many rows of each table the pipeline's live job has had confirmed by its targets, broken out by
     * the source operation that produced them: table, then operation symbol, then the running total. Empty
     * when it has no live job, and a table with nothing confirmed is absent rather than present at zero.
     *
     * <p>Counts from two sinks over the same table are <strong>added</strong>, unlike the per-chain
     * readings above which are kept at their widest. The difference is what each one measures: a distance
     * is one fact two sinks each have a view of, so a pipeline is as far behind as its furthest-behind
     * sink; a delivery is work done, and a row written to two targets was written twice. That is also what
     * the record count this sits beside has always reported, summed over the output sinks.
     */
    public Map<String, Map<String, Long>> recordsDelivered(String pipelineId) {
        Job job = liveJob(pipelineId);
        return job == null ? Map.of() : deliveredRowsIn(job.getMetrics());
    }

    /**
     * The delivery counts in {@code collected}, by table and source operation. Separated from finding
     * the job because that is the half with no behaviour in it: which names are read, which are passed
     * over, and how two sinks' figures combine are decisions, and a decision reachable only through a
     * running job is a decision nothing checks.
     */
    static Map<String, Map<String, Long>> deliveredRowsIn(JobMetrics collected) {
        Map<String, Map<String, Long>> byTable = new HashMap<>();
        for (String metric : collected.metrics()) {
            JetDeliveryGauge.Delivered delivered = JetDeliveryGauge.deliveredOf(metric);
            if (delivered == null) {
                continue;
            }
            for (Measurement measurement : collected.get(metric)) {
                byTable.computeIfAbsent(delivered.table(), ignored -> new HashMap<>())
                        .merge(delivered.op(), measurement.value(), Long::sum);
            }
        }
        return byTable;
    }

    /**
     * How many bytes of payload the pipeline's live job has had confirmed by its targets, by table; empty
     * when it has no live job, and absent for a table with nothing confirmed.
     *
     * <p>Added over sinks like the counts beside it and unlike the per-chain distances, for the reason
     * that decides between them: a row written to two targets was written twice, and its payload crossed
     * twice.
     */
    public Map<String, Long> bytesDelivered(String pipelineId) {
        Job job = liveJob(pipelineId);
        return job == null ? Map.of() : settledBytesIn(job.getMetrics());
    }

    /**
     * The settled payload sizes in {@code collected}, by table. Added over the sinks like the counts and
     * unlike the distances: a row written to two targets was written twice and its payload crossed twice.
     */
    static Map<String, Long> settledBytesIn(JobMetrics collected) {
        Map<String, Long> byTable = new HashMap<>();
        for (String metric : collected.metrics()) {
            String table = JetDeliveryGauge.carriedTableOf(metric);
            if (table == null) {
                continue;
            }
            for (Measurement measurement : collected.get(metric)) {
                byTable.merge(table, measurement.value(), Long::sum);
            }
        }
        return byTable;
    }

    /**
     * How long the rows of each table have taken to reach a target, from the source's stamp to the
     * confirmed write, as one distribution per table over the registered bounds; empty when the pipeline
     * has no live job, and absent for a table with nothing confirmed.
     *
     * <p>Added over sinks, bucket by bucket, like the counts and unlike the per-chain distances: a row
     * written to two targets was delivered twice, and each delivery took as long as it took.
     */
    public Map<String, HistogramValue> deliveryDurations(String pipelineId) {
        Job job = liveJob(pipelineId);
        return job == null ? Map.of() : settledDurationsIn(job.getMetrics());
    }

    /**
     * The delivery-duration distributions in {@code collected}, by table. A table is read back only when
     * all three of its parts are present and its buckets are as many as the registered bounds need: a
     * distribution with a bucket missing is not a distribution short one bucket, it is a collection that
     * caught a sink half-way through reporting, and it is left out rather than read as a shape.
     */
    static Map<String, HistogramValue> settledDurationsIn(JobMetrics collected) {
        HistogramBounds bounds = HistogramBounds.RECORD_DELIVERY_DURATION;
        Map<String, Long> counts = new HashMap<>();
        Map<String, Long> sums = new HashMap<>();
        Map<String, long[]> buckets = new HashMap<>();
        Map<String, Integer> bucketsSeen = new HashMap<>();
        for (String metric : collected.metrics()) {
            String countTable = JetDeliveryGauge.durationCountTableOf(metric);
            if (countTable != null) {
                for (Measurement measurement : collected.get(metric)) {
                    counts.merge(countTable, measurement.value(), Long::sum);
                }
                continue;
            }
            String sumTable = JetDeliveryGauge.durationSumTableOf(metric);
            if (sumTable != null) {
                for (Measurement measurement : collected.get(metric)) {
                    sums.merge(sumTable, measurement.value(), Long::sum);
                }
                continue;
            }
            JetDeliveryGauge.DurationBucket bucket = JetDeliveryGauge.durationBucketOf(metric);
            if (bucket == null || bucket.index() < 0 || bucket.index() >= bounds.buckets()) {
                continue;
            }
            long[] perBucket = buckets.computeIfAbsent(bucket.table(), ignored -> new long[bounds.buckets()]);
            for (Measurement measurement : collected.get(metric)) {
                perBucket[bucket.index()] += measurement.value();
            }
            bucketsSeen.merge(bucket.table(), 1, Integer::sum);
        }
        Map<String, HistogramValue> byTable = new HashMap<>();
        counts.forEach((table, count) -> {
            long[] perBucket = buckets.get(table);
            if (perBucket == null || bucketsSeen.getOrDefault(table, 0) != bounds.buckets()
                    || !sums.containsKey(table)) {
                return;
            }
            List<Long> bucketCounts = new ArrayList<>(perBucket.length);
            for (long bucketCount : perBucket) {
                bucketCounts.add(bucketCount);
            }
            byTable.put(table, bounds.value(count, sums.get(table) / 1000.0, bucketCounts));
        });
        return byTable;
    }

    /**
     * Where the pipeline's live job spends its time: one distribution per stage of how long that stage's
     * units of work took, over the registered bounds, with the moment timing began; nothing when it has
     * no live job or no stage has timed anything yet.
     *
     * <p>Added over the processors of one stage, bucket by bucket: a vertex may run as several processors
     * and each times its own share of the work, so the stage's distribution is the sum of theirs. The
     * start is the latest of theirs, for the reason the delivery start is the latest of its sinks'.
     */
    public StageReading stageDurations(String pipelineId) {
        Job job = liveJob(pipelineId);
        return job == null ? StageReading.NONE : stageDurationsIn(job.getMetrics());
    }

    /**
     * The stage distributions in {@code collected}. A stage is read back only when its count, its sum and
     * every bucket are present — a distribution caught with a bucket missing is a collection that caught a
     * processor half-way through reporting, and it is left out rather than read as a shape.
     */
    static StageReading stageDurationsIn(JobMetrics collected) {
        HistogramBounds bounds = HistogramBounds.PROCESS_DURATION;
        Map<String, Long> counts = new HashMap<>();
        Map<String, Long> sums = new HashMap<>();
        Map<String, long[]> buckets = new HashMap<>();
        Map<String, Integer> bucketsSeen = new HashMap<>();
        long since = Long.MIN_VALUE;
        for (String metric : collected.metrics()) {
            JetStageGauge.Part part = JetStageGauge.partOf(metric);
            // A statistic named for something that is not a stage of this build's graph is skipped here,
            // the way a bucket index out of range is skipped below. The job's statistics are aggregated
            // across members, so a rolling upgrade puts a stage word this build has never heard of in
            // front of this loop; carrying it into the reading, whose constructor exists to refuse a
            // stage outside the closed set, would throw out of the publish that called this. The driver
            // reads a throw from there as a failure to converge, so a pipeline running perfectly well
            // would be reported, once a second, as one the server keeps failing to bring up.
            if (part == null || !Stage.attributeValues().contains(part.stage())) {
                continue;
            }
            for (Measurement measurement : collected.get(metric)) {
                switch (part.kind()) {
                    case JetStageGauge.COUNT -> counts.merge(part.stage(), measurement.value(), Long::sum);
                    case JetStageGauge.SUM_MICROS -> sums.merge(part.stage(), measurement.value(), Long::sum);
                    case JetStageGauge.SINCE -> since = Math.max(since, measurement.value());
                    case JetStageGauge.BUCKET -> {
                        if (part.bucket() < 0 || part.bucket() >= bounds.buckets()) {
                            continue;
                        }
                        buckets.computeIfAbsent(part.stage(), ignored -> new long[bounds.buckets()])[part.bucket()]
                                += measurement.value();
                    }
                    default -> {
                    }
                }
            }
            if (JetStageGauge.BUCKET.equals(part.kind()) && part.bucket() >= 0 && part.bucket() < bounds.buckets()) {
                bucketsSeen.merge(part.stage(), 1, Integer::sum);
            }
        }
        Map<String, HistogramValue> byStage = new HashMap<>();
        counts.forEach((stage, count) -> {
            long[] perBucket = buckets.get(stage);
            if (perBucket == null || bucketsSeen.getOrDefault(stage, 0) != bounds.buckets()
                    || !sums.containsKey(stage)) {
                return;
            }
            List<Long> bucketCounts = new ArrayList<>(perBucket.length);
            for (long bucketCount : perBucket) {
                bucketCounts.add(bucketCount);
            }
            byStage.put(stage, bounds.value(count, sums.get(stage) / 1_000_000.0, bucketCounts));
        });
        if (byStage.isEmpty() || since == Long.MIN_VALUE) {
            return StageReading.NONE;
        }
        return new StageReading(byStage, Instant.ofEpochMilli(since));
    }

    /**
     * The moment the pipeline's live job began counting what it has delivered, as epoch milliseconds;
     * empty when it has no live job or nothing has counted yet.
     *
     * <p>The <strong>latest</strong> start among its sinks, not the earliest, and the choice is a trade
     * rather than a plain reading. A total summed over sinks that began at different moments is not exactly
     * true of either instant: the earliest is the only window that contains every term, so on containment
     * alone it would win. What decides it the other way is what a start is read for. It is how a consumer
     * is told the series began again, and the one reading that corrupts a rate is a total that drops with
     * no such signal beside it. A sink that restarts resets its own count, so the sum drops; taking the
     * latest moves this instant forward in the same breath and the drop reads as the restart it is, while
     * taking the earliest would leave it as a counter going backwards, which is the one thing a start time
     * exists to make impossible. A sink joining a running pipeline is read as a restart too, which loses
     * the history before it rather than reporting a rate that never happened.
     */
    public OptionalLong countingSince(String pipelineId) {
        Job job = liveJob(pipelineId);
        if (job == null) {
            return OptionalLong.empty();
        }
        JobMetrics collected = job.getMetrics();
        OptionalLong latest = OptionalLong.empty();
        for (Measurement measurement : collected.get(JetDeliveryGauge.SINCE_METRIC)) {
            latest = latest.isPresent()
                    ? OptionalLong.of(Math.max(latest.getAsLong(), measurement.value()))
                    : OptionalLong.of(measurement.value());
        }
        return latest;
    }

    /**
     * The event time of the newest row of each table the pipeline's live job has had confirmed, as epoch
     * milliseconds; empty when it has no live job, and absent for a table with nothing confirmed.
     *
     * <p>A reading, not a distance. How far behind a table is has to be worked out against the clock at the
     * moment somebody asks, because it goes on growing while nothing arrives -- a distance recorded in the
     * run would stand still for exactly as long as the pipeline did.
     *
     * <p>Two sinks over one table keep the newer reading: a row confirmed by either is a row that reached a
     * target, and the question this answers is how recent the newest such row is.
     */
    public Map<String, Long> newestDeliveredEventTime(String pipelineId) {
        return byChain(pipelineId, JetDeliveryGauge::reachedTableOf);
    }

    /**
     * The pipeline's per-chain readings whose metric names {@code chainOf} recognises, each kept at its
     * highest across every sink that reported it. A metric name it does not recognise is skipped, so the
     * readings that share this shape stay separate despite riding the same collection.
     */
    private Map<String, Long> byChain(String pipelineId, Function<String, String> chainOf) {
        Job job = liveJob(pipelineId);
        return job == null ? Map.of() : highestIn(job.getMetrics(), chainOf);
    }

    /**
     * The widest reading in {@code collected} for each key {@code chainOf} names. Kept at its widest and
     * not added, unlike the two above: a distance is one fact several sinks each have a view of, so the
     * answer is the furthest-behind of them, while a delivery is work done by each.
     */
    static Map<String, Long> highestIn(JobMetrics collected, Function<String, String> chainOf) {
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
        if (operatorStateStores == null) {
            return OptionalLong.empty();
        }
        String database = NestStatePlacement.databaseOf(
                member, namespace, operatorStateStores.defaultDatabase());
        return OptionalLong.of(operatorStateStores.inDatabase(database).state().count(namespace));
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
        Job job = jobNamed(pipelineId);
        return job == null || job.getStatus().isTerminal() ? null : job;
    }

    /**
     * The job named for the pipeline, or {@code null} when there is none, which is also what a member shut
     * down for want of memory has: every job went with it. Every lookup goes through here. A member that is
     * gone refuses the lookup with an error that says why nowhere, and passing that refusal on is what kept a
     * lost engine's pipelines reading as running; so a refusal from a member its out-of-memory handling took
     * down is the answer "no job". A member refusing for any other reason, the server shutting it down on
     * purpose among them, still refuses.
     */
    private Job jobNamed(String pipelineId) {
        try {
            return member.getJet().getJob(pipelineId);
        } catch (HazelcastInstanceNotActiveException gone) {
            if (MemberOutOfMemory.of(member).isPresent()) {
                return null;
            }
            throw gone;
        }
    }

    /** Why this engine can no longer run {@code pipelineId}, as the coded failure; empty while its member runs. */
    public Optional<TapstateException> lost(String pipelineId) {
        return MemberOutOfMemory.of(member).map(error ->
                new TapstateException(EngineError.OUT_OF_MEMORY, Map.of("pipeline", pipelineId), error));
    }

    /**
     * The pipeline's live job, or a coded refusal when there is none to act on: {@code engine.out-of-memory}
     * once the member has been lost, {@code engine.no-such-job} otherwise.
     */
    private Job requireJob(String pipelineId) {
        refuseIfLost(pipelineId);
        Job job = liveJob(pipelineId);
        if (job == null) {
            throw new TapstateException(EngineError.NO_SUCH_JOB, Map.of("pipeline", pipelineId), null);
        }
        return job;
    }
}
