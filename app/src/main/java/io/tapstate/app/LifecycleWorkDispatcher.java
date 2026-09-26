package io.tapstate.app;

import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.common.TapstateException;
import io.tapstate.core.lifecycle.HistogramBounds;
import io.tapstate.core.lifecycle.HistogramValue;
import io.tapstate.runtime.scheduler.ConvergeResult;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.slf4j.MDC;

/**
 * Bounds lifecycle reconciliation independently of the scheduler that observes every pipeline. One
 * pipeline has at most one accepted task, whether queued, executing or waiting for its result to be read.
 * A later tick reads the latest desired intent again, so a queue contains pipeline identities rather than
 * a history of verbs. A changed intent interrupts work already running under the older one.
 */
final class LifecycleWorkDispatcher implements AutoCloseable {

    static final int DEFAULT_MAX_CONCURRENCY = 4;
    static final int DEFAULT_QUEUE_CAPACITY = 64;

    enum Submission {
        ACCEPTED,
        COALESCED,
        CAPACITY
    }

    enum Verb {
        START, PAUSE, RESUME, STOP
    }

    record Health(boolean observed, int activeSlots, int pendingPipelines, int queueDepth,
            long queueHighWater, long coalesced, long cancelled, long capacityRefusals,
            Optional<HistogramValue> capacityWait, Map<Verb, HistogramValue> workDurations) {
    }

    record Outcome(ConvergeResult result, Throwable failure, boolean superseded) {
        Outcome {
            if (superseded && (result != null || failure != null)) {
                throw new IllegalArgumentException("superseded work has no result or failure");
            }
        }

        static Outcome result(ConvergeResult result) {
            return new Outcome(Objects.requireNonNull(result, "result"), null, false);
        }

        static Outcome failure(Throwable failure) {
            return new Outcome(null, Objects.requireNonNull(failure, "failure"), false);
        }

        static Outcome cancelled() {
            return new Outcome(null, null, true);
        }
    }

    private final ConcurrentHashMap<String, Work> workByPipeline = new ConcurrentHashMap<>();
    private final Semaphore capacity;
    private final ThreadPoolExecutor workers;
    private final Instant startedAt = Instant.now();
    private final AtomicBoolean observed = new AtomicBoolean();
    private final AtomicInteger activeSlots = new AtomicInteger();
    /** Accepted submissions waiting for their worker entry, including the immediate hand-off. */
    private final AtomicInteger queued = new AtomicInteger();
    private final AtomicLong queueHighWater = new AtomicLong();
    private final AtomicLong coalesced = new AtomicLong();
    private final AtomicLong cancelled = new AtomicLong();
    private final AtomicLong capacityRefusals = new AtomicLong();
    private final DurationHistogram capacityWait = new DurationHistogram(HistogramBounds.LIFECYCLE_CAPACITY_WAIT);
    private final EnumMap<Verb, DurationHistogram> workDurations = new EnumMap<>(Verb.class);

    private LifecycleWorkDispatcher() {
        this.capacity = null;
        this.workers = null;
        for (Verb verb : Verb.values()) {
            workDurations.put(verb, new DurationHistogram(HistogramBounds.LIFECYCLE_WORK_DURATION));
        }
    }

    /** Direct execution preserves focused tests whose subject is convergence rather than dispatch. */
    static LifecycleWorkDispatcher inline() {
        return new LifecycleWorkDispatcher();
    }

    LifecycleWorkDispatcher(int maxConcurrency, int queueCapacity) {
        if (maxConcurrency < 1 || queueCapacity < 1) {
            throw new TapstateException(ActuationError.LIFECYCLE_DISPATCHER_INVALID_CAPACITY,
                    Map.of("maxConcurrency", maxConcurrency, "queueCapacity", queueCapacity), null);
        }
        this.capacity = new Semaphore(Math.addExact(maxConcurrency, queueCapacity));
        for (Verb verb : Verb.values()) {
            workDurations.put(verb, new DurationHistogram(HistogramBounds.LIFECYCLE_WORK_DURATION));
        }
        AtomicInteger threadNumber = new AtomicInteger();
        this.workers = new ThreadPoolExecutor(maxConcurrency, maxConcurrency, 0L,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(queueCapacity), task -> {
                    Thread thread = new Thread(task, "tapstate-lifecycle-" + threadNumber.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * Accepts a bounded unit of work for a pipeline. The supplier reads desired again when it executes;
     * this method only remembers the desired value that made the caller offer it, so a newer intent can
     * cancel an obsolete running start. A refusal keeps no future or map entry and the next tick retries.
     */
    Submission offer(String pipelineId, DesiredState desired, Supplier<ConvergeResult> reconcile) {
        return offer(pipelineId, desired, reconcile, System.nanoTime());
    }

    /** A prior capacity refusal can supply its first monotonic wait instant without retaining another map. */
    Submission offer(String pipelineId, DesiredState desired, Supplier<ConvergeResult> reconcile,
            long capacityWaitSinceNanos) {
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(desired, "desired");
        Objects.requireNonNull(reconcile, "reconcile");
        observed.set(true);
        Work existing = workByPipeline.get(pipelineId);
        if (existing != null) {
            if (!desired.equals(existing.desired)) {
                existing.supersede();
            }
            coalesced.incrementAndGet();
            return Submission.COALESCED;
        }
        if (capacity != null && !capacity.tryAcquire()) {
            capacityRefusals.incrementAndGet();
            return Submission.CAPACITY;
        }
        Work offered = new Work(pipelineId, desired, reconcile, capacityWaitSinceNanos);
        existing = workByPipeline.putIfAbsent(pipelineId, offered);
        if (existing != null) {
            if (capacity != null) {
                capacity.release();
            }
            if (!desired.equals(existing.desired)) {
                existing.supersede();
            }
            coalesced.incrementAndGet();
            return Submission.COALESCED;
        }
        if (workers == null) {
            offered.run();
            return Submission.ACCEPTED;
        }
        offered.queued.set(true);
        int acceptedDepth = queued.incrementAndGet();
        try {
            workers.execute(offered);
            queueHighWater.accumulateAndGet(acceptedDepth, Math::max);
            return Submission.ACCEPTED;
        } catch (java.util.concurrent.RejectedExecutionException saturated) {
            offered.leaveQueue();
            if (workByPipeline.remove(pipelineId, offered)) {
                capacity.release();
            }
            capacityRefusals.incrementAndGet();
            return Submission.CAPACITY;
        }
    }

    Instant startedAt() {
        return startedAt;
    }

    Health health() {
        Map<Verb, HistogramValue> measured = new EnumMap<>(Verb.class);
        workDurations.forEach((verb, histogram) -> histogram.snapshot().ifPresent(value -> measured.put(verb, value)));
        int pending = (int) workByPipeline.values().stream().filter(work -> work.outcome == null).count();
        return new Health(observed.get(), activeSlots.get(), pending,
                queued.get(), queueHighWater.get(),
                coalesced.get(), cancelled.get(), capacityRefusals.get(), capacityWait.snapshot(),
                Map.copyOf(measured));
    }

    void recordVerb(Verb verb, long elapsedNanos) {
        observed.set(true);
        workDurations.get(Objects.requireNonNull(verb, "verb")).record(elapsedNanos);
    }

    /** Returns one completed result, releasing its global slot only after the caller has it. */
    Outcome take(String pipelineId) {
        Work work = workByPipeline.get(pipelineId);
        if (work == null || work.outcome == null || !workByPipeline.remove(pipelineId, work)) {
            return null;
        }
        if (capacity != null) {
            capacity.release();
        }
        return work.outcome;
    }

    /** Stops obsolete work when this member no longer drives the pipeline. */
    void cancel(String pipelineId) {
        Work work = workByPipeline.get(pipelineId);
        if (work != null) {
            work.supersede();
        }
    }

    /** A member that is no longer eligible must not keep starting work it queued while eligible. */
    void cancelAll() {
        workByPipeline.values().forEach(Work::supersede);
    }

    /** Cancels deleted pipelines and releases their completed slots. */
    void retain(Collection<String> pipelineIds) {
        for (String pipelineId : workByPipeline.keySet()) {
            if (!pipelineIds.contains(pipelineId)) {
                cancel(pipelineId);
                take(pipelineId);
            }
        }
    }

    int activeCount() {
        return workByPipeline.size();
    }

    @Override
    public void close() {
        cancelAll();
        if (workers == null) {
            return;
        }
        workers.shutdownNow();
        try {
            if (!workers.awaitTermination(Duration.ofSeconds(5).toNanos(), TimeUnit.NANOSECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            workers.shutdownNow();
        }
    }

    private final class Work implements Runnable {
        private final String pipelineId;
        private final DesiredState desired;
        private final Supplier<ConvergeResult> reconcile;
        private final PipelineLogContext logContext;
        private final long capacityWaitSinceNanos;
        private final AtomicBoolean queued = new AtomicBoolean();
        private volatile Thread runner;
        private final AtomicBoolean wasCancelled = new AtomicBoolean();
        private volatile Outcome outcome;

        private Work(String pipelineId, DesiredState desired, Supplier<ConvergeResult> reconcile,
                long capacityWaitSinceNanos) {
            this.pipelineId = pipelineId;
            this.desired = desired;
            this.reconcile = reconcile;
            this.logContext = PipelineLogContext.capture();
            this.capacityWaitSinceNanos = capacityWaitSinceNanos;
        }

        private void supersede() {
            if (outcome != null) {
                return;
            }
            if (!wasCancelled.compareAndSet(false, true)) {
                return;
            }
            cancelled.incrementAndGet();
            if (workers != null && workers.remove(this)) {
                leaveQueue();
                outcome = Outcome.cancelled();
                return;
            }
            Thread running = runner;
            if (running != null) {
                running.interrupt();
            }
        }

        @Override
        public void run() {
            leaveQueue();
            runner = Thread.currentThread();
            activeSlots.incrementAndGet();
            PipelineLogContext previousLogContext = PipelineLogContext.capture();
            logContext.restore();
            MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, pipelineId);
            try {
                if (wasCancelled.get()) {
                    outcome = Outcome.cancelled();
                    return;
                }
                capacityWait.record(System.nanoTime() - capacityWaitSinceNanos);
                ConvergeResult result = reconcile.get();
                outcome = wasCancelled.get() ? Outcome.cancelled() : Outcome.result(result);
            } catch (Throwable failure) {
                outcome = wasCancelled.get() ? Outcome.cancelled() : Outcome.failure(failure);
                if (failure instanceof Error error) {
                    throw error;
                }
            } finally {
                previousLogContext.restore();
                runner = null;
                activeSlots.decrementAndGet();
            }
        }

        private void leaveQueue() {
            if (queued.compareAndSet(true, false)) {
                LifecycleWorkDispatcher.this.queued.decrementAndGet();
            }
        }
    }

    /** A fixed bucket array avoids per-work allocations and never retains pipeline identities. */
    private static final class DurationHistogram {
        private final HistogramBounds bounds;
        private final long[] buckets;
        private long count;
        private double sumSeconds;

        private DurationHistogram(HistogramBounds bounds) {
            this.bounds = bounds;
            this.buckets = new long[bounds.buckets()];
        }

        private synchronized void record(long elapsedNanos) {
            double seconds = Math.max(0L, elapsedNanos) / 1_000_000_000.0;
            int bucket = 0;
            while (bucket < bounds.bounds().size() && seconds > bounds.bounds().get(bucket)) {
                bucket++;
            }
            buckets[bucket]++;
            count++;
            sumSeconds += seconds;
        }

        private synchronized Optional<HistogramValue> snapshot() {
            if (count == 0) {
                return Optional.empty();
            }
            List<Long> counts = new ArrayList<>(buckets.length);
            for (long bucket : buckets) {
                counts.add(bucket);
            }
            return Optional.of(bounds.value(count, sumSeconds, counts));
        }
    }
}
