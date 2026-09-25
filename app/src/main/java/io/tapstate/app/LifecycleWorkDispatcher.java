package io.tapstate.app;

import io.tapstate.core.lifecycle.DesiredState;
import io.tapstate.core.common.TapstateException;
import io.tapstate.runtime.scheduler.ConvergeResult;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
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

    private LifecycleWorkDispatcher() {
        this.capacity = null;
        this.workers = null;
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
        Objects.requireNonNull(pipelineId, "pipelineId");
        Objects.requireNonNull(desired, "desired");
        Objects.requireNonNull(reconcile, "reconcile");
        Work existing = workByPipeline.get(pipelineId);
        if (existing != null) {
            if (!desired.equals(existing.desired)) {
                existing.supersede();
            }
            return Submission.COALESCED;
        }
        if (capacity != null && !capacity.tryAcquire()) {
            return Submission.CAPACITY;
        }
        Work offered = new Work(pipelineId, desired, reconcile);
        existing = workByPipeline.putIfAbsent(pipelineId, offered);
        if (existing != null) {
            if (capacity != null) {
                capacity.release();
            }
            if (!desired.equals(existing.desired)) {
                existing.supersede();
            }
            return Submission.COALESCED;
        }
        if (workers == null) {
            offered.run();
            return Submission.ACCEPTED;
        }
        try {
            workers.execute(offered);
            return Submission.ACCEPTED;
        } catch (java.util.concurrent.RejectedExecutionException saturated) {
            if (workByPipeline.remove(pipelineId, offered)) {
                capacity.release();
            }
            return Submission.CAPACITY;
        }
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
        private volatile Thread runner;
        private volatile boolean cancelled;
        private volatile Outcome outcome;

        private Work(String pipelineId, DesiredState desired, Supplier<ConvergeResult> reconcile) {
            this.pipelineId = pipelineId;
            this.desired = desired;
            this.reconcile = reconcile;
        }

        private void supersede() {
            if (outcome != null) {
                return;
            }
            cancelled = true;
            if (workers != null && workers.remove(this)) {
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
            runner = Thread.currentThread();
            String previousPipeline = MDC.get(PipelineLogAppender.PIPELINE_ID_MDC_KEY);
            MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, pipelineId);
            try {
                if (cancelled) {
                    outcome = Outcome.cancelled();
                    return;
                }
                ConvergeResult result = reconcile.get();
                outcome = cancelled ? Outcome.cancelled() : Outcome.result(result);
            } catch (Throwable failure) {
                outcome = cancelled ? Outcome.cancelled() : Outcome.failure(failure);
                if (failure instanceof Error error) {
                    throw error;
                }
            } finally {
                if (previousPipeline == null) {
                    MDC.remove(PipelineLogAppender.PIPELINE_ID_MDC_KEY);
                } else {
                    MDC.put(PipelineLogAppender.PIPELINE_ID_MDC_KEY, previousPipeline);
                }
                runner = null;
            }
        }
    }
}
