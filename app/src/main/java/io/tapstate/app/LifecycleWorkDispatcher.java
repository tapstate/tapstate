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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.function.Consumer;
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
    private final int cleanupLimit;
    private final AtomicBoolean shuttingDown = new AtomicBoolean();
    private final AtomicInteger ownedCleanupPending = new AtomicInteger();
    private final Object shutdownWorkGate = new Object();
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
        this.cleanupLimit = 1;
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
        this.cleanupLimit = Math.addExact(maxConcurrency, queueCapacity);
        this.capacity = new Semaphore(cleanupLimit);
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
        if (shuttingDown.get()) { return Submission.CAPACITY; }
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
        if (shuttingDown.get()) {
            offered.outcome = Outcome.cancelled();
            offered.finished = true;
            if (workByPipeline.remove(pipelineId, offered) && capacity != null) { capacity.release(); }
            return Submission.CAPACITY;
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
        synchronized (shutdownWorkGate) {
            Work work = workByPipeline.get(pipelineId);
            if (work == null || work.outcome == null || (shuttingDown.get() && !work.finished)
                    || !workByPipeline.remove(pipelineId, work)) { return null; }
            if (capacity != null) { capacity.release(); }
            shutdownWorkGate.notifyAll();
            return work.outcome;
        }
    }

    /** A worker decision belongs only to the still accepted, uncancelled full intent. */
    boolean current(String pipelineId, DesiredState intent) {
        Work work = workByPipeline.get(pipelineId);
        return work != null && !work.wasCancelled.get() && work.outcome == null && work.desired.equals(intent);
    }

    Object currentIdentity(String pipelineId, DesiredState intent) {
        Work work = workByPipeline.get(pipelineId);
        return work != null && !work.wasCancelled.get() && work.outcome == null && work.desired.equals(intent)
                ? work : null;
    }

    /** Serializes a pure local projection with cancellation of this exact accepted work. */
    void withCurrentDecision(String pipelineId, DesiredState intent, Consumer<Object> record) {
        Work work = workByPipeline.get(pipelineId);
        if (work == null) { return; }
        synchronized (work) {
            if (workByPipeline.get(pipelineId) == work && work.runner == Thread.currentThread()
                    && !work.wasCancelled.get() && work.outcome == null && work.desired.equals(intent)) {
                record.accept(work);
            }
        }
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

    /** Stops reconciliation admission before local resource teardown, without inventing desired intent. */
    void beginOwnedShutdown() {
        shuttingDown.set(true);
        workByPipeline.forEach((pipelineId, work) -> {
            work.supersede();
            take(pipelineId);
        });
    }

    /** Shutdown cannot qualify while an older accepted worker can still return a native handle. */
    boolean awaitOwnedReconciliation(long deadline) throws InterruptedException {
        synchronized (shutdownWorkGate) {
            while (workByPipeline.values().stream().anyMatch(work -> !work.finished)) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0) { return false; }
                TimeUnit.NANOSECONDS.timedWait(shutdownWorkGate, remaining);
            }
            return true;
        }
    }

    boolean ownedCleanupAccepting() { return workers == null || !workers.isShutdown(); }
    int ownedCleanupLimit() { return cleanupLimit; }
    int ownedCleanupPending() { return ownedCleanupPending.get(); }

    /** A local cleanup uses the same worker and capacity budgets as lifecycle work. */
    Optional<CompletableFuture<Void>> offerOwnedCleanup(Runnable action, long deadline) throws InterruptedException {
        Objects.requireNonNull(action, "action");
        if (!shuttingDown.get()) { throw new IllegalStateException("owned cleanup requires stopped admission"); }
        if (!ownedCleanupAccepting()) { return Optional.empty(); }
        if (workers != null) {
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0 || !capacity.tryAcquire(remaining, TimeUnit.NANOSECONDS)) { return Optional.empty(); }
        }
        OwnedCleanup cleanup = new OwnedCleanup(action);
        ownedCleanupPending.incrementAndGet();
        observed.set(true);
        if (workers == null) {
            // Direct focused fixtures keep the same inline execution model as ordinary reconciliation.
            cleanup.run();
            return Optional.of(cleanup.completion);
        }
        cleanup.queued.set(true);
        int depth = queued.incrementAndGet();
        try {
            workers.execute(cleanup);
            queueHighWater.accumulateAndGet(depth, Math::max);
            return Optional.of(cleanup.completion);
        } catch (java.util.concurrent.RejectedExecutionException unavailable) {
            cleanup.discard();
            return Optional.empty();
        }
    }

    private final class OwnedCleanup implements Runnable {
        private final Runnable action;
        private final CompletableFuture<Void> completion = new CompletableFuture<>();
        private final AtomicBoolean queued = new AtomicBoolean();
        private final AtomicBoolean ended = new AtomicBoolean();

        private OwnedCleanup(Runnable action) { this.action = action; }
        private void leaveQueue() {
            if (queued.compareAndSet(true, false)) { LifecycleWorkDispatcher.this.queued.decrementAndGet(); }
        }
        private void finish() {
            if (ended.compareAndSet(false, true)) {
                ownedCleanupPending.decrementAndGet();
                if (capacity != null) { capacity.release(); }
            }
        }
        private void discard() {
            leaveQueue();
            completion.completeExceptionally(new TapstateException(ActuationError.CAPTURE_SHUTDOWN_INCOMPLETE,
                    Map.of("resources", 1, "timeout", Duration.ZERO.toString()),
                    new java.util.concurrent.CancellationException("owned cleanup did not enter its worker")));
            finish();
        }
        @Override public void run() {
            leaveQueue();
            activeSlots.incrementAndGet();
            Throwable failure = null;
            try { action.run(); }
            catch (Throwable refused) { failure = refused; }
            finally { activeSlots.decrementAndGet(); finish(); }
            if (failure == null) { completion.complete(null); }
            else { completion.completeExceptionally(failure); }
            if (failure instanceof Error defect) { throw defect; }
        }
    }

    @Override
    public void close() {
        beginOwnedShutdown();
        if (workers == null) {
            return;
        }
        for (Runnable dropped : workers.shutdownNow()) {
            if (dropped instanceof OwnedCleanup cleanup) { cleanup.discard(); }
        }
        try {
            if (!workers.awaitTermination(Duration.ofSeconds(5).toNanos(), TimeUnit.NANOSECONDS)) {
                workers.shutdownNow();
                if (ownedCleanupPending.get() > 0) { throw incompleteOwnedCleanup(null); }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            workers.shutdownNow();
            if (ownedCleanupPending.get() > 0) { throw incompleteOwnedCleanup(interrupted); }
        }
    }

    private TapstateException incompleteOwnedCleanup(Throwable cause) {
        return new TapstateException(ActuationError.CAPTURE_SHUTDOWN_INCOMPLETE,
                Map.of("resources", ownedCleanupPending.get(), "timeout", Duration.ofSeconds(5).toString()), cause);
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
        private volatile boolean finished;

        private Work(String pipelineId, DesiredState desired, Supplier<ConvergeResult> reconcile,
                long capacityWaitSinceNanos) {
            this.pipelineId = pipelineId;
            this.desired = desired;
            this.reconcile = reconcile;
            this.logContext = PipelineLogContext.capture();
            this.capacityWaitSinceNanos = capacityWaitSinceNanos;
        }

        private void supersede() {
            synchronized (this) {
                if (outcome != null || !wasCancelled.compareAndSet(false, true)) {
                    return;
                }
            }
            cancelled.incrementAndGet();
            if (workers != null && workers.remove(this)) {
                leaveQueue();
                outcome = Outcome.cancelled();
                finished = true;
                if (shuttingDown.get()) { take(pipelineId); }
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
                if (shuttingDown.get() || wasCancelled.get()) {
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
                synchronized (shutdownWorkGate) {
                    finished = true;
                    if (shuttingDown.get()) { take(pipelineId); }
                    shutdownWorkGate.notifyAll();
                }
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
