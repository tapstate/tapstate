package io.tapstate.app;

import io.tapstate.core.lifecycle.Observation;
import io.tapstate.runtime.scheduler.ObservationPublisher;
import io.tapstate.runtime.scheduler.RateSampler;
import io.tapstate.spi.metrics.MetricsExport;
import io.tapstate.spi.store.ObservationStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.OptionalLong;
import java.util.Map;
import java.util.function.BooleanSupplier;

/** Fixed worker budgets keep slow telemetry stores away from convergence and data-plane calls. */
final class TelemetryDispatcher implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(TelemetryDispatcher.class);
    static final int DEFAULT_LATEST_WORKERS = 4;
    static final int DEFAULT_QUEUE_CAPACITY = 64;
    private static final Duration DEFAULT_WRITE_DEADLINE = Duration.ofSeconds(5);
    private static final Duration BREAKER_COOLDOWN = Duration.ofSeconds(1);

    enum Sink {
        LATEST, HISTORY, EXPORT
    }

    record Health(int queueDepth, long highWater, int inFlight, long coalesced, long dropped,
            long successes, long failures, long timeouts, long maxDurationMillis,
            OptionalLong lastSuccessAgeMillis,
            boolean degraded) {
    }

    private static final class Stats {
        private static final class Operation {
            private final long started = System.nanoTime();
            /** 0 running, 1 timed out while running, 2 finished. */
            private final AtomicInteger state = new AtomicInteger();
        }

        private final AtomicLong highWater = new AtomicLong();
        private final AtomicLong coalesced = new AtomicLong();
        private final AtomicLong dropped = new AtomicLong();
        private final AtomicLong successes = new AtomicLong();
        private final AtomicLong failures = new AtomicLong();
        private final AtomicLong timeouts = new AtomicLong();
        private final AtomicLong maxDurationNanos = new AtomicLong();
        private final AtomicInteger consecutiveFailures = new AtomicInteger();
        private final AtomicLong openUntilNanos = new AtomicLong();
        private final AtomicBoolean probe = new AtomicBoolean();
        private final ConcurrentHashMap<Thread, Operation> inFlight = new ConcurrentHashMap<>();
        private volatile long lastSuccessNanos;
        private volatile long lastProblemNanos;

        private void queueDepth(int depth) {
            highWater.accumulateAndGet(depth, Math::max);
        }

        private void dropped() {
            dropped.incrementAndGet();
            lastProblemNanos = System.nanoTime();
        }

        private boolean allow() {
            long until = openUntilNanos.get();
            if (until == 0) {
                return true;
            }
            if (System.nanoTime() - until < 0) {
                return false;
            }
            return probe.compareAndSet(false, true);
        }

        private Operation begin() {
            Operation operation = new Operation();
            inFlight.put(Thread.currentThread(), operation);
            return operation;
        }

        private void skipped(Operation operation) {
            operation.state.set(2);
            inFlight.remove(Thread.currentThread(), operation);
            maxDurationNanos.accumulateAndGet(System.nanoTime() - operation.started, Math::max);
            if (probe.getAndSet(false)) {
                openUntilNanos.set(System.nanoTime() + BREAKER_COOLDOWN.toNanos());
            }
        }

        private void completed(Operation operation, boolean success) {
            boolean timedOut = operation.state.getAndSet(2) == 1;
            inFlight.remove(Thread.currentThread(), operation);
            maxDurationNanos.accumulateAndGet(System.nanoTime() - operation.started, Math::max);
            if (success && !timedOut) {
                successes.incrementAndGet();
                lastSuccessNanos = System.nanoTime();
                consecutiveFailures.set(0);
                openUntilNanos.set(0);
                probe.set(false);
            } else {
                failures.incrementAndGet();
                lastProblemNanos = System.nanoTime();
                if (timedOut || probe.get() || consecutiveFailures.incrementAndGet() >= 3) {
                    openUntilNanos.set(System.nanoTime() + BREAKER_COOLDOWN.toNanos());
                    probe.set(false);
                }
            }
        }

        private void watch(String sink, Duration deadline) {
            long now = System.nanoTime();
            for (Operation operation : inFlight.values()) {
                if (now - operation.started >= deadline.toNanos()
                        && operation.state.compareAndSet(0, 1)) {
                    timeouts.incrementAndGet();
                    lastProblemNanos = now;
                    openUntilNanos.set(now + BREAKER_COOLDOWN.toNanos());
                    probe.set(false);
                    LOG.warn("{} telemetry write exceeded its {} ms deadline", sink, deadline.toMillis());
                }
            }
        }

        private Health snapshot(int queueDepth) {
            long successfulAt = lastSuccessNanos;
            boolean timedOutInFlight = inFlight.values().stream().anyMatch(op -> op.state.get() == 1);
            return new Health(queueDepth, highWater.get(), inFlight.size(), coalesced.get(), dropped.get(),
                    successes.get(), failures.get(), timeouts.get(),
                    TimeUnit.NANOSECONDS.toMillis(maxDurationNanos.get()),
                    successfulAt == 0 ? OptionalLong.empty()
                            : OptionalLong.of(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - successfulAt)),
                    timedOutInFlight || (lastProblemNanos != 0
                            && (successfulAt == 0 || lastProblemNanos - successfulAt > 0)));
        }
    }

    private sealed interface Frame permits ObservationFrame, ReconcileFailureFrame {
    }

    private record ObservationFrame(ObservationPublisher.Prepared prepared, ObservationStore.Scope scope)
            implements Frame {
    }

    private record ReconcileFailureFrame(String pipelineId, long failures, ObservationStore.Scope scope)
            implements Frame {
    }

    private final ObservationPublisher publisher;
    private final RateSampler sampler;
    private final MetricsExport export;
    private final ObservationScopeRegistry scopes;
    private final ThreadPoolExecutor latestWorkers;
    private final ThreadPoolExecutor historyWorker;
    private final ThreadPoolExecutor exportWorker;
    private final Semaphore latestCapacity;
    private final AtomicInteger latestPending = new AtomicInteger();
    private final Stats latestStats = new Stats();
    private final Stats historyStats = new Stats();
    private final Stats exportStats = new Stats();
    private final ConcurrentHashMap<String, LatestSlot> latestByPipeline = new ConcurrentHashMap<>();
    private final ScheduledExecutorService watchdog;

    TelemetryDispatcher(ObservationPublisher publisher, RateSampler sampler, MetricsExport export,
            int latestConcurrency, int queueCapacity) {
        this(publisher, sampler, export, null, latestConcurrency, queueCapacity);
    }

    TelemetryDispatcher(ObservationPublisher publisher, RateSampler sampler, MetricsExport export,
            ObservationScopeRegistry scopes, int latestConcurrency, int queueCapacity) {
        this(publisher, sampler, export, scopes, latestConcurrency, queueCapacity, DEFAULT_WRITE_DEADLINE);
    }

    TelemetryDispatcher(ObservationPublisher publisher, RateSampler sampler, MetricsExport export,
            ObservationScopeRegistry scopes, int latestConcurrency, int queueCapacity, Duration writeDeadline) {
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.sampler = sampler;
        this.export = Objects.requireNonNull(export, "export");
        this.scopes = scopes;
        if (latestConcurrency < 1 || queueCapacity < 1) {
            throw new IllegalArgumentException("telemetry worker and queue budgets must be positive");
        }
        if (writeDeadline.isZero() || writeDeadline.isNegative()) {
            throw new IllegalArgumentException("telemetry write deadline must be positive");
        }
        latestCapacity = new Semaphore(Math.addExact(latestConcurrency, queueCapacity));
        latestWorkers = workers("latest", latestConcurrency, queueCapacity);
        historyWorker = workers("history", 1, queueCapacity);
        exportWorker = workers("export", 1, queueCapacity);
        watchdog = Executors.newSingleThreadScheduledExecutor(task -> {
            Thread thread = new Thread(task, "tapstate-telemetry-watchdog");
            thread.setDaemon(true);
            return thread;
        });
        long periodMillis = Math.max(10, Math.min(1000, writeDeadline.toMillis() / 4));
        watchdog.scheduleWithFixedDelay(() -> {
            latestStats.watch("latest", writeDeadline);
            historyStats.watch("history", writeDeadline);
            exportStats.watch("export", writeDeadline);
        }, periodMillis, periodMillis, TimeUnit.MILLISECONDS);
    }

    private static ThreadPoolExecutor workers(String sink, int count, int queueCapacity) {
        AtomicInteger next = new AtomicInteger();
        return new ThreadPoolExecutor(count, count, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(queueCapacity), task -> {
                    Thread thread = new Thread(task, "tapstate-telemetry-" + sink + "-" + next.incrementAndGet());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
    }

    void offer(ObservationPublisher.Prepared prepared, ObservationStore.Scope scope) {
        Objects.requireNonNull(prepared, "prepared");
        if (scopes != null && (scope == null || scopes.current(prepared.observation().pipelineId())
                .filter(scope::equals).isEmpty())) {
            return;
        }
        ObservationPublisher.Prepared frame = scopes == null ? prepared : scopes.continueFrame(prepared, scope);
        Observation observation = frame.observation();
        offerLatest(observation.pipelineId(), new ObservationFrame(frame, scope));
        if (sampler != null) {
            offerSide(historyWorker, historyStats, observation.pipelineId(), "history",
                    () -> stillCurrent(observation.pipelineId(), scope)
                            && sampler.appendIfDue(observation, scope));
        }
        if (export != MetricsExport.none()) {
            offerSide(exportWorker, exportStats, observation.pipelineId(), "export", () -> {
                if (!stillCurrent(observation.pipelineId(), scope)) {
                    return false;
                }
                export.offer(observation.pipelineId(), observation.state(),
                        observation.observedAt(), observation.facts());
                return true;
            });
        }
    }

    private boolean stillCurrent(String pipelineId, ObservationStore.Scope scope) {
        return scopes == null || (scope != null && scopes.current(pipelineId).filter(scope::equals).isPresent());
    }

    Map<Sink, Health> health() {
        return Map.of(
                Sink.LATEST, latestStats.snapshot(latestWorkers.getQueue().size() + latestPending.get()),
                Sink.HISTORY, historyStats.snapshot(historyWorker.getQueue().size()),
                Sink.EXPORT, exportStats.snapshot(exportWorker.getQueue().size()));
    }

    void offerReconcileFailure(String pipelineId, long failures, ObservationStore.Scope scope) {
        offerLatest(pipelineId, new ReconcileFailureFrame(pipelineId, failures, scope));
    }

    private void offerLatest(String pipelineId, Frame frame) {
        while (true) {
            LatestSlot existing = latestByPipeline.get(pipelineId);
            if (existing != null) {
                synchronized (existing) {
                    if (existing.retired) {
                        continue;
                    }
                    if (existing.pending != null) {
                        latestStats.coalesced.incrementAndGet();
                    } else if (existing.started) {
                        existing.pendingCounted = true;
                        latestPending.incrementAndGet();
                    }
                    existing.pending = frame;
                    latestStats.queueDepth(latestWorkers.getQueue().size() + latestPending.get());
                    return;
                }
            }
            if (!latestCapacity.tryAcquire()) {
                latestStats.dropped();
                LOG.warn("Latest observation for pipeline {} was dropped: telemetry queue is full", pipelineId);
                return;
            }
            LatestSlot created = new LatestSlot(pipelineId, frame);
            if (latestByPipeline.putIfAbsent(pipelineId, created) != null) {
                latestCapacity.release();
                continue;
            }
            try {
                latestWorkers.execute(created);
                latestStats.queueDepth(latestWorkers.getQueue().size() + latestPending.get());
            } catch (java.util.concurrent.RejectedExecutionException unavailable) {
                synchronized (created) {
                    created.retired = true;
                    created.pending = null;
                    latestByPipeline.remove(pipelineId, created);
                }
                latestCapacity.release();
                latestStats.dropped();
                LOG.warn("Latest observation for pipeline {} was dropped: telemetry workers stopped", pipelineId);
            }
            return;
        }
    }

    private static void offerSide(ThreadPoolExecutor workers, Stats stats, String pipelineId, String sink,
            BooleanSupplier write) {
        try {
            workers.execute(() -> {
                if (!stats.allow()) {
                    stats.dropped();
                    return;
                }
                Stats.Operation operation = stats.begin();
                try {
                    if (write.getAsBoolean()) {
                        stats.completed(operation, true);
                    } else {
                        stats.skipped(operation);
                    }
                } catch (RuntimeException failed) {
                    stats.completed(operation, false);
                    LOG.warn("Could not write {} telemetry for pipeline {}", sink, pipelineId, failed);
                } catch (Error defect) {
                    stats.completed(operation, false);
                    throw defect;
                }
            });
            stats.queueDepth(workers.getQueue().size());
        } catch (java.util.concurrent.RejectedExecutionException saturated) {
            stats.dropped();
            LOG.warn("{} telemetry for pipeline {} was dropped: queue is full", sink, pipelineId);
        }
    }

    void retain(Collection<String> pipelineIds) {
        for (String id : latestByPipeline.keySet()) {
            if (!pipelineIds.contains(id)) {
                LatestSlot slot = latestByPipeline.get(id);
                if (slot != null) {
                    synchronized (slot) {
                        if (slot.pendingCounted) {
                            latestPending.decrementAndGet();
                            slot.pendingCounted = false;
                        }
                        slot.pending = null;
                    }
                }
            }
        }
    }

    @Override
    public void close() {
        watchdog.shutdownNow();
        shutdown(latestWorkers);
        shutdown(historyWorker);
        shutdown(exportWorker);
    }

    private static void shutdown(ThreadPoolExecutor workers) {
        workers.shutdown();
        try {
            if (!workers.awaitTermination(Duration.ofSeconds(2).toNanos(), TimeUnit.NANOSECONDS)) {
                workers.shutdownNow();
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            workers.shutdownNow();
        }
    }

    private final class LatestSlot implements Runnable {
        private final String pipelineId;
        private Frame pending;
        private boolean retired;
        private boolean started;
        private boolean pendingCounted;

        private LatestSlot(String pipelineId, Frame first) {
            this.pipelineId = pipelineId;
            this.pending = first;
        }

        @Override
        public void run() {
            while (true) {
                Frame frame;
                synchronized (this) {
                    started = true;
                    if (pendingCounted) {
                        latestPending.decrementAndGet();
                        pendingCounted = false;
                    }
                    frame = pending;
                    pending = null;
                    if (frame == null) {
                        retired = true;
                        latestByPipeline.remove(pipelineId, this);
                        latestCapacity.release();
                        return;
                    }
                }
                if (!latestStats.allow()) {
                    latestStats.dropped();
                    continue;
                }
                Stats.Operation operation = latestStats.begin();
                try {
                    if (frame instanceof ObservationFrame observation) {
                        if (publisher.commit(observation.prepared(), observation.scope()).isPresent()) {
                            latestStats.completed(operation, true);
                        } else {
                            latestStats.skipped(operation);
                        }
                    } else if (frame instanceof ReconcileFailureFrame failure) {
                        if (failure.scope() == null) {
                            publisher.publishReconcileFailure(failure.pipelineId(), failure.failures());
                        } else {
                            publisher.publishReconcileFailureScoped(
                                    failure.pipelineId(), failure.failures(), failure.scope());
                        }
                        latestStats.completed(operation, true);
                    }
                } catch (RuntimeException failed) {
                    latestStats.completed(operation, false);
                    LOG.warn("Could not write latest observation for pipeline {}", pipelineId, failed);
                } catch (Error defect) {
                    latestStats.completed(operation, false);
                    throw defect;
                }
            }
        }
    }
}
